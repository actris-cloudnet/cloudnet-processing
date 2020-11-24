#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import os
import argparse
from typing import Tuple
import shutil
import warnings
from tempfile import TemporaryDirectory
from tempfile import NamedTemporaryFile
import netCDF4
from cloudnetpy.instruments import rpg2nc, ceilo2nc, mira2nc
from cloudnetpy.utils import date_range
from data_processing import utils
from data_processing.metadata_api import MetadataApi
from data_processing.storage_api import StorageApi
from data_processing.pid_utils import PidUtils
from data_processing import concat_lib
from data_processing import modifier

warnings.simplefilter("ignore", UserWarning)


def main():
    """The main function."""

    config = utils.read_main_conf(ARGS)
    site_meta = utils.read_site_info(ARGS.site[0])
    start_date = utils.date_string_to_date(ARGS.start)
    stop_date = utils.date_string_to_date(ARGS.stop)

    pid_utils = PidUtils(config['PID-SERVICE'])
    md_api = MetadataApi(config['METADATASERVER']['url'])
    storage_api = StorageApi(config['STORAGE-SERVICE']['url'],
                             (config['STORAGE-SERVICE']['username'],
                              config['STORAGE-SERVICE']['password']),
                             product_bucket=_get_product_bucket())

    for date in date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        print(f'{date_str}')
        process = Process(site_meta, date_str, md_api, storage_api, pid_utils)
        for processing_type in utils.get_raw_processing_types():
            try:
                print('{:<15}'.format(f'{processing_type}'), end='\t')
                getattr(process, f'process_{processing_type}')(processing_type)
                print('Created')
            except (AttributeError, NotImplementedError):
                print('Something not implemented')
            except InputFileMissing:
                print('No raw data or already processed')


class Process:
    def __init__(self, site_meta: dict, date_str: str, md_api: MetadataApi,
                 storage_api: StorageApi, pid_utils: PidUtils):
        self.site_meta = site_meta
        self.date_str = date_str  # YYYY-MM-DD
        self.md_api = md_api
        self.storage_api = storage_api
        self.pid_utils = pid_utils
        self._temp_file = NamedTemporaryFile()
        self._temp_dir = TemporaryDirectory()

    def process_mwr(self, instrument_type: str) -> None:
        """Process Cloudnet mwr file"""
        try:
            valid_checksums, original_filename = self._get_daily_raw_file(self._temp_file.name, 'hatpro')
        except ValueError:
            raise InputFileMissing(f'Raw {instrument_type}')
        modifier.fix_mwr_file(self._temp_file.name, original_filename, self.date_str, self.site_meta['name'])
        self._upload_data_and_metadata(self._temp_file.name, valid_checksums, instrument_type)

    def process_model(self, instrument_type: str) -> None:
        """Process Cloudnet model file"""
        try:
            valid_checksums, _ = self._get_daily_raw_file(self._temp_file.name)
        except ValueError:
            raise InputFileMissing(f'Raw {instrument_type}')
        modifier.fix_model_file(self._temp_file.name)
        self._upload_data_and_metadata(self._temp_file.name, valid_checksums, instrument_type)

    def process_lidar(self, instrument_type: str) -> None:
        """Process Cloudnet lidar file."""
        try:
            raw_daily_file = NamedTemporaryFile(suffix='.nc')
            valid_checksums = self._concatenate_chm15k(raw_daily_file.name)
        except ValueError:
            try:
                raw_daily_file = NamedTemporaryFile(suffix='.DAT')
                valid_checksums, _ = self._get_daily_raw_file(raw_daily_file.name, 'cl51')
            except ValueError:
                raise InputFileMissing(f'Raw {instrument_type}')

        ceilo2nc(raw_daily_file.name, self._temp_file.name, site_meta=self.site_meta)
        self._upload_data_and_metadata(self._temp_file.name, valid_checksums, instrument_type)

    def process_radar(self, instrument_type: str) -> None:
        """Process Cloudnet radar file."""
        try:
            full_paths, valid_checksums = self._download_data('rpg-fmcw-94')
            rpg2nc(self._temp_dir.name, self._temp_file.name, site_meta=self.site_meta)
        except ValueError:
            try:
                valid_checksums, _ = self._get_daily_raw_file(self._temp_file.name, 'mira')
                mira2nc(self._temp_file.name, self._temp_file.name, site_meta=self.site_meta)
            except ValueError:
                raise InputFileMissing(f'Raw {instrument_type}')
        self._upload_data_and_metadata(self._temp_file.name, valid_checksums, instrument_type)

    def _concatenate_chm15k(self, raw_daily_file: str) -> list:
        """Concatenate several chm15k files into one file for certain site / date."""
        full_paths, checksums = self._download_data('chm15k')
        valid_full_paths = concat_lib.concat_chm15k_files(full_paths, self.date_str, raw_daily_file)
        return [checksum for checksum, full_path in zip(checksums, full_paths) if full_path in valid_full_paths]

    def _get_daily_raw_file(self, raw_daily_file: str, instrument: str = None) -> Tuple[list, str]:
        """Downloads and saves to /tmp a single daily instrument or model file."""
        full_path, checksum = self._download_data(instrument)
        assert len(full_path) == 1 and len(checksum) == 1
        shutil.move(full_path[0], raw_daily_file)
        original_filename = os.path.basename(full_path[0])
        return checksum, original_filename

    def _download_data(self, instrument: str = None) -> Tuple[list, list]:
        all_metadata = self.md_api.get_uploaded_metadata(self.site_meta['id'], self.date_str)
        metadata = self.md_api.screen_metadata(all_metadata, ARGS.new_version, instrument)
        full_paths = self.storage_api.download_raw_files(metadata, self._temp_dir.name)
        checksums = [row['checksum'] for row in metadata]
        return full_paths, checksums

    def _upload_data_and_metadata(self, full_path: str, valid_checksums: list, product: str) -> None:
        s3_key = self._get_product_key(product)
        if ARGS.new_version:
            self.pid_utils.add_pid_to_file(full_path)
        file_info = self.storage_api.upload_product(full_path, s3_key)
        visualizations = self.storage_api.create_images(full_path, s3_key, file_info)
        payload = self._create_product_payload(full_path, product, file_info, visualizations)
        self.md_api.put(s3_key, payload)
        self._update_statuses(valid_checksums)

    def _create_product_payload(self, full_path: str, product: str, file_info: dict, visualizations: list) -> dict:
        nc = netCDF4.Dataset(full_path, 'r')
        payload = {
            'product': product,
            'visualizations': visualizations,
            'site': self.site_meta['id'],
            'measurementDate': self.date_str,
            'format': self._get_file_format(nc),
            'checksum': utils.sha256sum(full_path),
            'volatile': not ARGS.new_version,
            'uuid': getattr(nc, 'file_uuid', ''),
            'pid': getattr(nc, 'pid', ''),
            'history': getattr(nc, 'history', ''),
            'cloudnetpyVersion': getattr(nc, 'cloudnetpy_version', ''),
            ** file_info
        }
        nc.close()
        return payload

    @staticmethod
    def _get_file_format(nc: netCDF4.Dataset):
        file_format = nc.file_format.lower()
        if 'netcdf4' in file_format:
            return 'HDF5 (NetCDF4)'
        elif 'netcdf3' in file_format:
            return 'NetCDF3'
        raise RuntimeError('Unknown file type')

    def _get_product_key(self, product: str) -> str:
        return f"{self.date_str.replace('-', '')}_{self.site_meta['id']}_{product}.nc"

    def _update_statuses(self, checksums: list) -> None:
        for checksum in checksums:
            self.md_api.change_status_from_uploaded_to_processed(checksum)


def _get_product_bucket() -> str:
    return 'cloudnet-product' if ARGS.new_version else 'cloudnet-product-volatile'


class InputFileMissing(Exception):
    """Internal exception class."""
    def __init__(self, file_type: str):
        self.message = f'{file_type} file missing'
        super().__init__(self.message)


class CategorizeFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'Categorize file missing'
        super().__init__(self.message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('site', nargs='+', help='Site Name')
    parser.add_argument('--config-dir', dest='config_dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    parser.add_argument('--start', type=str, metavar='YYYY-MM-DD',
                        help='Starting date. Default is current day - 7.',
                        default=utils.get_date_from_past(7))
    parser.add_argument('--stop', type=str, metavar='YYYY-MM-DD',
                        help='Stopping date. Default is current day - 1.',
                        default=utils.get_date_from_past(1))
    parser.add_argument('--new-version', dest='new_version', action='store_true',
                        help='Process new version.', default=False)
    ARGS = parser.parse_args()
    main()
