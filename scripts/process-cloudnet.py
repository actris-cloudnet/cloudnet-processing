#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import os
import argparse
from typing import Tuple, Union
import cloudnetpy.utils
import shutil
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.instruments import rpg2nc, ceilo2nc, mira2nc
from data_processing import utils
from data_processing.metadata_api import MetadataApi
from data_processing.storage_api import StorageApi
from data_processing.pid_utils import PidUtils
from data_processing import concat_lib
from data_processing import modifier
from tempfile import TemporaryDirectory
from tempfile import NamedTemporaryFile
import netCDF4


PRODUCTS = ('classification', 'iwc-Z-T-method', 'lwc-scaled-adiabatic', 'drizzle')


def main():
    """The main function."""

    config = utils.read_main_conf(ARGS)

    site_name = ARGS.site[0]
    site_meta = utils.read_site_info(site_name)

    start_date = utils.date_string_to_date(ARGS.start)
    stop_date = utils.date_string_to_date(ARGS.stop)

    md_api = MetadataApi(config['METADATASERVER']['url'])
    pid_utils = PidUtils(config['PID-SERVICE'])
    storage_api = StorageApi(config['STORAGE-SERVICE']['url'],
                             (config['STORAGE-SERVICE']['username'],
                              config['STORAGE-SERVICE']['password']))

    for date in cloudnetpy.utils.date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        print(f'{site_name} {date_str}')
        process = Process(site_meta, date_str, md_api, storage_api)
        for instrument_type in ('mwr', 'lidar', 'radar', 'model'):
            try:
                getattr(process, f'process_{instrument_type}')()
            except InputFileMissing:
                print(f'No raw {instrument_type} data or already processed.')


class Process:
    def __init__(self, site_meta: dict, date_str: str, md_api: MetadataApi, storage_api: StorageApi):
        self.site_meta = site_meta
        self.date_str = date_str  # YYYY-MM-DD
        self.md_api = md_api
        self.storage_api = storage_api

    def process_mwr(self):
        """Process Cloudnet mwr file"""
        try:
            raw_daily_file = NamedTemporaryFile(suffix='.nc')
            valid_checksums, original_filename = self._get_daily_raw_file(raw_daily_file.name, 'hatpro')
        except ValueError:
            raise InputFileMissing('Raw mwr')
        uuid = modifier.fix_mwr_file(raw_daily_file.name, original_filename, self.date_str, self.site_meta['name'])
        self._upload_data_and_metadata(raw_daily_file.name, uuid, valid_checksums)

    def process_model(self):
        """Process Cloudnet model file"""
        try:
            raw_daily_file = NamedTemporaryFile(suffix='.nc')
            valid_checksums, _ = self._get_daily_raw_file(raw_daily_file.name)
        except ValueError:
            raise InputFileMissing('Raw model')
        uuid = modifier.fix_model_file(raw_daily_file.name)
        self._upload_data_and_metadata(raw_daily_file.name, uuid, valid_checksums)

    def process_lidar(self):
        """Process Cloudnet lidar file."""
        try:
            raw_daily_file = NamedTemporaryFile(suffix='.nc')
            valid_checksums = self._concatenate_chm15k(raw_daily_file.name)
        except ValueError:
            try:
                raw_daily_file = NamedTemporaryFile(suffix='.DAT')
                valid_checksums, _ = self._get_daily_raw_file(raw_daily_file.name, 'cl51')
            except ValueError:
                raise InputFileMissing('Raw lidar')

        lidar_file = NamedTemporaryFile()
        uuid = ceilo2nc(raw_daily_file.name, lidar_file.name, site_meta=self.site_meta)
        self._upload_data_and_metadata(lidar_file.name, uuid, valid_checksums)

    def process_radar(self):
        """Process Cloudnet radar file."""
        radar_file = NamedTemporaryFile()
        try:
            temp_dir = TemporaryDirectory()
            full_paths, valid_checksums = self._download_data(temp_dir, 'rpg-fmcw-94')
            uuid = rpg2nc(temp_dir.name, radar_file.name, site_meta=self.site_meta)
        except ValueError:
            try:
                raw_daily_file = NamedTemporaryFile(suffix='.mmclx')
                valid_checksums, _ = self._get_daily_raw_file(raw_daily_file.name, 'mira')
                uuid = mira2nc(raw_daily_file.name, radar_file.name, site_meta=self.site_meta)
            except ValueError:
                raise InputFileMissing('Raw radar')
        self._upload_data_and_metadata(radar_file.name, uuid, valid_checksums)

    def _concatenate_chm15k(self, raw_daily_file: str) -> list:
        """Concatenate several chm15k files into one file for certain site / date."""
        temp_dir = TemporaryDirectory()
        full_paths, checksums = self._download_data(temp_dir, 'chm15k')
        print('Concatenating CHM15k files...')
        valid_full_paths = concat_lib.concat_chm15k_files(full_paths, self.date_str, raw_daily_file)
        return [checksum for checksum, full_path in zip(checksums, full_paths) if full_path in valid_full_paths]

    def _get_daily_raw_file(self, raw_daily_file: str, instrument: str = None) -> Tuple[list, str]:
        """Downloads and saves to /tmp a single daily instrument or model file."""
        temp_dir = TemporaryDirectory()
        full_paths, checksums = self._download_data(temp_dir, instrument)
        full_path = full_paths[0]
        shutil.move(full_path, raw_daily_file)
        original_filename = os.path.basename(full_path)
        return checksums, original_filename

    def _download_data(self, temp_dir: TemporaryDirectory, instrument: str = None) -> Tuple[list, list]:
        if instrument == 'hatpro':
            all_metadata_for_day = self.md_api.get_uploaded_metadata(self.site_meta['id'], self.date_str, instrument)
            metadata = [row for row in all_metadata_for_day if row['filename'].lower().endswith('.lwp.nc')]
        elif instrument:
            metadata = self.md_api.get_uploaded_metadata(self.site_meta['id'], self.date_str, instrument=instrument)
        else:
            all_metadata_for_day = self.md_api.get_uploaded_metadata(self.site_meta['id'], self.date_str)
            model_metadata = [row for row in all_metadata_for_day if row['model']]
            metadata = sorted(model_metadata, key=lambda k: k['model']['optimumOrder'])
        return self.storage_api.download_raw_files(metadata, temp_dir.name)

    def _upload_data_and_metadata(self, full_path: str, uuid, valid_checksums: list):
        self.md_api.put(uuid, full_path)
        self.storage_api.upload_product(full_path, uuid)
        self._update_statuses(valid_checksums)

    def _update_statuses(self, checksums):
        for checksum in checksums:
            self.md_api.change_status_from_uploaded_to_processed(checksum)


class InputFileMissing(Exception):
    """Internal exception class."""
    def __init__(self, file_type: str):
        self.message = f'{file_type} file missing'
        super().__init__(self.message)


class UploadedFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'Uploaded file missing'
        super().__init__(self.message)


class CategorizeFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'Categorize file missing'
        super().__init__(self.message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('site', nargs='+', help='Site Name',
                        choices=['bucharest', 'norunda', 'granada', 'mace-head'])
    parser.add_argument('--config-dir', dest='config_dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    parser.add_argument('--start', type=str, metavar='YYYY-MM-DD',
                        help='Starting date. Default is current day - 7.',
                        default=utils.get_date_from_past(7))
    parser.add_argument('--stop', type=str, metavar='YYYY-MM-DD',
                        help='Stopping date. Default is current day - 1.',
                        default=utils.get_date_from_past(1))
    parser.add_argument('-na', '--no-api', dest='no_api', action='store_true',
                        help='Disable API calls. Useful for testing.', default=False)
    parser.add_argument('--new-version', dest='new_version', action='store_true',
                        help='Process new version.', default=False)
    parser.add_argument('--plot-quicklooks', dest='plot_quicklooks', action='store_true',
                        help='Also plot quicklooks.', default=False)
    ARGS = parser.parse_args()
    main()
