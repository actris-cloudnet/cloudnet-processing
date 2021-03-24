#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import argparse
import gzip
import importlib
import os
import shutil
import sys
import re
import warnings
from tempfile import NamedTemporaryFile
from typing import Tuple, Union, Optional
import requests
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.instruments import rpg2nc, ceilo2nc, mira2nc, basta2nc, hatpro2nc
from cloudnetpy.utils import date_range, is_timestamp
from requests.exceptions import HTTPError
from data_processing import concat_wrapper
from data_processing import nc_header_augmenter
from data_processing import utils
from data_processing.utils import MiscError, RawDataMissingError
from data_processing import processing_tools
from data_processing.processing_tools import Uuid, ProcessBase


warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)

temp_file = NamedTemporaryFile()


def main(args, storage_session=requests.session()):
    args = _parse_args(args)
    config = utils.read_main_conf(args)
    start_date, stop_date = _get_processing_dates(args)
    process = ProcessCloudnet(args, config, storage_session)
    for date in date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        process.date_str = date_str
        print(f'{args.site[0]} {date_str}')
        for product in args.products:
            if product not in utils.get_product_types():
                raise ValueError('No such product')
            if product == 'model':
                continue
            print(f'{product.ljust(20)}', end='\t')
            uuid = Uuid()
            try:
                uuid.volatile = process.check_product_status(product)
                if product in utils.get_product_types(level=2):
                    uuid, identifier = process.process_level2(uuid, product)
                else:
                    uuid, identifier = getattr(process, f'process_{product}')(uuid)
                process.add_pid(temp_file.name, uuid)
                process.upload_product_and_images(temp_file.name, product, uuid, identifier)
                process.print_info(uuid)
            except (RawDataMissingError, MiscError, HTTPError, ConnectionError, RuntimeError,
                    KeyError, ValueError) as err:
                print(err)
        processing_tools.clean_dir(process.temp_dir.name)


class ProcessCloudnet(ProcessBase):

    def process_mwr(self, uuid: Uuid) -> Tuple[Uuid, str]:
        instrument = 'hatpro'
        try:
            full_paths, raw_uuids = self._download_instrument(instrument, '^(?!.*scan).*\.lwp$')
            uuid.product, valid_full_paths = hatpro2nc(self.temp_dir.name, temp_file.name,
                                                       self.site_meta, uuid=uuid.volatile,
                                                       date=self.date_str)
        except RawDataMissingError:
            pattern = '(clwvi.*.nc$|.lwp.*.nc$)'
            full_paths, raw_uuids = self._download_instrument(instrument, pattern)
            valid_full_paths = concat_wrapper.concat_hatpro_files(full_paths, self.date_str,
                                                                  temp_file.name)
            uuid.product = self._fix_calibrated_daily_file(uuid, temp_file.name, instrument)
        uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)
        return uuid, instrument

    def process_radar(self, uuid: Uuid) -> Tuple[Uuid, str]:
        try:
            instrument = 'rpg-fmcw-94'
            full_paths, raw_uuids = self._download_instrument(instrument, '.lv1$')
            uuid.product, valid_full_paths = rpg2nc(self.temp_dir.name, temp_file.name, self.site_meta,
                                                    uuid=uuid.volatile, date=self.date_str)
            uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)
        except RawDataMissingError:
            try:
                instrument = 'mira'
                full_paths, uuid.raw = self._download_instrument(instrument)
                dir_name = _unzip_gz_files(full_paths)
                uuid.product = mira2nc(dir_name, temp_file.name, self.site_meta, uuid=uuid.volatile,
                                       date=self.date_str)
            except RawDataMissingError:
                instrument = 'basta'
                full_path, uuid.raw = self._download_instrument(instrument, largest_only=True)
                uuid.product = basta2nc(full_path, temp_file.name, self.site_meta,
                                        uuid=uuid.volatile, date=self.date_str)
        return uuid, instrument

    def process_lidar(self, uuid: Uuid) -> Tuple[Uuid, str]:
        try:
            instrument = 'chm15k'
            raw_daily_file = NamedTemporaryFile(suffix='.nc')
            full_paths, raw_uuids = self._download_instrument(instrument)
            valid_full_paths = concat_wrapper.concat_chm15k_files(full_paths, self.date_str,
                                                                  raw_daily_file.name)
            uuid.raw = _get_valid_uuids(raw_uuids, full_paths, valid_full_paths)
        except RawDataMissingError:
            try:
                instrument = 'cl51'
                raw_daily_file = NamedTemporaryFile(suffix='.DAT')
                if self._site == 'norunda':
                    full_paths, uuid.raw = self._download_adjoining_daily_files(instrument)
                    utils.concatenate_text_files(full_paths, raw_daily_file.name)
                    _fix_cl51_timestamps(raw_daily_file.name, 'Europe/Stockholm')
                else:
                    full_path, uuid.raw = self._download_instrument(instrument, largest_only=True)
                    shutil.move(full_path, raw_daily_file.name)

            except RawDataMissingError:
                instrument = 'halo-doppler-lidar'
                full_path, uuid.raw = self._download_instrument(instrument, largest_only=True)
                uuid.product = self._fix_calibrated_daily_file(uuid, full_path, instrument)
                raw_daily_file = None

        if instrument != 'halo-doppler-lidar':
            site_meta = self._fetch_calibration_factor(instrument)
            uuid.product = ceilo2nc(raw_daily_file.name, temp_file.name, site_meta=site_meta,
                                    uuid=uuid.volatile, date=self.date_str)
        return uuid, instrument

    def process_categorize(self, uuid: Uuid) -> Tuple[Uuid, str]:
        l1_products = utils.get_product_types(level=1)
        input_files = {key: '' for key in l1_products}
        for product in l1_products:
            if product == 'model':
                payload = self._get_payload()
                metadata = self._md_api.get('api/model-files', payload)
            else:
                payload = self._get_payload(product=product)
                metadata = self._md_api.get('api/files', payload)
            self._check_response_length(metadata)
            if metadata:
                input_files[product] = self._storage_api.download_product(metadata[0],
                                                                          self.temp_dir.name)
        if not input_files['mwr'] and 'rpg-fmcw-94' in input_files['radar']:
            input_files['mwr'] = input_files['radar']
        missing = [product for product in l1_products if not input_files[product]]
        if missing:
            raise MiscError(f'Missing required input files: {", ".join(missing)}')
        uuid.product = generate_categorize(input_files, temp_file.name, uuid=uuid.volatile)
        return uuid, 'categorize'

    def process_level2(self, uuid: Uuid, product: str) -> Tuple[Uuid, str]:
        payload = self._get_payload(product='categorize')
        metadata = self._md_api.get('api/files', payload)
        self._check_response_length(metadata)
        if metadata:
            categorize_file = self._storage_api.download_product(metadata[0], self.temp_dir.name)
        else:
            raise MiscError(f'Missing input categorize file')
        module = importlib.import_module(f'cloudnetpy.products.{product}')
        fun = getattr(module, f'generate_{product}')
        uuid.product = fun(categorize_file, temp_file.name, uuid=uuid.volatile)
        identifier = utils.get_product_identifier(product)
        return uuid, identifier

    def check_product_status(self, product: str) -> Union[str, None, bool]:
        payload = self._get_payload(product=product)
        payload['showLegacy'] = True
        metadata = self._md_api.get(f'api/files', payload)
        return self._check_meta(metadata)

    def add_pid(self, full_path: str, uuid: Uuid) -> None:
        if self._is_new_version(uuid):
            self._pid_utils.add_pid_to_file(full_path)

    def _download_instrument(self,
                             instrument: str,
                             pattern: Optional[str] = None,
                             largest_only: Optional[bool] = False) -> Tuple[Union[list, str], list]:
        payload = self._get_payload(instrument=instrument)
        upload_metadata = self._md_api.get('upload-metadata', payload)
        if pattern is not None:
            upload_metadata = _screen_by_filename(upload_metadata, pattern)
        arg = temp_file if largest_only else None
        self._check_raw_data_status(upload_metadata)
        return self._download_raw_files(upload_metadata, arg)

    def _download_adjoining_daily_files(self, instrument: str) -> Tuple[list, list]:
        next_day = utils.get_date_from_past(-1, self.date_str)
        payload = self._get_payload(instrument=instrument)
        payload['dateFrom'] = self.date_str
        payload['dateTo'] = next_day
        upload_metadata = self._md_api.get('upload-metadata', payload)
        upload_metadata = _order_metadata(upload_metadata)
        if not upload_metadata:
            raise RawDataMissingError('No raw data')
        if not self._is_unprocessed_data(upload_metadata) and not self.is_reprocess:
            raise MiscError('Raw data already processed')
        return self._download_raw_files(upload_metadata)

    def _fix_calibrated_daily_file(self,
                                   uuid: Uuid,
                                   full_path: str,
                                   instrument: str) -> str:
        data = {
            'site_name': self._site,
            'date': self.date_str,
            'uuid': uuid.volatile,
            'full_path': full_path,
            'instrument': instrument,
            }
        uuid_product = nc_header_augmenter.harmonize_nc_file(data)
        return uuid_product

    def _fetch_calibration_factor(self, instrument: str) -> dict:
        meta = self.site_meta
        meta['calibration_factor'] = utils.get_calibration_factor(self._site, self.date_str,
                                                                  instrument)
        return meta


def _order_metadata(metadata: list) -> list:
    key = 'measurementDate'
    if len(metadata) == 2 and metadata[0][key] > metadata[1][key]:
        metadata.reverse()
    return metadata


def _get_valid_uuids(uuids: list, full_paths: list, valid_full_paths: list) -> list:
    return [uuid for uuid, full_path in zip(uuids, full_paths) if full_path in valid_full_paths]


def _unzip_gz_files(full_paths: list) -> str:
    for full_path in full_paths:
        if full_path.endswith('.gz'):
            filename = full_path.replace('.gz', '')
            with gzip.open(full_path, 'rb') as file_in:
                with open(filename, 'wb') as file_out:
                    shutil.copyfileobj(file_in, file_out)
    return os.path.dirname(full_paths[0])


def _screen_by_filename(metadata: list, pattern: str) -> list:
    return [row for row in metadata if re.search(pattern.lower(), row['filename'].lower())]


def _get_processing_dates(args):
    if args.date is not None:
        start_date = args.date
        stop_date = utils.get_date_from_past(-1, start_date)
    else:
        start_date = args.start
        stop_date = args.stop
    start_date = utils.date_string_to_date(start_date)
    stop_date = utils.date_string_to_date(stop_date)
    return start_date, stop_date


def _fix_cl51_timestamps(filename: str, time_zone: str) -> None:
    with open(filename, 'r') as file:
        lines = file.readlines()
    for ind, line in enumerate(lines):
        if is_timestamp(line):
            date_time = line.strip('-').strip('\n')
            date_time_utc = utils.datetime_to_utc(date_time, time_zone)
            lines[ind] = f'-{date_time_utc}\n'
    with open(filename, 'w') as file:
        file.writelines(lines)


def _parse_args(args):
    parser = argparse.ArgumentParser(description='Process Cloudnet Level 1 and 2 data.')
    parser = processing_tools.add_default_arguments(parser)
    parser.add_argument('--start',
                        type=str,
                        metavar='YYYY-MM-DD',
                        help='Starting date. Default is current day - 5 (included).',
                        default=utils.get_date_from_past(5))
    parser.add_argument('--stop',
                        type=str,
                        metavar='YYYY-MM-DD',
                        help='Stopping date. Default is current day + 1 (excluded).',
                        default=utils.get_date_from_past(-1))
    parser.add_argument('-d', '--date',
                        type=str,
                        metavar='YYYY-MM-DD',
                        help='Single date to be processed.')
    parser.add_argument('-r', '--reprocess',
                        action='store_true',
                        help='Process new version of the stable files and reprocess volatile '
                             'files.',
                        default=False)
    parser.add_argument('-p', '--products',
                        help='Products to be processed, e.g., radar,lidar,mwr,categorize,iwc',
                        type=lambda s: s.split(','),
                        default=utils.get_product_types())
    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
