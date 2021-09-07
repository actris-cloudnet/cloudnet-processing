#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import argparse
import importlib
import sys
import re
import warnings
from tempfile import NamedTemporaryFile
from typing import Tuple, Union, Optional
import requests
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.utils import date_range
from requests.exceptions import HTTPError
from data_processing import nc_header_augmenter
from data_processing import utils
from data_processing.utils import MiscError, RawDataMissingError
from data_processing import processing_tools
from data_processing.processing_tools import Uuid, ProcessBase
from data_processing.instrument_process import ProcessRadar, ProcessLidar, ProcessMwr
import logging


warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)

temp_file = NamedTemporaryFile()


def main(args, storage_session=requests.session()):
    args = _parse_args(args)
    utils.init_logger(args)
    config = utils.read_main_conf()
    start_date, stop_date = _get_processing_dates(args)
    process = ProcessCloudnet(args, config, storage_session=storage_session)
    for date in date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        process.date_str = date_str
        for product in args.products:
            if product not in utils.get_product_types():
                raise ValueError('No such product')
            if product == 'model':
                continue
            logging.info(f'Processing {product} product, {args.site} {date_str}')
            uuid = Uuid()
            try:
                uuid.volatile = process.fetch_volatile_uuid(product)
                if product in utils.get_product_types(level='2'):
                    uuid, identifier = process.process_level2(uuid, product)
                elif product == 'categorize':
                    uuid, identifier = process.process_categorize(uuid)
                else:
                    uuid, identifier = process.process_instrument(uuid, product)
                process.add_pid(temp_file.name)
                process.upload_product_and_images(temp_file.name, product, uuid, identifier)
                process.print_info()
            except (RawDataMissingError, MiscError, NotImplementedError) as err:
                logging.warning(err)
            except (HTTPError, ConnectionError, RuntimeError, ValueError) as err:
                utils.send_slack_alert(err, 'data', args.site, date_str, product)
        processing_tools.clean_dir(process.temp_dir.name)


class ProcessCloudnet(ProcessBase):
    def process_instrument(self, uuid: Uuid, instrument_type: str):
        instrument = self._detect_uploaded_instrument(instrument_type)
        if instrument_type == 'lidar':
            process_class = ProcessLidar
        elif instrument_type == 'radar':
            process_class = ProcessRadar
        else:
            process_class = ProcessMwr
        process = process_class(self, temp_file, uuid)
        getattr(process, f'process_{instrument.replace("-", "_")}')()
        return process.uuid, instrument

    def process_categorize(self, uuid: Uuid) -> Tuple[Uuid, str]:
        l1_products = utils.get_product_types(level='1b')
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

    def fetch_volatile_uuid(self, product: str) -> Union[str, None]:
        payload = self._get_payload(product=product)
        payload['showLegacy'] = True
        metadata = self._md_api.get(f'api/files', payload)
        uuid = self._read_volatile_uuid(metadata)
        self._create_new_version = self._is_create_new_version(metadata)
        return uuid

    def add_pid(self, full_path: str) -> None:
        if self._create_new_version:
            self._pid_utils.add_pid_to_file(full_path)

    def download_instrument(self,
                            instrument: str,
                            pattern: Optional[str] = None,
                            largest_only: Optional[bool] = False) -> Tuple[Union[list, str], list]:
        payload = self._get_payload(instrument=instrument, skip_created=True)
        upload_metadata = self._md_api.get('upload-metadata', payload)
        if pattern is not None:
            upload_metadata = _screen_by_filename(upload_metadata, pattern)
        arg = temp_file if largest_only else None
        self._check_raw_data_status(upload_metadata)
        return self._download_raw_files(upload_metadata, arg)

    def download_adjoining_daily_files(self, instrument: str) -> Tuple[list, list]:
        next_day = utils.get_date_from_past(-1, self.date_str)
        payload = self._get_payload(instrument=instrument, skip_created=True)
        payload['dateFrom'] = self.date_str
        payload['dateTo'] = next_day
        upload_metadata = self._md_api.get('upload-metadata', payload)
        upload_metadata = _order_metadata(upload_metadata)
        if not upload_metadata:
            raise RawDataMissingError('No raw data')
        if not self._is_unprocessed_data(upload_metadata) and not self.is_reprocess:
            raise MiscError('Raw data already processed')
        return self._download_raw_files(upload_metadata)

    def fix_calibrated_daily_file(self,
                                  uuid: Uuid,
                                  full_path: str,
                                  instrument: str) -> str:
        data = {
            'site_name': self.site,
            'date': self.date_str,
            'uuid': uuid.volatile,
            'full_path': full_path,
            'instrument': instrument,
            'altitude': self.site_meta['altitude']
            }
        uuid_product = nc_header_augmenter.harmonize_nc_file(data)
        return uuid_product

    def _detect_uploaded_instrument(self, instrument_type: str) -> str:
        instrument_metadata = self._md_api.get('api/instruments')
        possible_instruments = [x['id'] for x in instrument_metadata if x['type'] == instrument_type]
        payload = self._get_payload()
        upload_metadata = self._md_api.get('upload-metadata', payload)
        uploaded_instruments = set([x['instrument']['id'] for x in upload_metadata])
        instrument = list(set(possible_instruments) & uploaded_instruments)
        if len(instrument) == 0:
            raise RawDataMissingError('Missing raw data')
        if len(instrument) > 1:
            logging.warning(f'More than one type of {instrument_type} data, using {instrument[0]}')
        return instrument[0]


def _order_metadata(metadata: list) -> list:
    key = 'measurementDate'
    if len(metadata) == 2 and metadata[0][key] > metadata[1][key]:
        metadata.reverse()
    return metadata


def _get_valid_uuids(uuids: list, full_paths: list, valid_full_paths: list) -> list:
    return [uuid for uuid, full_path in zip(uuids, full_paths) if full_path in valid_full_paths]


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
