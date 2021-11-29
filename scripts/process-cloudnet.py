#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import argparse
import importlib
import sys
import warnings
from typing import Tuple, Union, Optional
import requests
import logging
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.utils import date_range
from requests.exceptions import HTTPError
from data_processing import utils
from data_processing.utils import MiscError, RawDataMissingError
from data_processing import processing_tools
from data_processing.processing_tools import Uuid, ProcessBase
from data_processing import instrument_process
from cloudnetpy.exceptions import InconsistentDataError, DisdrometerDataError, ValidTimeStampError


warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)


def main(args, storage_session=requests.session()):
    args = _parse_args(args)
    utils.init_logger(args)
    config = utils.read_main_conf()
    start_date, stop_date = utils.get_processing_dates(args)
    process = ProcessCloudnet(args, config, storage_session=storage_session)
    for date in date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        process.date_str = date_str
        for product in args.products:
            processing_tools.clean_dir(process.temp_dir.name)
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
                elif product in utils.get_product_types(level='1b'):
                    uuid, identifier = process.process_instrument(uuid, product)
                else:
                    logging.info(f'Skipping product {product}')
                    continue
                process.add_pid(process.temp_file.name)
                process.upload_product(process.temp_file.name, product, uuid, identifier)
                process.create_and_upload_images(process.temp_file.name, product, uuid, identifier)
                process.upload_quality_report(process.temp_file.name, uuid)
                process.print_info()
            except (RawDataMissingError, MiscError, NotImplementedError) as err:
                logging.warning(err)
            except (InconsistentDataError, DisdrometerDataError, ValidTimeStampError) as err:
                logging.error(err)
            except (HTTPError, ConnectionError, RuntimeError, ValueError) as err:
                utils.send_slack_alert(err, 'data', args.site, date_str, product)


class ProcessCloudnet(ProcessBase):
    def process_instrument(self, uuid: Uuid, instrument_type: str):
        instrument = self._detect_uploaded_instrument(instrument_type)
        process_class = getattr(instrument_process, f'Process{instrument_type.capitalize()}')
        process = process_class(self, self.temp_file, uuid)
        getattr(process, f'process_{instrument.replace("-", "_")}')()
        return process.uuid, instrument

    def process_categorize(self, uuid: Uuid) -> Tuple[Uuid, str]:
        l1_products = utils.get_product_types(level='1b')
        l1_products.remove('disdrometer')  # Not yet used
        meta_records = self._get_level1b_metadata_for_categorize(l1_products)
        missing = self._get_missing_level1b_products(meta_records, l1_products)
        if missing:
            raise MiscError(f'Missing required input files: {", ".join(missing)}')
        else:
            self._check_source_status('categorize', meta_records)
            input_files = {key: '' for key in l1_products}
            for product, metadata in meta_records.items():
                input_files[product] = self._storage_api.download_product(metadata,
                                                                          self.temp_dir.name)
            if not input_files['mwr'] and 'rpg-fmcw-94' in input_files['radar']:
                input_files['mwr'] = input_files['radar']
        uuid.product = generate_categorize(input_files, self.temp_file.name, uuid=uuid.volatile)
        return uuid, 'categorize'

    def _get_level1b_metadata_for_categorize(self, source_products: list) -> dict:
        meta_records = {}
        for product in source_products:
            if product == 'model':
                payload = self._get_payload()
                metadata = self.md_api.get('api/model-files', payload)
            else:
                payload = self._get_payload(product=product)
                metadata = self.md_api.get('api/files', payload)
            self._check_response_length(metadata)
            if metadata:
                meta_records[product] = metadata[0]
        return meta_records

    @staticmethod
    def _get_missing_level1b_products(meta_records: dict, required_products: list) -> list:
        existing_products = list(meta_records.keys())
        if 'mwr' not in meta_records and ('radar' in meta_records and 'rpg-fmcw' in
                                          meta_records['radar']['filename']):
            existing_products.append('mwr')
        return [product for product in required_products if product not in existing_products]

    def process_level2(self, uuid: Uuid, product: str) -> Tuple[Uuid, str]:
        payload = self._get_payload(product='categorize')
        metadata = self.md_api.get('api/files', payload)
        self._check_response_length(metadata)
        if metadata:
            categorize_file = self._storage_api.download_product(metadata[0], self.temp_dir.name)
            meta_record = {'categorize': metadata[0]}
        else:
            raise MiscError(f'Missing input categorize file')
        self._check_source_status(product, meta_record)
        module = importlib.import_module(f'cloudnetpy.products.{product}')
        fun = getattr(module, f'generate_{product}')
        uuid.product = fun(categorize_file, self.temp_file.name, uuid=uuid.volatile)
        identifier = utils.get_product_identifier(product)
        return uuid, identifier

    def add_pid(self, full_path: str) -> None:
        if self._create_new_version:
            self._pid_utils.add_pid_to_file(full_path)

    def download_instrument(self,
                            instrument: str,
                            include_pattern: Optional[str] = None,
                            largest_only: Optional[bool] = False,
                            exclude_pattern: Optional[str] = None) -> Tuple[Union[list, str], list]:
        payload = self._get_payload(instrument=instrument, skip_created=True)
        upload_metadata = self.md_api.get('upload-metadata', payload)
        if include_pattern is not None:
            upload_metadata = utils.include_records_with_pattern_in_filename(upload_metadata,
                                                                             include_pattern)
        if exclude_pattern is not None:
            upload_metadata = utils.exclude_records_with_pattern_in_filename(upload_metadata,
                                                                             exclude_pattern)
        arg = self.temp_file if largest_only else None
        self._check_raw_data_status(upload_metadata)
        return self._download_raw_files(upload_metadata, arg)

    def download_uploaded(self,
                          instrument: str,
                          exclude_pattern: Optional[str]) -> Tuple[Union[list, str], list]:
        payload = self._get_payload(instrument=instrument)
        payload['status'] = 'uploaded'
        upload_metadata = self.md_api.get('upload-metadata', payload)
        if exclude_pattern is not None:
            upload_metadata = utils.exclude_records_with_pattern_in_filename(upload_metadata,
                                                                             exclude_pattern)
        return self._download_raw_files(upload_metadata)

    def download_adjoining_daily_files(self, instrument: str) -> Tuple[list, list]:
        next_day = utils.get_date_from_past(-1, self.date_str)
        payload = self._get_payload(instrument=instrument, skip_created=True)
        payload['dateFrom'] = self.date_str
        payload['dateTo'] = next_day
        upload_metadata = self.md_api.get('upload-metadata', payload)
        upload_metadata = utils.order_metadata(upload_metadata)
        if not upload_metadata:
            raise RawDataMissingError
        if not self._is_unprocessed_data(upload_metadata) and not (self.is_reprocess
                                                                   or self.is_reprocess_volatile):
            raise MiscError('Raw data already processed')
        full_paths, uuids = self._download_raw_files(upload_metadata)
        uuids_of_current_day = [meta['uuid'] for meta in upload_metadata
                                if meta['measurementDate'] == self.date_str]
        return full_paths, uuids_of_current_day

    def _detect_uploaded_instrument(self, instrument_type: str) -> str:
        instrument_metadata = self.md_api.get('api/instruments')
        possible_instruments = [x['id'] for x in instrument_metadata if x['type'] == instrument_type]
        payload = self._get_payload()
        upload_metadata = self.md_api.get('upload-metadata', payload)
        uploaded_instruments = set([x['instrument']['id'] for x in upload_metadata])
        instrument = list(set(possible_instruments) & uploaded_instruments)
        if len(instrument) == 0:
            raise RawDataMissingError
        selected_instrument = instrument[0]
        if len(instrument) > 1:
            preferred = ('rpg-fmcw-94', 'chm15k')
            for instru in instrument:
                if instru in preferred:
                    selected_instrument = instru
                    break
            logging.warning(f'More than one type of {instrument_type} data, '
                            f'using {selected_instrument}')
        return selected_instrument


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
                        help='Process a) new version of the stable files, b) reprocess volatile '
                             'files, c) create volatile file from unprocessed files.',
                        default=False)
    parser.add_argument('-p', '--products',
                        help='Products to be processed, e.g., radar,lidar,mwr,categorize,iwc',
                        type=lambda s: s.split(','),
                        default=utils.get_product_types())
    parser.add_argument('--reprocess_volatile',
                        action='store_true',
                        help='Reprocess unprocessed and volatile files only.',
                        default=False)
    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
