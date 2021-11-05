#!/usr/bin/env python3
"""Master script for Model evaluation processing."""
import argparse
import importlib
import json
import sys
import re
import warnings
from typing import Tuple, Union, Optional
import requests
from model_evaluation.products import product_resampling
from cloudnetpy.utils import date_range
from requests.exceptions import HTTPError
from data_processing import nc_header_augmenter
from data_processing import utils
from data_processing.utils import MiscError, RawDataMissingError
from data_processing import processing_tools
from data_processing.processing_tools import Uuid, ProcessBase
from cloudnetpy.exceptions import InconsistentDataError
import logging


warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)


def main(args, storage_session=requests.session()):
    args = _parse_args(args)
    utils.init_logger(args)
    config = utils.read_main_conf()
    start_date, stop_date = _get_processing_dates(args)
    process = ProcessModelEvaluation(args, config, storage_session=storage_session)
    for date in date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        process.date_str = date_str
        for product in args.products:
            if product not in utils.get_product_types('3'):
                raise ValueError('No such product')
            if product == 'model':
                continue
            processing_tools.clean_dir(process.temp_dir.name)
            logging.info(f'Processing {product} product, {args.site} {date_str}')
            uuid = Uuid()
            uuid.volatile = process.fetch_volatile_uuid(product)
            models_metadata = process.fetch_model_params()
            for model, m_meta in models_metadata.items():
                if product in utils.get_product_types(level='3'):
                    uuid = process.process_level3_day(uuid, product, model, m_meta)
                    process.upload_product(process.temp_file.name, product, uuid, model)
                    process.upload_images(process.temp_file.name, product, uuid, model)
                    process.print_info()


class ProcessModelEvaluation(ProcessBase):

    def process_level3_day(self, uuid: Uuid, full_product: str, model: str,
                           model_meta: list) -> Uuid:
        l3_product = utils.full_product_to_l3_product(full_product)
        l2_product = self.get_l2for_l3_product(l3_product)
        input_model_files = []
        payload = self._get_payload(product=l2_product)
        metadata = self.md_api.get('api/files', payload)
        self._check_response_length(metadata)
        if metadata:
            l2_file = self._storage_api.download_product(metadata[0], self.temp_dir.name)
        else:
            raise MiscError(f'Missing input level 2 file')
        for m_meta in model_meta:
            m_file = self._storage_api.download_product(m_meta[0], self.temp_dir.name)
            input_model_files.append(m_file)
            #TODO: Raise error if no model meta, warning if only one meta missing
        uuid.product = product_resampling.process_L3_day_product(model, l3_product, input_model_files, l2_file,
                                                                 self.temp_file.name, overwrite=True)
        return uuid

    def fetch_volatile_uuid(self, product: str) -> Union[str, None]:
        payload = self._get_payload(product=product)
        payload['showLegacy'] = True
        metadata = self.md_api.get(f'api/files', payload)
        uuid = self._read_volatile_uuid(metadata)
        self._create_new_version = self._is_create_new_version(metadata)
        return uuid

    def add_pid(self, full_path: str) -> None:
        if self._create_new_version:
            self._pid_utils.add_pid_to_file(full_path)

    def fetch_model_params(self, model: Union[str, None] = 'ecmwf') -> dict:
        # POC: only EC
        payload = self._get_payload()
        if model:
            payload = self._get_payload(model=model)
        else:
            payload['allModels'] = True
        metadata = self.md_api.get(f'api/model-files', payload)
        model_metas = self._sort_model_meta2dict(metadata)
        return model_metas

    @staticmethod
    def get_l2for_l3_product(product: str):
        if product == 'cf':
            return 'categorize'
        if product == 'iwc':
            return 'iwc'
        if product == 'lwc':
            return 'lwc'


def _order_metadata(metadata: list) -> list:
    key = 'measurementDate'
    if len(metadata) == 2 and metadata[0][key] > metadata[1][key]:
        metadata.reverse()
    return metadata


def _get_valid_uuids(uuids: list, full_paths: list, valid_full_paths: list) -> list:
    return [uuid for uuid, full_path in zip(uuids, full_paths) if full_path in valid_full_paths]


def _include_records_with_pattern_in_filename(metadata: list, pattern: str) -> list:
    return [row for row in metadata if re.search(pattern.lower(), row['filename'].lower())]


def _exclude_records_with_pattern_in_filename(metadata: list, pattern: str) -> list:
    return [row for row in metadata if not re.search(pattern.lower(), row['filename'].lower())]


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
    parser = argparse.ArgumentParser(description='Process Cloudnet Level 3 data, model evaluation.')
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
                        help='Products to be processed, e.g., cf, iwc, lwc',
                        type=lambda s: s.split(','),
                        default=utils.get_product_types('3'))
    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
