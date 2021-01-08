#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import os
import sys
import requests
import argparse
from typing import Tuple, Union
import shutil
import warnings
import importlib
from tempfile import TemporaryDirectory
from tempfile import NamedTemporaryFile
from cloudnetpy.instruments import rpg2nc, ceilo2nc, mira2nc
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.utils import date_range
from data_processing import utils
from data_processing.metadata_api import MetadataApi
from data_processing.storage_api import StorageApi
from data_processing.pid_utils import PidUtils
from data_processing import concat_lib
from data_processing import nc_header_augmenter
from data_processing.utils import MiscError, RawDataMissingError

warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)

temp_file = NamedTemporaryFile()


def main(args, storage_session=requests.session()):
    args = _parse_args(args)
    config = utils.read_main_conf(args)
    start_date = utils.date_string_to_date(args.start)
    stop_date = utils.date_string_to_date(args.stop)

    for date in date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        print(f'{date_str}')
        process = Process(args, date_str, config, storage_session)
        for product in args.products:
            print(f'{product.ljust(20)}', end='\t')
            if product == 'model':
                print('')
                for model in utils.get_model_types():
                    print(f'{model.ljust(20)}', end='\t')
                    uuid = Uuid()
                    try:
                        uuid.volatile = process.check_product_status(product, model=model)
                        uuid = process.process_model(uuid, model)
                        process.upload_product_and_images(temp_file.name, product, uuid,
                                                          model=model)
                        process.print_info(uuid)
                    except (RawDataMissingError, MiscError) as err:
                        print(err)
            else:
                uuid = Uuid()
                try:
                    uuid.volatile = process.check_product_status(product)
                    if product in utils.get_product_types(level=2):
                        uuid, identifier = process.process_level2(uuid, product)
                    else:
                        uuid, identifier = getattr(process, f'process_{product}')(uuid)
                    process.upload_product_and_images(temp_file.name, product, uuid,
                                                      product_type=identifier)
                    process.print_info(uuid)
                except (RawDataMissingError, MiscError, RuntimeError) as err:
                    print(err)


class Uuid:

    __slots__ = ['raw', 'product', 'volatile']

    def __init__(self):
        self.raw: list = []
        self.product: str = ''
        self.volatile: Union[str, bool, None] = None


class Process:
    def __init__(self,
                 args,
                 date_str: str,
                 config: dict,
                 storage_session):
        self.site_meta = utils.read_site_info(args.site[0])
        self.date_str = date_str  # YYYY-MM-DD
        self.is_reprocess = args.reprocess
        self._md_api = MetadataApi(config)
        self._storage_api = StorageApi(config, storage_session)
        self._pid_utils = PidUtils(config)
        self._temp_dir = TemporaryDirectory()
        self._site = self.site_meta['id']

    def process_model(self, uuid: Uuid, model: str) -> Uuid:
        uuid.raw, upload_filename = self._get_daily_raw_file(temp_file.name, model=model)
        uuid.product = nc_header_augmenter.fix_model_file(temp_file.name, self._site, uuid.volatile)
        return uuid

    def process_mwr(self, uuid: Uuid) -> Tuple[Uuid, str]:
        instrument = 'hatpro'
        uuid.raw, upload_filename = self._get_daily_raw_file(temp_file.name, instrument=instrument)
        uuid.product = nc_header_augmenter.fix_mwr_file(temp_file.name, upload_filename,
                                                        self.date_str, self._site, uuid.volatile)
        return uuid, instrument

    def process_radar(self, uuid: Uuid) -> Tuple[Uuid, str]:
        try:
            instrument = 'rpg-fmcw-94'
            full_paths, uuids = self._download_raw_data(instrument=instrument)
            uuid.product, valid_full_paths = rpg2nc(self._temp_dir.name, temp_file.name,
                                                    self.site_meta, uuid=uuid.volatile,
                                                    date=self.date_str)
            uuid.raw = _get_valid_uuids(uuids, full_paths, valid_full_paths)

        except RawDataMissingError:
            instrument = 'mira'
            raw_daily_file = NamedTemporaryFile()
            uuid.raw, _ = self._get_daily_raw_file(raw_daily_file.name, instrument=instrument)
            uuid.product = mira2nc(raw_daily_file.name, temp_file.name, self.site_meta,
                                   uuid=uuid.volatile)
        return uuid, instrument

    def process_lidar(self, uuid: Uuid) -> Tuple[Uuid, str]:

        def _concatenate_chm15k() -> list:
            full_paths, uuids = self._download_raw_data(instrument=instrument)
            valid_full_paths = concat_lib.concat_chm15k_files(full_paths, self.date_str,
                                                              raw_daily_file.name)
            return _get_valid_uuids(uuids, full_paths, valid_full_paths)
        try:
            instrument = 'chm15k'
            raw_daily_file = NamedTemporaryFile(suffix='.nc')
            uuid.raw = _concatenate_chm15k()
        except RawDataMissingError:
            instrument = 'cl51'
            raw_daily_file = NamedTemporaryFile(suffix='.DAT')
            uuid.raw, _ = self._get_daily_raw_file(raw_daily_file.name, instrument=instrument)

        uuid.product = ceilo2nc(raw_daily_file.name, temp_file.name, self.site_meta,
                                uuid=uuid.volatile)
        return uuid, instrument

    def process_categorize(self, uuid: Uuid) -> Tuple[Uuid, str]:
        l1_products = utils.get_product_types(level=1)
        input_files = {key: '' for key in l1_products}
        for product in l1_products:
            payload = self._get_payload()
            all_metadata = self._md_api.get('api/files', payload)
            metadata = self._md_api.screen_metadata(all_metadata, product=product)
            if metadata:
                input_files[product] = self._storage_api.download_product(metadata[0],
                                                                          self._temp_dir.name)
        if not input_files['mwr'] and 'rpg-fmcw-94' in input_files['radar']:
            input_files['mwr'] = input_files['radar']
        missing = [product for product in l1_products if not input_files[product]]
        if missing:
            raise MiscError(f'Missing required input files: {", ".join(missing)}')
        uuid.product = generate_categorize(input_files, temp_file.name, uuid=uuid.volatile)
        return uuid, 'categorize'

    def process_level2(self, uuid: Uuid, product: str) -> Tuple[Uuid, str]:
        payload = self._get_payload()
        all_metadata = self._md_api.get('api/files', payload)
        metadata = self._md_api.screen_metadata(all_metadata, product='categorize')
        assert len(metadata) <= 1
        if metadata:
            categorize_file = self._storage_api.download_product(metadata[0], self._temp_dir.name)
        else:
            raise MiscError(f'Missing input categorize file')
        module = importlib.import_module(f'cloudnetpy.products.{product}')
        fun = getattr(module, f'generate_{product}')
        uuid.product = fun(categorize_file, temp_file.name, uuid=uuid.volatile)
        identifier = utils.get_product_identifier(product)
        return uuid, identifier

    def check_product_status(self, product: str, model: str = None) -> Union[str, None, bool]:
        payload = self._get_payload()
        if model:
            payload['model'] = model
        all_metadata = self._md_api.get('api/files', payload)
        metadata = self._md_api.screen_metadata(all_metadata, product=product)
        assert len(metadata) <= 1
        if metadata:
            if not metadata[0]['volatile'] and not self.is_reprocess:
                raise MiscError('Existing freezed file and no "reprocess" flag')
            if metadata[0]['volatile']:
                return metadata[0]['uuid']
            else:
                return False
        return None

    def upload_product_and_images(self, full_path: str, product: str, uuid: Uuid,
                                  product_type: str = None, model: str = None) -> None:

        assert product_type is not None or model is not None
        identifier = product_type if product_type else model

        if self._is_new_version(uuid):
            self._pid_utils.add_pid_to_file(full_path)

        s3key = self._get_product_key(identifier)
        file_info = self._storage_api.upload_product(full_path, s3key)

        img_metadata = self._storage_api.create_and_upload_images(full_path, s3key, uuid.product,
                                                                  product)

        payload = utils.create_product_put_payload(full_path, file_info, model=model)
        self._md_api.put(s3key, payload)
        for data in img_metadata:
            self._md_api.put_img(data, uuid.product)
        if product in utils.get_product_types(level=1):
            self._update_statuses(uuid.raw)

    def print_info(self, uuid: Uuid) -> None:
        print(f'Created: {"New version" if self._is_new_version(uuid) else "Volatile file"}')

    def _get_daily_raw_file(self,
                            raw_daily_file: str,
                            instrument: str = None,
                            model: str = None) -> Tuple[list, str]:
        full_path, uuid = self._download_raw_data(instrument=instrument, model=model,
                                                  largest_file_only=True)
        shutil.move(full_path[0], raw_daily_file)
        original_filename = os.path.basename(full_path[0])
        return uuid, original_filename

    def _download_raw_data(self,
                           instrument: str = None,
                           model: str = None,
                           largest_file_only: bool = False) -> Tuple[list, list]:
        payload = self._get_payload()
        all_upload_metadata = self._md_api.get('upload-metadata', payload)
        upload_metadata = self._md_api.screen_metadata(all_upload_metadata,
                                                       instrument=instrument,
                                                       model=model)
        self._check_raw_data_status(upload_metadata)
        if largest_file_only:
            if len(upload_metadata) > 1:
                print('Warning: several daily raw files (probably submitted without '
                      '"allowUpdate")', end='\t')
            upload_metadata = [upload_metadata[0]]
        full_paths = self._storage_api.download_raw_files(upload_metadata, self._temp_dir.name)
        uuids = [row['uuid'] for row in upload_metadata]
        return full_paths, uuids

    def _check_raw_data_status(self, metadata: list) -> None:
        if not metadata:
            raise RawDataMissingError('No raw data')
        is_unprocessed_data = any([row['status'] == 'uploaded' for row in metadata])
        if not is_unprocessed_data and not self.is_reprocess:
            raise MiscError('Raw data already processed')

    def _update_statuses(self, uuids: list) -> None:
        for uuid in uuids:
            payload = {'uuid': uuid, 'status': 'processed'}
            self._md_api.post('upload-metadata', payload)

    def _get_payload(self, args: dict = None) -> dict:
        payload = {
            'dateFrom': self.date_str,
            'dateTo': self.date_str,
            'site': self._site,
            'developer': True
        }
        if args:
            for key, value in args.items():
                payload[key] = value
        return payload

    def _get_product_key(self, identifier: str) -> str:
        return f"{self.date_str.replace('-', '')}_{self._site}_{identifier}.nc"

    def _is_new_version(self, uuid: Uuid) -> bool:
        return self.is_reprocess and uuid.volatile is False


def _get_valid_uuids(uuids: list, full_paths: list, valid_full_paths: list) -> list:
    return [uuid for uuid, full_path in zip(uuids, full_paths) if full_path in valid_full_paths]


def _parse_args(args):
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('site',
                        nargs='+',
                        help='Site Name')
    parser.add_argument('--config-dir',
                        dest='config_dir',
                        type=str,
                        metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    parser.add_argument('--start',
                        type=str,
                        metavar='YYYY-MM-DD',
                        help='Starting date. Default is current day - 7.',
                        default=utils.get_date_from_past(7))
    parser.add_argument('--stop',
                        type=str,
                        metavar='YYYY-MM-DD',
                        help='Stopping date. Default is current day - 1.',
                        default=utils.get_date_from_past(1))
    parser.add_argument('-r', '--reprocess',
                        action='store_true',
                        help='Process new version of the stable files and reprocess volatile '
                             'files.',
                        default=False)
    parser.add_argument('-p', '--products',
                        help='Products to be processed, e.g., radar,lidar,model,categorize,'
                             'classification',
                        type=lambda s: s.split(','),
                        default=utils.get_product_types())
    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
