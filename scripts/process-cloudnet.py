#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import os
import argparse
from typing import Tuple, Union, NamedTuple
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
from data_processing import modifier
from data_processing.utils import MiscError, RawDataMissingError

warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)

temp_file = NamedTemporaryFile()


def main():
    config = utils.read_main_conf(ARGS)
    site_meta = utils.read_site_info(ARGS.site[0])
    start_date = utils.date_string_to_date(ARGS.start)
    stop_date = utils.date_string_to_date(ARGS.stop)

    for date in date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        print(f'{date_str}')
        process = Process(site_meta, date_str, config)
        for product in ARGS.products:
            try:
                print(f'{product.ljust(15)}', end='\t')
                uuid = Uuid
                uuid.volatile = process.check_product_status(product)
                if product in utils.get_product_types(level=2):
                    uuid, identifier = process.process_level2(uuid, product)
                else:
                    uuid, identifier = getattr(process, f'process_{product}')(uuid)
                process.upload_product_and_images(temp_file.name, product, uuid, identifier)
                _print_info(uuid)
            except (RawDataMissingError, MiscError) as err:
                print(err)


class Uuid(NamedTuple):
    raw: list
    product: str
    volatile: str


class Process:
    def __init__(self, site_meta: dict, date_str: str, config: dict):
        self.site_meta = site_meta
        self.date_str = date_str  # YYYY-MM-DD
        self._md_api = MetadataApi(config)
        self._storage_api = StorageApi(config)
        self._pid_utils = PidUtils(config)
        self._temp_dir = TemporaryDirectory()
        self._site = self.site_meta['id']

    def process_model(self, uuid: Uuid) -> Tuple[Uuid, str]:
        uuid.raw, upload_filename = self._get_daily_raw_file(temp_file.name)
        uuid.product = modifier.fix_model_file(temp_file.name, self._site, uuid.volatile)
        identifier = upload_filename.split('_')[-1][:-3]
        return uuid, identifier

    def process_mwr(self, uuid: Uuid) -> Tuple[Uuid, str]:
        identifier = 'hatpro'
        uuid.raw, upload_filename = self._get_daily_raw_file(temp_file.name, identifier)
        uuid.product = modifier.fix_mwr_file(temp_file.name, upload_filename, self.date_str,
                                             self._site, uuid.volatile)
        return uuid, identifier

    def process_radar(self, uuid: Uuid) -> Tuple[Uuid, str]:
        try:
            identifier = 'rpg-fmcw-94'
            _, uuid.raw = self._download_raw_data(identifier)
            uuid.product = rpg2nc(self._temp_dir.name, temp_file.name, self.site_meta,
                                  uuid=uuid.volatile)
        except RawDataMissingError:
            identifier = 'mira'
            raw_daily_file = NamedTemporaryFile()
            uuid.raw, _ = self._get_daily_raw_file(raw_daily_file.name, identifier)
            uuid.product = mira2nc(raw_daily_file.name, temp_file.name, self.site_meta,
                                   uuid=uuid.volatile)
        return uuid, identifier

    def process_lidar(self, uuid: Uuid) -> Tuple[Uuid, str]:

        def _concatenate_chm15k() -> list:
            full_paths, uuids = self._download_raw_data(identifier)
            valid_full_paths = concat_lib.concat_chm15k_files(full_paths, self.date_str,
                                                              raw_daily_file.name)
            return [valid_uuid for valid_uuid, full_path in zip(uuids, full_paths)
                    if full_path in valid_full_paths]

        try:
            identifier = 'chm15k'
            raw_daily_file = NamedTemporaryFile(suffix='.nc')
            uuid.raw = _concatenate_chm15k()
        except RawDataMissingError:
            identifier = 'cl51'
            raw_daily_file = NamedTemporaryFile(suffix='.DAT')
            uuid.raw, _ = self._get_daily_raw_file(raw_daily_file.name, identifier)

        uuid.product = ceilo2nc(raw_daily_file.name, temp_file.name, self.site_meta,
                                uuid=uuid.volatile)
        return uuid, identifier

    def process_categorize(self, uuid: Uuid) -> Tuple[Uuid, str]:
        l1_products = utils.get_product_types(level=1)
        input_files = {key: '' for key in l1_products}
        for product in l1_products:
            payload = self._get_payload({'product': product})
            metadata = self._md_api.get('api/files', payload)
            if metadata:
                input_files[product] = self._storage_api.download_product(metadata[0],
                                                                          self._temp_dir.name)
        if not input_files['mwr']:
            input_files['mwr'] = input_files['radar']  # This is OK with RPG radar but not with MIRA
        missing = [product for product in l1_products if not input_files[product]]
        if missing:
            raise MiscError(f'Missing required input files: {", ".join(missing)}')
        uuid.product = generate_categorize(input_files, temp_file.name, uuid=uuid.volatile)
        return uuid, 'categorize'

    def process_level2(self, uuid: Uuid, product: str) -> Tuple[Uuid, str]:
        payload = self._get_payload({'product': 'categorize'})
        metadata = self._md_api.get('api/files', payload)
        if metadata:
            categorize_file = self._storage_api.download_product(metadata[0], self._temp_dir.name)
        else:
            raise MiscError(f'Missing input categorize file')
        module = importlib.import_module(f'cloudnetpy.products.{product}')
        fun = getattr(module, f'generate_{product}')
        uuid.product = fun(categorize_file, temp_file.name, uuid=uuid.volatile)
        identifier = _get_product_identifier(product)
        return uuid, identifier

    def check_product_status(self, product: str) -> Union[str, None]:
        payload = self._get_payload({'product': product})
        metadata = self._md_api.get('api/files', payload)
        assert len(metadata) <= 1
        if metadata:
            if not metadata[0]['volatile'] and not ARGS.reprocess:
                raise MiscError('Existing freezed file and no "reprocess" flag')
            if metadata[0]['volatile']:
                return metadata[0]['uuid']
        return None

    def upload_product_and_images(self, full_path: str, product: str, uuid: Uuid,
                                  identifier: str) -> None:
        if ARGS.reprocess and uuid.volatile is None:
            self._pid_utils.add_pid_to_file(full_path)
        s3key = self._get_product_key(identifier)
        file_info = self._storage_api.upload_product(full_path, s3key)
        img_metadata = self._storage_api.create_and_upload_images(full_path, s3key, uuid.product, product)
        payload = utils.create_product_put_payload(full_path, file_info)
        self._md_api.put(s3key, payload)
        for data in img_metadata:
            self._md_api.put_img(data, uuid.product)
        if product in utils.get_product_types(level=1):
            self._update_statuses(uuid.raw)

    def _get_daily_raw_file(self, raw_daily_file: str, instrument: str = None) -> Tuple[list, str]:
        full_path, uuid = self._download_raw_data(instrument)
        assert len(full_path) == 1 and len(uuid) == 1
        shutil.move(full_path[0], raw_daily_file)
        original_filename = os.path.basename(full_path[0])
        return uuid, original_filename

    def _download_raw_data(self, instrument: str = None) -> Tuple[list, list]:
        payload = self._get_payload()
        all_upload_metadata = self._md_api.get('upload-metadata', payload)
        upload_metadata = self._md_api.screen_metadata(all_upload_metadata, instrument)
        self._check_raw_data_status(upload_metadata)
        full_paths = self._storage_api.download_raw_files(upload_metadata, self._temp_dir.name)
        uuids = [row['uuid'] for row in upload_metadata]
        return full_paths, uuids

    @staticmethod
    def _check_raw_data_status(metadata: list) -> None:
        if not metadata:
            raise RawDataMissingError('No raw data')
        is_unprocessed_data = any([row['status'] == 'uploaded' for row in metadata])
        if not is_unprocessed_data and not ARGS.reprocess:
            raise MiscError('Raw data already processed')

    def _update_statuses(self, uuids: list) -> None:
        for uuid in uuids:
            payload = {'uuid': uuid, 'status': 'processed'}
            self._md_api.post('upload-metadata', payload)

    def _get_payload(self, args: dict = None) -> dict:
        payload = {
            'dateFrom': self.date_str,
            'dateTo': self.date_str,
            'location': self._site,
        }
        if args:
            for key, value in args.items():
                payload[key] = value
        return payload

    def _get_product_key(self, identifier: str) -> str:
        return f"{self.date_str.replace('-', '')}_{self._site}_{identifier}.nc"


def _get_product_identifier(product: str) -> str:
    if product == 'iwc':
        return 'iwc-Z-T-method'
    elif product == 'lwc':
        return 'lwc-scaled-adiabatic'
    else:
        return product


def _print_info(uuid: Uuid) -> None:
    is_new_version = ARGS.reprocess and uuid.volatile is None
    print(f'Created: {"New version" if is_new_version else "Volatile file"}')


if __name__ == "__main__":
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
    ARGS = parser.parse_args()
    main()
