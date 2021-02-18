#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import argparse
import glob
import gzip
import importlib
import os
import shutil
import sys
import warnings
from tempfile import NamedTemporaryFile
from tempfile import TemporaryDirectory
from typing import Tuple, Union, Optional
import requests
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.instruments import rpg2nc, ceilo2nc, mira2nc, basta2nc
from cloudnetpy.utils import date_range
from requests.exceptions import HTTPError, ConnectionError
from data_processing import concat_wrapper
from data_processing import nc_header_augmenter
from data_processing import utils
from data_processing.metadata_api import MetadataApi
from data_processing.pid_utils import PidUtils
from data_processing.storage_api import StorageApi
from data_processing.utils import MiscError, RawDataMissingError

warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)

temp_file = NamedTemporaryFile()
temp_dir = TemporaryDirectory()


def main(args, storage_session=requests.session()):
    args = _parse_args(args)
    config = utils.read_main_conf(args)
    start_date = utils.date_string_to_date(args.start)
    stop_date = utils.date_string_to_date(args.stop)
    process = Process(args, config, storage_session)

    if 'model' in args.products:
        models_to_process = process.get_models_to_process(args)

    for date in date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        process.date_str = date_str
        print(f'{args.site[0]} {date_str}')
        for product in args.products:
            print(f'{product.ljust(20)}', end='\t')
            if product == 'model':
                print('')
                for model in models_to_process:
                    print(f'  {model.ljust(20)}', end='\t')
                    uuid = Uuid()
                    try:
                        uuid.volatile = process.check_product_status(product, model=model)
                        uuid = process.process_model(uuid, model)
                        process.upload_product_and_images(temp_file.name, product, uuid,
                                                          model=model)
                        process.print_info(uuid)
                    except (RawDataMissingError, MiscError, HTTPError, ConnectionError) as err:
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
                except (RawDataMissingError, MiscError, HTTPError, ConnectionError,
                        RuntimeError) as err:
                    print(err)
        _clean_temp_dir()


class Uuid:

    __slots__ = ['raw', 'product', 'volatile']

    def __init__(self):
        self.raw: list = []
        self.product: str = ''
        self.volatile: Union[str, bool, None] = None


class Process:
    def __init__(self,
                 args,
                 config: dict,
                 storage_session):
        self.site_meta, self._site, self._site_type = _read_site_info(args)
        self.is_reprocess = args.reprocess
        self.plot_images = self.check_if_plot_images(args)
        self.date_str = None
        self._md_api = MetadataApi(config)
        self._storage_api = StorageApi(config, storage_session)
        self._pid_utils = PidUtils(config)

    def check_if_plot_images(self, args) -> bool:
        plot_images = not args.no_img
        if 'hidden' in self._site_type:
            plot_images = False
        return plot_images

    def process_model(self, uuid: Uuid, model: str) -> Uuid:
        uuid = self._fix_calibrated_daily_file(uuid, 'model', model=model)
        return uuid

    def process_mwr(self, uuid: Uuid) -> Tuple[Uuid, str]:
        instrument = 'hatpro'
        uuid = self._fix_calibrated_daily_file(uuid, 'mwr', instrument=instrument)
        return uuid, instrument

    def process_radar(self, uuid: Uuid) -> Tuple[Uuid, str]:
        try:
            instrument = 'rpg-fmcw-94'
            full_paths, uuids = self._download_raw_data(instrument=instrument)
            uuid.product, valid_full_paths = rpg2nc(temp_dir.name, temp_file.name, self.site_meta,
                                                    uuid=uuid.volatile, date=self.date_str)
            uuid.raw = _get_valid_uuids(uuids, full_paths, valid_full_paths)
        except RawDataMissingError:
            try:
                instrument = 'mira'
                full_paths, uuid.raw = self._download_raw_data(instrument=instrument)
                dir_name = _unzip_gz_files(full_paths)
                uuid.product = mira2nc(dir_name, temp_file.name, self.site_meta,
                                       date=self.date_str, uuid=uuid.volatile)
            except RawDataMissingError:
                instrument = 'basta'
                full_paths, uuid.raw = self._download_raw_data(instrument=instrument,
                                                               largest_file_only=True)
                uuid.product = basta2nc(full_paths[0], temp_file.name, self.site_meta,
                                        date=self.date_str, uuid=uuid.volatile)
        return uuid, instrument

    def process_lidar(self, uuid: Uuid) -> Tuple[Uuid, str]:
        def _concatenate_chm15k() -> list:
            full_paths, uuids = self._download_raw_data(instrument=instrument)
            valid_full_paths = concat_wrapper.concat_chm15k_files(full_paths, self.date_str,
                                                                  raw_daily_file.name)
            return _get_valid_uuids(uuids, full_paths, valid_full_paths)
        try:
            instrument = 'chm15k'
            raw_daily_file = NamedTemporaryFile(suffix='.nc')
            uuid.raw = _concatenate_chm15k()
        except RawDataMissingError:
            try:
                instrument = 'cl51'
                raw_daily_file = NamedTemporaryFile(suffix='.DAT')
                uuid.raw, _ = self._get_daily_raw_file(raw_daily_file.name, instrument=instrument)
            except RawDataMissingError:
                instrument = 'halo-doppler-lidar'
                uuid = self._fix_calibrated_daily_file(uuid, 'lidar', instrument=instrument)
                raw_daily_file = temp_file

        if instrument != 'halo-doppler-lidar':
            uuid.product = ceilo2nc(raw_daily_file.name, temp_file.name, self.site_meta,
                                    uuid=uuid.volatile)
        return uuid, instrument

    def process_categorize(self, uuid: Uuid) -> Tuple[Uuid, str]:
        l1_products = utils.get_product_types(level=1)
        input_files = {key: '' for key in l1_products}
        for product in l1_products:
            payload = self._get_payload()
            if product == 'model':
                end_point = 'model-files'
            else:
                end_point = 'files'
            all_metadata = self._md_api.get(f'api/{end_point}', payload)
            metadata = self._md_api.screen_metadata(all_metadata, product=product)
            if metadata:
                input_files[product] = self._storage_api.download_product(metadata[0],
                                                                          temp_dir.name)
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
            categorize_file = self._storage_api.download_product(metadata[0], temp_dir.name)
        else:
            raise MiscError(f'Missing input categorize file')
        module = importlib.import_module(f'cloudnetpy.products.{product}')
        fun = getattr(module, f'generate_{product}')
        uuid.product = fun(categorize_file, temp_file.name, uuid=uuid.volatile)
        identifier = utils.get_product_identifier(product)
        return uuid, identifier

    def check_product_status(self, product: str,
                             model: Optional[str] = None) -> Union[str, None, bool]:
        payload = self._get_payload()
        if model is not None:
            end_point = 'model-files'
            payload['model'] = model
        else:
            end_point = 'files'
            payload['showLegacy'] = True
        all_metadata = self._md_api.get(f'api/{end_point}', payload)
        metadata = self._md_api.screen_metadata(all_metadata, product=product)
        if metadata:
            if not metadata[0]['volatile'] and not self.is_reprocess:
                raise MiscError('Existing freezed file and no "reprocess" flag')
            if metadata[0]['volatile']:
                return metadata[0]['uuid']
            else:
                return False
        return None

    def upload_product_and_images(self, full_path: str, product: str, uuid: Uuid,
                                  product_type: Optional[str] = None,
                                  model: Optional[str] = None) -> None:

        assert product_type is not None or model is not None
        identifier = product_type if product_type else model

        if self._is_new_version(uuid):
            self._pid_utils.add_pid_to_file(full_path)

        s3key = self._get_product_key(identifier)
        file_info = self._storage_api.upload_product(full_path, s3key)

        if self.plot_images:
            img_metadata = self._storage_api.create_and_upload_images(full_path, s3key,
                                                                      uuid.product, product)
        else:
            img_metadata = []

        payload = utils.create_product_put_payload(full_path, file_info, model=model,
                                                   site=self._site)
        self._md_api.put(s3key, payload)
        for data in img_metadata:
            self._md_api.put_img(data, uuid.product)
        if product in utils.get_product_types(level=1):
            self._update_statuses(uuid.raw)

    def get_models_to_process(self, args) -> list:
        payload = {
            'site': self._site,
            'dateFrom': args.start,
            'dateTo': args.stop,
        }
        if not self.is_reprocess:
            payload['status'] = 'uploaded'
        metadata = self._md_api.get('upload-metadata', payload)
        model_metadata = [row for row in metadata if 'model' in row]
        model_ids = [row['model']['id'] for row in model_metadata]
        return list(set(model_ids))

    def print_info(self, uuid: Uuid) -> None:
        print(f'Created: {"New version" if self._is_new_version(uuid) else "Volatile file"}')

    def _get_daily_raw_file(self,
                            raw_daily_file: str,
                            instrument: Optional[str] = None,
                            model: Optional[str] = None) -> Tuple[list, str]:
        full_path, uuid = self._download_raw_data(instrument=instrument, model=model,
                                                  largest_file_only=True)
        shutil.move(full_path[0], raw_daily_file)
        original_filename = os.path.basename(full_path[0])
        return uuid, original_filename

    def _download_raw_data(self,
                           instrument: Optional[str] = None,
                           model: Optional[str] = None,
                           largest_file_only: Optional[bool] = False) -> Tuple[list, list]:
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
        full_paths = self._storage_api.download_raw_files(upload_metadata, temp_dir.name)
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

    def _get_payload(self, args: Optional[dict] = None) -> dict:
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

    def _fix_calibrated_daily_file(self, uuid: Uuid, file_type: str,
                                   instrument: Optional[str] = None,
                                   model: Optional[str] = None) -> Uuid:
        uuid.raw, upload_filename = self._get_daily_raw_file(temp_file.name, instrument=instrument,
                                                             model=model)
        data = {
            'site_name': self._site,
            'date': self.date_str,
            'uuid': uuid.volatile,
            'full_path': temp_file.name,
            'cloudnet_file_type': file_type,
            'instrument': instrument,
            'original_filename': upload_filename
            }
        uuid.product = nc_header_augmenter.harmonize_nc_file(data)
        return uuid

    def _get_product_key(self, identifier: str) -> str:
        return f"{self.date_str.replace('-', '')}_{self._site}_{identifier}.nc"

    def _is_new_version(self, uuid: Uuid) -> bool:
        return self.is_reprocess and uuid.volatile is False


def _get_valid_uuids(uuids: list, full_paths: list, valid_full_paths: list) -> list:
    return [uuid for uuid, full_path in zip(uuids, full_paths) if full_path in valid_full_paths]


def _clean_temp_dir():
    for filename in glob.glob(f'{temp_dir.name}/*'):
        os.remove(filename)


def _unzip_gz_files(full_paths: list) -> str:
    for full_path in full_paths:
        if full_path.endswith('.gz'):
            filename = full_path.replace('.gz', '')
            with gzip.open(full_path, 'rb') as file_in:
                with open(filename, 'wb') as file_out:
                    shutil.copyfileobj(file_in, file_out)
    return os.path.dirname(full_paths[0])


def _read_site_info(args) -> tuple:
    site_info = utils.read_site_info(args.site[0])
    site_id = site_info['id']
    site_type = site_info['type']
    site_meta = {key: site_info[key] for key in ('latitude', 'longitude', 'altitude', 'name')}
    return site_meta, site_id, site_type


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
                        help='Starting date. Default is current day - 5 (included).',
                        default=utils.get_date_from_past(5))
    parser.add_argument('--stop',
                        type=str,
                        metavar='YYYY-MM-DD',
                        help='Stopping date. Default is current day + 1 (excluded).',
                        default=utils.get_date_from_past(-1))
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
    parser.add_argument('--no-img',
                        dest='no_img',
                        action='store_true',
                        help='Skip image creation.',
                        default=False)
    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
