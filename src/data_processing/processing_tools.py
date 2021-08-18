import os
import glob
import shutil
import logging
import requests
from typing import Union, Tuple, Optional
from tempfile import NamedTemporaryFile, TemporaryDirectory
from data_processing import utils
from data_processing.utils import MiscError, RawDataMissingError
from data_processing.metadata_api import MetadataApi
from data_processing.storage_api import StorageApi
from data_processing.pid_utils import PidUtils


class Uuid:
    __slots__ = ['raw', 'product', 'volatile']

    def __init__(self):
        self.raw: list = []
        self.product: str = ''
        self.volatile: Union[str, bool, None] = None


def clean_dir(dir_name: str) -> None:
    for filename in glob.glob(f'{dir_name}/*'):
        os.remove(filename)


def _get_temp_dir(config: dict) -> str:
    return config.get('TEMP_DIR', '/tmp')


class ProcessBase:
    def __init__(self,
                 args,
                 config: dict,
                 storage_session: Optional[requests.Session] = requests.Session(),
                 metadata_session: Optional[requests.Session] = requests.Session()):
        self.site_meta, self._site, self._site_type = _read_site_info(args)
        self.is_reprocess = getattr(args, 'reprocess', False)
        self.date_str = None
        self._md_api = MetadataApi(config, metadata_session)
        self._storage_api = StorageApi(config, storage_session)
        self._pid_utils = PidUtils(config)
        self.temp_dir = TemporaryDirectory(dir=_get_temp_dir(config))

    def print_info(self, uuid: Uuid) -> None:
        logging.info(f'Created: {"New version" if self._is_new_version(uuid) else "Volatile file"}')

    def upload_product_and_images(self,
                                  full_path: str,
                                  product: str,
                                  uuid: Uuid,
                                  model_or_product_id: str) -> None:
        s3key = self._get_product_key(model_or_product_id)
        file_info = self._storage_api.upload_product(full_path, s3key)
        if 'hidden' not in self._site_type:
            img_metadata = self._storage_api.create_and_upload_images(full_path, s3key,
                                                                      uuid.product, product)
        else:
            img_metadata = []
        payload = utils.create_product_put_payload(full_path, file_info, site=self._site)
        if product == 'model':
            del payload['cloudnetpyVersion']
            payload['model'] = model_or_product_id
        self._md_api.put(s3key, payload)
        for data in img_metadata:
            self._md_api.put_img(data, uuid.product)
        if product in utils.get_product_types(level='1b'):
            self._update_statuses(uuid.raw)

    def _check_meta(self, metadata: list) -> Union[str, None, bool]:
        self._check_response_length(metadata)
        if metadata:
            if not metadata[0]['volatile'] and not self.is_reprocess:
                raise MiscError('Existing freezed file and no "reprocess" flag')
            if metadata[0]['volatile']:
                return metadata[0]['uuid']
            else:
                return False
        return None

    def _download_raw_files(self,
                            upload_metadata: list,
                            temp_file: Optional[NamedTemporaryFile] = None) -> Tuple[Union[list, str], list]:
        if temp_file is not None:
            if len(upload_metadata) > 1:
                logging.warning('Several daily raw files')
            upload_metadata = [upload_metadata[0]]
        full_paths, uuids = self._storage_api.download_raw_files(upload_metadata,
                                                                 self.temp_dir.name)
        if temp_file is not None:
            shutil.move(full_paths[0], temp_file.name)
            full_paths = temp_file.name
        return full_paths, uuids

    def _check_raw_data_status(self, metadata: list) -> None:
        if not metadata:
            raise RawDataMissingError('No raw data')
        is_unprocessed_data = self._is_unprocessed_data(metadata)
        if not is_unprocessed_data and not self.is_reprocess:
            raise MiscError('Raw data already processed')

    @staticmethod
    def _is_unprocessed_data(metadata: list) -> bool:
        return any([row['status'] == 'uploaded' for row in metadata])

    def _get_payload(self,
                     instrument: Optional[str] = None,
                     product: Optional[str] = None,
                     model: Optional[str] = None,
                     skip_created: Optional[bool] = False) -> dict:
        payload = {
            'dateFrom': self.date_str,
            'dateTo': self.date_str,
            'site': self._site,
            'developer': True
        }
        if instrument is not None:
            payload['instrument'] = instrument
        if product is not None:
            payload['product'] = product
        if model is not None:
            payload['model'] = model
        if skip_created is True:
            payload['status[]'] = ['uploaded', 'processed']
        return payload

    def _update_statuses(self, uuids: list, status: Optional[str] = 'processed') -> None:
        for uuid in uuids:
            payload = {'uuid': uuid, 'status': status}
            self._md_api.post('upload-metadata', payload)

    def _get_product_key(self, identifier: str) -> str:
        return f"{self.date_str.replace('-', '')}_{self._site}_{identifier}.nc"

    def _is_new_version(self, uuid: Uuid) -> bool:
        return self.is_reprocess and uuid.volatile is False

    @staticmethod
    def _check_response_length(metadata: list) -> None:
        if len(metadata) > 1:
            logging.warning('API responded with several files')


def _read_site_info(args) -> tuple:
    site_info = utils.read_site_info(args.site)
    site_id = site_info['id']
    site_type = site_info['type']
    site_meta = {key: site_info[key] for key in ('latitude', 'longitude', 'altitude', 'name')}
    return site_meta, site_id, site_type


def add_default_arguments(parser):
    parser.add_argument('site', help='Site Name')
    return parser
