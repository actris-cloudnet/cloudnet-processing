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
        self.volatile: Union[str, None] = None


def clean_dir(dir_name: str) -> None:
    for filename in glob.glob(f'{dir_name}/*'):
        os.remove(filename)


class ProcessBase:
    def __init__(self,
                 args,
                 config: dict,
                 storage_session: Optional[requests.Session] = requests.Session(),
                 metadata_session: Optional[requests.Session] = requests.Session()):
        self.site_meta, self.site, self._site_type = _read_site_info(args)
        self.config = config
        self.is_reprocess = getattr(args, 'reprocess', False)
        self.date_str = None
        self.md_api = MetadataApi(config, metadata_session)
        self._storage_api = StorageApi(config, storage_session)
        self._pid_utils = PidUtils(config)
        self._create_new_version = False
        self.temp_dir_target = utils.get_temp_dir(config)
        self.temp_dir = TemporaryDirectory(dir=self.temp_dir_target)
        self.temp_file = NamedTemporaryFile(dir=self.temp_dir_target)

    def print_info(self) -> None:
        logging.info(f'Created: '
                     f'{"New version" if self._create_new_version is True else "Volatile file"}')

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
        payload = utils.create_product_put_payload(full_path, file_info, site=self.site)
        if product == 'model':
            del payload['cloudnetpyVersion']
            payload['model'] = model_or_product_id
        self.md_api.put('files', s3key, payload)
        self.md_api.put_images(img_metadata, uuid.product)
        if product in utils.get_product_types(level='1b'):
            self.update_statuses(uuid.raw)
        quality_report = utils.create_quality_report(full_path)
        self.md_api.put('quality', payload['uuid'], quality_report)

    def _read_volatile_uuid(self, metadata: list) -> Union[str, None]:
        if self._parse_volatile_value(metadata) is True:
            uuid = metadata[0]['uuid']
            assert isinstance(uuid, str) and len(uuid) > 0
            return uuid
        return None

    def _is_create_new_version(self, metadata) -> bool:
        if self._parse_volatile_value(metadata) is False:
            if self.is_reprocess is True:
                return True
            else:
                raise MiscError('Existing freezed file and no "reprocess" flag')
        return False

    def _parse_volatile_value(self, metadata: list) -> Union[bool, None]:
        self._check_response_length(metadata)
        if metadata:
            value = str(metadata[0]['volatile'])
            if value == 'True':
                return True
            elif value == 'False':
                return False
            else:
                raise RuntimeError(f'Unexpected value in metadata: {value}')
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
            raise RawDataMissingError
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
            'site': self.site,
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

    def update_statuses(self, uuids: list, status: Optional[str] = 'processed') -> None:
        for uuid in uuids:
            payload = {'uuid': uuid, 'status': status}
            self.md_api.post('upload-metadata', payload)

    def _get_product_key(self, identifier: str) -> str:
        return f"{self.date_str.replace('-', '')}_{self.site}_{identifier}.nc"

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
