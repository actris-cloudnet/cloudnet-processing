#!/usr/bin/env python3
"""Master script for Cloudnet model processing."""
import logging
import warnings
from typing import Union
import requests
from data_processing import nc_header_augmenter
from data_processing import utils
from data_processing.processing_tools import Uuid, ProcessBase
from data_processing.utils import MiscError
from requests.exceptions import HTTPError

warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)


def main(args, storage_session=requests.session()):
    config = utils.read_main_conf()
    for site in args.sites:
        args.site = site
        process = ProcessModel(args, config, storage_session=storage_session)
        for date_str, model, raw_uuid in process.get_models_to_process():
            process.date_str = date_str
            logging.info(f'{args.site}, {date_str}, {model}')
            uuid = Uuid()
            try:
                uuid.volatile = process.fetch_volatile_model_uuid(model, raw_uuid)
                uuid = process.process_model(uuid, model)
                process.upload_product(process.temp_file.name, 'model', uuid, model)
                process.create_and_upload_images(process.temp_file.name, 'model', uuid.product, model)
                process.upload_quality_report(process.temp_file.name, uuid.product)
                process.print_info()
            except MiscError as err:
                logging.warning(err)
            except (HTTPError, ConnectionError, RuntimeError) as err:
                utils.send_slack_alert(err, 'model', args.site, date_str, model)


class ProcessModel(ProcessBase):

    def get_models_to_process(self) -> list:
        minimum_size = 20200
        payload = {
            'site': self.site,
            'status': 'uploaded'
        }
        metadata = self.md_api.get('upload-model-metadata', payload)
        return [(row['measurementDate'], row['model']['id'], row['uuid']) for row in metadata
                if int(row['size']) > minimum_size]

    def process_model(self, uuid: Uuid, model: str) -> Uuid:
        payload = self._get_payload(model=model, skip_created=True)
        upload_metadata = self.md_api.get('upload-model-metadata', payload)
        self._check_raw_data_status(upload_metadata)
        full_path, uuid.raw = self._download_raw_files(upload_metadata, self.temp_file)
        data = {
            'site_name': self.site,
            'date': self.date_str,
            'uuid': uuid.volatile,
            'full_path': full_path,
            'model': model,
            'instrument': None
            }
        uuid.product = nc_header_augmenter.harmonize_model_file(data)
        return uuid

    def fetch_volatile_model_uuid(self, model: str, raw_uuid: str) -> Union[str, None]:
        payload = self._get_payload(model=model)
        metadata = self.md_api.get('api/model-files', payload)
        try:
            uuid = self._read_stable_uuid(metadata)
            if uuid is not None:
                # We have new submission but stable file -> replace with volatile file
                logging.warning('Stable model file found. Changing to volatile and reprocess.')
                payload = {
                    'volatile': True,
                    'uuid': uuid
                }
                self.md_api.post('files', payload)
            else:
                uuid = self._read_volatile_uuid(metadata)
                self._create_new_version = self._is_create_new_version(metadata)
        except MiscError as err:
            self.update_statuses([raw_uuid], status='invalid')
            msg = f'{err.message}: Setting status of {metadata[0]["filename"]} to "invalid"'
            raise MiscError(msg) from None
        return uuid


def add_arguments(subparser):
    subparser.add_parser('model', help='Process Cloudnet model data.')
    return subparser