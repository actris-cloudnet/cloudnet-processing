#!/usr/bin/env python3
"""Master script for Cloudnet model processing."""
import argparse
import sys
import warnings
from tempfile import NamedTemporaryFile
from typing import Union
import requests
import logging
from requests.exceptions import HTTPError
from data_processing import nc_header_augmenter
from data_processing import utils
from data_processing.utils import MiscError
from data_processing import processing_tools
from data_processing.processing_tools import Uuid, ProcessBase


warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)

temp_file = NamedTemporaryFile()


def main(args, storage_session=requests.session()):
    args = _parse_args(args)
    utils.init_logger(args)
    config = utils.read_main_conf()
    process = ProcessModel(args, config, storage_session)
    for date_str, model in process.get_models_to_process(args):
        process.date_str = date_str
        logging.info(f'{args.site}, {date_str}, {model}')
        uuid = Uuid()
        try:
            uuid.volatile = process.check_product_status(model)
            uuid = process.process_model(uuid, model)
            process.upload_product_and_images(temp_file.name, 'model', uuid, model)
            process.print_info(uuid)
        except MiscError as err:
            logging.warning(err)
        except (HTTPError, ConnectionError, RuntimeError) as err:
            utils.send_slack_alert(err, 'model', args.site, date_str, model)


class ProcessModel(ProcessBase):

    def get_models_to_process(self, args) -> list:
        minimum_size = 20200
        payload = {
            'site': self._site,
            'status': 'uploaded'
        }
        if hasattr(args, 'start'):
            payload['dateFrom'] = args.start
        metadata = self._md_api.get('upload-model-metadata', payload)
        return [(row['measurementDate'], row['model']['id']) for row in metadata
                if int(row['size']) > minimum_size]

    def process_model(self, uuid: Uuid, model: str) -> Uuid:
        payload = self._get_payload(model=model, skip_created=True)
        upload_metadata = self._md_api.get('upload-model-metadata', payload)
        self._check_raw_data_status(upload_metadata)
        full_path, uuid.raw = self._download_raw_files(upload_metadata, temp_file)
        data = {
            'site_name': self._site,
            'date': self.date_str,
            'uuid': uuid.volatile,
            'full_path': full_path,
            'model': model,
            'instrument': None
            }
        uuid.product = nc_header_augmenter.harmonize_nc_file(data)
        return uuid

    def check_product_status(self, model: str) -> Union[str, None, bool]:
        payload = self._get_payload(model=model)
        metadata = self._md_api.get(f'api/model-files', payload)
        return self._check_meta(metadata)


def _parse_args(args):
    parser = argparse.ArgumentParser(description='Process Cloudnet model data.')
    parser = processing_tools.add_default_arguments(parser)
    parser.add_argument('--start',
                        type=str,
                        metavar='YYYY-MM-DD',
                        help='Starting date.')
    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
