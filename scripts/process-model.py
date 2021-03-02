#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import argparse
import sys
import warnings
from tempfile import NamedTemporaryFile
from tempfile import TemporaryDirectory
from typing import Union
import requests
from cloudnetpy.utils import date_range
from requests.exceptions import HTTPError
from data_processing import nc_header_augmenter
from data_processing import utils
from data_processing.utils import MiscError, RawDataMissingError
from data_processing import processing_tools
from data_processing.processing_tools import Uuid, ProcessBase


warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)

temp_file = NamedTemporaryFile()
temp_dir = TemporaryDirectory()


def main(args, storage_session=requests.session()):
    args = _parse_args(args)
    config = utils.read_main_conf(args)
    start_date = utils.date_string_to_date(args.start)
    stop_date = utils.date_string_to_date(args.stop)
    process = ProcessModel(args, config, storage_session)
    models_to_process = process.get_models_to_process(args)

    for date in date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        process.date_str = date_str
        print(f'{args.site[0]} {date_str}')
        for model in models_to_process:
            print(f'  {model.ljust(20)}', end='\t')
            uuid = Uuid()
            try:
                uuid.volatile = process.check_product_status(model)
                uuid = process.process_model(uuid, model)
                process.upload_product_and_images(temp_file.name, uuid, model)
                process.print_info(uuid)
            except (RawDataMissingError, MiscError, HTTPError, ConnectionError) as err:
                print(err)
        processing_tools.clean_dir(temp_dir.name)


class ProcessModel(ProcessBase):
    def __init__(self, args, config: dict, storage_session):
        super().__init__(args, config, storage_session)
        self.plot_images = self.check_if_plot_images()

    def process_model(self, uuid: Uuid, model: str) -> Uuid:
        payload = self._get_payload(model=model)
        upload_metadata = self._md_api.get('upload-model-metadata', payload)
        full_path, uuid.raw = self._download_raw_files(upload_metadata, temp_dir, temp_file)
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

    def upload_product_and_images(self,
                                  full_path: str,
                                  uuid: Uuid,
                                  identifier: str) -> None:

        if self._is_new_version(uuid):
            self._pid_utils.add_pid_to_file(full_path)
        s3key = self._get_product_key(identifier)
        file_info = self._storage_api.upload_product(full_path, s3key)
        if self.plot_images:
            img_metadata = self._storage_api.create_and_upload_images(full_path, s3key,
                                                                      uuid.product, 'model')
        else:
            img_metadata = []
        payload = utils.create_product_put_payload(full_path, file_info, site=self._site)
        payload['model'] = identifier
        self._md_api.put(s3key, payload)
        for data in img_metadata:
            self._md_api.put_img(data, uuid.product)
        self._update_statuses(uuid.raw)

    def get_models_to_process(self, args) -> list:
        payload = {
            'site': self._site,
            'dateFrom': args.start,
            'dateTo': args.stop,
        }
        if not self.is_reprocess:
            payload['status'] = 'uploaded'
        metadata = self._md_api.get('upload-model-metadata', payload)
        model_ids = [row['model']['id'] for row in metadata]
        unique_models = list(set(model_ids))
        return unique_models

    def check_if_plot_images(self) -> bool:
        if 'hidden' in self._site_type:
            return False
        return True

    def check_product_status(self, model: str) -> Union[str, None, bool]:
        payload = self._get_payload(model=model)
        metadata = self._md_api.get(f'api/model-files', payload)
        return self._check_meta(metadata)


def _parse_args(args):
    parser = argparse.ArgumentParser(description='Process Cloudnet model data.')
    parser = processing_tools.add_default_arguments(parser)
    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
