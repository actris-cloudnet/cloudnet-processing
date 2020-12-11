#!/usr/bin/env python3
"""Script for creating and putting missing images into s3 / database."""
import argparse
import os
from pathlib import Path, PurePath
from typing import Generator
from tempfile import NamedTemporaryFile
import netCDF4
from requests import HTTPError
from data_processing.metadata_api import MetadataApi
from data_processing.storage_api import StorageApi
from data_processing.pid_utils import PidUtils
from data_processing import utils
from data_processing.nc_header_augmenter import fix_legacy_file
from data_processing.utils import MiscError


def main():
    """The main function."""

    config = utils.read_main_conf(ARGS)
    md_api = MetadataApi(config)
    storage_api = StorageApi(config)
    pid_utils = PidUtils(config)

    site = PurePath(ARGS.path[0]).name

    dir_names = [
        'processed/categorize/',
        'products/iwc-Z-T-method/',
        'products/lwc-scaled-adiabatic/',
        'products/drizzle/',
        'products/classification/'
    ]
    for dir_name in dir_names:
        files = _get_files(ARGS.path[0], dir_name)

        for file in files:
            legacy_file = LegacyFile(file)

            try:
                info = {
                    'date_str': legacy_file.get_date_str(),
                    'product': legacy_file.get_product_type(),
                    'identifier': legacy_file.get_identifier(),
                    'site': site
                }
                _check_if_exists(md_api, info)
            except (MiscError, HTTPError) as err:
                print(err)
                continue
            finally:
                legacy_file.close()

            s3key = _get_s3key(info)
            print(s3key)

            temp_file = NamedTemporaryFile()
            uuid = fix_legacy_file(file, temp_file.name)

            pid_utils.add_pid_to_file(temp_file.name)
            upload_info = storage_api.upload_product(temp_file.name, s3key)
            img_metadata = storage_api.create_and_upload_images(temp_file.name, s3key, uuid,
                                                                info['product'], legacy=True)
            payload = utils.create_product_put_payload(temp_file.name, upload_info)
            payload['legacy'] = True
            md_api.put(s3key, payload)
            for data in img_metadata:
                md_api.put_img(data, uuid)


class LegacyFile:
    def __init__(self, full_path: str):
        self.filename = os.path.basename(full_path)
        self.nc = netCDF4.Dataset(full_path)

    def get_date_str(self) -> str:
        year = self.filename[:4]
        month = self.filename[4:6]
        day = self.filename[6:8]
        if (int(self.nc.year) != int(year) or int(self.nc.month) != int(month)
                or int(self.nc.day) != int(day)):
            raise utils.MiscError('Not sure which date this is')
        return f'{year}-{month}-{day}'

    def get_product_type(self):
        if 'iwc-Z-T-method' in self.filename:
            return 'iwc'
        if 'lwc-scaled-adiabatic' in self.filename:
            return 'lwc'
        for file_type in ('drizzle', 'classification', 'categorize'):
            if file_type in self.filename:
                return file_type
        raise utils.MiscError('Undetected legacy file')

    def get_identifier(self):
        product = self.get_product_type()
        return utils.get_product_identifier(product)

    def close(self):
        self.nc.close()


def _check_if_exists(md_api, info: dict):
    payload = {
        'site': info['site'],
        'dateFrom': info['date_str'],
        'dateTo': info['date_str'],
        'product': info['product'],
        'showLegacy': True
    }
    try:
        metadata = md_api.get('api/files', payload)
    except HTTPError as err:
        raise err
    if metadata:
        raise MiscError(f'{info["site"]} {info["date_str"]} {info["product"]} already uploaded')


def _get_s3key(info: dict) -> str:
    return f"legacy/{info['date_str'].replace('-', '')}_{info['site']}_{info['identifier']}.nc"


def _get_files(root_path: str, dir_name: str) -> Generator:
    full_path = os.path.join(root_path, dir_name)
    return Path(full_path).rglob('*.nc')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fix legacy files and upload to S3.')
    parser.add_argument('path',
                        nargs='+',
                        help='Path to the specific site directory containing legacy products '
                             'in the subdirectories, e.g., /ibrix/arch/dmz/cloudnet/data/arm-sgp')
    parser.add_argument('--config-dir',
                        type=str,
                        metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    ARGS = parser.parse_args()
    main()
