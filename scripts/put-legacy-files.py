#!/usr/bin/env python3
"""Script for creating and putting missing images into s3 / database."""
import argparse
import os
from pathlib import Path, PurePath
from typing import Generator
from tempfile import NamedTemporaryFile
import netCDF4
import requests
from requests import HTTPError
from data_processing.metadata_api import MetadataApi
from data_processing.storage_api import StorageApi
from data_processing.pid_utils import PidUtils
from data_processing import utils
from data_processing.nc_header_augmenter import fix_legacy_file
from data_processing.utils import MiscError
from cloudnetpy.plotting import generate_legacy_figure
import logging


def main():
    """The main function."""

    config = utils.read_main_conf()
    md_api = MetadataApi(config, requests.session())
    storage_api = StorageApi(config, requests.session())
    pid_utils = PidUtils(config)

    site = PurePath(ARGS.path[0]).name

    dir_names = [
        'processed/categorize/',
        'products/iwc-Z-T-method/',
        'products/lwc-scaled-adiabatic/',
        'products/drizzle/',
        'products/classification/'
    ]
    if ARGS.year is not None:
        dir_names = [f'{dir_name}{ARGS.year}' for dir_name in dir_names]

    for dir_name in dir_names:
        files = _get_files(ARGS.path[0], dir_name)

        for file in files:

            try:
                legacy_file = LegacyFile(file)
            except OSError as err:
                print(err)
                continue

            try:
                product = legacy_file.get_product_type()
                info = {
                    'date_str': legacy_file.get_date_str(),
                    'product': legacy_file.get_product_type(),
                    'identifier': legacy_file.get_identifier(),
                    'site': site
                }
                _check_if_exists(md_api, info)
            except (MiscError, HTTPError, ValueError) as err:
                print(err)
                continue
            finally:
                legacy_file.close()

            # FIX FILE
            s3key = _get_s3key(info)
            print(s3key)
            temp_file = NamedTemporaryFile()
            uuid = fix_legacy_file(file, temp_file.name)
            pid_utils.add_pid_to_file(temp_file.name)
            upload_info = storage_api.upload_product(temp_file.name, s3key)

            # IMAGES
            temp_img_file = NamedTemporaryFile(suffix='.png')
            visualizations = []
            fields, max_alt = utils.get_fields_for_plot(product)
            for field in fields:
                try:
                    generate_legacy_figure(temp_file.name, product, field, image_name=temp_img_file.name,
                                           max_y=max_alt, dpi=120)
                except (IndexError, ValueError, TypeError) as err:
                    logging.warning(err)
                    continue

                img_s3key = s3key.replace('.nc', f"-{uuid[:8]}-{field}.png")
                storage_api.upload_image(full_path=temp_img_file.name, s3key=img_s3key)
                img_meta = {'s3key': img_s3key, 'variable_id': utils.get_var_id(product, field)}
                visualizations.append(img_meta)

            payload = utils.create_product_put_payload(temp_file.name,
                                                       upload_info,
                                                       product=info['product'],
                                                       date_str=info['date_str'],
                                                       site=site)
            payload['legacy'] = True
            md_api.put('files', s3key, payload)
            md_api.put_images(visualizations, uuid)
            temp_file.close()
            temp_img_file.close()


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
        'developer': True,
        'showLegacy': True,
        'allVersions': True,
        'allModels': True
    }
    try:
        metadata = md_api.get('api/files', payload)
    except HTTPError as err:
        raise err

    if metadata:
        if metadata[0]['volatile']:
            raise MiscError(f'{info["site"]} {info["date_str"]} {info["product"]} Not allowed: '
                            f'volatile file exists')
        if metadata[-1]['legacy']:
            raise MiscError(f'{info["site"]} {info["date_str"]} {info["product"]} Already uploaded')


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
                             'in the subdirectories, e.g., /temp/data/davos')
    parser.add_argument('--year',
                        '-y',
                        type=int,
                        help='Year to be processed. Default is all years.',
                        default=None)

    ARGS = parser.parse_args()
    main()
