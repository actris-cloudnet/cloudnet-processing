#!/usr/bin/env python3
"""A script for creating static Cloudnet images."""
import os
import glob
import logging
from tempfile import TemporaryDirectory
import requests
from requests.exceptions import HTTPError
from data_processing import metadata_api
from data_processing import utils
from data_processing.utils import read_main_conf
from data_processing.storage_api import StorageApi
from data_processing.processing_tools import ProcessBase


def main(args, storage_session=requests.session()):
    config = read_main_conf()
    md_api = metadata_api.MetadataApi(config, requests.session())
    storage_api = StorageApi(config, storage_session)
    args.site = args.sites[0]
    metadata = md_api.find_files_for_plotting(args)
    temp_dir_root = utils.get_temp_dir(config)
    temp_dir = TemporaryDirectory(dir=temp_dir_root)
    img = Img(args, config)
    for row in metadata:
        img.date_str = row['measurementDate']
        product = row['product']['id']
        site = row['site']['id']
        logging.info(f'Plotting images for: {site} - {img.date_str} - {product}')
        try:
            full_path = storage_api.download_product(row, temp_dir.name)
        except HTTPError as err:
            utils.send_slack_alert(err, 'img', site, img.date_str, product)
            continue
        try:
            identifier = row['downloadUrl'].split('_')[-1][:-3]
            img.create_and_upload_images(full_path,
                                         product,
                                         row['uuid'],
                                         identifier,
                                         legacy=row.get('legacy', False))
        except OSError as err:
            utils.send_slack_alert(err, 'img', site, img.date_str, product)
        for filename in glob.glob(f'{temp_dir.name}/*'):
            os.remove(filename)


class Img(ProcessBase):
    pass


def add_arguments(subparser):
    subparser.add_parser('plot', help='Plot Cloudnet images.')
    return subparser