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
    metadata = md_api.find_product_metadata(args)
    temp_dir_root = utils.get_temp_dir(config)
    temp_dir = TemporaryDirectory(dir=temp_dir_root)
    img = Img(args, config)

    if args.missing:
        logging.info('Finding files without images..')
        payload = {
            'site': img.site,
            'showLegacy': True,
        }
        img_metadata = img.md_api.get(f'api/visualizations/', payload=payload)
        source_file_uuids = [x['sourceFileId'] for x in img_metadata]
        metadata = [row for row in metadata if row['uuid'] not in source_file_uuids]
        if metadata:
            logging.info(f'Plotting images for {len(metadata)} files..')
        else:
            logging.info('Images exist for all products')

    for row in metadata:
        assert img.site == args.site == row['site']['id']
        img.date_str = row['measurementDate']
        product = row['product']['id']
        logging.info(f'Plotting images for: {img.site} - {img.date_str} - {product}')
        try:
            full_path = storage_api.download_product(row, temp_dir.name)
        except HTTPError as err:
            utils.send_slack_alert(err, 'img', args, img.date_str, product)
            continue
        try:
            identifier = row['downloadUrl'].split('_')[-1][:-3]
            img.create_and_upload_images(full_path,
                                         product,
                                         row['uuid'],
                                         identifier,
                                         legacy=row.get('legacy', False))
        except OSError as err:
            utils.send_slack_alert(err, 'img', args, img.date_str, product)
        for filename in glob.glob(f'{temp_dir.name}/*'):
            os.remove(filename)


class Img(ProcessBase):
    pass


def add_arguments(subparser):
    parser = subparser.add_parser('plot', help='Plot Cloudnet images.')
    parser.add_argument('-m', '--missing',
                        action='store_true',
                        help='Plot missing images only.',
                        default=False)
    return subparser
