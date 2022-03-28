#!/usr/bin/env python3
"""A script for creating Cloudnet quality control (QC) reports."""
import os
import glob
import logging
from tempfile import TemporaryDirectory
import requests
from requests.exceptions import HTTPError
from cloudnet_processing import utils
from cloudnet_processing import metadata_api
from cloudnet_processing.utils import read_main_conf
from cloudnet_processing.storage_api import StorageApi
from cloudnet_processing.processing_tools import ProcessBase


def main(args, storage_session=requests.session()):
    config = read_main_conf()
    md_api = metadata_api.MetadataApi(config, requests.session())
    storage_api = StorageApi(config, storage_session)
    metadata = md_api.find_product_metadata(args, legacy_files=False)
    temp_dir_root = utils.get_temp_dir(config)
    temp_dir = TemporaryDirectory(dir=temp_dir_root)
    qc = ProcessBase(args, config)
    for row in metadata:
        logging.info(f'Creating QC report: {row["site"]["id"]} - {row["measurementDate"]} - {row["product"]["id"]}')
        try:
            full_path = storage_api.download_product(row, temp_dir.name)
        except HTTPError as err:
            logging.error(err)
            continue
        try:
            qc.upload_quality_report(full_path, row['uuid'])
        except OSError as err:
            logging.error(err)
        for filename in glob.glob(f'{temp_dir.name}/*'):
            os.remove(filename)


def add_arguments(subparser):
    subparser.add_parser('qc', help='Create Quality Control reports.')
    return subparser
