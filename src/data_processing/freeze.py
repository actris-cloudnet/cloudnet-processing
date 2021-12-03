#!/usr/bin/env python3
"""A script for assigning PIDs for data files."""
import sys
import os
import requests
import glob
import logging
from tempfile import TemporaryDirectory
from data_processing.utils import read_main_conf
from data_processing import metadata_api
from data_processing.pid_utils import PidUtils
from data_processing.storage_api import StorageApi
from data_processing import utils
from requests.exceptions import HTTPError


def main(args, storage_session=requests.session()):
    utils.init_logger(args)
    config = read_main_conf()
    pid_utils = PidUtils(config)
    md_api = metadata_api.MetadataApi(config, requests.session())
    storage_api = StorageApi(config, storage_session)
    metadata = md_api.find_files_to_freeze(args)
    logging.info(f'Found {len(metadata)} files to freeze.')
    temp_dir = TemporaryDirectory()
    for row in metadata:
        try:
            full_path = storage_api.download_product(row, temp_dir.name)
        except HTTPError as err:
            utils.send_slack_alert(err, 'pid', row['site']['id'], row['measurementDate'],
                                   row['product']['id'])
            continue
        s3key = row['filename']
        try:
            uuid, pid = pid_utils.add_pid_to_file(full_path)
            logging.info(f'{uuid} => {pid}')
            response_data = storage_api.upload_product(full_path, s3key)
            payload = {
                'uuid': uuid,
                'checksum': utils.sha256sum(full_path),
                'volatile': False,
                'pid': pid,
                **response_data
            }
            md_api.post('files', payload)
            storage_api.delete_volatile_product(s3key)
        except OSError as err:
            utils.send_slack_alert(err, 'pid', row['site']['id'], row['measurementDate'],
                                   row['product']['id'])
        for filename in glob.glob(f'{temp_dir.name}/*'):
            os.remove(filename)


def add_arguments(subparser):
    freeze_parser = subparser.add_parser('freeze', help='Freeze files.')
    freeze_parser.add_argument('-f', '--force',
                               action='store_true',
                               help='Ignore freeze after days configuration option. \
                               Allows freezing recently changed files.',
                               default=False)
    return subparser


if __name__ == "__main__":
    main(sys.argv[1:])
