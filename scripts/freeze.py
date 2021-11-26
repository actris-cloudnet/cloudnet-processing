#!/usr/bin/env python3
"""A script for assigning PIDs for data files."""
import sys
import os
import requests
import glob
import logging
import argparse
from tempfile import TemporaryDirectory
from data_processing.utils import read_main_conf
from data_processing import metadata_api
from data_processing.pid_utils import PidUtils
from data_processing.storage_api import StorageApi
from data_processing import utils
from requests.exceptions import HTTPError


def main(args, storage_session=requests.session()):
    args = _parse_args(args)
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


def _parse_args(args):
    parser = argparse.ArgumentParser(description='Freeze Cloudnet files')
    parser.add_argument('site', help='Site Name', type=str, default=None, nargs='?')
    parser.add_argument('-f', '--force',
                        action='store_true',
                        help='Override freeze after days configuration option.\
                        Use in conjunction with --start, --stop, or --date',
                        default=False)
    parser.add_argument('--start',
                        type=str,
                        metavar='YYYY-MM-DD',
                        help='Starting date. Freeze all dates by default.',
                        default=None)
    parser.add_argument('--stop',
                        type=str,
                        metavar='YYYY-MM-DD',
                        help='Stopping date. Freeze all dates by default.',
                        default=None)
    parser.add_argument('-d', '--date',
                        type=str,
                        metavar='YYYY-MM-DD',
                        help='Single date to be freezed. Freeze all dates by default.')
    parser.add_argument('-p', '--products',
                        help='Products to be freezed, e.g., radar,lidar,mwr,categorize,iwc \
                              By default freezes all products, including L3 products.',
                        type=lambda s: s.split(','),
                        default=None)
    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
