#!/usr/bin/env python3
"""A script for assigning PIDs for data files."""
import glob
import logging
import os
import sys
from tempfile import TemporaryDirectory
from uuid import UUID
import requests
from data_processing import metadata_api
from data_processing import utils
from data_processing.pid_utils import PidUtils
from data_processing.storage_api import StorageApi
from data_processing.utils import read_main_conf
from requests.exceptions import HTTPError


def main(args, storage_session=requests.session()):

    if args.site == 'all':
        args.site = None

    if args.start == utils.get_date_from_past(5) and args.stop == utils.get_date_from_past(-1):
        args.start = None
        args.stop = None

    config = read_main_conf()
    pid_utils = PidUtils(config)
    md_api = metadata_api.MetadataApi(config, requests.session())
    storage_api = StorageApi(config, storage_session)
    metadata = md_api.find_files_to_freeze(args)
    logging.info(f'Found {len(metadata)} files to freeze.')
    temp_dir = TemporaryDirectory()
    error = False
    for row in metadata:
        args.site = row['site']['id']  # Needed for slack alerts
        try:
            full_path = storage_api.download_product(row, temp_dir.name)
        except HTTPError as err:
            utils.send_slack_alert(err, 'pid', args, row['measurementDate'], row['product']['id'])
            continue
        s3key = row['filename']
        try:
            uuid, pid = pid_utils.add_pid_to_file(full_path)
            if UUID(uuid) != UUID(row['uuid']):
                logging.error(f"File {s3key} UUID mismatch (DB: {row['uuid']}, File: {uuid})")
                error = True
                continue
            logging.info(f'Mapping UUID "{uuid}" to PID "{pid}"...')
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
            utils.send_slack_alert(err, 'pid', args, row['measurementDate'], row['product']['id'])
        for filename in glob.glob(f'{temp_dir.name}/*'):
            os.remove(filename)
    if error is True:
        sys.exit(1)


def add_arguments(subparser):
    freeze_parser = subparser.add_parser('freeze', help='Freeze files.')
    freeze_parser.add_argument('-f', '--force',
                               action='store_true',
                               help='Ignore freeze after days configuration option. \
                               Allows freezing recently changed files.',
                               default=False)
    return subparser
