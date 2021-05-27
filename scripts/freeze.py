#!/usr/bin/env python3
"""A script for assigning PIDs for data files."""
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


def main(storage_session=requests.session()):
    utils.init_logger()
    config = read_main_conf()
    pid_utils = PidUtils(config)
    md_api = metadata_api.MetadataApi(config)
    storage_api = StorageApi(config, storage_session)
    regular_files = md_api.find_volatile_regular_files_to_freeze()
    model_files = md_api.find_volatile_model_files_to_freeze()
    metadata = regular_files + model_files
    logging.info(f'Found {len(metadata)} files to freeze.')
    temp_dir = TemporaryDirectory()
    for row in metadata:
        try:
            full_path = storage_api.download_product(row, temp_dir.name)
        except HTTPError as err:
            utils.send_slack_alert(config, row['site']['id'], row['measurementDate'],
                                   row['product']['id'], err, 'pid')
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
            utils.send_slack_alert(config, row['site']['id'], row['measurementDate'],
                                   row['product']['id'], err, 'pid')
        for filename in glob.glob(f'{temp_dir.name}/*'):
            os.remove(filename)


if __name__ == "__main__":
    main()
