#!/usr/bin/env python3
"""A script for assigning PIDs for data files."""
import os
import sys
import argparse
import requests
import glob
from tempfile import TemporaryDirectory
from data_processing.utils import read_main_conf
from data_processing import metadata_api
from data_processing.pid_utils import PidUtils
from data_processing.storage_api import StorageApi
from data_processing import utils


def main(args, storage_session=requests.session()):
    args = _parse_args(args)
    config = read_main_conf(args)
    pid_utils = PidUtils(config)
    md_api = metadata_api.MetadataApi(config)
    storage_api = StorageApi(config, storage_session)
    regular_files = md_api.find_volatile_regular_files_to_freeze()
    model_files = md_api.find_volatile_model_files_to_freeze()
    metadata = regular_files + model_files
    print(f'Found {len(metadata)} files to freeze.')
    temp_dir = TemporaryDirectory()
    for row in metadata:
        full_path = storage_api.download_product(row, temp_dir.name)
        s3key = row['filename']
        try:
            uuid, pid = pid_utils.add_pid_to_file(full_path)
            print(f'{uuid} => {pid}')
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
        except OSError as e:
            print(f'Error: corrupted file in pid-freezing: {full_path}\n{e}', file=sys.stderr)
        for filename in glob.glob(f'{temp_dir.name}/*'):
            os.remove(filename)


def _parse_args(args):
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('--config-dir',
                        dest='config_dir',
                        type=str,
                        metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    return parser.parse_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
