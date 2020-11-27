#!/usr/bin/env python3
"""A script for assigning PIDs for data files."""
import argparse
from tempfile import TemporaryDirectory
from data_processing.utils import read_main_conf
from data_processing import metadata_api
from data_processing.pid_utils import PidUtils
from data_processing.storage_api import StorageApi
from data_processing import utils
import sys


def main():
    config = read_main_conf(ARGS)
    md_api = metadata_api.MetadataApi(config)
    storage_api = StorageApi(config)
    metadata = md_api.find_volatile_files_to_freeze()
    print(f'Found {len(metadata)} files to freeze.')
    temp_dir = TemporaryDirectory()
    for row in metadata:
        full_path = storage_api.download_product(row, temp_dir.name)
        freeze_file(config, full_path, row['filename'])


def freeze_file(config, full_path: str, s3key: str) -> None:
    """Freeze file that is in cloudnet-product-volatile bucket."""
    md_api = metadata_api.MetadataApi(config)
    storage_api = StorageApi(config)
    pid_utils = PidUtils(config)
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
        md_api.post('files', payload)  # assumes the file was already in the dataportal database
        storage_api.delete_volatile_product(s3key)  # does not fail if the file did not exist
    except OSError as e:
        print(f'Error: corrupted file in pid-freezing: {full_path}\n{e}', file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Freeze selected files.')
    parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    ARGS = parser.parse_args()
    main()
