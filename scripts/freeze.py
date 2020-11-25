#!/usr/bin/env python3
"""A script for assigning PIDs for data files."""
import argparse
import sys
from tempfile import TemporaryDirectory
from data_processing import metadata_api
from data_processing.pid_utils import PidUtils
from data_processing.storage_api import StorageApi
from data_processing.utils import read_main_conf
from data_processing import utils


def main():
    config = read_main_conf(ARGS)

    md_api = metadata_api.MetadataApi(config['METADATASERVER']['url'])
    storage_api = StorageApi(config)
    pid_utils = PidUtils(config['PID-SERVICE'])

    freeze_after = {k: int(v) for k, v in dict(config['FREEZE_AFTER']).items()}
    s3keys = md_api.get_volatile_files_updated_before(**freeze_after)
    print(f'Found {len(s3keys)} files to freeze.')

    temp_dir = TemporaryDirectory()
    full_paths = storage_api.download_products(s3keys, temp_dir.name, volatile=True)

    for full_path, s3key in zip(full_paths, s3keys):
        try:
            uuid, pid = pid_utils.add_pid_to_file(full_path)
            print(f'{uuid} => {pid}')

            data = storage_api.upload_product(full_path, s3key, volatile=False)

            payload = {
                'uuid': uuid,
                'checksum': utils.sha256sum(full_path),
                'volatile': False,
                'pid': pid,
                **data
            }
            md_api.post('files', payload)

            storage_api.delete_volatile_product(s3key)

        except OSError as e:
            print(f'Error: corrupted file in pid-freezing: {full_path}\n{e}', file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Freeze selected files.')
    parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    ARGS = parser.parse_args()
    main()
