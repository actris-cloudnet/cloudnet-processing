#!/usr/bin/env python3
"""A script for assigning PIDs for data files."""
import argparse
import os.path as path
import sys

import requests
from netCDF4 import Dataset
from requests import HTTPError

from data_processing import metadata_api
from data_processing.utils import read_conf


def main():
    config = read_conf(ARGS)

    md_api = metadata_api.MetadataApi(config['main']['METADATASERVER']['url'])

    freeze_after = {k: int(v) for k, v in dict(config['main']['FREEZE_AFTER']).items()}
    stable_files = md_api.get_volatile_files_updated_before(**freeze_after)
    print(f'Found {len(stable_files)} files to freeze.')

    public_path = config['main']['PATH']['public']
    resolved_filepaths = [path.realpath(path.join(public_path, file)) for file in stable_files]

    for filepath in resolved_filepaths:
        try:
            rootgrp = Dataset(filepath, 'r+')
            uuid = getattr(rootgrp, 'file_uuid')
            payload = {
                'type': 'file',
                'uuid': uuid
            }
            res = requests.post(config['main']['PID-SERVICE']['url'], data=payload)

            res.raise_for_status()

            pid = res.json()['pid']
            rootgrp.pid = pid
            rootgrp.close()

            print(f'{uuid} => {pid}')

            if not ARGS.no_api:
                md_api.put(uuid, filepath, freeze=True)
        except HTTPError:
            print(f'PID service failed with status {res.status_code}:\n{res.json()["detail"]}', file=sys.stderr)
            break
        except OSError as e:
            print(f'Error: corrupted file in pid-freezing: {filepath}\n{e}', file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Freeze selected files.')
    parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    parser.add_argument('-na', '--no-api', dest='no_api', action='store_true',
                        help='Disable metadata API calls. Useful for testing.', default=False)
    ARGS = parser.parse_args()
    main()
