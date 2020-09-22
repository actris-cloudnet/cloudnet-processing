#!/usr/bin/env python3
"""A script for assigning PIDs for data files."""
import argparse
import os.path as path
from data_processing import pid_generator, metadata_api
from data_processing.utils import read_conf


def main():
    config = read_conf(ARGS)

    md_api = metadata_api.MetadataApi(config['main']['METADATASERVER']['url'])
    pid_gen = pid_generator.PidGenerator(config['main']['PID'])

    freeze_after = {k: int(v) for k, v in dict(config['main']['FREEZE_AFTER']).items()}
    stable_files = md_api.get_volatile_files_updated_before(**freeze_after)
    print(f'Found {len(stable_files)} files to freeze.')

    public_path = config['main']['PATH']['public']
    resolved_filepaths = [path.realpath(path.join(public_path, file)) for file in stable_files]

    for filepath in resolved_filepaths:

        pid, uuid = pid_generator.add_pid_to_file(pid_gen, filepath)

        print(f'{uuid} => {pid}')

        if not ARGS.no_api:
            md_api.put(uuid, filepath, freeze=True)

    del pid_gen


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Freeze selected files.')
    parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    parser.add_argument('-na', '--no-api', dest='no_api', action='store_true',
                        help='Disable metadata API calls. Useful for testing.', default=False)
    ARGS = parser.parse_args()
    main()
