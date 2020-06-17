#!/usr/bin/env python3
import glob
import argparse
import netCDF4
from data_processing import metadata_api
import data_processing.utils as process_utils


def main():

    config = process_utils.read_conf(ARGS)
    md_api = metadata_api.MetadataApi(config['main']['METADATASERVER']['url'])

    print('Reading files...')
    files = _get_files()

    for file in files:
        uuid = _read_uuid(file)
        if uuid and not md_api.exists(uuid):
            print(f'Putting: {file} - {uuid}')
            md_api.put(uuid, file)


def _get_files():
    return glob.glob(f"{ARGS.data_path[0]}/**/*.nc", recursive=True)


def _read_uuid(file):
    nc = netCDF4.Dataset(file)
    uuid = getattr(nc, 'file_uuid', None)
    nc.close()
    return uuid


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Put missing files to database.')
    parser.add_argument('data_path', nargs='+', metavar='/PATH/TO',
                        help='Path to files to be checked.')
    parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')

    ARGS = parser.parse_args()
    main()
