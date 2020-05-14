#!python3
from sys import argv
import argparse
from netCDF4 import Dataset
from operational_processing import generate_pid
from operational_processing import metadata_api
from operational_processing import utils as process_utils

md_api = metadata_api.MetadataApi({})
metadata_api.MetadataApi()

def main():
    config = process_utils.read_conf(ARGS)

    md_api = metadata_api.MetadataApi(config['main']['METADATASERVER']['url'])
    pid_gen = generate_pid.PidGenerator(config['main']['PID'])

    for filepath in ARGS.filepath:
        rootgrp = Dataset(filepath, 'r+')
        uuid = rootgrp.file_uuid

        pid = pid_gen.generate_pid(uuid)

        rootgrp.pid = pid
        rootgrp.close()

        print(f'{uuid} => {pid}')

        if not ARGS.no_api:
            md_api.put(uuid, filepath, freeze=True)

    del pid_gen


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Freeze selected files.')
    parser.add_argument('filepath', nargs='+', help='Paths to files to freeze.')
    parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    parser.add_argument('-na', '--no-api', dest='no_api', action='store_true',
                        help='Disable metadata API calls. Useful for testing.', default=False)
    ARGS = parser.parse_args()
    main()