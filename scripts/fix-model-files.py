#!/usr/bin/env python3
import os
import glob
import shutil
import argparse
from tqdm import tqdm
from data_processing import metadata_api, fix_attributes
import data_processing.utils as process_utils


def main():

    config = process_utils.read_conf(ARGS)
    md_api = metadata_api.MetadataApi(config['main']['METADATASERVER']['url'])
    paths = _build_data_paths()

    print('Reading files...')
    files = _get_files(paths)

    print('Synchronizing directories:')
    _sync_dirs(files, paths, md_api)


def _build_data_paths():
    return {key: '/'.join((getattr(ARGS, key), ARGS.site[0], 'calibrated'))
            for key in ('input', 'output')}


def _get_files(paths):
    return glob.glob(f"{paths['input']}/**/{ARGS.model_type}/**/*.nc", recursive=True)


def _sync_dirs(files, paths, md_api):
    for file in tqdm(files):
        target_file = _create_target_name(file, paths)
        if not os.path.isfile(target_file):
            if ARGS.dry_run:
                print(file, target_file)
            else:
                _deliver_file(file, target_file, md_api)


def _create_target_name(file, paths):
    return file.replace(paths['input'], paths['output'])


def _deliver_file(source_file, target_file, md_api):
    os.makedirs(os.path.dirname(target_file), exist_ok=True)
    shutil.copyfile(source_file, target_file)
    uuid = fix_attributes(target_file, overwrite=False)
    if uuid and not ARGS.no_api:
        md_api.put(uuid, target_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fix model files for data portal')
    parser.add_argument('site', nargs='+', metavar='SITE', help='Site Name')
    parser.add_argument('--model-type', metavar='MODEL', dest='model_type',
                        help='Model type name. Default: ecmwf', default='ecmwf')
    parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    parser.add_argument('--input', metavar='/FOO/BAR',
                        help='Optional path to input directory (overrides config file value).')
    parser.add_argument('--output', metavar='/FOO/BAR',
                        help='Optional path to output directory (overrides config file value).')
    parser.add_argument('-d', '--dry', dest='dry_run', action='store_true',
                        help='Try the script without writing any files or calling API. Useful for testing.',
                        default=False)
    parser.add_argument('-na', '--no-api', dest='no_api', action='store_true',
                        help='Disable API calls. Useful for testing.',
                        default=False)

    ARGS = parser.parse_args()
    main()
