#!/usr/bin/env python3
"""Script for creating and putting missing images into s3 / database."""
import argparse
import os
from pathlib import Path
from data_processing.metadata_api import MetadataApi
from data_processing.storage_api import StorageApi
from data_processing import utils
from tempfile import NamedTemporaryFile
from data_processing.nc_header_augmenter import fix_legacy_file


ROOT = '/ibrix/arch/dmz/cloudnet/data/'
SERVER = 'http://localhost:3000'


def main():
    """The main function."""

    config = utils.read_main_conf(ARGS)
    md_api = MetadataApi(config)
    storage_api = StorageApi(config)

    site_metadata = md_api.get('api/sites', {'modelSites': True})
    sites = [site['id'] for site in site_metadata]

    for site in sites:

        dir_names = [
            'processed/categorize/',
            'products/iwc-Z-T-method/',
            'products/lwc-scaled-adiabatic/',
            'products/drizzle/',
            'products/classification/'
        ]
        for dir_name in dir_names:

            auth = (site, 'kissa')
            files = _get_files(site, dir_name)

            for file in files:
                temp_file = NamedTemporaryFile()
                date_str = fix_legacy_file(file, temp_file.name)


def _get_files(site: str, dir_name: str) -> list:
    full_path = os.path.join(ROOT, site, dir_name)
    files = Path(full_path).rglob('*.nc')
    files = [file for file in files]
    return files


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fix legacy files and upload to S3.')
    parser.add_argument('--config-dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')

    ARGS = parser.parse_args()
    main()
