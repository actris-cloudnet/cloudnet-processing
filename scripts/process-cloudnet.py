#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import os
import sys
import argparse
from typing import Tuple, Union
import importlib
import shutil
import subprocess
import cloudnetpy.utils
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.instruments import rpg2nc, ceilo2nc
import requests
from data_processing import utils
from data_processing.file_paths import FilePaths
from data_processing.metadata_api import MetadataApi
from data_processing.pid_utils import PidUtils
import sys

SERVER = 'http://localhost:3000/'
PRODUCTS = ('classification', 'iwc-Z-T-method', 'lwc-scaled-adiabatic', 'drizzle')


def main():
    """The main function."""

    config = utils.read_conf(ARGS)

    site_name = ARGS.site[0]
    site_info = utils.read_site_info(site_name)

    start_date = utils.date_string_to_date(ARGS.start)
    stop_date = utils.date_string_to_date(ARGS.stop)

    md_api = MetadataApi(config['main']['METADATASERVER']['url'])
    pid_utils = PidUtils(config['main']['PID-SERVICE'])

    for date in cloudnetpy.utils.date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        print(f'{site_name} {date_str}')

        upload_url = os.path.join(SERVER, 'metadata')
        payload = {'dateFrom': date_str,
                   'dateTo': date_str,
                   'site': site_name}
        uploaded_files = requests.get(upload_url, payload).json()

        print(len(uploaded_files))
        print('')


class CalibratedFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'Calibrated file missing'
        super().__init__(self.message)


class CategorizeFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'Categorize file missing'
        super().__init__(self.message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process Cloudnet data.')
    parser.add_argument('site', nargs='+', help='Site Name',
                        choices=['bucharest', 'norunda', 'granada', 'mace-head'])
    parser.add_argument('--config-dir', dest='config_dir', type=str, metavar='/FOO/BAR',
                        help='Path to directory containing config files. Default: ./config.',
                        default='./config')
    parser.add_argument('--start', type=str, metavar='YYYY-MM-DD',
                        help='Starting date. Default is current day - 7.',
                        default=utils.get_date_from_past(7))
    parser.add_argument('--stop', type=str, metavar='YYYY-MM-DD',
                        help='Stopping date. Default is current day - 1.',
                        default=utils.get_date_from_past(1))
    parser.add_argument('-na', '--no-api', dest='no_api', action='store_true',
                        help='Disable API calls. Useful for testing.', default=False)
    parser.add_argument('--new-version', dest='new_version', action='store_true',
                        help='Process new version.', default=False)
    parser.add_argument('--plot-quicklooks', dest='plot_quicklooks', action='store_true',
                        help='Also plot quicklooks.', default=False)
    ARGS = parser.parse_args()
    main()
