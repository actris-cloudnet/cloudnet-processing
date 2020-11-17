#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import os
import argparse
from typing import Tuple, Union
import cloudnetpy.utils
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.instruments import rpg2nc, ceilo2nc
import requests
from data_processing import utils
from data_processing.metadata_api import MetadataApi
from data_processing.pid_utils import PidUtils
from data_processing import concat_lib
from tempfile import TemporaryDirectory
from tempfile import NamedTemporaryFile, TemporaryFile


PRODUCTS = ('classification', 'iwc-Z-T-method', 'lwc-scaled-adiabatic', 'drizzle')


def main():
    """The main function."""

    config = utils.read_main_conf(ARGS)

    site_name = ARGS.site[0]
    site_meta = utils.read_site_info(site_name)

    start_date = utils.date_string_to_date(ARGS.start)
    stop_date = utils.date_string_to_date(ARGS.stop)

    md_api = MetadataApi(config['METADATASERVER']['url'])
    pid_utils = PidUtils(config['PID-SERVICE'])

    for date in cloudnetpy.utils.date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        #print(f'{site_name} {date_str}')

        process = Process(site_meta, date_str, config, md_api)
        try:
            process.process_lidar()
        except RawLidarFileMissing:
            print('No raw lidar file for this day.')


class Process:
    def __init__(self, site_meta: dict, date_str: str, config: dict, md_api: MetadataApi):
        self.site_meta = site_meta
        self.date_str = date_str  # YYYY-MM-DD
        self.config = config
        self.md_api = md_api

    def process_model(self):
        pass

    def process_radar(self):
        pass

    def process_lidar(self):
        """Process Cloudnet lidar file."""
        try:
            raw_daily_file = NamedTemporaryFile(suffix='.nc')
            valid_checksums = self._concatenate_chm15k(raw_daily_file.name)
        except CHM15kFileMissing:
            try:
                raw_daily_file = NamedTemporaryFile(suffix='.DAT')
                valid_checksums = self._download_unique_daily_file('cl51', raw_daily_file.name)
            except UploadedFileMissing:
                raise RawLidarFileMissing

        lidar_file = NamedTemporaryFile()
        print('Creating lidar file...')
        ceilo2nc(raw_daily_file.name, lidar_file.name, site_meta=self.site_meta)
        self._update_statuses(valid_checksums)

    def _update_statuses(self, checksums):
        print('Updating statuses of processed raw files..')
        for checksum in checksums:
            self.md_api.change_status_from_uploaded_to_processed(checksum)

    def _concatenate_chm15k(self, raw_daily_file: str) -> list:
        """Concatenate several chm15k files into one file for certain site / date."""
        metadata = self.md_api.get_uploaded_metadata(self.site_meta['id'], self.date_str, 'chm15k')
        temp_dir = TemporaryDirectory()
        full_paths, checksums = self._download_files(metadata, temp_dir.name)
        if len(full_paths) == 0:
            raise CHM15kFileMissing
        print('Concatenating CHM15k files...')
        valid_full_paths = concat_lib.concat_chm15k_files(full_paths, self.date_str, raw_daily_file)
        return [checksum for checksum, full_path in zip(checksums, full_paths) if full_path in valid_full_paths]

    # Make storage-service class for these:

    def _download_files(self, metadata: list, dir_name: str) -> Tuple[list, list]:
        """From a list of upload-metadata, download files."""
        if metadata:
            print('Downloading multiple files from S3...')
        full_paths = []
        checksums = []
        for row in metadata:
            download_url = os.path.join(self.config['STORAGE-SERVICE']['url'], 'cloudnet-upload', row['s3Key'])
            res = requests.get(download_url)
            if res.status_code == 200:
                full_path = os.path.join(dir_name, row['filename'])
                with open(full_path, 'wb') as f:
                    f.write(res.content)
                full_paths.append(full_path)
                checksums.append(row['checksum'])
        return full_paths, checksums

    def _download_unique_daily_file(self, instrument: str, full_path: str) -> list:
        """Download single uploaded daily file for certain site / date / instrument."""
        metadata = self.md_api.get_uploaded_metadata(self.site_meta['id'], self.date_str, instrument)
        assert len(metadata) <= 1
        if len(metadata) == 0:
            raise UploadedFileMissing
        print('Downloading (unique) daily file from S3...')
        download_url = os.path.join(self.config['STORAGE-SERVICE']['url'], 'cloudnet-upload', metadata[0]['s3Key'])
        res = requests.get(download_url)
        with open(full_path, 'wb') as file:
            file.write(res.content)
        return [metadata[0]['checksum']]


class CalibratedFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'Calibrated file missing'
        super().__init__(self.message)


class UnCalibratedFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'Calibrated file missing'
        super().__init__(self.message)


class RawLidarFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'Raw lidar file missing'
        super().__init__(self.message)


class UploadedFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'Uploaded file missing'
        super().__init__(self.message)


class CHM15kFileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'CHM15k file(s) missing'
        super().__init__(self.message)


class CL51FileMissing(Exception):
    """Internal exception class."""
    def __init__(self):
        self.message = 'CL51 file missing'
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
