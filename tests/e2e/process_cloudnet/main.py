#!/usr/bin/env python3
import subprocess
import os
from os import path
import argparse
import pytest
import netCDF4
from test_utils.utils import start_server, remove_dirs, remove_files, remove_dir

SCRIPT_PATH = path.dirname(path.realpath(__file__))


def _clean_dirs(lidar_root, output_folder, site):
    remove_files(path.join(lidar_root, '2020'))
    remove_dirs(path.join(output_folder, site, 'calibrated'), keep='ecmwf')
    remove_dir(path.join(output_folder, site, 'processed'))
    remove_dir(path.join(output_folder, site, 'products'))


def _freeze_files(output_folder):
    for root, dirs, files in os.walk(output_folder):
        for file in files:
            if file.endswith(".nc") and 'ecmwf' not in file:
                filename = os.path.abspath(os.path.join(root, file))
                root_grp = netCDF4.Dataset(filename, 'r+')
                root_grp.pid = "www.cloudnet.com"
                root_grp.close()


def main():
    input_folder = 'tests/data/input'
    output_folder = 'tests/data/output'
    site = 'bucharest'
    start = '2020-04-02'
    stop = '2020-04-03'

    #start_server(5000, 'tests/data/server/metadata', f'{SCRIPT_PATH}/md.log')
    #start_server(5001, 'tests/data/server/pid', f'{SCRIPT_PATH}/pid.log')

    #process_cmd = ['python3', '-W', 'ignore', 'scripts/process-cloudnet.py', site,
    #               f"--config-dir=tests/data/config", f"--start={start}", f"--stop={stop}"]

    #test_cmd = ['-v', 'tests/e2e/process_cloudnet/tests.py', '--site', site, '--date', start,
    #            '--input', input_folder, '--output', output_folder]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cloudnet processing e2e test.')
    ARGS = parser.parse_args()
    main()
