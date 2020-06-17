#!/usr/bin/env python3
import subprocess
import os
import argparse
import pytest
from test_utils.utils import start_server, remove_dirs, remove_files

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))


def main():
    input_folder = 'tests/data/input'
    output_folder = 'tests/data/output'
    site = 'bucharest'
    start = '2020-04-02'
    stop = '2020-04-03'
    lidar_root = '/'.join((input_folder, site, 'uncalibrated', 'chm15k'))

    if ARGS.clean or not ARGS.skip_processing:
        remove_files('/'.join((lidar_root, start[:4])))
        remove_dirs('/'.join((output_folder, site)), 'calibrated')
        remove_dirs('/'.join((output_folder, site, 'calibrated')), 'ecmwf')

    if ARGS.clean:
        return

    if not ARGS.skip_processing:
        start_server(5000, 'tests/data/server/metadata', f'{SCRIPT_PATH}/md.log')

        subprocess.check_call(['python3', 'scripts/concat-lidar.py', f"{lidar_root}"])
        subprocess.check_call(['python3', 'scripts/process-cloudnet.py', site,
                               f"--config-dir=tests/data/config",
                               f"--start={start}",
                               f"--stop={stop}"])

    pytest.main(['-v', 'tests/e2e/process_cloudnet/tests.py',
                 '--site', site,
                 '--date', start,
                 '--input', input_folder,
                 '--output', output_folder])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cloudnet processing e2e test.')
    parser.add_argument('-s', '--skip_processing', dest='skip_processing', action='store_true',
                        help='Skip processing steps but run tests.', default=False)
    parser.add_argument('--clean', dest='clean', action='store_true',
                        help='Clean test data folders only.', default=False)
    ARGS = parser.parse_args()
    main()
