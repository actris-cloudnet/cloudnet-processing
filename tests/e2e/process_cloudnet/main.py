#!/usr/bin/env python3
import subprocess
from os import path
import argparse
import pytest
from test_utils.utils import start_server, remove_dirs, remove_files

SCRIPT_PATH = path.dirname(path.realpath(__file__))


def main():
    input_folder = 'tests/data/input'
    output_folder = 'tests/data/output'
    site = 'bucharest'
    start = '2020-04-02'
    stop = '2020-04-03'
    lidar_root = path.join(input_folder, site, 'uncalibrated', 'chm15k')

    remove_files(path.join(lidar_root, start[:4]))
    remove_dirs(path.join(output_folder, site), 'calibrated')
    remove_dirs(path.join(output_folder, site, 'calibrated'), 'ecmwf')

    start_server(5000, 'tests/data/server/metadata', f'{SCRIPT_PATH}/md.log')

    subprocess.check_call(['python3', 'scripts/concat-lidar.py', f"{lidar_root}"])

    process_cmd = ['python3', '-W', 'ignore', 'scripts/process-cloudnet.py', site,
                   f"--config-dir=tests/data/config", f"--start={start}", f"--stop={stop}"]

    test_cmd = ['-v', 'tests/e2e/process_cloudnet/tests.py', '--site', site, '--date', start,
                '--input', input_folder, '--output', output_folder]

    subprocess.check_call(process_cmd)
    pytest.main(test_cmd + ['-m', 'first_run'])

    subprocess.check_call(process_cmd)
    pytest.main(test_cmd + ['-m', 'append_data'])

    subprocess.check_call(process_cmd + ['--new-version'])
    pytest.main(test_cmd + ['-m', 'new_version'])

    subprocess.check_call(process_cmd)
    pytest.main(test_cmd + ['-m', 'append_fail'])

    remove_files(f'{lidar_root}/2020')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cloudnet processing e2e test.')
    ARGS = parser.parse_args()
    main()
