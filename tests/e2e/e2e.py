#!venv/bin/python3
import subprocess
import os
import shutil
import pytest
import argparse


def _remove_dirs(target, keep=()):
    for item in os.listdir(target):
        if item not in keep:
            shutil.rmtree('/'.join((target, item)))


def _remove_files(target):
    for file in os.listdir(target):
        full_path = '/'.join((target, file))
        if os.path.isfile(full_path):
            os.remove(full_path)


def main():

    input_folder = 'tests/data/input'
    output_folder = 'tests/data/output'
    site = 'bucharest'
    start = '2020-04-02'
    stop = '2020-04-03'
    lidar_root = '/'.join((input_folder, site, 'uncalibrated', 'chm15k'))

    if ARGS.clean or not ARGS.skip_processing:
        _remove_files('/'.join((lidar_root, start[:4])))
        _remove_dirs('/'.join((output_folder, site)), 'calibrated')
        _remove_dirs('/'.join((output_folder, site, 'calibrated')), 'ecmwf')

    if ARGS.clean:
        return

    if not ARGS.skip_processing:
        subprocess.call(['python3', 'scripts/concat-lidar.py', f"{lidar_root}"])
        subprocess.call(['python3', 'scripts/process-cloudnet.py', site,
                         f"--input={input_folder}",
                         f"--output={output_folder}",
                         f"--start={start}",
                         f"--stop={stop}"])

    pytest.main(['-v', 'tests/e2e/tests.py',
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
