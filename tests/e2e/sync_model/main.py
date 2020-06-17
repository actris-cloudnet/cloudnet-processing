#!/usr/bin/env python3
import subprocess
import os
import argparse
import pytest
from test_utils.utils import start_server, remove_dir

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))


def main():
    input_folder = 'tests/data/input'
    output_folder = 'tests/data/output_for_model_sync'
    site = 'bucharest'
    remove_dir(output_folder)
    os.makedirs(os.path.join(output_folder, site))

    start_server(5000, 'tests/data/server/metadata', f'{SCRIPT_PATH}/md.log')

    subprocess.check_call(['python3', 'scripts/sync-folders.py', site,
                           f'--input={input_folder}',
                           f'--output={output_folder}',
                           '--config-dir=tests/data/config'])

    pytest.main(['-v', 'tests/e2e/sync_model/tests.py',
                 '--site', site,
                 '--input', input_folder,
                 '--output', output_folder])

    remove_dir(output_folder)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cloudnet model file syncing e2e test.')
    ARGS = parser.parse_args()
    main()
