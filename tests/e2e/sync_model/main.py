#!/usr/bin/env python3
import subprocess
import os
import shutil
import pytest
import argparse
from test_utils.utils import start_server

script_path = os.path.dirname(os.path.realpath(__file__))


def main():
    input_folder = 'tests/data/input'
    output_folder = 'tests/data/output_for_model_sync'
    site = 'bucharest'
    try:
        shutil.rmtree(output_folder)
    except FileNotFoundError:
        pass
    os.makedirs(os.path.join(output_folder, site))

    start_server(5000, 'tests/data/server/metadata', f'{script_path}/md.log')

    subprocess.check_call(['python3', 'scripts/sync-folders.py', site,
                           f'--input={input_folder}',
                           f'--output={output_folder}',
                           '--config-dir=tests/data/config'])

    pytest.main(['-v', 'tests/e2e/sync_model/tests.py',
                 '--site', site,
                 '--input', input_folder,
                 '--output', output_folder])

    shutil.rmtree(output_folder)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cloudnet model file syncing e2e test.')
    ARGS = parser.parse_args()
    main()
