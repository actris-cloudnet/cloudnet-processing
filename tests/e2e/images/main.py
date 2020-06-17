#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path
import pytest
from test_utils.utils import remove_files, start_server, copy_files, remove_dir

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))


def main():
    output_folder = 'tests/data/images'

    Path(output_folder).mkdir(parents=True, exist_ok=True)
    remove_files(output_folder)
    copy_files('tests/data/output_fixed', output_folder)

    start_server(5000, 'tests/data/server/metadata', f'{SCRIPT_PATH}/md.log')

    subprocess.check_call(['python3', 'scripts/plot-quicklooks.py', output_folder,
                           '--start=2020-05-14',
                           '--config-dir=tests/data/config'
                           ])
    remove_dir(output_folder)

    pytest.main(['-v', 'tests/e2e/images/tests.py',
                 '--output', output_folder])


if __name__ == "__main__":
    main()
