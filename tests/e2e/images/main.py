#!/usr/bin/env python3
import subprocess
import pytest
import os
from pathlib import Path
from test_utils.utils import remove_files, start_server, copy_files

script_path = os.path.dirname(os.path.realpath(__file__))


def main():
    test_output = 'tests/data/images'

    Path(test_output).mkdir(parents=True, exist_ok=True)
    remove_files(test_output)
    copy_files('tests/data/output_fixed', test_output)

    start_server(5000, 'tests/data/server/metadata', f'{script_path}/md.log')

    subprocess.check_call(['python3', 'scripts/plot-quicklooks.py', test_output,
                           '--start=2020-05-14',
                           '--config-dir=tests/data/config'
                           ])

    pytest.main(['-v', 'tests/e2e/images/tests.py',
                 '--output', test_output])


if __name__ == "__main__":
    main()
