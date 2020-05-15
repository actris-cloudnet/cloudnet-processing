#!/usr/bin/env python3
import subprocess
import pytest
import os
from pathlib import Path
from test_utils.utils import remove_files, start_server, copy_files

script_path = os.path.dirname(os.path.realpath(__file__))


def main():
    test_output = 'tests/data/freeze'

    Path(test_output).mkdir(parents=True, exist_ok=True)
    remove_files(test_output)
    copy_files('tests/data/output_fixed', test_output)

    start_server(5000, 'tests/data/server/metadata', f'{script_path}/md.log')
    start_server(5001, 'tests/data/server/pid', f'{script_path}/pid.log')

    subprocess.check_call(['python3', 'scripts/freeze.py',
                           '--config-dir=tests/data/config'
                           ])

    pytest.main(['-v', 'tests/e2e/freeze/tests.py',
                 '--output', test_output])


if __name__ == "__main__":
    main()
