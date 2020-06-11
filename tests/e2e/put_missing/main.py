#!/usr/bin/env python3
import subprocess
import os
import pytest
import argparse
from test_utils.utils import start_server

script_path = os.path.dirname(os.path.realpath(__file__))


def main():
    folder = 'tests/data/output_fixed'

    start_server(5000, 'tests/data/server/metadata', f'{script_path}/md.log')

    subprocess.check_call(['python3', 'scripts/put-missing-files.py', folder,
                           '--config-dir=tests/data/config'])

    pytest.main(['-v', 'tests/e2e/put_missing/tests.py',
                 '--input', folder])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cloudnet missing-file-put e2e test.')
    ARGS = parser.parse_args()
    main()
