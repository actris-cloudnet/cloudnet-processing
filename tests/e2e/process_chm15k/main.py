#!/usr/bin/env python3
import subprocess
import os
from os import path
import sys
import re
import argparse
import test_utils.utils as utils
from tempfile import NamedTemporaryFile
sys.path.append('scripts/')
process_cloudnet = __import__("process-cloudnet")

SCRIPT_PATH = path.dirname(path.realpath(__file__))
session, adapter, mock_addr = utils.init_test_session()
temp_file = NamedTemporaryFile()


def register_storage_urls():

    def save_file(request):
        with open(temp_file.name, mode='wb') as file:
            file.write(request.body.read())
        return True

    raw_data = [
        ('27d8ac0d-3bab-45fe-9d85-1cc2528e9f95', '00100_A202010221900_CHM170137.nc'),
        ('80c2fab5-2dc5-4692-bafe-a7274071770e', '00100_A202010221205_CHM170137.nc'),
        ('d72d71af-a949-4094-aa14-73d1894c6aa5', '00100_A202010220835_CHM170137.nc'),
        ('ada7f659-68e8-45aa-b88d-e5cd54520052', '00100_A202010212350_CHM170137.nc')  # incorrect file, shoud not process this
    ]
    for uuid, filename in raw_data:
        url = f'{mock_addr}cloudnet-upload/bucharest/{uuid}/{filename}'
        adapter.register_uri('GET', url, body=open(f'tests/data/raw/chm15k/{filename}', 'rb'))
    # product file:
    url = f'{mock_addr}cloudnet-product/20201022_bucharest_chm15k.nc'
    adapter.register_uri('PUT', url, additional_matcher=save_file, json={'size': 65, 'version': ''})
    # images:
    adapter.register_uri('PUT', re.compile(f'{mock_addr}cloudnet-img/(.*?)'))


def main():
    utils.start_server(5000, 'tests/data/server/metadata/process_chm15k', f'{SCRIPT_PATH}/md.log')
    utils.start_server(5001, 'tests/data/server/pid', f'{SCRIPT_PATH}/pid.log')
    register_storage_urls()

    # This should fail because we have existing stable product:
    _process(extra_pytest_args=('-m', 'first_run'))

    # Processes new version:
    _process(extra_main_args=['-r'], extra_pytest_args=('-m', 'reprocess'))


def _process(extra_main_args=(), extra_pytest_args=()):
    main_args = ['bucharest', f"--config-dir=tests/data/config", f"--start=2020-10-22",
                 f"--stop=2020-10-23", '-p=lidar']
    std_args = utils.start_output_capturing()
    process_cloudnet.main(main_args + list(extra_main_args), storage_session=session)
    output = utils.reset_output(*std_args)
    pytest_args = ['pytest', '-v', f'{SCRIPT_PATH}/tests.py', '--output', output,
                   '--full_path', temp_file.name]
    subprocess.call(pytest_args + list(extra_pytest_args))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cloudnet Lidar product processing e2e test.')
    ARGS = parser.parse_args()
    main()
