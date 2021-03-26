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
        ('1bd2bd3b-45ae-455e-ab2b-627d9fde3a53', '201022_070003_P06_ZEN.LV1'),
        ('01425ad8-4acd-429b-b395-88723538e308', '201022_100001_P06_ZEN.LV1'),
        ('d5859d33-c7b0-4f1b-bf40-66a236be76c6', '201023_160000_P06_ZEN.LV1')  # wrong file, should not process this
    ]
    for uuid, filename in raw_data:
        url = f'{mock_addr}cloudnet-upload/bucharest/{uuid}/{filename}'
        adapter.register_uri('GET', url, body=open(f'tests/data/raw/rpg-fmcw-94/{filename}', 'rb'))
    # product file:
    url = f'{mock_addr}cloudnet-product-volatile/20201022_bucharest_rpg-fmcw-94.nc'
    adapter.register_uri('PUT', url, additional_matcher=save_file, json={'size': 65, 'version': ''})
    # images:
    adapter.register_uri('PUT', re.compile(f'{mock_addr}cloudnet-img/(.*?)'))


def main():
    utils.start_server(5000, 'tests/data/server/metadata/process_rpg-fmcw-94', f'{SCRIPT_PATH}/md.log')
    utils.start_server(5001, 'tests/data/server/pid', f'{SCRIPT_PATH}/pid.log')
    register_storage_urls()

    main_args = ['bucharest', f"--start=2020-10-22",
                 f"--stop=2020-10-23", '-p=radar']
    std_args = utils.start_output_capturing()
    process_cloudnet.main(main_args, storage_session=session)
    output = utils.reset_output(*std_args)
    pytest_args = ['pytest', '-v', f'{SCRIPT_PATH}/tests.py', '--output', output,
                   '--full_path', temp_file.name]
    try:
        subprocess.check_call(pytest_args)
    except subprocess.CalledProcessError:
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cloudnet RPG radar processing e2e test.')
    ARGS = parser.parse_args()
    main()
