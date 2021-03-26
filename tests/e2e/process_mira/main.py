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
        ('18641b51-ac8c-4a53-a1b9-54f41c045296', '20210127_0206.mmclx.gz'),
        ('2c3a10dc-ebab-4fd6-bbb3-f559eb64bc37', '20210127_0106.mmclx.gz'),
    ]
    for uuid, filename in raw_data:
        url = f'{mock_addr}cloudnet-upload/juelich/{uuid}/{filename}'
        adapter.register_uri('GET', url, body=open(f'tests/data/raw/mira/{filename}', 'rb'))
    # product file:
    url = f'{mock_addr}cloudnet-product-volatile/20210127_juelich_mira.nc'
    adapter.register_uri('PUT', url, additional_matcher=save_file, json={'size': 65, 'version': ''})
    # images:
    adapter.register_uri('PUT', re.compile(f'{mock_addr}cloudnet-img/(.*?)'))


def main():
    utils.start_server(5000, 'tests/data/server/metadata/process_mira', f'{SCRIPT_PATH}/md.log')
    utils.start_server(5001, 'tests/data/server/pid', f'{SCRIPT_PATH}/pid.log')
    register_storage_urls()

    main_args = ['juelich', f"--start=2021-01-27",
                 f"--stop=2021-01-28", '-p=radar', '-r']
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
