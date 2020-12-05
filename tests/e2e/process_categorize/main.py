#!/usr/bin/env python3
import subprocess
from os import path
import sys
import argparse
import test_utils.utils as utils
from tempfile import NamedTemporaryFile
import re
sys.path.append('scripts/')
process_cloudnet = __import__("process-cloudnet")

SCRIPT_PATH = path.dirname(path.realpath(__file__))
session, adapter, mock_addr = utils.init_test_session()
temp_file = NamedTemporaryFile()


def register_storage_urls():

    def save_product(request):
        with open(temp_file.name, mode='wb') as file:
            file.write(request.body.read())
        return True

    data_path = 'tests/data/products/'
    prod_path = f'{mock_addr}cloudnet-product/'
    volatile_prod_path = prod_path.replace('-product', '-product-volatile')
    prefix = '20201022_bucharest'
    filename = f'{prefix}_chm15k.nc'
    adapter.register_uri('GET', f'{prod_path}{filename}', body=open(f'{data_path}{filename}', 'rb'))
    filename = f'{prefix}_rpg-fmcw-94.nc'
    adapter.register_uri('GET', f'{prod_path}{filename}', body=open(f'{data_path}{filename}', 'rb'))
    filename = f'{prefix}_ecmwf.nc'
    adapter.register_uri('GET', f'{prod_path}{filename}', body=open(f'{data_path}{filename}', 'rb'))
    filename = f'{prefix}_categorize.nc'
    adapter.register_uri('PUT', f'{prod_path}{filename}', additional_matcher=save_product,
                         json={'size': 667, 'version': 'abc'})
    adapter.register_uri('PUT', f'{volatile_prod_path}{filename}', additional_matcher=save_product,
                         json={'size': 667, 'version': 'abc'})
    adapter.register_uri('PUT', re.compile(f'{mock_addr}cloudnet-img/(.*?)'))


def main():
    utils.start_server(5000, 'tests/data/server/metadata/process_categorize',
                       f'{SCRIPT_PATH}/md.log')
    utils.start_server(5001, 'tests/data/server/pid', f'{SCRIPT_PATH}/pid.log')
    register_storage_urls()

    # Processes new version of existing stable categorize file:

    args = ['bucharest', f"--config-dir=tests/data/config", f"--start=2020-10-22",
            f"--stop=2020-10-23", '-p=categorize', '-r']
    process_cloudnet.main(args, storage_session=session)
    subprocess.call(['pytest', '-v', f'{SCRIPT_PATH}/tests.py', '--full_path', temp_file.name])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cloudnet Categorize processing e2e test.')
    ARGS = parser.parse_args()
    main()
