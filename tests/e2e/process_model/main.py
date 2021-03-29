#!/usr/bin/env python3
import subprocess
from os import path
import sys
import argparse
import test_utils.utils as utils
from tempfile import NamedTemporaryFile
import re
sys.path.append('scripts/')
process_cloudnet = __import__("process-model")

SCRIPT_PATH = path.dirname(path.realpath(__file__))
session, adapter, mock_addr = utils.init_test_session()


def register_storage_urls(temp_file):

    def save_product(request):
        with open(temp_file.name, mode='wb') as f:
            f.write(request.body.read())
        return True

    raw_data = [
        ('eb176ca3-374e-471c-9c82-fc9a45578883', '20201022_bucharest_ecmwf.nc'),
        ('80c2fab5-2dc5-4692-bafe-a7274071770e', '20201022_bucharest_gdas1.nc'),
    ]
    for uuid, filename in raw_data:
        url = f'{mock_addr}cloudnet-upload/bucharest/{uuid}/{filename}'
        adapter.register_uri('GET', url, body=open(f'tests/data/raw/model/{filename}', 'rb'))

    product_files = ('20201022_bucharest_ecmwf.nc', '20201022_bucharest_gdas1.nc')
    for file in product_files:
        url = f'{mock_addr}cloudnet-product-volatile/{file}'
        adapter.register_uri('PUT', url, additional_matcher=save_product,
                             json={'size': 667, 'version': 'abc'})
    # images:
    adapter.register_uri('PUT', re.compile(f'{mock_addr}cloudnet-img/(.*?)'))


def main():
    utils.start_server(5000, 'tests/data/server/metadata/process_model', f'{SCRIPT_PATH}/md.log')
    _process()


def _process(main_extra_args=()):
    with open(f'{SCRIPT_PATH}/md.log', 'w'):
        pass
    args = ['bucharest']
    temp_file = NamedTemporaryFile()
    register_storage_urls(temp_file)
    std_args = utils.start_output_capturing()
    process_cloudnet.main(args + list(main_extra_args), storage_session=session)
    output = utils.reset_output(*std_args)
    try:
        subprocess.check_call(['pytest', '-v', f'{SCRIPT_PATH}/tests.py', '--output', output,
                               '--full_path', temp_file.name])
    except subprocess.CalledProcessError:
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cloudnet Classification processing e2e test.')
    ARGS = parser.parse_args()
    main()
