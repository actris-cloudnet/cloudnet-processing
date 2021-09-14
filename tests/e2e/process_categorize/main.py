#!/usr/bin/env python3
from os import path
import argparse
import test_utils.utils as utils
from tempfile import NamedTemporaryFile

SCRIPT_PATH = path.dirname(path.realpath(__file__))
temp_file = NamedTemporaryFile()

site = 'bucharest'
date = '2020-10-22'
product = 'categorize'
source_data = [
    ('', '20201022_bucharest_chm15k.nc'),
    ('', '20201022_bucharest_rpg-fmcw-94.nc'),
    ('', '20201022_bucharest_ecmwf.nc')
]


def main():
    utils.start_test_servers(product, SCRIPT_PATH)
    session = utils.register_storage_urls(temp_file, source_data, site, date, product, False)
    main_args = [site, f'--date={date}', f'-p={product}', '-r']
    utils.process(session, main_args, temp_file, SCRIPT_PATH)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f'Cloudnet {product} processing e2e test.')
    ARGS = parser.parse_args()
    main()
