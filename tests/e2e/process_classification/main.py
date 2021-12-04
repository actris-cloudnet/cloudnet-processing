#!/usr/bin/env python3
from os import path
import argparse
import test_utils.utils as utils
from tempfile import NamedTemporaryFile

SCRIPT_PATH = path.dirname(path.realpath(__file__))
temp_file = NamedTemporaryFile()

site = 'bucharest'
date = '2020-10-22'
product = 'classification'
source_data = [('', '20201022_bucharest_categorize.nc')]


def main():
    utils.start_test_servers(product, SCRIPT_PATH)
    session = utils.register_storage_urls(temp_file, source_data, site, date, product, True)
    main_args = [f'-s={site}', f'--date={date}', '-p=classification', 'process']
    utils.process(session, main_args, temp_file, SCRIPT_PATH)


if __name__ == "__main__":
    main()
