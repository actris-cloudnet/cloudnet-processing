#!/usr/bin/env python3
from os import path
import argparse
import test_utils.utils as utils
from tempfile import NamedTemporaryFile

SCRIPT_PATH = path.dirname(path.realpath(__file__))

site = 'bucharest'
date = '2020-10-22'
identifier = 'model_evaluation'
source_data = [('', '20201022_bucharest_categorize.nc'),
               ('', '20201022_bucharest_ecmwf.nc')]
products = ['l3-cf_downsampled_ecmwf']


def main():
    utils.start_test_servers(identifier, SCRIPT_PATH)
    temp_file = NamedTemporaryFile()
    session = utils.register_storage_urls(temp_file, source_data, site, date, identifier, True, products)
    main_args = [site, f'--date={date}', '-p=l3-cf']
    utils.process(session, main_args, temp_file, SCRIPT_PATH, processing_mode='model_evaluation')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f'Cloudnet {identifier} processing e2e test.')
    ARGS = parser.parse_args()
    main()