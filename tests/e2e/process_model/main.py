#!/usr/bin/env python3
from os import path
import argparse
import test_utils.utils as utils
from tempfile import NamedTemporaryFile

SCRIPT_PATH = path.dirname(path.realpath(__file__))
temp_file = NamedTemporaryFile()

site = 'bucharest'
date = '2020-10-22'
instrument = 'model'
source_data = [
    ('eb176ca3-374e-471c-9c82-fc9a45578883', '20201022_bucharest_ecmwf.nc'),
    ('80c2fab5-2dc5-4692-bafe-a7274071770e', '20201022_bucharest_gdas1.nc'),
]


def main():
    utils.start_test_servers(instrument, SCRIPT_PATH)
    session = utils.register_storage_urls(temp_file, source_data, site, date, instrument, True,
                                          products=['ecmwf', 'gdas1'])
    utils.process(session, [site], temp_file, SCRIPT_PATH, processing_mode='model')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f'Cloudnet {instrument} processing e2e test.')
    ARGS = parser.parse_args()
    main()
