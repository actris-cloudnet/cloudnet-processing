#!/usr/bin/env python3
from os import path
import argparse
import test_utils.utils as utils
from tempfile import NamedTemporaryFile

SCRIPT_PATH = path.dirname(path.realpath(__file__))
temp_file = NamedTemporaryFile()

site = 'hyytiala'
date = '2021-09-11'
identifier = 'cl61d_2'
source_data = [
    ('5d5aae0d-4351-4d40-a359-5e7ff0560a56', 'HYYlive_20210911_182940.nc'),
    ('14cf8fa5-7d0f-471a-b85d-6ed0ee0d8c6d', 'HYYlive_20210911_182740.nc'),
    ('be23a333-1a65-4d7d-99df-5f4134fb616e', 'HYYlive_20210911_182640.nc'),
    ('9c2a67e0-ecfb-4914-93bf-3b77ee95a06b', 'HYYlive_20210911_191440.nc'),
    ('da57a605-91dc-4dba-9be5-bbc8fe72fff5', '20210911_hyytiala_cl61d_daily.nc')
]


def main():
    utils.start_test_servers(identifier, SCRIPT_PATH)
    session = utils.register_storage_urls(temp_file, source_data, site, date, identifier, True)
    main_args = [site, f'-d={date}', '-p=lidar']
    utils.process(session, main_args, temp_file, SCRIPT_PATH)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f'Cloudnet {identifier} processing e2e test.')
    ARGS = parser.parse_args()
    main()
