#!/usr/bin/env python3
from os import path
import argparse
import test_utils.utils as utils
from tempfile import NamedTemporaryFile

SCRIPT_PATH = path.dirname(path.realpath(__file__))
temp_file = NamedTemporaryFile()

site = 'bucharest'
date = '2020-10-22'
instrument = 'chm15k'
source_data = [
    ('27d8ac0d-3bab-45fe-9d85-1cc2528e9f95', '00100_A202010221900_CHM170137.nc'),
    ('80c2fab5-2dc5-4692-bafe-a7274071770e', '00100_A202010221205_CHM170137.nc'),
    ('d72d71af-a949-4094-aa14-73d1894c6aa5', '00100_A202010220835_CHM170137.nc'),
    ('ada7f659-68e8-45aa-b88d-e5cd54520052', '00100_A202010212350_CHM170137.nc')
]


def main():
    utils.start_test_servers(instrument, SCRIPT_PATH)
    session = utils.register_storage_urls(temp_file, source_data, site, date, instrument, False)
    main_args = [site, f'--date={date}', '-p=lidar']
    utils.process(session, main_args, temp_file, SCRIPT_PATH, 'first_run')  # Should fail
    utils.reset_log_file(SCRIPT_PATH)
    main_args += ['-r']
    utils.process(session, main_args, temp_file, SCRIPT_PATH, 'reprocess')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f'Cloudnet {instrument} processing e2e test.')
    ARGS = parser.parse_args()
    main()
