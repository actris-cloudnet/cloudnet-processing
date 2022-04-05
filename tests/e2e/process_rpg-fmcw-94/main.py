#!/usr/bin/env python3
import argparse
from os import path
from tempfile import NamedTemporaryFile

import test_utils.utils as utils

SCRIPT_PATH = path.dirname(path.realpath(__file__))
temp_file = NamedTemporaryFile()

site = "bucharest"
date = "2020-10-22"
instrument = "rpg-fmcw-94"
source_data = [
    ("1bd2bd3b-45ae-455e-ab2b-627d9fde3a53", "201022_070003_P06_ZEN.LV1"),
    ("01425ad8-4acd-429b-b395-88723538e308", "201022_100001_P06_ZEN.LV1"),
    ("d5859d33-c7b0-4f1b-bf40-66a236be76c6", "201023_160000_P06_ZEN.LV1"),
]


def main():
    utils.start_test_servers(instrument, SCRIPT_PATH)
    session = utils.register_storage_urls(temp_file, source_data, site, date, instrument, True)
    main_args = [f"-s={site}", f"--date={date}", "-p=radar", "process"]
    utils.process(session, main_args, temp_file, SCRIPT_PATH)


if __name__ == "__main__":
    main()
