#!/usr/bin/env python3
import argparse
from os import path
from tempfile import NamedTemporaryFile

import test_utils.utils as utils

SCRIPT_PATH = path.dirname(path.realpath(__file__))
temp_file = NamedTemporaryFile()

site = "lindenberg"
date = "2021-09-15"
instrument = "thies-lnm"
source_data = [
    ("737df008-b8f3-4798-bbe6-12168bf0f843", "2021091510.txt"),
    ("8db08368-f92c-44ab-af25-37189d15f4da", "2021091509.txt"),
]


def main():
    utils.start_test_servers(instrument, SCRIPT_PATH)
    session = utils.register_storage_urls(temp_file, source_data, site, date, instrument, True)
    main_args = [f"-s={site}", f"--date={date}", "-p=disdrometer", "process"]
    utils.process(session, main_args, temp_file, SCRIPT_PATH)


if __name__ == "__main__":
    main()
