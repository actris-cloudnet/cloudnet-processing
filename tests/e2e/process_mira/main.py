#!/usr/bin/env python3
import argparse
from os import path
from tempfile import NamedTemporaryFile

import test_utils.utils as utils

SCRIPT_PATH = path.dirname(path.realpath(__file__))
temp_file = NamedTemporaryFile()

site = "juelich"
date = "2021-01-27"
instrument = "mira"
raw_data = [
    ("18641b51-ac8c-4a53-a1b9-54f41c045296", "20210127_0206.mmclx.gz"),
    ("2c3a10dc-ebab-4fd6-bbb3-f559eb64bc37", "20210127_0106.mmclx.gz"),
]


def main():
    utils.start_test_servers(instrument, SCRIPT_PATH)
    session = utils.register_storage_urls(temp_file, raw_data, site, date, instrument, True)
    main_args = [f"-s={site}", f"--date={date}", "-p=radar", "process", "-r"]
    utils.process(session, main_args, temp_file, SCRIPT_PATH)


if __name__ == "__main__":
    main()
