#!/usr/bin/env python3
import argparse
from os import path
from tempfile import NamedTemporaryFile

import test_utils.utils as utils

SCRIPT_PATH = path.dirname(path.realpath(__file__))
temp_file = NamedTemporaryFile()

site = "bucharest"
date = "2020-10-22"
instrument = "model"
source_data = [
    (
        "eb176ca3-374e-471c-9c82-fc9a45578883",
        "tests/data/raw/model/20201022_bucharest_ecmwf.nc",
        "cloudnet-upload",
    ),
    (
        "80c2fab5-2dc5-4692-bafe-a7274071770e",
        "tests/data/raw/model/20201022_bucharest_gdas1.nc",
        "cloudnet-upload",
    ),
]


def main():
    utils.start_test_servers(instrument, SCRIPT_PATH)
    session = utils.register_storage_urls(
        temp_file, source_data, site, date, instrument, True, products=["ecmwf", "gdas1"]
    )

    main_args = [f"-s={site}", f"--date={date}", "-p=radar", "model"]
    utils.process(session, main_args, temp_file, SCRIPT_PATH, processing_mode="model")


if __name__ == "__main__":
    main()
