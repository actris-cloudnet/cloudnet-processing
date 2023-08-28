#!/usr/bin/env python3
from os import path
from tempfile import NamedTemporaryFile

import test_utils.utils as utils

SCRIPT_PATH = path.dirname(path.realpath(__file__))
temp_file = NamedTemporaryFile()

site = "hyytiala"
date = "2021-09-11"
instrument = "cl61d"
source_data = [
    (
        "5d5aae0d-4351-4d40-a359-5e7ff0560a56",
        "tests/data/raw/cl61d_2/HYYlive_20210911_182940.nc",
        "cloudnet-upload",
    ),
    (
        "14cf8fa5-7d0f-471a-b85d-6ed0ee0d8c6d",
        "tests/data/raw/cl61d_2/HYYlive_20210911_182740.nc",
        "cloudnet-upload",
    ),
    (
        "be23a333-1a65-4d7d-99df-5f4134fb616e",
        "tests/data/raw/cl61d_2/HYYlive_20210911_182640.nc",
        "cloudnet-upload",
    ),
    (
        "9c2a67e0-ecfb-4914-93bf-3b77ee95a06b",
        "tests/data/raw/cl61d_2/HYYlive_20210911_191440.nc",
        "cloudnet-upload",
    ),
]


def main():
    utils.start_test_servers(instrument, SCRIPT_PATH)
    session = utils.register_storage_urls(
        temp_file,
        source_data,
        site,
        date,
        instrument,
        True,
    )
    main_args = [f"-s={site}", f"-d={date}", "-p=lidar", "process"]
    utils.process(session, main_args, temp_file, SCRIPT_PATH)


if __name__ == "__main__":
    main()
