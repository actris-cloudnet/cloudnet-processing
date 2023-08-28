#!/usr/bin/env python3
from os import path
from tempfile import NamedTemporaryFile

import test_utils.utils as utils

SCRIPT_PATH = path.dirname(path.realpath(__file__))

site = "bucharest"
date = "2020-10-22"
identifier = "model_evaluation"
source_data = [
    ("", "tests/data/products/20201022_bucharest_categorize.nc", "cloudnet-product"),
    ("", "tests/data/products/20201022_bucharest_ecmwf.nc", "cloudnet-product"),
]
products = ["l3-cf_downsampled_ecmwf"]


def main():
    utils.start_test_servers(identifier, SCRIPT_PATH)
    temp_file = NamedTemporaryFile()
    session = utils.register_storage_urls(
        temp_file,
        source_data,
        site,
        date,
        identifier,
        True,
        products,
        instrument_pid="",
    )
    main_args = [f"-s={site}", f"--date={date}", "-p=l3-cf", "me"]
    utils.process(
        session, main_args, temp_file, SCRIPT_PATH, processing_mode="model_evaluation"
    )


if __name__ == "__main__":
    main()
