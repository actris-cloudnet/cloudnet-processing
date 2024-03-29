#!/usr/bin/env python3
from os import path
from tempfile import NamedTemporaryFile

import test_utils.utils as utils

SCRIPT_PATH = path.dirname(path.realpath(__file__))

site = "bucharest"
date = "2020-10-22"
identifier = "classification_2"
source_data = [
    ("", "tests/data/products/20201022_bucharest_categorize.nc", "cloudnet-product"),
    (
        "",
        "tests/data/products/20201022_bucharest_classification.nc",
        "cloudnet-product",
    ),
]


def main():
    utils.start_test_servers(identifier, SCRIPT_PATH)
    temp_file = NamedTemporaryFile()
    session = utils.register_storage_urls(
        temp_file, source_data, site, date, identifier, True, instrument_pid=""
    )
    main_args = [f"-s={site}", f"--date={date}", "-p=classification", "process"]
    utils.process(session, main_args, temp_file, SCRIPT_PATH, "first_run")
    utils.reset_log_file(SCRIPT_PATH)
    temp_file = NamedTemporaryFile()
    session = utils.register_storage_urls(
        temp_file, source_data, site, date, identifier, False, instrument_pid=""
    )
    main_args += ["-r"]
    utils.process(session, main_args, temp_file, SCRIPT_PATH, "reprocess")


if __name__ == "__main__":
    main()
