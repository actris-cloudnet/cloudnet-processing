#!/usr/bin/env python3
from os import path
from tempfile import NamedTemporaryFile

import test_utils.utils as utils

SCRIPT_PATH = path.dirname(path.realpath(__file__))
temp_file = NamedTemporaryFile()

site = "bucharest"
date = "2020-10-22"
instrument = "chm15k"
source_data = [
    (
        "27d8ac0d-3bab-45fe-9d85-1cc2528e9f95",
        "tests/data/raw/chm15k/00100_A202010221900_CHM170137.nc",
        "cloudnet-upload",
    ),
    (
        "80c2fab5-2dc5-4692-bafe-a7274071770e",
        "tests/data/raw/chm15k/00100_A202010221205_CHM170137.nc",
        "cloudnet-upload",
    ),
    (
        "d72d71af-a949-4094-aa14-73d1894c6aa5",
        "tests/data/raw/chm15k/00100_A202010220835_CHM170137.nc",
        "cloudnet-upload",
    ),
    (
        "ada7f659-68e8-45aa-b88d-e5cd54520052",
        "tests/data/raw/chm15k/00100_A202010212350_CHM170137.nc",
        "cloudnet-upload",
    ),
    ("", "tests/data/products/20201022_bucharest_chm15k.nc", "cloudnet-product"),
]


def main():
    utils.start_test_servers(instrument, SCRIPT_PATH)
    session = utils.register_storage_urls(
        temp_file,
        source_data,
        site,
        date,
        instrument,
        False,
        instrument_pid="",
    )
    main_args = [f"-s={site}", f"--date={date}", "-p=lidar", "process"]
    utils.process(
        session, main_args, temp_file, SCRIPT_PATH, "first_run"
    )  # Should fail
    utils.reset_log_file(SCRIPT_PATH)
    main_args += ["-r"]
    utils.process(session, main_args, temp_file, SCRIPT_PATH, "reprocess")


if __name__ == "__main__":
    main()
