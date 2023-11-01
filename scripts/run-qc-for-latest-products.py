#!/usr/bin/env python3
import datetime
import subprocess

from data_processing import utils
from data_processing.metadata_api import MetadataApi


def main():
    config = utils.read_main_conf()
    md_api = MetadataApi(config)

    payload = {
        "updatedAtFrom": datetime.date.today() - datetime.timedelta(days=1),
    }
    metadata = md_api.get("api/files", payload)

    unique_sites = list(set([m["site"]["id"] for m in metadata]))
    for site in unique_sites:
        dates = [m["measurementDate"] for m in metadata if m["site"]["id"] == site]
        unique_dates = list(set(dates))
        for date in unique_dates:
            products = [
                m["product"]["id"]
                for m in metadata
                if m["site"]["id"] == site and m["measurementDate"] == date
            ]
            subprocess.check_call(
                [
                    "python3",
                    "scripts/wrapper.py",
                    "python3",
                    "scripts/cloudnet.py",
                    "-s",
                    site,
                    "-p",
                    ",".join(products),
                    "-d",
                    date,
                    "qc",
                    "-f",
                ]
            )


if __name__ == "__main__":
    main()
