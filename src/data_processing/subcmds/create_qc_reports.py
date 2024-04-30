#!/usr/bin/env python3
"""A script for creating Cloudnet quality control (QC) reports."""
import glob
import logging
import os
from tempfile import TemporaryDirectory

import cloudnetpy_qc.version
import requests
from requests.exceptions import HTTPError

from data_processing import metadata_api, utils
from data_processing.processing_tools import ProcessBase, build_file_landing_page_url
from data_processing.storage_api import StorageApi
from data_processing.utils import make_session, read_main_conf


def main(args, storage_session: requests.Session | None = None):
    if storage_session is None:
        storage_session = make_session()
    config = read_main_conf()
    md_api = metadata_api.MetadataApi(config)
    storage_api = StorageApi(config, storage_session)
    metadata = md_api.find_product_metadata(args, legacy_files=False)
    temp_dir_root = utils.get_temp_dir()
    temp_dir = TemporaryDirectory(dir=temp_dir_root)
    qc = ProcessBase(args, config)
    for row in metadata:
        product = row["product"]["id"]
        uuid = row["uuid"]
        qc_info = md_api.get_qc_version(uuid)
        if (
            not args.force
            and qc_info
            and qc_info["qc_version"] == cloudnetpy_qc.version.__version__
        ):
            logging.info("Same cloudnetpy-qc version, skipping.")
            continue
        try:
            full_path = storage_api.download_product(row, temp_dir.name)
        except HTTPError as err:
            logging.error(err)
            continue
        try:
            if row["legacy"]:
                result = qc.upload_quality_report(str(full_path), uuid, product)
            else:
                result = qc.upload_quality_report(str(full_path), uuid)
            url = build_file_landing_page_url(uuid)
            url = f"{url}/quality"
            logging.info(
                f'Creating QC report for {row["site"]["id"]} {row["measurementDate"]} {product}: {url} {result.upper()}'
            )

        except OSError as err:
            logging.error(err)
        for filename in glob.glob(f"{temp_dir.name}/*"):
            os.remove(filename)


def add_arguments(subparser):
    parser = subparser.add_parser("qc", help="Create Quality Control reports.")
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Force creation of the QC report.",
        default=False,
    )
    return subparser
