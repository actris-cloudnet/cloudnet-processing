#!/usr/bin/env python3
"""A script for creating Cloudnet quality control (QC) reports."""
import glob
import logging
import os
from tempfile import TemporaryDirectory
from typing import Optional

import cloudnetpy_qc.version
import requests
from requests.exceptions import HTTPError

from data_processing import metadata_api, utils
from data_processing.processing_tools import ProcessBase
from data_processing.storage_api import StorageApi
from data_processing.utils import make_session, read_main_conf


def main(args, storage_session: Optional[requests.Session] = None):
    if storage_session is None:
        storage_session = make_session()
    config = read_main_conf()
    md_api = metadata_api.MetadataApi(config, make_session())
    storage_api = StorageApi(config, storage_session)
    metadata = md_api.find_product_metadata(args, legacy_files=False)
    temp_dir_root = utils.get_temp_dir(config)
    temp_dir = TemporaryDirectory(dir=temp_dir_root)
    qc = ProcessBase(args, config)
    for row in metadata:
        product = row["product"]["id"]
        uuid = row["uuid"]
        qc_info = md_api.get_qc_version(uuid)
        if not args.force and qc_info and qc_info["qc_version"] == cloudnetpy_qc.version.__version__:
            logging.info("Same cloudnetpy-qc version, skipping.")
            continue
        logging.info(
            f'Creating QC report: {row["site"]["id"]} - {row["measurementDate"]} - {product}'
        )
        try:
            full_path = storage_api.download_product(row, temp_dir.name)
        except HTTPError as err:
            logging.error(err)
            continue
        try:
            if row["legacy"]:
                qc.upload_quality_report(full_path, uuid, product)
            else:
                qc.upload_quality_report(full_path, uuid)
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
