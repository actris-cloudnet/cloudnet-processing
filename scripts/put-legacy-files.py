#!/usr/bin/env python3
"""Script for creating and putting missing images into s3 / database."""
import argparse
import logging
import os
from pathlib import Path, PurePath
from tempfile import NamedTemporaryFile

import cloudnetpy.utils
import netCDF4
import requests
from requests import HTTPError

from cloudnet_processing import utils
from cloudnet_processing.metadata_api import MetadataApi
from cloudnet_processing.nc_header_augmenter import fix_legacy_file
from cloudnet_processing.pid_utils import PidUtils
from cloudnet_processing.processing_tools import ProcessBase
from cloudnet_processing.storage_api import StorageApi
from cloudnet_processing.utils import MiscError


def main():
    """The main function."""

    logging.basicConfig(level="INFO")

    config = utils.read_main_conf()
    md_api = MetadataApi(config, requests.session())
    storage_api = StorageApi(config, requests.session())
    pid_utils = PidUtils(config)
    ARGS.site = PurePath(ARGS.path[0]).name

    dir_names = [
        "processed/categorize/",
        "products/iwc-Z-T-method/",
        "products/lwc-scaled-adiabatic/",
        "products/drizzle/",
        "products/classification/",
    ]
    if ARGS.year is not None:
        dir_names = [f"{dir_name}{ARGS.year}" for dir_name in dir_names]

    for dir_name in dir_names:
        files = _get_files(ARGS.path[0], dir_name)
        for file in files:
            try:
                legacy_file = LegacyFile(ARGS, config, file)
            except OSError as err:
                logging.error(err)
                continue
            try:
                product = cloudnetpy.utils.get_file_type(file)
                info = {
                    "date_str": legacy_file.date_str,
                    "product": product,
                    "identifier": utils.get_product_identifier(product),
                    "site": ARGS.site,
                }
                _check_if_exists(md_api, info)
            except (MiscError, HTTPError, ValueError) as err:
                logging.error(err)
                continue

            # fix file
            s3key = _get_s3key(info)
            logging.info(s3key)
            temp_file = NamedTemporaryFile(suffix=legacy_file.filename)
            uuid = fix_legacy_file(file, temp_file.name)
            pid_utils.add_pid_to_file(temp_file.name)
            utils.add_version_to_global_attributes(temp_file.name)
            upload_info = storage_api.upload_product(temp_file.name, s3key)
            payload = utils.create_product_put_payload(
                temp_file.name,
                upload_info,
                product=product,
                date_str=legacy_file.date_str,
                site=ARGS.site,
            )
            payload["legacy"] = True
            md_api.put("files", s3key, payload)

            # images
            legacy_file.create_and_upload_images(
                temp_file.name, product, uuid, info["identifier"], legacy=True
            )
            temp_file.close()


class LegacyFile(ProcessBase):
    def __init__(self, args, config, full_path: str):
        super().__init__(args, config)
        self.full_path = full_path
        self.filename = os.path.basename(self.full_path)
        self.date_str = self.get_date_str()

    def get_date_str(self) -> str:
        year = self.filename[:4]
        month = self.filename[4:6]
        day = self.filename[6:8]
        nc = netCDF4.Dataset(self.full_path)
        if int(nc.year) != int(year) or int(nc.month) != int(month) or int(nc.day) != int(day):
            raise utils.MiscError("Not sure which date this is")
        nc.close()
        return f"{year}-{month}-{day}"


def _check_if_exists(md_api, info: dict):
    payload = {
        "site": info["site"],
        "dateFrom": info["date_str"],
        "dateTo": info["date_str"],
        "product": info["product"],
        "developer": True,
        "showLegacy": True,
        "allVersions": True,
        "allModels": True,
    }
    try:
        metadata = md_api.get("api/files", payload)
    except HTTPError as err:
        raise err

    if metadata:
        if metadata[0]["volatile"]:
            raise MiscError(
                f'{info["site"]} {info["date_str"]} {info["product"]} Not allowed: '
                f"volatile file exists"
            )
        if metadata[-1]["legacy"]:
            raise MiscError(f'{info["site"]} {info["date_str"]} {info["product"]} Already uploaded')


def _get_s3key(info: dict) -> str:
    return f"legacy/{info['date_str'].replace('-', '')}_{info['site']}_{info['identifier']}.nc"


def _get_files(root_path: str, dir_name: str) -> list:
    full_path = os.path.join(root_path, dir_name)
    return [str(filename) for filename in Path(full_path).rglob("*.nc")]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix legacy files and upload to S3.")
    parser.add_argument(
        "path",
        nargs="+",
        help="Path to the specific site directory containing legacy products "
        "in the subdirectories, e.g., /temp/data/davos",
    )
    parser.add_argument(
        "--year", "-y", type=int, help="Year to be processed. Default is all years.", default=None
    )

    ARGS = parser.parse_args()
    main()
