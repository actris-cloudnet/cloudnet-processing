#!/usr/bin/env python3
"""Script for putting non-standard Cloudnet files into s3 / database."""
import argparse
import logging
import os
from pathlib import Path, PurePath
from tempfile import NamedTemporaryFile

import cloudnetpy.utils
import netCDF4
import requests.exceptions
from requests import HTTPError

from data_processing import utils
from data_processing.metadata_api import MetadataApi
from data_processing.nc_header_augmenter import fix_legacy_file
from data_processing.pid_utils import PidUtils
from data_processing.processing_tools import ProcessBase
from data_processing.storage_api import StorageApi
from data_processing.utils import MiscError, make_session


def main():
    """The main function."""

    logging.basicConfig(level="INFO")

    config = utils.read_main_conf()
    md_api = MetadataApi(config, make_session())
    storage_api = StorageApi(config, make_session())
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
                uuid = _check_if_exists(md_api, info)
                if uuid is None:
                    uuid = _check_if_file_contains_uuid(file)
            except (MiscError, HTTPError, ValueError) as err:
                logging.error(err)
                continue

            # fix file
            s3key = _get_s3key(info)
            logging.info(s3key)
            legacy_file.temp_file = NamedTemporaryFile(suffix=legacy_file.filename)
            uuid = fix_legacy_file(file, legacy_file.temp_file.name, data={"uuid": uuid})
            try:
                legacy_file.compare_file_content(product)
            except MiscError as err:
                logging.info(err)
                continue
            if ARGS.freeze:
                pid_utils.add_pid_to_file(legacy_file.temp_file.name)
            utils.add_version_to_global_attributes(legacy_file.temp_file.name)
            upload_info = storage_api.upload_product(legacy_file.temp_file.name, s3key)
            payload = utils.create_product_put_payload(
                legacy_file.temp_file.name,
                upload_info,
                product=product,
                date_str=legacy_file.date_str,
                site=ARGS.site,
            )
            payload["legacy"] = True
            try:
                md_api.put("files", s3key, payload)
            except requests.exceptions.HTTPError as err:
                logging.info(err)
                continue

            # images
            legacy_file.create_and_upload_images(product, uuid, info["identifier"], legacy=True)
            legacy_file.upload_quality_report(legacy_file.temp_file.name, uuid)
            legacy_file.temp_file.close()


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


def _check_if_exists(md_api, info: dict) -> str | None:
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
        if len(metadata) == 1 and metadata[0]["volatile"] and not metadata[0]["legacy"]:
            raise MiscError(
                f'{info["site"]} {info["date_str"]} {info["product"]} Not allowed: '
                f"volatile non-legacy file exists"
            )
        if len(metadata) == 1 and metadata[0]["volatile"] and metadata[0]["legacy"]:
            # Replace volatile legacy file
            return metadata[0]["uuid"]
        if metadata[-1]["legacy"] and not metadata[-1]["volatile"]:
            raise MiscError(
                f'{info["site"]} {info["date_str"]} {info["product"]} Already uploaded and freezed.'
            )
    return None


def _get_s3key(info: dict) -> str:
    return f"legacy/{info['date_str'].replace('-', '')}_{info['site']}_{info['identifier']}.nc"


def _get_files(root_path: str, dir_name: str) -> list:
    full_path = os.path.join(root_path, dir_name)
    return [str(filename) for filename in Path(full_path).rglob("*.nc")]


def _check_if_file_contains_uuid(file: str) -> str | None:
    with netCDF4.Dataset(file) as nc:
        return getattr(nc, "file_uuid", None)


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
    parser.add_argument(
        "--freeze", "-f", help="Add pid to files.", default=False, action="store_true"
    )

    ARGS = parser.parse_args()
    main()
