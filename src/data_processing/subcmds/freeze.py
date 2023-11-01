#!/usr/bin/env python3
"""A script for assigning PIDs for data files."""
import glob
import logging
import os
import sys
from tempfile import TemporaryDirectory
from uuid import UUID

import requests
from requests.exceptions import HTTPError

from data_processing import metadata_api, utils
from data_processing.dvas import Dvas
from data_processing.pid_utils import PidUtils
from data_processing.storage_api import StorageApi
from data_processing.utils import MiscError, make_session, read_main_conf


def main(args, storage_session: requests.Session | None = None):
    if storage_session is None:
        storage_session = make_session()

    if args.site == "all":
        args.site = None

    if args.start == utils.get_date_from_past(
        5
    ) and args.stop == utils.get_date_from_past(-1):
        args.start = None
        args.stop = None

    config = read_main_conf()
    pid_utils = PidUtils(config)
    md_api = metadata_api.MetadataApi(config)
    storage_api = StorageApi(config, storage_session)
    metadata = md_api.find_files_to_freeze(args)
    logging.info(f"Found {len(metadata)} files to freeze.")
    temp_dir = TemporaryDirectory()
    dvas = Dvas()
    error = False
    for row in metadata:
        args.site = row["site"]["id"]  # Needed for slack alerts
        try:
            full_path = storage_api.download_product(row, temp_dir.name)
        except HTTPError as err:
            utils.send_slack_alert(
                err, "pid", args, row["measurementDate"], row["product"]["id"]
            )
            continue
        s3key = f"legacy/{row['filename']}" if row["legacy"] else row["filename"]
        try:
            uuid, pid, url = pid_utils.add_pid_to_file(full_path)
            if UUID(uuid) != UUID(row["uuid"]):
                logging.error(
                    f"File {s3key} UUID mismatch (DB: {row['uuid']}, File: {uuid})"
                )
                error = True
                continue
            logging.info(f'Minting PID "{pid}" to URL "{url}')
            response_data = storage_api.upload_product(full_path, s3key)
            payload = {
                "uuid": uuid,
                "checksum": utils.sha256sum(full_path),
                "volatile": False,
                "pid": pid,
                **response_data,
            }
            md_api.post("files", payload)
            storage_api.delete_volatile_product(s3key)
            if args.dvas and row["product"]["level"] == "2":
                row = md_api.get(f"api/files/{row['uuid']}")
                dvas.upload(md_api, row)
        except (OSError, MiscError) as err:
            utils.send_slack_alert(
                err, "pid", args, row["measurementDate"], row["product"]["id"]
            )
        for filename in glob.glob(f"{temp_dir.name}/*"):
            os.remove(filename)
    if error is True:
        sys.exit(1)


def add_arguments(subparser):
    freeze_parser = subparser.add_parser("freeze", help="Freeze files.")
    freeze_parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Ignore freeze after days configuration option. \
                               Allows freezing recently changed files.",
        default=False,
    )
    freeze_parser.add_argument(
        "-e",
        "--experimental",
        action="store_true",
        help="Include experimental products.",
        default=False,
    )
    freeze_parser.add_argument(
        "--dvas",
        action="store_true",
        help="Upload metadata to DVAS.",
        default=False,
    )
    return subparser
