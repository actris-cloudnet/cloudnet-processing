#!/usr/bin/env python3
"""Master script for Cloudnet model processing."""
import logging
import warnings

import requests
from requests.exceptions import HTTPError

from data_processing import nc_header_augmenter, utils
from data_processing.processing_tools import ProcessBase, Uuid
from data_processing.utils import MiscError, make_session

warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)


def main(args, storage_session: requests.Session | None):
    if storage_session is None:
        storage_session = make_session()
    config = utils.read_main_conf()
    process = ProcessModel(args, config, storage_session=storage_session)
    for row in process.get_uploaded_model_metadata():
        model_id = row["model"]["id"]
        process.date_str = row["measurementDate"]
        logging.info(f"{process.site}, {process.date_str}, {model_id}")
        uuid = Uuid()
        try:
            uuid.volatile = process.fetch_volatile_model_uuid(row)
            uuid = process.process_model(uuid, row)
            process.upload_product("model", uuid, model_id)
            process.create_and_upload_images("model", uuid.product, model_id)
            logging.info("Creating QC report")
            process.upload_quality_report(process.temp_file.name, uuid.product)
            process.print_info()
        except MiscError as err:
            logging.warning(err)
        except (HTTPError, ConnectionError, RuntimeError) as err:
            utils.send_slack_alert(err, "model", args, process.date_str, model_id)


class ProcessModel(ProcessBase):
    def get_uploaded_model_metadata(self) -> list:
        minimum_size = 20200
        payload = {"site": self.site, "status": "uploaded"}
        metadata = self.md_api.get("upload-model-metadata", payload)
        return [row for row in metadata if int(row["size"]) > minimum_size]

    def process_model(self, uuid: Uuid, metadata: dict) -> Uuid:
        full_path, uuid.raw, _ = self._download_raw_files([metadata], self.temp_file)
        data = {
            "site_name": self.site,
            "date": self.date_str,
            "uuid": uuid.volatile,
            "full_path": full_path,
            "model": metadata["model"]["id"],
            "instrument": None,
        }
        uuid.product = nc_header_augmenter.harmonize_model_file(data)
        return uuid

    def fetch_volatile_model_uuid(self, row: dict) -> str | None:
        payload = self._get_payload(model=row["model"]["id"])
        metadata = self.md_api.get("api/model-files", payload)
        try:
            uuid = self._read_stable_uuid(metadata)
            if uuid is not None:
                # We have new submission but stable file -> replace with volatile file
                logging.warning("Stable model file found. Changing to volatile and reprocess.")
                payload = {"volatile": True, "uuid": uuid}
                self.md_api.post("files", payload)
            else:
                uuid = self._read_volatile_uuid(metadata)
                self._create_new_version = self._is_create_new_version(metadata)
        except MiscError as err:
            self.update_statuses([row["uuid"]], status="invalid")
            msg = f'{err.message}: Setting status of {metadata[0]["filename"]} to "invalid"'
            raise MiscError(msg) from None
        return uuid


def add_arguments(subparser):
    subparser.add_parser("model", help="Process Cloudnet model data.")
    return subparser
