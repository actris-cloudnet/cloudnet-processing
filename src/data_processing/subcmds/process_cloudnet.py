#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import importlib
import logging
import warnings
from typing import List, Optional, Tuple, Union

import netCDF4
import requests
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.exceptions import (
    DisdrometerDataError,
    InconsistentDataError,
    ModelDataError,
    ValidTimeStampError,
)
from cloudnetpy.utils import date_range
from requests.exceptions import HTTPError

from data_processing import instrument_process, processing_tools, utils
from data_processing.processing_tools import ProcessBase, Uuid
from data_processing.utils import MiscError, RawDataMissingError, make_session

warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", RuntimeWarning)


def main(args, storage_session: Optional[requests.Session] = None):
    if storage_session is None:
        storage_session = make_session()
    config = utils.read_main_conf()
    start_date, stop_date = utils.get_processing_dates(args)
    process = ProcessCloudnet(args, config, storage_session=storage_session)
    for date in date_range(start_date, stop_date):
        date_str = date.strftime("%Y-%m-%d")
        process.date_str = date_str
        for product in args.products:
            processing_tools.clean_dir(process.temp_dir.name)
            process.init_temp_files()
            if product not in utils.get_product_types():
                raise ValueError("No such product")
            if product == "model":
                continue
            logging.info(f"Processing {product} product, {args.site} {date_str}")
            uuid = Uuid()
            try:
                uuid.volatile = process.fetch_volatile_uuid(product)
                if product in utils.get_product_types(level="2"):
                    uuid, identifier = process.process_level2(uuid, product)
                elif product == "categorize":
                    uuid, identifier = process.process_categorize(uuid)
                elif product in utils.get_product_types(level="1b"):
                    uuid, identifier, instrument_pids = process.process_instrument(uuid, product)
                    process.add_instrument_pid(process.temp_file.name, instrument_pids)
                else:
                    logging.info(f"Skipping product {product}")
                    continue
                process.add_pid(process.temp_file.name)
                utils.add_version_to_global_attributes(process.temp_file.name)
                process.upload_product(process.temp_file.name, product, uuid, identifier)
                process.create_and_upload_images(
                    process.temp_file.name, product, uuid.product, identifier
                )
                process.upload_quality_report(process.temp_file.name, uuid.product)
                process.print_info()
            except (RawDataMissingError, MiscError, NotImplementedError) as err:
                logging.warning(err)
            except (InconsistentDataError, DisdrometerDataError, ValidTimeStampError) as err:
                logging.error(err)
            except (HTTPError, ConnectionError, RuntimeError, ValueError) as err:
                utils.send_slack_alert(err, "data", args, date_str, product)


class ProcessCloudnet(ProcessBase):
    def process_instrument(self, uuid: Uuid, instrument_type: str):
        instrument = self._detect_uploaded_instrument(instrument_type)
        process_class = getattr(instrument_process, f"Process{instrument_type.capitalize()}")
        process = process_class(self, self.temp_file, uuid)
        getattr(process, f'process_{instrument.replace("-", "_")}')()
        instrument = (
            "halo-doppler-lidar" if instrument == "halo-doppler-lidar-calibrated" else instrument
        )
        return process.uuid, instrument, process.instrument_pids

    def process_categorize(self, uuid: Uuid) -> Tuple[Uuid, str]:
        l1_products = utils.get_product_types(level="1b")
        l1_products.remove("disdrometer")  # Not yet used
        meta_records = self._get_level1b_metadata_for_categorize(l1_products)
        missing = self._get_missing_level1b_products(meta_records, l1_products)
        if missing:
            raise MiscError(f'Missing required input files: {", ".join(missing)}')
        self._check_source_status("categorize", meta_records)
        input_files = {key: "" for key in l1_products}
        for product, metadata in meta_records.items():
            input_files[product] = self._storage_api.download_product(metadata, self.temp_dir.name)
        if not input_files["mwr"] and "rpg-fmcw-94" in input_files["radar"]:
            input_files["mwr"] = input_files["radar"]
        try:
            uuid.product = generate_categorize(input_files, self.temp_file.name, uuid=uuid.volatile)
        except ModelDataError:
            payload = self._get_payload(model="gdas1")
            metadata = self.md_api.get("api/model-files", payload)
            if len(metadata) == 1:
                input_files["model"] = self._storage_api.download_product(
                    metadata[0], self.temp_dir.name
                )
                uuid.product = generate_categorize(
                    input_files, self.temp_file.name, uuid=uuid.volatile
                )
            else:
                raise MiscError("Bad ecmwf model data and no gdas1")
        return uuid, "categorize"

    def _get_level1b_metadata_for_categorize(self, source_products: list) -> dict:
        meta_records = {}
        for product in source_products:
            if product == "model":
                payload = self._get_payload()
                metadata = self.md_api.get("api/model-files", payload)
            else:
                payload = self._get_payload(product=product)
                metadata = self.md_api.get("api/files", payload)
            self._check_response_length(metadata)
            if metadata:
                meta_records[product] = metadata[0]
        return meta_records

    @staticmethod
    def _get_missing_level1b_products(meta_records: dict, required_products: list) -> list:
        existing_products = list(meta_records.keys())
        if "mwr" not in meta_records and (
            "radar" in meta_records and "rpg-fmcw" in meta_records["radar"]["filename"]
        ):
            existing_products.append("mwr")
        return [product for product in required_products if product not in existing_products]

    def process_level2(self, uuid: Uuid, product: str) -> Tuple[Uuid, str]:
        payload = self._get_payload(product="categorize")
        metadata = self.md_api.get("api/files", payload)
        self._check_response_length(metadata)
        if metadata:
            categorize_file = self._storage_api.download_product(metadata[0], self.temp_dir.name)
            meta_record = {"categorize": metadata[0]}
        else:
            raise MiscError("Missing input categorize file")
        self._check_source_status(product, meta_record)
        module = importlib.import_module(f"cloudnetpy.products.{product}")
        fun = getattr(module, f"generate_{product}")
        uuid.product = fun(categorize_file, self.temp_file.name, uuid=uuid.volatile)
        identifier = utils.get_product_identifier(product)
        return uuid, identifier

    def add_pid(self, full_path: str) -> None:
        if self._create_new_version:
            self._pid_utils.add_pid_to_file(full_path)

    def download_instrument(
        self,
        instrument: str,
        include_pattern: Optional[str] = None,
        largest_only: bool = False,
        exclude_pattern: Optional[str] = None,
    ) -> Tuple[Union[List, str], List, List]:
        """Download raw files for given instrument."""
        payload = self._get_payload(instrument=instrument, skip_created=True)
        upload_metadata = self.md_api.get("upload-metadata", payload)
        if include_pattern is not None:
            upload_metadata = utils.include_records_with_pattern_in_filename(
                upload_metadata, include_pattern
            )
        if exclude_pattern is not None:
            upload_metadata = utils.exclude_records_with_pattern_in_filename(
                upload_metadata, exclude_pattern
            )
        arg = self.temp_file if largest_only else None
        self._check_raw_data_status(upload_metadata)
        return self._download_raw_files(upload_metadata, arg)

    def download_uploaded(
        self, instrument: str, exclude_pattern: Optional[str]
    ) -> Tuple[Union[list, str], List, List]:
        """Download self-generated daily files (e.g. CL61-D)."""
        payload = self._get_payload(instrument=instrument)
        payload["status"] = "uploaded"
        upload_metadata = self.md_api.get("upload-metadata", payload)
        if exclude_pattern is not None:
            upload_metadata = utils.exclude_records_with_pattern_in_filename(
                upload_metadata, exclude_pattern
            )
        return self._download_raw_files(upload_metadata)

    def download_adjoining_daily_files(
        self, instrument: str
    ) -> Tuple[Union[List, str], List, List]:
        next_day = utils.get_date_from_past(-1, self.date_str)
        payload = self._get_payload(instrument=instrument, skip_created=True)
        payload["dateFrom"] = self.date_str
        payload["dateTo"] = next_day
        upload_metadata = self.md_api.get("upload-metadata", payload)
        upload_metadata = utils.order_metadata(upload_metadata)
        if not upload_metadata:
            raise RawDataMissingError
        if not self._is_unprocessed_data(upload_metadata) and not (
            self.is_reprocess or self.is_reprocess_volatile
        ):
            raise MiscError("Raw data already processed")
        full_paths, _, instrument_pids = self._download_raw_files(upload_metadata)
        # Return all full paths but only current day UUIDs
        uuids_of_current_day = [
            meta["uuid"] for meta in upload_metadata if meta["measurementDate"] == self.date_str
        ]
        return full_paths, uuids_of_current_day, instrument_pids

    def _detect_uploaded_instrument(self, instrument_type: str) -> str:
        instrument_metadata = self.md_api.get("api/instruments")
        possible_instruments = {
            x["id"] for x in instrument_metadata if x["type"] == instrument_type
        }
        payload = self._get_payload()
        upload_metadata = self.md_api.get("upload-metadata", payload)
        uploaded_instruments = {x["instrument"]["id"] for x in upload_metadata}
        instrument = list(possible_instruments & uploaded_instruments)
        if len(instrument) == 0:
            raise RawDataMissingError
        selected_instrument = instrument[0]
        if len(instrument) > 1:
            # First choose the preferred instrument
            preferred = ("rpg-fmcw-94", "mira", "chm15k", "chm15kx", "chm15x")
            for instru in instrument:
                if instru in preferred:
                    selected_instrument = instru
                    break
            # If something already processed we must use it
            payload = self._get_payload(product=instrument_type)
            product_metadata = self.md_api.get("api/files", payload)
            for instru in instrument:
                if product_metadata and instru in product_metadata[0]["filename"]:
                    selected_instrument = instru
                    break
            logging.warning(
                f"More than one type of {instrument_type} data, " f"using {selected_instrument}"
            )
        return selected_instrument

    @staticmethod
    def add_instrument_pid(full_path: str, instrument_pids: list) -> None:
        if len(set(instrument_pids)) > 1:
            logging.error("Several instrument PIDs found")
        if instrument_pids and (instrument_pid := instrument_pids[0]) is not None:
            nc = netCDF4.Dataset(full_path, "r+")
            nc.instrument_pid = instrument_pid
            nc.close()


def add_arguments(subparser):
    parser = subparser.add_parser("process", help="Process Cloudnet Level 1 and 2 data.")
    parser.add_argument(
        "-r",
        "--reprocess",
        action="store_true",
        help="Process a) new version of the stable files, b) reprocess volatile "
        "files, c) create volatile file from unprocessed files.",
        default=False,
    )
    parser.add_argument(
        "--reprocess_volatile",
        action="store_true",
        help="Reprocess unprocessed and volatile files only.",
        default=False,
    )
    return subparser
