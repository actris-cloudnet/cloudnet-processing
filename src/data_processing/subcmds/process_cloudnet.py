#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import datetime
import importlib
import logging
import warnings
from tempfile import TemporaryDirectory

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


def main(args, storage_session: requests.Session | None = None):
    if storage_session is None:
        storage_session = make_session()
    config = utils.read_main_conf()
    process = ProcessCloudnet(args, config, storage_session=storage_session)
    if args.updated_since:
        process.process_raw_data_using_updated_at()
    else:
        _start_date, _stop_date = utils.get_processing_dates(args)
        start_date = utils.isodate2date(_start_date)
        stop_date = utils.isodate2date(_stop_date)
        for date in date_range(start_date, stop_date):
            process.date_str = date.strftime("%Y-%m-%d")
            for product in args.products:
                process.process_product(product)


class ProcessCloudnet(ProcessBase):
    def process_product(self, product: str):
        processing_tools.clean_dir(self.temp_dir.name)
        self.init_temp_files()
        if product == "model":
            return
        logging.info(f"Processing {product} product, {self.site} {self.date_str}")
        uuid = Uuid()
        try:
            uuid.volatile = self.fetch_volatile_uuid(product)
            match product:
                case product if product in utils.get_product_types(level="2"):
                    uuid, identifier = self.process_level2(uuid, product)
                case "categorize":
                    uuid, identifier = self.process_categorize(uuid)
                case product if product in utils.get_product_types(level="1b"):
                    uuid, identifier, instrument_pids = self.process_instrument(uuid, product)
                    self.add_instrument_pid(instrument_pids)
                case bad_product:
                    raise ValueError(f"Bad product: {bad_product}")
            self.compare_file_content(product)
            self.add_pid()
            utils.add_version_to_global_attributes(self.temp_file.name)
            self.upload_product(product, uuid, identifier)
            self.create_and_upload_images(product, uuid.product, identifier)
            self.upload_quality_report(self.temp_file.name, uuid.product)
            self.print_info()
        except (RawDataMissingError, MiscError, NotImplementedError) as err:
            logging.warning(err)
        except (InconsistentDataError, DisdrometerDataError, ValidTimeStampError) as err:
            logging.error(err)
        except (HTTPError, ConnectionError, RuntimeError, ValueError) as err:
            utils.send_slack_alert(err, "data", self.args, self.date_str, product)

    def process_raw_data_using_updated_at(self):
        metadata = self._get_uploaded_raw_metadata()
        for row in metadata:
            self.date_str = row["date"]
            self.process_product(row["product"])

    def _get_uploaded_raw_metadata(self) -> list:
        updated_at_from = datetime.date.today() - datetime.timedelta(days=self.args.updated_since)
        payload = {"updatedAtFrom": updated_at_from, "status": "uploaded", "site": self.args.site}
        metadata = self.md_api.get("api/raw-files", payload)
        ignored_extensions = (".lv0", ".hkd")
        metadata = [
            row for row in metadata if not row["filename"].lower().endswith(ignored_extensions)
        ]
        metadata = [
            {"date": row["measurementDate"], "product": row["instrument"]["type"]}
            for row in metadata
        ]
        metadata = utils.remove_duplicate_dicts(metadata)
        return metadata

    def compare_file_content(self, product: str):
        payload = {"site": self.site, "product": product, "date": self.date_str}
        meta = self.md_api.get("api/files", payload)
        if not meta:
            return
        with TemporaryDirectory() as temp_dir:
            full_path = self._storage_api.download_product(meta[0], temp_dir)
            if utils.are_identical_nc_files(full_path, self.temp_file.name) is True:
                raise MiscError("Abort processing: File has not changed")

    def process_instrument(self, uuid: Uuid, instrument_type: str):
        instrument = self._detect_uploaded_instrument(instrument_type)
        process_class = getattr(instrument_process, f"Process{instrument_type.capitalize()}")
        process = process_class(self, self.temp_file, uuid)
        getattr(process, f'process_{instrument.replace("-", "_")}')()
        instrument = (
            "halo-doppler-lidar" if instrument == "halo-doppler-lidar-calibrated" else instrument
        )
        return process.uuid, instrument, process.instrument_pids

    def process_categorize(self, uuid: Uuid) -> tuple[Uuid, str]:
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

    def process_level2(self, uuid: Uuid, product: str) -> tuple[Uuid, str]:
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

    def add_pid(self) -> None:
        if self._create_new_version:
            self._pid_utils.add_pid_to_file(self.temp_file.name)

    def download_instrument(
        self,
        instrument: str,
        include_pattern: str | None = None,
        largest_only: bool = False,
        exclude_pattern: str | None = None,
    ) -> tuple[list | str, list, list]:
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
        self, instrument: str, exclude_pattern: str | None
    ) -> tuple[list | str, list, list]:
        """Download self-generated daily files (e.g. CL61-D)."""
        payload = self._get_payload(instrument=instrument)
        payload["status"] = "uploaded"
        upload_metadata = self.md_api.get("upload-metadata", payload)
        if exclude_pattern is not None:
            upload_metadata = utils.exclude_records_with_pattern_in_filename(
                upload_metadata, exclude_pattern
            )
        return self._download_raw_files(upload_metadata)

    def download_adjoining_daily_files(self, instrument: str) -> tuple[list | str, list, list]:
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

    def add_instrument_pid(self, instrument_pids: list) -> None:
        if len(set(instrument_pids)) > 1:
            logging.error("Several instrument PIDs found")
        if instrument_pids and (instrument_pid := instrument_pids[0]) is not None:
            with netCDF4.Dataset(self.temp_file.name, "r+") as nc:
                nc.instrument_pid = instrument_pid


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
    parser.add_argument(
        "-u",
        "--updated_since",
        type=int,
        help="Process all raw files submitted within `--updated_since` in days. Ignores other "
        "arguments than `--site`",
    )
    return subparser
