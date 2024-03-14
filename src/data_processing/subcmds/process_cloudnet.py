#!/usr/bin/env python3
"""Master script for CloudnetPy processing."""
import datetime
import importlib
import logging
import random
import warnings

import doppy
import housekeeping
import netCDF4
import requests
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.exceptions import CloudnetException, ModelDataError
from cloudnetpy.utils import date_range

from data_processing import instrument_process, processing_tools, utils
from data_processing.processing_tools import ProcessBase, Uuid
from data_processing.utils import MiscError, RawApi, RawDataMissingError, make_session

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
        instrument_pid = None
        processing_tools.clean_dir(self.temp_dir.name)
        self.init_temp_files()
        if product == "model":
            return
        # Will be processed as part of weather station.
        if product == "rain-gauge":
            return
        logging.info(f"Processing {product} product, {self.site} {self.date_str}")
        uuid = Uuid()
        try:
            match product:
                case product if product in utils.get_product_types(level="2"):
                    uuid.volatile, filename = self.fetch_volatile_uuid(product)
                    uuid, identifier = self.process_level2(uuid, product)
                case "categorize" | "categorize-voodoo":
                    uuid.volatile, filename = self.fetch_volatile_uuid(product)
                    uuid, identifier = self.process_categorize(uuid, product)
                case product if product in utils.get_product_types(
                    level="1b"
                ) or product in ("mwr-l1c", "doppler-lidar-wind"):
                    instrument_id, instrument_pid = self._fetch_instrument_to_process(
                        product
                    )
                    uuid.volatile, filename = self.fetch_volatile_uuid(
                        product, instrument_pid=instrument_pid
                    )
                    uuid, identifier = self.process_instrument(
                        uuid, product, instrument_id, instrument_pid
                    )
                    self.add_instrument_pid(instrument_pid)
                case bad_product:
                    raise ValueError(f"Bad product: {bad_product}")
            if product == "mwr-l1c":
                identifier = "hatpro-l1c"
            if product == "doppler-lidar-wind":
                identifier = product
            if not self.args.force:
                self.compare_file_content(product)
            self.add_pid()
            utils.add_version_to_global_attributes(self.temp_file.name)
            if filename is None:
                filename = self._get_product_key(identifier, instrument_pid)
            self.upload_product(product, uuid, identifier, filename)
            self.create_and_upload_images(product, uuid.product, filename)
            result = self.upload_quality_report(self.temp_file.name, uuid.product)
            self.print_info(uuid, result)
        except (
            RawDataMissingError,
            MiscError,
            NotImplementedError,
            doppy.exceptions.NoDataError,
        ) as err:
            logging.warning(err)
        except CloudnetException as err:
            logging.error(err)
        except Exception as err:
            utils.send_slack_alert(err, "data", self.args, self.date_str, product)

    def process_raw_data_using_updated_at(self):
        metadata = self._get_uploaded_raw_metadata()
        for row in metadata:
            self.date_str = row["date"]
            self.process_product(row["product"])

    def _get_uploaded_raw_metadata(self) -> list:
        updated_at_from = datetime.date.today() - datetime.timedelta(
            hours=self.args.updated_since
        )
        payload = {
            "updatedAtFrom": updated_at_from,
            "status": "uploaded",
            "site": self.args.site,
        }
        metadata = self.md_api.get("api/raw-files", payload)
        ignored_extensions = (".lv0", ".hkd")
        metadata = [
            row
            for row in metadata
            if not row["filename"].lower().endswith(ignored_extensions)
        ]
        metadata = [
            {"date": row["measurementDate"], "product": row["instrument"]["type"]}
            for row in metadata
        ]
        metadata = utils.remove_duplicate_dicts(metadata)
        return metadata

    def process_instrument(
        self, uuid: Uuid, product: str, instrument: str, instrument_pid: str
    ):
        product_camel_case = "".join([part.capitalize() for part in product.split("-")])
        process_class = getattr(instrument_process, f"Process{product_camel_case}")
        process = process_class(self, self.temp_file, uuid, instrument_pid)
        getattr(process, f'process_{instrument.replace("-", "_")}')()
        instrument = (
            "halo-doppler-lidar"
            if instrument == "halo-doppler-lidar-calibrated"
            else instrument
        )
        if self.args.housekeeping:
            logging.info("Processing housekeeping data")
            try:
                self._process_housekeeping(instrument_pid)
            except housekeeping.HousekeepingException as err:
                logging.error(err)

        return process.uuid, instrument

    def _process_housekeeping(self, instrument_pid: str):
        raw_api = RawApi(session=make_session())
        records = self.md_api.get(
            "upload-metadata", self._get_payload(instrument_pid=instrument_pid)
        )
        with housekeeping.Database() as db:
            for record in records:
                housekeeping.process_record(record, raw_api=raw_api, db=db)

    def process_categorize(self, uuid: Uuid, cat_variant: str) -> tuple[Uuid, str]:
        is_voodoo = cat_variant == "categorize-voodoo"
        meta_records = self._get_level1b_metadata_for_categorize(is_voodoo)
        self._check_source_status(cat_variant, meta_records)
        input_files: dict[str, str | list[str]] = {
            product: self._storage_api.download_product(metadata, self.temp_dir.name)
            for product, metadata in meta_records.items()
        }
        if is_voodoo:
            input_files["lv0_files"], lv0_uuid = self._get_input_files_for_voodoo()
        else:
            lv0_uuid = []
        try:
            uuid.product = generate_categorize(
                input_files, self.temp_file.name, uuid=uuid.volatile
            )
            uuid.raw.extend(lv0_uuid)
        except ModelDataError as exc:
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
                raise MiscError("Bad ecmwf model data and no gdas1") from exc
        return uuid, cat_variant

    def _get_level1b_metadata_for_categorize(self, is_voodoo: bool) -> dict:
        instrument_order = {
            "mwr": ("hatpro", "radiometrics"),
            "radar": ("mira", "rpg-fmcw-94", "copernicus"),
            "lidar": ("chm15k", "chm15kx", "cl61d", "cl51", "cl31"),
            "disdrometer": ("thies-lnm", "parsivel"),
            "model": "",  # You always get 1 and it's the best one
        }
        meta_records = {}
        for product in instrument_order:
            if product == "model":
                payload = self._get_payload()
                route = "api/model-files"
            else:
                payload = self._get_payload(product=product)
                route = "api/files"
            if is_voodoo and product == "radar":
                payload["instrument"] = "rpg-fmcw-94"
            metadata = self.md_api.get(route, payload)
            if product == "mwr" and not metadata:
                # Use RPG-FMCW-94 as a fallback MWR
                payload["product"] = "radar"
                payload["instrument"] = "rpg-fmcw-94"
                metadata = self.md_api.get("api/files", payload)
            if product == "disdrometer" and not metadata:
                continue
            if not metadata:
                raise MiscError(f"Missing required input product: {product}")
            meta_records[product] = metadata[0]
            if len(metadata) == 1:
                continue
            found = False
            for preferred_instrument in instrument_order[product]:
                for row in metadata:
                    if row["instrument"]["id"] == preferred_instrument and not found:
                        meta_records[product] = row
                        found = True
            logging.info(
                f"Several options for {product}, using {meta_records[product]['instrument']['id']} with PID {meta_records[product]['instrumentPid']}"
            )
        return meta_records

    def _get_input_files_for_voodoo(self) -> tuple[list[str], list[str]]:
        payload = self._get_payload(instrument="rpg-fmcw-94")
        metadata = self.md_api.get("upload-metadata", payload)
        unique_pids = list(set(row["instrumentPid"] for row in metadata))
        if unique_pids:
            instrument_pid = unique_pids[0]
        else:
            raise RawDataMissingError("No rpg-fmcw-94 cloud radar found")
        (
            full_paths,
            uuids,
        ) = self.download_instrument(instrument_pid, include_pattern=".LV0")
        full_paths_list = [full_paths] if isinstance(full_paths, str) else full_paths
        return full_paths_list, uuids

    def process_level2(self, uuid: Uuid, product: str) -> tuple[Uuid, str]:
        if product == "mwr-single":
            cat_file = "mwr-l1c"
            module_name = "mwr_tools"
        elif product == "mwr-multi":
            cat_file = "mwr-l1c"
            module_name = "mwr_tools"
        elif product == "classification-voodoo":
            cat_file = "categorize-voodoo"
            module_name = "classification"
        else:
            cat_file = "categorize"
            module_name = product
        payload = self._get_payload(product=cat_file)
        metadata = self.md_api.get("api/files", payload)
        self._check_response_length(metadata)
        if metadata:
            categorize_file = self._storage_api.download_product(
                metadata[0], self.temp_dir.name
            )
            meta_record = {"categorize": metadata[0]}
        else:
            raise MiscError(f"Missing required input file: {cat_file}")
        self._check_source_status(product, meta_record)
        module = importlib.import_module(f"cloudnetpy.products.{module_name}")
        prod = (
            "classification"
            if product == "classification-voodoo"
            else product.replace("-", "_")
        )
        fun = getattr(module, f"generate_{prod}")
        uuid.product = fun(categorize_file, self.temp_file.name, uuid=uuid.volatile)
        identifier = utils.get_product_identifier(product)
        return uuid, identifier

    def add_pid(self) -> None:
        if self._create_new_version:
            self._pid_utils.add_pid_to_file(self.temp_file.name)

    def download_instrument(
        self,
        instrument_pid: str,
        include_pattern: str | None = None,
        largest_only: bool = False,
        exclude_pattern: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        include_tag_subset: set[str] | None = None,
        exclude_tag_subset: set[str] | None = None,
    ) -> tuple[list | str, list]:
        """Download raw files for given instrument PID."""
        payload = self._get_payload(
            skip_created=True,
            date_from=date_from,
            date_to=date_to,
            instrument_pid=instrument_pid,
        )
        upload_metadata = self.md_api.get("upload-metadata", payload)
        if include_pattern is not None:
            upload_metadata = utils.include_records_with_pattern_in_filename(
                upload_metadata, include_pattern
            )
        if exclude_pattern is not None:
            upload_metadata = utils.exclude_records_with_pattern_in_filename(
                upload_metadata, exclude_pattern
            )
        if include_tag_subset is not None:
            upload_metadata = [
                record
                for record in upload_metadata
                if include_tag_subset.issubset(set(record["tags"]))
            ]
        if exclude_tag_subset is not None:
            upload_metadata = [
                record
                for record in upload_metadata
                if not exclude_tag_subset.issubset(set(record["tags"]))
            ]
        arg = self.temp_file if largest_only else None
        self._check_raw_data_status(upload_metadata)
        return self._download_raw_files(upload_metadata, arg)

    def download_adjoining_daily_files(
        self, instrument_pid: str
    ) -> tuple[list | str, list]:
        next_day = utils.get_date_from_past(-1, self.date_str)
        payload = self._get_payload(skip_created=True, instrument_pid=instrument_pid)
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
        full_paths, _ = self._download_raw_files(upload_metadata)
        # Return all full paths but only current day UUIDs
        uuids_of_current_day = [
            meta["uuid"]
            for meta in upload_metadata
            if meta["measurementDate"] == self.date_str
        ]
        return full_paths, uuids_of_current_day

    def add_instrument_pid(self, instrument_pid: str):
        with netCDF4.Dataset(self.temp_file.name, "r+") as nc:
            nc.instrument_pid = instrument_pid

    def _fetch_instrument_to_process(self, product: str) -> tuple[str, str]:
        possible_instruments = self._get_possible_instruments(product)
        upload_metadata = self._get_upload_metadata(possible_instruments)
        upload_metadata = screen_upload_metadata(upload_metadata)
        if len(upload_metadata) == 0:
            raise RawDataMissingError
        if self.args.pid is not None:
            for item in upload_metadata:
                if item["instrumentPid"] == self.args.pid:
                    return item["instrument"]["id"], item["instrumentPid"]
            raise RawDataMissingError(f"{product} data from {self.args.pid} not found")
        instrument_id, instrument_pid = decide_instrument_to_process(upload_metadata)
        logging.info(f"Using {instrument_id} with PID {instrument_pid}")
        return instrument_id, instrument_pid

    def _get_possible_instruments(self, product: str) -> list:
        """Get all possible instruments for given product."""
        instrument_metadata = self.md_api.get("api/instruments")
        if product == "mwr-l1c":
            return ["hatpro"]
        elif product == "doppler-lidar-wind":
            return [
                item["id"]
                for item in instrument_metadata
                if item["type"] == "doppler-lidar"
            ]
        return [item["id"] for item in instrument_metadata if item["type"] == product]

    def _get_upload_metadata(self, instruments: list[str]):
        """Get all upload metadata for given instruments."""
        payload = self._get_payload(skip_created=True)
        upload_metadata = self.md_api.get("upload-metadata", payload)
        return [
            item for item in upload_metadata if item["instrument"]["id"] in instruments
        ]


def screen_upload_metadata(metadata: list) -> list:
    filtered_metadata = [
        row
        for row in metadata
        if not (
            row["instrument"]["id"] == "rpg-fmcw-94"
            and row["filename"].lower().endswith(".lv0")
        )
    ]
    return filtered_metadata


def decide_instrument_to_process(metadata: list) -> tuple[str, str]:
    uploaded_data = [row for row in metadata if row["status"] == "uploaded"]
    if uploaded_data:
        unique_instruments = list(
            {(row["instrument"]["id"], row["instrumentPid"]) for row in uploaded_data}
        )
        return random.choice(unique_instruments)
    else:
        unique_pids = list(set(row["instrumentPid"] for row in metadata))
        if len(unique_pids) == 1:
            return metadata[0]["instrument"]["id"], metadata[0]["instrumentPid"]
        raise MiscError(
            f"Several instrument PIDs with processed data found {unique_pids}. Please use --pid argument."
        )


def add_arguments(subparser):
    parser = subparser.add_parser(
        "process", help="Process Cloudnet Level 1 and 2 data."
    )
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
        type=float,
        help="Process all raw files submitted within `--updated_since` in hours. Ignores other "
        "arguments than `--site`",
    )
    parser.add_argument(
        "--pid",
        type=str,
        help="Specific Level 1b instrument PID to process. "
        "See https://instrumentdb.out.ocp.fmi.fi/.",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Skip checking of file contents compared to data portal.",
        default=False,
    )
    parser.add_argument(
        "-H",
        "--housekeeping",
        action="store_true",
        help="Process housekeeping data.",
        default=False,
    )
    return subparser
