import glob
import logging
import os
import shutil
from tempfile import NamedTemporaryFile, TemporaryDirectory

import numpy as np
import requests
from cloudnetpy.exceptions import PlottingError
from cloudnetpy.plotting import PlotParameters, generate_figure
from cloudnetpy_qc import quality
from cloudnetpy_qc.quality import ErrorLevel

from data_processing import utils
from data_processing.metadata_api import MetadataApi
from data_processing.pid_utils import PidUtils
from data_processing.storage_api import StorageApi
from data_processing.utils import (
    MiscError,
    RawDataMissingError,
    build_file_landing_page_url,
    make_session,
)


class Uuid:
    __slots__ = ["raw", "product", "volatile"]

    def __init__(self):
        self.raw: list = []
        self.product: str = ""
        self.volatile: str | None = None


def clean_dir(dir_name: str) -> None:
    for filename in glob.glob(f"{dir_name}/*"):
        os.remove(filename)


class ProcessBase:
    def __init__(
        self,
        args,
        config: dict,
        storage_session: requests.Session | None = None,
        metadata_session: requests.Session | None = None,
    ):
        if storage_session is None:
            storage_session = make_session()
        if metadata_session is None:
            metadata_session = make_session()
        self.args = args
        self.site_meta, self.site, self._site_type = _read_site_info(args)
        self.config = config
        self.is_reprocess = getattr(args, "reprocess", False)
        self.is_reprocess_volatile = getattr(args, "reprocess_volatile", False)
        self.date_str: str | None = None
        self.md_api = MetadataApi(config, metadata_session)
        self._storage_api = StorageApi(config, storage_session)
        self._pid_utils = PidUtils(config)
        self._create_new_version = False
        self._temp_dir_root = utils.get_temp_dir(config)
        self.temp_dir = TemporaryDirectory(dir=self._temp_dir_root)
        self.temp_file = NamedTemporaryFile(dir=self._temp_dir_root, suffix=".nc")
        self.daily_file = NamedTemporaryFile(dir=self._temp_dir_root, suffix=".nc")

    def init_temp_files(self):
        self.temp_file = NamedTemporaryFile(dir=self._temp_dir_root, suffix=".nc")
        self.daily_file = NamedTemporaryFile(dir=self._temp_dir_root, suffix=".nc")

    def fetch_volatile_uuid(
        self, product: str, instrument_pid: str | None = None
    ) -> tuple[str | None, str | None]:
        payload = self._get_payload(product=product, instrument_pid=instrument_pid)
        payload["showLegacy"] = True
        metadata = self.md_api.get("api/files", payload)
        filename = metadata[0]["filename"] if metadata else None
        uuid = self._read_volatile_uuid(metadata)
        self._create_new_version = self._is_create_new_version(metadata)
        return uuid, filename

    def print_info(self, uuid: Uuid, result: str | None = None) -> None:
        kind = "new version" if self._create_new_version else "volatile file"
        link = build_file_landing_page_url(uuid.product)
        qc_str = f" QC: {result.upper()}" if result is not None else ""
        logging.info(f"Created {kind}: {link}{qc_str}")

    def upload_product(
        self,
        product: str,
        uuid: Uuid,
        model_or_instrument_id: str,
        s3key: str,
    ) -> None:
        file_info = self._storage_api.upload_product(self.temp_file.name, s3key)
        payload = utils.create_product_put_payload(
            self.temp_file.name, file_info, site=self.site
        )
        if product == "model":
            payload["model"] = model_or_instrument_id
        elif product == "mwr-l1c":
            payload["instrument"] = "hatpro"
        elif product in utils.get_product_types(level="1b"):
            payload["instrument"] = model_or_instrument_id
        else:
            payload["instrument"] = None

        payload["product"] = product  # L3 files use different products in NC vars
        self.md_api.put("files", s3key, payload)
        if product in utils.get_product_types(level="1b") or product == "mwr-l1c":
            self.update_statuses(uuid.raw)

    def create_and_upload_images(
        self,
        product: str,
        uuid: str,
        product_s3key: str,
        legacy: bool = False,
    ) -> None:
        if "hidden" in self._site_type:
            logging.info("Skipping plotting for hidden site")
            return
        temp_file = NamedTemporaryFile(suffix=".png")
        visualizations = []
        product_s3key = f"legacy/{product_s3key}" if legacy is True else product_s3key
        try:
            fields, max_alt = utils.get_fields_for_plot(product)
        except NotImplementedError:
            logging.warning(f"Plotting for {product} not implemented")
            return
        options = PlotParameters()
        options.max_y = max_alt
        options.title = False
        options.subtitle = False
        for field in fields:
            try:
                dimensions = generate_figure(
                    self.temp_file.name,
                    [field],
                    show=False,
                    output_filename=temp_file.name,
                    options=options,
                )
            except PlottingError as err:
                logging.debug(f"Skipping plotting {field}: {err}")
                continue

            visualizations.append(
                self._upload_img(
                    temp_file.name, product_s3key, uuid, product, field, dimensions
                )
            )
        self.md_api.put_images(visualizations, uuid)

    def compare_file_content(self, product: str):
        payload = {
            "site": self.site,
            "product": product,
            "date": self.date_str,
            "showLegacy": True,
        }
        meta = self.md_api.get("api/files", payload)
        if not meta:
            return
        with TemporaryDirectory() as temp_dir:
            full_path = self._storage_api.download_product(meta[0], temp_dir)
            if utils.are_identical_nc_files(full_path, self.temp_file.name) is True:
                raise MiscError("Skipping PUT to data portal, file has not changed")

    def _upload_img(
        self,
        img_path: str,
        product_s3key: str,
        product_uuid: str,
        product: str,
        field: str,
        dimensions,
    ) -> dict:
        img_s3key = product_s3key.replace(".nc", f"-{product_uuid[:8]}-{field}.png")
        self._storage_api.upload_image(full_path=img_path, s3key=img_s3key)
        return {
            "s3key": img_s3key,
            "variable_id": utils.get_var_id(product, field),
            "dimensions": utils.dimensions2dict(dimensions)
            if dimensions is not None
            else None,
        }

    def upload_quality_report(
        self, full_path: str, uuid: str, product: str | None = None
    ) -> str:
        try:
            quality_report = quality.run_tests(full_path, product=product)
        except ValueError:
            logging.exception("Failed to run quality control")
            return "FATAL"
        quality_dict = {
            "timestamp": quality_report.timestamp.isoformat(),
            "qcVersion": quality_report.qc_version,
            "tests": [
                {
                    "testId": test.test_id,
                    "exceptions": [
                        {"result": exception.result.value, "message": exception.message}
                        for exception in test.exceptions
                    ],
                }
                for test in quality_report.tests
            ],
        }
        exception_results = [
            exception.result
            for test in quality_report.tests
            for exception in test.exceptions
        ]
        result = "OK"
        for error_level in [ErrorLevel.ERROR, ErrorLevel.WARNING, ErrorLevel.INFO]:
            if error_level in exception_results:
                result = error_level.value
                break
        self.md_api.put("quality", uuid, quality_dict)
        return result

    def _read_volatile_uuid(self, metadata: list) -> str | None:
        if self._parse_volatile_value(metadata) is True:
            uuid = metadata[0]["uuid"]
            assert isinstance(uuid, str) and len(uuid) > 0
            return uuid
        return None

    def _read_stable_uuid(self, metadata: list) -> str | None:
        if self._parse_volatile_value(metadata) is False:
            uuid = metadata[0]["uuid"]
            assert isinstance(uuid, str) and len(uuid) > 0
            return uuid
        return None

    @staticmethod
    def _sort_model_meta2dict(metadata: list) -> dict:
        """Sort models and cycles to same dict key.

        Removes Gdas
        """
        if metadata:
            models_meta = {}
            models = [metadata[i]["model"]["id"] for i in range(len(metadata))]
            for m_id in models:
                m_metas = [
                    metadata
                    for i in range(len(metadata))
                    if m_id in metadata[i]["model"]["id"]
                ]
                models_meta[m_id] = m_metas
            return models_meta
        raise MiscError("No existing model files")

    def _is_create_new_version(self, metadata) -> bool:
        if self._parse_volatile_value(metadata) is False:
            if self.is_reprocess_volatile is True:
                raise MiscError("Skip reprocessing of a stable file.")
            if self.is_reprocess is True:
                return True
            raise MiscError('Existing freezed file and no "reprocess" flag')
        return False

    def _parse_volatile_value(self, metadata: list) -> bool | None:
        self._check_response_length(metadata)
        if metadata:
            value = str(metadata[0]["volatile"])
            if value == "True":
                return True
            if value == "False":
                return False
            raise RuntimeError(f"Unexpected value in metadata: {value}")
        return None

    def _download_raw_files(
        self, upload_metadata: list, temp_file=None
    ) -> tuple[list | str, list]:
        if temp_file is not None:
            if len(upload_metadata) > 1:
                logging.warning("Several daily raw files")
            upload_metadata = [upload_metadata[0]]
        full_paths, uuids = self._storage_api.download_raw_data(
            upload_metadata, self.temp_dir.name
        )
        if temp_file is not None:
            shutil.move(full_paths[0], temp_file.name)
            full_paths = temp_file.name
        return full_paths, uuids

    def _check_raw_data_status(self, metadata: list) -> None:
        if not metadata:
            raise RawDataMissingError
        is_unprocessed_data = self._is_unprocessed_data(metadata)
        if not is_unprocessed_data and not (
            self.is_reprocess or self.is_reprocess_volatile
        ):
            raise MiscError("Raw data already processed")

    @staticmethod
    def _is_unprocessed_data(metadata: list) -> bool:
        return any(row["status"] == "uploaded" for row in metadata)

    def _get_payload(
        self,
        instrument: str | None = None,
        product: str | None = None,
        model: str | None = None,
        skip_created: bool = False,
        date_from: str | None = None,
        date_to: str | None = None,
        instrument_pid: str | None = None,
    ) -> dict:
        payload = {
            "dateFrom": date_from if date_from is not None else self.date_str,
            "dateTo": date_to if date_to is not None else self.date_str,
            "site": self.site,
            "developer": True,
        }
        if instrument is not None:
            payload["instrument"] = instrument
        if instrument_pid is not None:
            payload["instrumentPid"] = instrument_pid
        if product is not None:
            payload["product"] = product
        if model is not None:
            payload["model"] = model
        if skip_created is True:
            payload["status[]"] = ["uploaded", "processed"]
        return payload

    def update_statuses(self, uuids: list, status: str = "processed") -> None:
        for uuid in uuids:
            payload = {"uuid": uuid, "status": status}
            self.md_api.post("upload-metadata", payload)

    def _get_product_key(self, identifier: str, instrument_pid: str | None) -> str:
        assert isinstance(self.date_str, str)
        if instrument_pid is not None:
            suffix = f"_{instrument_pid.split('.')[-1][:8]}"
        else:
            suffix = ""
        return f"{self.date_str.replace('-', '')}_{self.site}_{identifier}{suffix}.nc"

    @staticmethod
    def _check_response_length(metadata) -> None:
        if isinstance(metadata, list) and len(metadata) > 1:
            logging.info("API responded with several files")

    def _check_source_status(self, product: str, meta_records: dict) -> None:
        product_timestamp = self._get_product_timestamp(product)
        if product_timestamp is None:
            return
        source_timestamps = [meta["updatedAt"] for _, meta in meta_records.items()]
        if np.all([timestamp < product_timestamp for timestamp in source_timestamps]):
            raise MiscError("Source data already processed")

    def _get_product_timestamp(self, product: str) -> str | None:
        payload = self._get_payload(product=product)
        product_metadata = self.md_api.get("api/files", payload)
        if (
            product_metadata
            and self.is_reprocess is False
            and self.is_reprocess_volatile is False
        ):
            return product_metadata[0]["updatedAt"]
        return None


def _read_site_info(args) -> tuple:
    site_info = utils.read_site_info(args.site)
    site_id = site_info["id"]
    site_type = site_info["type"]
    site_meta = {
        key: site_info[key] for key in ("latitude", "longitude", "altitude", "name")
    }
    return site_meta, site_id, site_type


def add_default_arguments(parser):
    parser.add_argument("site", help="Site Name")
    return parser
