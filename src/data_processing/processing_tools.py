import glob
import logging
import os
import shutil
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Optional, Tuple, Union

import numpy as np
import requests
from cloudnetpy.plotting import generate_figure

from data_processing import utils
from data_processing.metadata_api import MetadataApi
from data_processing.pid_utils import PidUtils
from data_processing.storage_api import StorageApi
from data_processing.utils import MiscError, RawDataMissingError, make_session


class Uuid:
    __slots__ = ["raw", "product", "volatile"]

    def __init__(self):
        self.raw: list = []
        self.product: str = ""
        self.volatile: Union[str, None] = None


def clean_dir(dir_name: str) -> None:
    for filename in glob.glob(f"{dir_name}/*"):
        os.remove(filename)


class ProcessBase:
    def __init__(
        self,
        args,
        config: dict,
        storage_session: Optional[requests.Session] = None,
        metadata_session: Optional[requests.Session] = None,
    ):
        if storage_session is None:
            storage_session = make_session()
        if metadata_session is None:
            metadata_session = make_session()
        self.site_meta, self.site, self._site_type = _read_site_info(args)
        self.config = config
        self.is_reprocess = getattr(args, "reprocess", False)
        self.is_reprocess_volatile = getattr(args, "reprocess_volatile", False)
        self.date_str: Optional[str] = None
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

    def fetch_volatile_uuid(self, product: str) -> Union[str, None]:
        payload = self._get_payload(product=product)
        payload["showLegacy"] = True
        metadata = self.md_api.get(f"api/files", payload)
        uuid = self._read_volatile_uuid(metadata)
        self._create_new_version = self._is_create_new_version(metadata)
        return uuid

    def print_info(self) -> None:
        logging.info(
            f"Created: " f'{"New version" if self._create_new_version is True else "Volatile file"}'
        )

    def upload_product(
        self, full_path: str, product: str, uuid: Uuid, model_or_instrument_id: str
    ) -> None:
        if product in utils.get_product_types(level="3"):
            s3key = self._get_l3_product_key(product, model_or_instrument_id)
        else:
            s3key = self._get_product_key(model_or_instrument_id)
        file_info = self._storage_api.upload_product(full_path, s3key)
        payload = utils.create_product_put_payload(full_path, file_info, site=self.site)
        if product == "model":
            del payload["cloudnetpyVersion"]
            del payload["instrumentPid"]
            payload["model"] = model_or_instrument_id
        payload["product"] = product  # L3 files use different products in NC vars
        self.md_api.put("files", s3key, payload)
        if product in utils.get_product_types(level="1b"):
            self.update_statuses(uuid.raw)

    def create_and_upload_images(
        self,
        nc_file_full_path: str,
        product: str,
        uuid: str,
        model_or_instrument_id: str,
        legacy: bool = False,
    ) -> None:
        if "hidden" in self._site_type:
            logging.info("Skipping plotting for hidden site")
            return
        temp_file = NamedTemporaryFile(suffix=".png")
        visualizations = []
        product_s3key = self._get_product_key(model_or_instrument_id)
        product_s3key = f"legacy/{product_s3key}" if legacy is True else product_s3key
        try:
            fields, max_alt = utils.get_fields_for_plot(product)
        except NotImplementedError:
            logging.warning(f"Plotting for {product} not implemented")
            return
        for field in fields:
            try:
                dimensions = generate_figure(
                    nc_file_full_path,
                    [field],
                    show=False,
                    image_name=temp_file.name,
                    max_y=max_alt,
                    sub_title=False,
                    title=False,
                    dpi=120,
                )
            except (IndexError, ValueError, TypeError) as err:
                logging.warning(f"Skipping {field}")
                continue

            visualizations.append(
                self._upload_img(temp_file.name, product_s3key, uuid, product, field, dimensions)
            )
        self.md_api.put_images(visualizations, uuid)

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
            "dimensions": utils.dimensions2dict(dimensions) if dimensions is not None else None,
        }

    def upload_quality_report(self, full_path: str, uuid: str) -> None:
        quality_report = utils.create_quality_report(full_path)
        self.md_api.put("quality", uuid, quality_report)

    def _read_volatile_uuid(self, metadata: list) -> Union[str, None]:
        if self._parse_volatile_value(metadata) is True:
            uuid = metadata[0]["uuid"]
            assert isinstance(uuid, str) and len(uuid) > 0
            return uuid
        return None

    def _read_stable_uuid(self, metadata: list) -> Union[str, None]:
        if self._parse_volatile_value(metadata) is False:
            uuid = metadata[0]["uuid"]
            assert isinstance(uuid, str) and len(uuid) > 0
            return uuid
        return None

    @staticmethod
    def _sort_model_meta2dict(metadata: list) -> dict:
        """Sort models and cycles to same dict key. Removes Gdas"""
        if metadata:
            models_meta = {}
            models = [metadata[i]["model"]["id"] for i in range(len(metadata))]
            for m_id in models:
                m_metas = [
                    metadata for i in range(len(metadata)) if m_id in metadata[i]["model"]["id"]
                ]
                models_meta[m_id] = m_metas
            return models_meta
        else:
            raise MiscError("No existing model files")

    def _is_create_new_version(self, metadata) -> bool:
        if self._parse_volatile_value(metadata) is False:
            if self.is_reprocess_volatile is True:
                raise MiscError("Skip reprocessing of a stable file.")
            if self.is_reprocess is True:
                return True
            else:
                raise MiscError('Existing freezed file and no "reprocess" flag')
        return False

    def _parse_volatile_value(self, metadata: list) -> Union[bool, None]:
        self._check_response_length(metadata)
        if metadata:
            value = str(metadata[0]["volatile"])
            if value == "True":
                return True
            elif value == "False":
                return False
            else:
                raise RuntimeError(f"Unexpected value in metadata: {value}")
        return None

    def _download_raw_files(
        self, upload_metadata: list, temp_file=None
    ) -> Tuple[Union[list, str], list, list]:
        if temp_file is not None:
            if len(upload_metadata) > 1:
                logging.warning("Several daily raw files")
            upload_metadata = [upload_metadata[0]]
        full_paths, uuids, instrument_pids = self._storage_api.download_raw_data(
            upload_metadata, self.temp_dir.name
        )
        if temp_file is not None:
            shutil.move(full_paths[0], temp_file.name)
            full_paths = temp_file.name
        return full_paths, uuids, instrument_pids

    def _check_raw_data_status(self, metadata: list) -> None:
        if not metadata:
            raise RawDataMissingError
        is_unprocessed_data = self._is_unprocessed_data(metadata)
        if not is_unprocessed_data and not (self.is_reprocess or self.is_reprocess_volatile):
            raise MiscError("Raw data already processed")

    @staticmethod
    def _is_unprocessed_data(metadata: list) -> bool:
        return any([row["status"] == "uploaded" for row in metadata])

    def _get_payload(
        self,
        instrument: Optional[str] = None,
        product: Optional[str] = None,
        model: Optional[str] = None,
        skip_created: bool = False,
    ) -> dict:
        payload = {
            "dateFrom": self.date_str,
            "dateTo": self.date_str,
            "site": self.site,
            "developer": True,
        }
        if instrument is not None:
            payload["instrument"] = instrument
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

    def _get_product_key(self, identifier: str) -> str:
        assert isinstance(self.date_str, str)
        return f"{self.date_str.replace('-', '')}_{self.site}_{identifier}.nc"

    def _get_l3_product_key(self, product: str, model: str) -> str:
        assert isinstance(self.date_str, str)
        return f"{self.date_str.replace('-', '')}_{self.site}_{product}_downsampled_{model}.nc"

    @staticmethod
    def _check_response_length(metadata) -> None:
        if isinstance(metadata, list) and len(metadata) > 1:
            logging.warning("API responded with several files")

    def _check_source_status(self, product: str, meta_records: dict) -> None:
        product_timestamp = self._get_product_timestamp(product)
        if product_timestamp is None:
            return
        source_timestamps = [meta["updatedAt"] for _, meta in meta_records.items()]
        if np.all([timestamp < product_timestamp for timestamp in source_timestamps]):
            raise MiscError("Source data already processed")

    def _get_product_timestamp(self, product: str) -> Union[str, None]:
        payload = self._get_payload(product=product)
        product_metadata = self.md_api.get(f"api/files", payload)
        if product_metadata and self.is_reprocess is False and self.is_reprocess_volatile is False:
            return product_metadata[0]["updatedAt"]
        return None


def _read_site_info(args) -> tuple:
    site_info = utils.read_site_info(args.site)
    site_id = site_info["id"]
    site_type = site_info["type"]
    site_meta = {key: site_info[key] for key in ("latitude", "longitude", "altitude", "name")}
    return site_meta, site_id, site_type


def add_default_arguments(parser):
    parser.add_argument("site", help="Site Name")
    return parser
