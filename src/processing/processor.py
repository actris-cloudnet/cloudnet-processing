import datetime
import logging
import uuid
from pathlib import Path
from typing import NamedTuple

from cloudnetpy.exceptions import PlottingError
from cloudnetpy.plotting import Dimensions, PlotParameters, generate_figure
from cloudnetpy_qc import quality
from cloudnetpy_qc.quality import ErrorLevel
from data_processing import utils
from data_processing.metadata_api import MetadataApi
from data_processing.storage_api import StorageApi


class ModelParams(NamedTuple):
    site: str
    date: datetime.date
    model: str


class Processor:
    def __init__(self, md_api: MetadataApi, storage_api: StorageApi):
        self.md_api = md_api
        self.storage_api = storage_api

    def get_site(self, site: str) -> dict:
        return self.md_api.get(f"api/sites/{site}")

    def download_raw_data(
        self, upload_metadata: list[dict], directory: Path
    ) -> tuple[list, list]:
        return self.storage_api.download_raw_data(upload_metadata, directory)

    def get_unprocessed_model_uploads(self) -> list:
        minimum_size = 20200
        # payload = {"status": "uploaded"}
        payload = {"status": "processed"}
        metadata = self.md_api.get("api/raw-model-files", payload)
        return [row for row in metadata if int(row["size"]) > minimum_size]

    def get_model_upload(self, params: ModelParams) -> dict | None:
        minimum_size = 20200
        payload = {
            "site": params.site,
            "date": params.date.isoformat(),
            "model": params.model,
        }
        rows = self.md_api.get("api/raw-model-files", payload)
        rows = [row for row in rows if int(row["size"]) > minimum_size]
        if len(rows) == 0:
            return None
        if len(rows) > 1:
            raise ValueError("Multiple model files found")
        return rows[0]

    def get_model_file(self, params: ModelParams) -> dict | None:
        payload = {
            "site": params.site,
            "date": params.date.isoformat(),
            "model": params.model,
        }
        rows = self.md_api.get("api/model-files", payload)
        if len(rows) == 0:
            return None
        if len(rows) > 1:
            raise ValueError("Multiple model files found")
        return rows[0]

    def upload_file(self, params: ModelParams, full_path: Path, s3key: str):
        file_info = self.storage_api.upload_product(full_path, s3key)
        payload = utils.create_product_put_payload(
            full_path, file_info, site=params.site
        )
        payload["model"] = params.model
        self.md_api.put("files", s3key, payload)

    def update_statuses(self, raw_uuids: list[str], status: str):
        for raw_uuid in raw_uuids:
            payload = {"uuid": raw_uuid, "status": status}
            self.md_api.post("upload-metadata", payload)

    def create_and_upload_images(
        self,
        full_path: Path,
        product: str,
        product_uuid: uuid.UUID,
        product_s3key: str,
        directory: Path,
        legacy: bool = False,
    ) -> None:
        img_path = directory / "plot.png"
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
        valid_images = []
        for field in fields:
            try:
                dimensions = generate_figure(
                    full_path,
                    [field],
                    show=False,
                    output_filename=img_path,
                    options=options,
                )
                valid_images.append(field)
            except PlottingError as err:
                logging.debug(f"Skipping plotting {field}: {err}")
                continue

            visualizations.append(
                self.upload_img(
                    img_path, product_s3key, product_uuid, product, field, dimensions
                )
            )
        self.md_api.put_images(visualizations, product_uuid)
        # delete_obsolete_images(uuid, product, valid_images)

    def upload_img(
        self,
        img_path: Path,
        product_s3key: str,
        product_uuid: uuid.UUID,
        product: str,
        field: str,
        dimensions: Dimensions,
    ) -> dict:
        img_s3key = product_s3key.replace(".nc", f"-{product_uuid.hex[:8]}-{field}.png")
        self.storage_api.upload_image(full_path=img_path, s3key=img_s3key)
        return {
            "s3key": img_s3key,
            "variable_id": utils.get_var_id(product, field),
            "dimensions": utils.dimensions2dict(dimensions)
            if dimensions is not None
            else None,
        }

    def upload_quality_report(
        self, full_path: Path, uuid: uuid.UUID, product: str | None = None
    ) -> str:
        try:
            # is_dev = self.config.get("PID_SERVICE_TEST_ENV", "").lower() == "true"
            # ignore_tests = ["TestInstrumentPid"] if is_dev else None
            quality_report = quality.run_tests(
                full_path,
                product=product,  # ignore_tests=ignore_tests
            )
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
        self.md_api.put("quality", str(uuid), quality_dict)
        return result
