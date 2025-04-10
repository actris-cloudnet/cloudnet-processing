import datetime
import logging
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import numpy as np
from cloudnetpy.exceptions import PlottingError
from cloudnetpy.model_evaluation.plotting.plotting import generate_L3_day_plots
from cloudnetpy.plotting import Dimensions, PlotParameters, generate_figure
from cloudnetpy_qc import quality
from cloudnetpy_qc.quality import ErrorLevel

from processing import utils
from processing.dvas import Dvas
from processing.metadata_api import MetadataApi
from processing.pid_utils import PidUtils
from processing.storage_api import StorageApi

MIN_MODEL_FILESIZE = 20200
TIMEDELTA_ZERO = datetime.timedelta(0)


@dataclass(frozen=True)
class Instrument:
    uuid: str
    pid: str
    type: str
    derived_product_ids: frozenset[str]


@dataclass(frozen=True)
class Model:
    id: str
    source_model_id: str | None
    forecast_start: int | None
    forecast_end: int | None


@dataclass(frozen=True)
class Site:
    id: str
    name: str
    latitude: float
    longitude: float
    altitude: float
    raw_time: np.ndarray | None
    raw_latitude: np.ndarray | None
    raw_longitude: np.ndarray | None
    types: set[str]


@dataclass(frozen=True)
class Product:
    id: str
    type: set[Literal["instrument", "geophysical"]]
    source_instrument_ids: set[str]
    source_product_ids: set[str]
    derived_product_ids: set[str]
    experimental: bool


@dataclass(frozen=True)
class ProcessParams:
    site: Site
    date: datetime.date
    product: Product


@dataclass(frozen=True)
class ModelParams(ProcessParams):
    model: Model


@dataclass(frozen=True)
class InstrumentParams(ProcessParams):
    instrument: Instrument


@dataclass(frozen=True)
class ProductParams(ProcessParams):
    instrument: Instrument | None


class Processor:
    def __init__(
        self,
        md_api: MetadataApi,
        storage_api: StorageApi,
        pid_utils: PidUtils,
        dvas: Dvas,
    ):
        self.md_api = md_api
        self.storage_api = storage_api
        self.pid_utils = pid_utils
        self.dvas = dvas

    def get_site(self, site_id: str, date: datetime.date) -> Site:
        site = self.md_api.get(f"api/sites/{site_id}")
        if site["latitude"] is None and site["longitude"] is None:
            location = self.md_api.get(
                f"api/sites/{site_id}/locations", {"date": date.isoformat()}
            )
            site["latitude"] = location["latitude"]
            site["longitude"] = location["longitude"]

            prev_date = date - datetime.timedelta(days=1)
            next_date = date + datetime.timedelta(days=1)
            raw_locations = np.array(
                self._fetch_raw_locations(site_id, prev_date, -1)
                + self._fetch_raw_locations(site_id, date)
                + self._fetch_raw_locations(site_id, next_date, 0),
            )
            site["raw_time"] = raw_locations[:, 0]
            site["raw_latitude"] = raw_locations[:, 1].astype(np.float32)
            site["raw_longitude"] = raw_locations[:, 2].astype(np.float32)
        return Site(
            id=site["id"],
            name=site["humanReadableName"],
            latitude=site["latitude"],
            longitude=site["longitude"],
            raw_time=site.get("raw_time"),
            raw_latitude=site.get("raw_latitude"),
            raw_longitude=site.get("raw_longitude"),
            altitude=site["altitude"],
            types=set(site["type"]),
        )

    def _fetch_raw_locations(
        self, site_id: str, date: datetime.date, index: int | None = None
    ) -> list[tuple[datetime.datetime, float, float]]:
        locations = self.md_api.get(
            f"api/sites/{site_id}/locations",
            {"date": date.isoformat(), "raw": "1"},
        )
        if index is not None:
            if index < 0:
                index += len(locations)
            locations = locations[index : index + 1]
        output = []
        for location in locations:
            output.append(
                (
                    datetime.datetime.fromisoformat(location["date"]),
                    location["latitude"],
                    location["longitude"],
                )
            )
        return output

    def get_product(self, product_id: str) -> Product:
        product = self.md_api.get(f"api/products/{product_id}")
        return Product(
            id=product["id"],
            type=set(product["type"]),
            source_instrument_ids={i["id"] for i in product["sourceInstruments"]},
            source_product_ids={p["id"] for p in product["sourceProducts"]},
            derived_product_ids={p["id"] for p in product["derivedProducts"]},
            experimental=product["experimental"],
        )

    def get_instrument(self, uuid: str) -> Instrument:
        instrument = self.md_api.get(f"api/instrument-pids/{uuid}")
        return Instrument(
            uuid=instrument["uuid"],
            pid=instrument["pid"],
            type=instrument["instrument"]["id"],
            derived_product_ids=self.get_derived_products(
                instrument["instrument"]["id"]
            ),
        )

    def get_derived_products(self, instrument_id: str) -> frozenset[str]:
        instrument = self.md_api.get(f"api/instruments/{instrument_id}")
        return frozenset(p["id"] for p in instrument["derivedProducts"])

    def get_model(self, model_id: str) -> Model:
        models = self.md_api.get("api/models")
        model = next(model for model in models if model["id"] == model_id)
        return Model(
            id=model["id"],
            source_model_id=model["sourceModelId"],
            forecast_start=model["forecastStart"],
            forecast_end=model["forecastEnd"],
        )

    def download_raw_data(
        self, upload_metadata: list[dict], directory: Path
    ) -> tuple[list, list]:
        return self.storage_api.download_raw_data(upload_metadata, directory)

    def get_model_upload(
        self, params: ModelParams, start_date: datetime.date, end_date: datetime.date
    ) -> list[dict]:
        payload = {
            "site": params.site.id,
            "dateFrom": start_date.isoformat(),
            "dateTo": end_date.isoformat(),
            "model": params.model.source_model_id or params.model.id,
            "status": ["uploaded", "processed"],
        }
        rows = self.md_api.get("api/raw-model-files", payload)
        rows = [row for row in rows if int(row["size"]) > MIN_MODEL_FILESIZE]
        return rows

    def get_model_file(self, params: ModelParams) -> dict | None:
        payload = {
            "site": params.site.id,
            "date": params.date.isoformat(),
            "model": params.model.id,
        }
        metadata = self.md_api.get("api/model-files", payload)
        if len(metadata) == 0:
            return None
        if len(metadata) > 1:
            raise RuntimeError(f"Multiple {params.model.id} files found")
        return metadata[0]

    def fetch_product(self, params: ProcessParams) -> dict | None:
        payload = {
            "site": params.site.id,
            "date": params.date.isoformat(),
            "product": params.product.id,
            "showLegacy": True,
        }
        if isinstance(params, InstrumentParams):
            payload["instrumentPid"] = params.instrument.pid
        elif isinstance(params, ProductParams) and params.instrument is not None:
            payload["instrumentPid"] = params.instrument.pid
        metadata = self.md_api.get("api/files", payload)
        if len(metadata) == 0:
            return None
        if len(metadata) > 1:
            raise RuntimeError(f"Multiple {params.product.id} files found")
        return metadata[0]

    def download_instrument(
        self,
        site_id: str,
        instrument_id: str,
        directory: Path,
        date: datetime.date | tuple[datetime.date, datetime.date],
        instrument_pid: str | None = None,
        include_pattern: str | None = None,
        largest_only: bool = False,
        exclude_pattern: str | None = None,
        include_tag_subset: set[str] | None = None,
        exclude_tag_subset: set[str] | None = None,
        allow_empty=False,
        filename_prefix: str | None = None,
        filename_suffix: str | None = None,
        time_offset: datetime.timedelta | None = None,
    ):
        """Download raw files matching the given parameters."""
        if isinstance(date, datetime.date):
            start_date = date
            end_date = date
        else:
            start_date, end_date = date
        start_date_ext, end_date_ext = start_date, end_date
        if time_offset is not None:
            if largest_only:
                raise ValueError("Cannot use both time_offset and largest_only")
            if abs(time_offset / datetime.timedelta(hours=1)) >= 24:
                raise ValueError("time_offset must be less than 24 hours")
            if time_offset < TIMEDELTA_ZERO:
                start_date_ext -= datetime.timedelta(days=1)
            elif time_offset > TIMEDELTA_ZERO:
                end_date_ext += datetime.timedelta(days=1)
        payload = self._get_payload(
            site=site_id,
            date=(start_date_ext, end_date_ext),
            instrument=instrument_id,
            instrument_pid=instrument_pid,
            skip_created=True,
            filename_prefix=filename_prefix,
            filename_suffix=filename_suffix,
        )
        upload_metadata = self.md_api.get("api/raw-files", payload)
        if include_pattern is not None:
            upload_metadata = _include_records_with_pattern_in_filename(
                upload_metadata, include_pattern
            )
        if exclude_pattern is not None:
            upload_metadata = _exclude_records_with_pattern_in_filename(
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
        if not upload_metadata:
            if allow_empty:
                return [], []
            else:
                raise utils.RawDataMissingError
        if largest_only:
            upload_metadata = [max(upload_metadata, key=lambda item: int(item["size"]))]
        full_paths, uuids = self.storage_api.download_raw_data(
            upload_metadata, directory
        )
        if time_offset is not None:
            uuids = [
                meta["uuid"]
                for meta in upload_metadata
                if start_date
                <= datetime.date.fromisoformat(meta["measurementDate"])
                <= end_date
            ]
        if largest_only:
            return full_paths[0], uuids
        return full_paths, uuids

    def _get_payload(
        self,
        site: str | None = None,
        instrument: str | None = None,
        product: str | None = None,
        model: str | None = None,
        skip_created: bool = False,
        date: datetime.date | tuple[datetime.date, datetime.date] | None = None,
        instrument_pid: str | None = None,
        filename_prefix: str | None = None,
        filename_suffix: str | None = None,
    ) -> dict:
        payload: dict = {"developer": True}
        if site is not None:
            payload["site"] = site
        if isinstance(date, datetime.date):
            payload["date"] = date.isoformat()
        elif isinstance(date, tuple):
            payload["dateFrom"] = date[0].isoformat()
            payload["dateTo"] = date[1].isoformat()
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
        if filename_prefix is not None:
            payload["filenamePrefix"] = filename_prefix
        if filename_suffix is not None:
            payload["filenameSuffix"] = filename_suffix
        return payload

    def upload_file(
        self,
        params: ProcessParams,
        full_path: Path,
        s3key: str,
        volatile: bool,
        patch: bool,
    ):
        file_info = self.storage_api.upload_product(full_path, s3key, volatile)
        payload = utils.create_product_put_payload(
            full_path,
            file_info,
            volatile,
            site=params.site.id,
        )
        if isinstance(params, ModelParams) and "evaluation" not in params.product.type:
            payload["model"] = params.model.id
        elif isinstance(params, InstrumentParams):
            payload["instrument"] = params.instrument.type
        elif isinstance(params, ProductParams) and params.instrument:
            payload["instrument"] = params.instrument.type
        payload["patch"] = patch
        self.md_api.put("files", s3key, payload)

    def update_statuses(self, raw_uuids: list[uuid.UUID], status: str):
        for raw_uuid in raw_uuids:
            payload = {"uuid": str(raw_uuid), "status": status}
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
            fields, max_alt = self._get_fields_for_plot(product)
        except NotImplementedError:
            logging.warning(f"Plotting for {product} not implemented")
            return
        options = PlotParameters()
        options.max_y = max_alt
        options.title = False
        options.subtitle = False
        options.raise_on_empty = True
        options.plot_above_ground = True
        options.minor_ticks = True
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
                self._upload_img(
                    img_path, product_s3key, product_uuid, product, field, dimensions
                )
            )
        self.md_api.put_images(visualizations, product_uuid)
        self._delete_obsolete_images(product_uuid, product, valid_images)

    def _get_fields_for_plot(self, product: str) -> tuple[list, int]:
        variables = self.md_api.get(f"api/products/{product}/variables")
        variable_ids = [var["id"] for var in variables]
        match product:
            case "lwc" | "der" | "mwr" | "mwr-single" | "mwr-multi":
                max_alt = 6
            case "drizzle":
                max_alt = 4
            case "rain-radar":
                max_alt = 3
            case _:
                max_alt = 12
        return variable_ids, max_alt

    def create_and_upload_l3_images(
        self,
        full_path: Path,
        product: str,
        model_id: str,
        product_uuid: uuid.UUID,
        product_s3key: str,
        directory: Path,
    ) -> None:
        img_path = directory / "plot.png"
        visualizations = []
        fields = _get_fields_for_l3_plot(product, model_id)
        l3_product = _full_product_to_l3_product(product)
        valid_images = []
        for stat in ("area", "error"):
            dimensions = generate_L3_day_plots(
                str(full_path),
                l3_product,
                model_id,
                var_list=fields,
                image_name=str(img_path),
                fig_type="statistic",
                stats=(stat,),
                title=False,
            )
            if len(dimensions) > 1:
                raise ValueError(f"More than one dimension in the plot: {dimensions}")
            visualizations.append(
                self._upload_img(
                    img_path, product_s3key, product_uuid, product, stat, dimensions[0]
                )
            )
            valid_images.append(stat)

        for field in fields:
            dimensions = generate_L3_day_plots(
                str(full_path),
                l3_product,
                model_id,
                var_list=[field],
                image_name=str(img_path),
                fig_type="single",
                title=False,
            )
            if len(dimensions) > 1:
                raise ValueError(f"More than one dimension in the plot: {dimensions}")
            visualizations.append(
                self._upload_img(
                    img_path, product_s3key, product_uuid, product, field, dimensions[0]
                )
            )
            valid_images.append(field)
        self.md_api.put_images(visualizations, product_uuid)
        self._delete_obsolete_images(product_uuid, product, valid_images)

    def _delete_obsolete_images(
        self, product_uuid: uuid.UUID, product: str, valid_images: list[str]
    ):
        url = f"api/visualizations/{product_uuid}"
        image_metadata = self.md_api.get(url).get("visualizations", [])
        images_on_portal = {image["productVariable"]["id"] for image in image_metadata}
        expected_images = {f"{product}-{image}" for image in valid_images}
        if obsolete_images := images_on_portal - expected_images:
            self.md_api.delete(url, {"images": obsolete_images})

    def _upload_img(
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
            "variable_id": _get_var_id(product, field),
            "dimensions": _dimensions2dict(dimensions)
            if dimensions is not None
            else None,
        }

    def upload_quality_report(
        self, full_path: Path, uuid: uuid.UUID, site: Site, product: str | None = None
    ) -> str:
        try:
            # is_dev = self.config.get("PID_SERVICE_TEST_ENV", "").lower() == "true"
            # ignore_tests = ["TestInstrumentPid"] if is_dev else None
            has_raw = site.raw_time is not None and site.raw_time.size > 0
            site_meta: quality.SiteMeta = {
                "time": site.raw_time if has_raw else None,
                "latitude": site.raw_latitude if has_raw else site.latitude,
                "longitude": site.raw_longitude if has_raw else site.longitude,
                "altitude": site.altitude,
            }
            quality_report = quality.run_tests(
                full_path,
                site_meta,
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
        if quality_report.data_coverage is not None:
            payload = {"uuid": str(uuid), "coverage": quality_report.data_coverage}
            self.md_api.post("files", payload)
        return result


def _get_var_id(cloudnet_file_type: str, field: str) -> str:
    """Return identifier for variable / Cloudnet file combination."""
    return f"{cloudnet_file_type}-{field}"


def _get_fields_for_l3_plot(product: str, model: str) -> list:
    """Return list of variables and maximum altitude for Cloudnet quicklooks.

    Args:
        product (str): Name of product, e.g., 'iwc'.
        model (str): Name of the model, e.g., 'ecmwf'.
    Returns:
        list: list of wanted variables
    """
    match product:
        case "l3-iwc":
            return [f"{model}_iwc", f"iwc_{model}"]
        case "l3-lwc":
            return [f"{model}_lwc", f"lwc_{model}"]
        case "l3-cf":
            return [f"{model}_cf", f"cf_V_{model}"]
        case unknown_product:
            raise NotImplementedError(f"Unknown product: {unknown_product}")


def _dimensions2dict(dimensions: Dimensions) -> dict:
    """Converts dimensions object to dictionary."""
    return {
        "width": dimensions.width,
        "height": dimensions.height,
        "marginTop": dimensions.margin_top,
        "marginLeft": dimensions.margin_left,
        "marginBottom": dimensions.margin_bottom,
        "marginRight": dimensions.margin_right,
    }


def _include_records_with_pattern_in_filename(metadata: list, pattern: str) -> list:
    """Includes only records with certain pattern."""
    return [
        row
        for row in metadata
        if re.search(pattern, row["filename"], flags=re.IGNORECASE)
    ]


def _exclude_records_with_pattern_in_filename(metadata: list, pattern: str) -> list:
    """Excludes records with certain pattern."""
    return [
        row
        for row in metadata
        if not re.search(pattern, row["filename"], flags=re.IGNORECASE)
    ]


def _full_product_to_l3_product(full_product: str):
    """Returns l3 product name."""
    return full_product.split("-")[1]
