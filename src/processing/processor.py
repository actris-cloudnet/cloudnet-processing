import datetime
import logging
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

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


@dataclass(frozen=True)
class Instrument:
    uuid: str
    pid: str
    type: str
    derived_product_ids: frozenset[str]


@dataclass(frozen=True)
class Site:
    id: str
    name: str
    latitude: float
    longitude: float
    altitude: float
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
    model_id: str


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

    def get_site(self, site_id: str) -> Site:
        site = self.md_api.get(f"api/sites/{site_id}")
        return Site(
            id=site["id"],
            name=site["humanReadableName"],
            latitude=site["latitude"],
            longitude=site["longitude"],
            altitude=site["altitude"],
            types=set(site["type"]),
        )

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

    def download_raw_data(
        self, upload_metadata: list[dict], directory: Path
    ) -> tuple[list, list]:
        return self.storage_api.download_raw_data(upload_metadata, directory)

    def get_model_upload(self, params: ModelParams) -> dict | None:
        payload = {
            "site": params.site.id,
            "date": params.date.isoformat(),
            "model": params.model_id,
        }
        rows = self.md_api.get("api/raw-model-files", payload)
        rows = [row for row in rows if int(row["size"]) > MIN_MODEL_FILESIZE]
        if len(rows) == 0:
            return None
        if len(rows) > 1:
            raise ValueError("Multiple model files found")
        return rows[0]

    def get_model_file(self, params: ModelParams) -> dict | None:
        payload = {
            "site": params.site.id,
            "date": params.date.isoformat(),
            "model": params.model_id,
        }
        metadata = self.md_api.get("api/model-files", payload)
        if len(metadata) == 0:
            return None
        if len(metadata) > 1:
            raise RuntimeError(f"Multiple {params.model_id} files found")
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
        instrument_pid: str | None = None,
        include_pattern: str | None = None,
        largest_only: bool = False,
        exclude_pattern: str | None = None,
        include_tag_subset: set[str] | None = None,
        exclude_tag_subset: set[str] | None = None,
        date: datetime.date | tuple[datetime.date, datetime.date] | None = None,
        allow_empty=False,
        filename_prefix: str | None = None,
        filename_suffix: str | None = None,
    ):
        """Download raw files matching the given parameters."""
        payload = self._get_payload(
            site=site_id,
            date=date,
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
        if largest_only:
            return full_paths[0], uuids
        return full_paths, uuids

    def download_adjoining_daily_files(
        self,
        params: InstrumentParams,
        directory: Path,
    ) -> tuple[list[Path], list[uuid.UUID]]:
        """Download raw files from today and tomorrow to handle non-UTC timestamps."""
        next_day = params.date + datetime.timedelta(days=1)
        payload = self._get_payload(
            site=params.site.id,
            date=(params.date, next_day),
            instrument_pid=params.instrument.pid,
            skip_created=True,
        )
        upload_metadata = self.md_api.get("api/raw-files", payload)
        upload_metadata = _order_metadata(upload_metadata)
        if not upload_metadata:
            raise utils.RawDataMissingError
        full_paths, _ = self.storage_api.download_raw_data(upload_metadata, directory)
        # Return all full paths but only current day UUIDs
        uuids_of_current_day = [
            meta["uuid"]
            for meta in upload_metadata
            if meta["measurementDate"] == params.date.isoformat()
        ]
        return full_paths, uuids_of_current_day

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

    def upload_file(self, params: ProcessParams, full_path: Path, s3key: str):
        file_info = self.storage_api.upload_product(full_path, s3key)
        payload = utils.create_product_put_payload(
            full_path, file_info, site=params.site.id
        )
        if isinstance(params, ModelParams) and "evaluation" not in params.product.type:
            payload["model"] = params.model_id
        elif isinstance(params, InstrumentParams):
            payload["instrument"] = params.instrument.type
        elif isinstance(params, ProductParams) and params.instrument:
            payload["instrument"] = params.instrument.type
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
            fields, max_alt = _get_fields_for_plot(product)
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
                self._upload_img(
                    img_path, product_s3key, product_uuid, product, field, dimensions
                )
            )
        self.md_api.put_images(visualizations, product_uuid)
        self._delete_obsolete_images(product_uuid, product, valid_images)

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


def _get_var_id(cloudnet_file_type: str, field: str) -> str:
    """Return identifier for variable / Cloudnet file combination."""
    return f"{cloudnet_file_type}-{field}"


def _get_fields_for_plot(cloudnet_file_type: str) -> tuple[list, int]:
    """Return list of variables and maximum altitude for Cloudnet quicklooks.

    Args:
        cloudnet_file_type (str): Name of Cloudnet file type, e.g., 'classification'.

    Returns:
        tuple: 2-element tuple containing feasible variables for plots
        (list) and maximum altitude (int).

    """
    max_alt = 12
    match cloudnet_file_type:
        case "categorize-voodoo":
            fields = ["v", "liquid_prob"]
        case "categorize":
            fields = [
                "Z",
                "v",
                "width",
                "ldr",
                "sldr",
                "v_sigma",
                "beta",
                "lwp",
                "Tw",
                "radar_gas_atten",
                "radar_liquid_atten",
                "radar_rain_atten",
                "radar_melting_atten",
                "rainfall_rate",
                "Z_error",
            ]
        case "classification":
            fields = ["target_classification", "detection_status"]
        case "classification-voodoo":
            fields = ["target_classification", "detection_status"]
        case "iwc":
            fields = ["iwc", "iwc_error", "iwc_retrieval_status"]
        case "lwc":
            fields = ["lwc", "lwc_error", "lwc_retrieval_status"]
            max_alt = 6
        case "ier":
            fields = ["ier", "ier_error", "ier_retrieval_status"]
        case "der":
            fields = ["der", "der_error", "der_retrieval_status"]
            max_alt = 6
        case "model":
            fields = [
                "cloud_fraction",
                "uwind",
                "vwind",
                "temperature",
                "q",
                "pressure",
            ]
        case "lidar":
            fields = [
                "beta",
                "beta_raw",
                "depolarisation",
                "depolarisation_raw",
                "beta_1064",
                "beta_532",
                "beta_355",
                "depolarisation_532",
                "depolarisation_355",
            ]
        case "doppler-lidar":
            fields = [
                "beta",
                "beta_raw",
                "v",
                "depolarisation",
                "depolarisation_raw",
            ]
        case "doppler-lidar-wind":
            fields = [
                "uwind",
                "uwind_raw",
                "vwind",
                "vwind_raw",
            ]
        case "mwr":
            fields = ["lwp", "iwv"]
        case "mwr-l1c":
            fields = [
                "tb_0",
                "tb_01",
                "tb_02",
                "tb_03",
                "tb_04",
                "tb_05",
                "tb_06",
                "tb_07",
                "tb_08",
                "tb_09",
                "tb_10",
                "tb_11",
                "tb_12",
                "tb_13",
                "irt_0",
                "irt_01",
            ]
        case "mwr-single":
            fields = [
                "lwp",
                "iwv",
                "temperature",
                "absolute_humidity",
                "relative_humidity",
                "potential_temperature",
                "equivalent_potential_temperature",
            ]
            max_alt = 6
        case "mwr-multi":
            fields = [
                "temperature",
                "relative_humidity",
                "potential_temperature",
                "equivalent_potential_temperature",
            ]
            max_alt = 6
        case "radar":
            fields = [
                "Zh",
                "v",
                "width",
                "ldr",
                "sldr",
                "zdr",
                "rho_hv",
                "srho_hv",
                "rho_cx",
                "lwp",
                "rainfall_rate",
            ]
        case "disdrometer":
            fields = [
                "rainfall_rate",
                "snowfall_rate",
                "n_particles",
                "number_concentration",
                "fall_velocity",
            ]
        case "drizzle":
            fields = ["Do", "drizzle_N"]
            max_alt = 4
        case "weather-station":
            fields = [
                "air_temperature",
                "wind_speed",
                "wind_direction",
                "air_pressure",
                "relative_humidity",
                "rainfall_rate",
                "rainfall_amount",
            ]
        case "rain-radar":
            fields = [
                "Zh",
                "lwc",
                "pia",
                "rainfall_rate",
                "v",
                "width",
            ]
            max_alt = 3
        case _:
            raise NotImplementedError(cloudnet_file_type)
    return fields, max_alt


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


def _order_metadata(metadata: list) -> list:
    """Orders 2-element metadata according to measurementDate."""
    key = "measurementDate"
    if len(metadata) == 2 and metadata[0][key] > metadata[1][key]:
        metadata.reverse()
    return metadata


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
