import datetime
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from uuid import UUID

import numpy as np
from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import ProductMetadata, RawModelMetadata
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


from cloudnet_api_client.containers import (
    ExtendedInstrument,
    ExtendedProduct,
    Model,
    Site,
)


@dataclass(frozen=True, slots=True)
class ExtendedSite(Site):
    raw_time: np.ndarray
    raw_latitude: np.ndarray
    raw_longitude: np.ndarray


@dataclass(frozen=True)
class ProcessParams:
    site: Site | ExtendedSite
    date: datetime.date
    product: ExtendedProduct


@dataclass(frozen=True)
class ModelParams(ProcessParams):
    model: Model


@dataclass(frozen=True)
class InstrumentParams(ProcessParams):
    instrument: ExtendedInstrument


@dataclass(frozen=True)
class ProductParams(ProcessParams):
    instrument: ExtendedInstrument | None


class Processor:
    def __init__(
        self,
        md_api: MetadataApi,
        storage_api: StorageApi,
        pid_utils: PidUtils,
        dvas: Dvas,
        client: APIClient,
    ):
        self.md_api = md_api
        self.storage_api = storage_api
        self.pid_utils = pid_utils
        self.dvas = dvas
        self.client = client

    def get_site(self, site_id: str, date: datetime.date) -> Site | ExtendedSite:
        site = self.client.site(site_id)
        if site.latitude is None and site.longitude is None:
            return self._create_extended_site(site, date)
        return site

    def _create_extended_site(self, site: Site, date: datetime.date) -> ExtendedSite:
        mean_location = self.client.moving_site_mean_location(site.id, date)
        prev_date = date - datetime.timedelta(days=1)
        next_date = date + datetime.timedelta(days=1)
        loc1 = self.client.moving_site_locations(site.id, prev_date, -1)
        loc2 = self.client.moving_site_locations(site.id, date)
        loc3 = self.client.moving_site_locations(site.id, next_date, 0)
        locations = loc1 + loc2 + loc3
        site_dict = asdict(site)
        site_dict["latitude"] = mean_location.latitude
        site_dict["longitude"] = mean_location.longitude
        site_dict["raw_time"] = [t.time for t in locations]
        site_dict["raw_latitude"] = [t.latitude for t in locations]
        site_dict["raw_longitude"] = [t.longitude for t in locations]
        return ExtendedSite(**site_dict)

    def get_model_upload(
        self, params: ModelParams, start_date: datetime.date, end_date: datetime.date
    ) -> list[RawModelMetadata]:
        rows = self.client.raw_model_files(
            site_id=params.site.id,
            model_id=params.model.source_model_id or params.model.id,
            date_from=start_date,
            date_to=end_date,
            status=["uploaded", "processed"],
        )
        return [row for row in rows if int(row.size) > MIN_MODEL_FILESIZE]

    def get_model_file(self, params: ModelParams) -> ProductMetadata | None:
        metadata = self.client.files(
            product="model",
            model_id=params.model.id,
            site_id=params.site.id,
            date=params.date,
        )
        if len(metadata) == 0:
            return None
        if len(metadata) > 1:
            raise RuntimeError(f"Multiple {params.model.id} files found")
        return metadata[0]

    def fetch_product(self, params: ProcessParams) -> ProductMetadata | None:
        if isinstance(params, InstrumentParams):
            instrument_pid = params.instrument.pid
        elif isinstance(params, ProductParams) and params.instrument is not None:
            instrument_pid = params.instrument.pid
        else:
            instrument_pid = None
        metadata = self.client.files(
            site_id=params.site.id,
            product=params.product.id,
            date=params.date,
            show_legacy=True,
            instrument_pid=instrument_pid,
        )
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
    ) -> tuple[list[Path], list[str]]:
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
        upload_metadata = self.client.raw_files(
            site_id,
            date_from=start_date_ext,
            date_to=end_date_ext,
            instrument_id=instrument_id,
            instrument_pid=instrument_pid,
            filename_prefix=filename_prefix,
            filename_suffix=filename_suffix,
            status=["uploaded", "processed"],
        )
        if include_pattern:
            upload_metadata = self.client.filter(
                upload_metadata, include_pattern=include_pattern
            )
        if exclude_pattern:
            upload_metadata = self.client.filter(
                upload_metadata, exclude_pattern=exclude_pattern
            )
        if include_tag_subset:
            upload_metadata = self.client.filter(
                upload_metadata, include_tag_subset=include_tag_subset
            )
        if exclude_tag_subset:
            upload_metadata = self.client.filter(
                upload_metadata, exclude_tag_subset=exclude_tag_subset
            )

        if not upload_metadata:
            if allow_empty:
                return [], []
            else:
                raise utils.RawDataMissingError
        if largest_only:
            upload_metadata = [max(upload_metadata, key=lambda item: int(item.size))]

        full_paths, uuids = self.storage_api.download_raw_data(
            upload_metadata, directory
        )

        if time_offset is not None:
            uuids = [
                str(meta.uuid)
                for meta in upload_metadata
                if start_date <= meta.measurement_date <= end_date
            ]
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
            payload["instrument"] = params.instrument.instrument_id
        elif isinstance(params, ProductParams) and params.instrument:
            payload["instrument"] = params.instrument.instrument_id
        payload["patch"] = patch
        self.md_api.put("files", s3key, payload)

    def update_statuses(self, raw_uuids: list[str], status: str) -> None:
        for raw_uuid in raw_uuids:
            payload = {"uuid": raw_uuid, "status": status}
            self.md_api.post("upload-metadata", payload)

    def create_and_upload_images(
        self,
        full_path: Path,
        product: str,
        product_uuid: UUID,
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
        product_uuid: UUID,
        product_s3key: str,
        directory: Path,
    ) -> None:
        img_path = directory / "plot.png"
        visualizations = []
        fields = _get_fields_for_l3_plot(product, model_id)
        l3_product = product.split("-")[1]
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
        self, product_uuid: UUID, product: str, valid_images: list[str]
    ) -> None:
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
        product_uuid: UUID,
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
        self,
        full_path: Path,
        uuid: UUID,
        site: Site | ExtendedSite,
        product: str | None = None,
    ) -> str:
        try:
            site_meta: quality.SiteMeta = {
                "time": site.raw_time if isinstance(site, ExtendedSite) else None,
                "latitude": site.raw_latitude
                if isinstance(site, ExtendedSite)
                else site.latitude,
                "longitude": site.raw_longitude
                if isinstance(site, ExtendedSite)
                else site.longitude,
                "altitude": site.altitude,
            }
            quality_report = quality.run_tests(
                full_path,
                site_meta,
                product=product,
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
    return f"{cloudnet_file_type}-{field}"


def _get_fields_for_l3_plot(product: str, model: str) -> list:
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
    return {
        "width": dimensions.width,
        "height": dimensions.height,
        "marginTop": dimensions.margin_top,
        "marginLeft": dimensions.margin_left,
        "marginBottom": dimensions.margin_bottom,
        "marginRight": dimensions.margin_right,
    }
