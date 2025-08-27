import datetime
import logging
from dataclasses import asdict, dataclass
from pathlib import Path
from uuid import UUID

import numpy as np
import numpy.typing as npt
from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import (
    ProductMetadata,
    RawMetadata,
    RawModelMetadata,
)
from cloudnetpy.exceptions import PlottingError
from cloudnetpy.model_evaluation.plotting.plotting import generate_L3_day_plots
from cloudnetpy.plotting import Dimensions, PlotParameters, generate_figure
from cloudnetpy_qc import quality
from cloudnetpy_qc.quality import ErrorLevel

import housekeeping
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
    raw_time: npt.NDArray[np.datetime64]
    raw_latitude: npt.NDArray[np.float64]
    raw_longitude: npt.NDArray[np.float64]


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
    ) -> None:
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
        loc1 = self.client.moving_site_locations(site.id, prev_date)
        loc2 = self.client.moving_site_locations(site.id, date)
        loc3 = self.client.moving_site_locations(site.id, next_date)
        locations = [loc1[-1]] + loc2 + [loc3[0]]
        site_dict = asdict(site)
        site_dict["latitude"] = mean_location.latitude
        site_dict["longitude"] = mean_location.longitude
        site_dict["raw_time"] = np.array([t.time for t in locations])
        site_dict["raw_latitude"] = np.array([t.latitude for t in locations])
        site_dict["raw_longitude"] = np.array([t.longitude for t in locations])
        return ExtendedSite(**site_dict)

    def get_product(
        self,
        params: ProcessParams,
        product_id: str | None = None,
        model_id: str | None = None,
    ) -> ProductMetadata | None:
        if isinstance(params, (InstrumentParams, ProductParams)):
            instrument_pid = params.instrument.pid if params.instrument else None
            model = None
        else:
            assert isinstance(params, ModelParams)
            instrument_pid = None
            model = params.model.id

        metadata = self.client.files(
            site_id=params.site.id,
            product_id=product_id or params.product.id,
            date=params.date,
            show_legacy=True,
            instrument_pid=instrument_pid,
            model_id=model_id or model,
        )
        if len(metadata) == 0:
            return None
        if len(metadata) > 1:
            raise RuntimeError(f"Multiple {params.product.id} files found")
        return metadata[0]

    def get_raw_model_files(
        self, params: ModelParams, start_date: datetime.date, end_date: datetime.date
    ) -> list[RawModelMetadata]:
        rows = self.client.raw_model_files(
            site_id=params.site.id,
            model_id=params.model.source_model_id or params.model.id,
            date_from=start_date,
            date_to=end_date,
            status=["uploaded", "processed"],
        )
        return [row for row in rows if row.size > MIN_MODEL_FILESIZE]

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
        allow_empty: bool = False,
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
            upload_metadata = [max(upload_metadata, key=lambda item: item.size)]

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

    def upload_file(
        self,
        params: ProcessParams,
        full_path: Path,
        s3key: str,
        volatile: bool,
        patch: bool,
    ) -> None:
        file_info = self.storage_api.upload_product(full_path, s3key, volatile)
        payload = utils.create_product_put_payload(
            full_path,
            file_info,
            params.site.id,
            volatile,
            patch,
        )
        if isinstance(params, ModelParams) and "evaluation" not in params.product.type:
            payload["model"] = params.model.id
        elif (
            isinstance(params, (InstrumentParams, ProductParams)) and params.instrument
        ):
            payload["instrument"] = params.instrument.instrument_id
        self.md_api.put("files", s3key, payload)

    def update_statuses(self, raw_uuids: list[str], status: str) -> None:
        for raw_uuid in raw_uuids:
            payload = {"uuid": raw_uuid, "status": status}
            self.md_api.post("upload-metadata", payload)

    def create_and_upload_images(
        self,
        filepath: Path,
        product_id: str,
        uuid: UUID,
        s3key: str,
        directory: Path,
        legacy: bool = False,
    ) -> None:
        img_path = directory / "plot.png"
        visualizations = []
        s3key = f"legacy/{s3key}" if legacy is True else s3key
        try:
            fields, max_alt = self._get_fields_for_plot(product_id)
        except NotImplementedError:
            logging.warning(f"Plotting for {product_id} not implemented")
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
                    filepath,
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
                self._upload_img(img_path, s3key, uuid, product_id, field, dimensions)
            )
        self.md_api.put_images(visualizations, uuid)
        self._delete_obsolete_images(uuid, product_id, valid_images)

    def _get_fields_for_plot(self, product_id: str) -> tuple[list, int]:
        variables = self.md_api.get(f"api/products/{product_id}/variables")
        variable_ids = [var["id"] for var in variables]
        match product_id:
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
        filepath: Path,
        product_id: str,
        model_id: str,
        uuid: UUID,
        s3key: str,
        directory: Path,
    ) -> None:
        img_path = directory / "plot.png"
        visualizations = []
        fields = _get_fields_for_l3_plot(product_id, model_id)
        l3_product = product_id.split("-")[1]
        valid_images = []
        for stat in ("area", "error"):
            dimensions = generate_L3_day_plots(
                str(filepath),
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
                self._upload_img(img_path, s3key, uuid, product_id, stat, dimensions[0])
            )
            valid_images.append(stat)

        for field in fields:
            dimensions = generate_L3_day_plots(
                str(filepath),
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
                    img_path, s3key, uuid, product_id, field, dimensions[0]
                )
            )
            valid_images.append(field)
        self.md_api.put_images(visualizations, uuid)
        self._delete_obsolete_images(uuid, product_id, valid_images)

    def _delete_obsolete_images(
        self, uuid: UUID, product_id: str, valid_images: list[str]
    ) -> None:
        url = f"api/visualizations/{uuid}"
        image_metadata = self.md_api.get(url).get("visualizations", [])
        images_on_portal = {image["productVariable"]["id"] for image in image_metadata}
        expected_images = {f"{product_id}-{image}" for image in valid_images}
        if obsolete_images := images_on_portal - expected_images:
            self.md_api.delete(url, {"images": obsolete_images})

    def _upload_img(
        self,
        img_path: Path,
        s3key: str,
        uuid: UUID,
        product_id: str,
        field: str,
        dimensions: Dimensions,
    ) -> dict:
        img_s3key = s3key.replace(".nc", f"-{uuid.hex[:8]}-{field}.png")
        self.storage_api.upload_image(full_path=img_path, s3key=img_s3key)
        return {
            "s3key": img_s3key,
            "variable_id": f"{product_id}-{field}",
            "dimensions": _dimensions2dict(dimensions)
            if dimensions is not None
            else None,
        }

    def upload_quality_report(
        self,
        filepath: Path,
        uuid: UUID | str,
        site: Site | ExtendedSite,
        product_id: str | None = None,
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
                filepath,
                site_meta,
                product=product_id,
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

    def process_housekeeping(self, params: InstrumentParams) -> None:
        if params.date < utils.utctoday() - self.md_api.config.housekeeping_retention:
            logging.info("Skipping housekeeping for old data")
            return
        logging.info("Processing housekeeping data")
        records = self._get_housekeeping_records(params)
        try:
            with housekeeping.Database() as db:
                for record in records:
                    housekeeping.process_record(record, client=self.client, db=db)
        except housekeeping.HousekeepingException:
            logging.exception("Housekeeping failed")

    def _get_housekeeping_records(self, params: InstrumentParams) -> list[RawMetadata]:
        if params.instrument.instrument_id == "halo-doppler-lidar":
            first_day_of_month = params.date.replace(day=1)
            records = self.client.raw_files(
                site_id=params.site.id,
                date_from=first_day_of_month,
                date_to=params.date,
                instrument_pid=params.instrument.pid,
                filename_prefix="system_parameters",
            )
            return _select_halo_doppler_lidar_hkd_records(records)
        return self.client.raw_files(
            site_id=params.site.id,
            date=params.date,
            instrument_pid=params.instrument.pid,
        )


def _select_halo_doppler_lidar_hkd_records(
    records: list[RawMetadata],
) -> list[RawMetadata]:
    if not records:
        return []
    return [
        max(
            records,
            key=lambda x: (
                x.measurement_date,
                x.created_at,
                x.updated_at,
                x.size,
            ),
        )
    ]


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
