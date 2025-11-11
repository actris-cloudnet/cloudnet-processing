import datetime
import importlib
import logging
from pathlib import Path
from typing import cast
from uuid import UUID

import netCDF4
from cloudnet_api_client.containers import ProductMetadata
from cloudnetpy.categorize import CategorizeInput, generate_categorize
from cloudnetpy.exceptions import CloudnetException, ModelDataError
from cloudnetpy.model_evaluation.products import product_resampling
from cloudnetpy.products import (
    generate_mwr_lhumpro,
    generate_mwr_multi,
    generate_mwr_single,
)
from cloudnetpy.products.epsilon import generate_epsilon_from_lidar
from earthcare_downloader import search
from numpy import ma
from orbital_radar import Suborbital
from requests import HTTPError

from earthcare.ec import MissingEarthCAREDataError, cloudnet_earthcare
from processing import utils
from processing.netcdf_comparer import NCDiff, nc_difference
from processing.processor import ModelParams, Processor, ProductParams
from processing.utils import RawDataMissingError, SkipTaskError, Uuid


def process_product(
    processor: Processor, params: ProductParams | ModelParams, directory: Path
) -> None:
    uuid = Uuid()
    pid_to_new_file = None
    if existing_product := processor.get_product(params):
        if existing_product.volatile:
            uuid.volatile = existing_product.uuid
        filename = existing_product.filename
        existing_file = processor.storage_api.download_product(
            existing_product, directory
        )
    else:
        filename = _generate_filename(params)
        existing_file = None

    volatile = not existing_file or uuid.volatile is not None

    try:
        if isinstance(params, ModelParams):
            new_file = _process_l3(processor, params, uuid, directory)
        elif params.product.id in ("mwr-single", "mwr-multi"):
            new_file = _process_mwrpy(processor, params, uuid, directory)
        elif params.product.id in ("categorize", "categorize-voodoo"):
            new_file = _process_categorize(processor, params, uuid, directory)
        elif params.product.id == "cpr-simulation":
            new_file = _process_cpr_simulation(processor, params, uuid, directory)
        elif params.product.id == "cpr-validation":
            new_file = _process_cpr_validation(processor, params, uuid, directory)
        elif params.product.id == "epsilon-lidar":
            new_file = _process_epsilon_from_lidar(processor, params, uuid, directory)
        else:
            new_file = _process_level2(processor, params, uuid, directory)
    except CloudnetException as err:
        raise utils.SkipTaskError(str(err)) from err

    if not params.product.experimental:
        processor.pid_utils.add_pid_to_file(new_file, pid_to_new_file)

    utils.add_global_attributes(
        new_file,
        params.instrument.pid
        if isinstance(params, ProductParams) and params.instrument
        else None,
    )

    upload = True
    patch = False
    if existing_product and existing_file:
        difference = nc_difference(existing_file, new_file)
        if difference == NCDiff.NONE:
            upload = False
            new_file = existing_file
            uuid.product = existing_product.uuid
        elif difference == NCDiff.MINOR:
            # Replace existing file
            patch = True
            if not params.product.experimental:
                processor.pid_utils.add_pid_to_file(new_file, existing_product.pid)
            with netCDF4.Dataset(new_file, "r+") as nc:
                nc.file_uuid = str(existing_product.uuid)
            uuid.product = existing_product.uuid

    if upload:
        processor.upload_file(params, new_file, filename, volatile, patch)
    else:
        logging.info("Skipping PUT to data portal, file has not changed")

    if isinstance(params, ModelParams):
        processor.create_and_upload_l3_images(
            new_file,
            params.product.id,
            params.model.id,
            uuid.product,
            filename,
            directory,
        )
    else:
        processor.create_and_upload_images(
            new_file,
            params.product.id,
            uuid.product,
            filename,
            directory,
        )
    qc_result = processor.upload_quality_report(
        new_file, uuid.product, params.site, params.product.id
    )
    utils.print_info(uuid, volatile, patch, upload, qc_result)
    if processor.md_api.config.is_production and isinstance(params, ProductParams):
        _update_dvas_metadata(processor, uuid.product)


def _generate_filename(params: ProductParams | ModelParams) -> str:
    match params.product.id:
        case "mwr-single" | "mwr-multi":
            assert isinstance(params, ProductParams) and params.instrument
            identifier = params.product.id.replace(
                "mwr", params.instrument.instrument_id
            )
        case "iwc":
            identifier = "iwc-Z-T-method"
        case "lwc":
            identifier = "lwc-scaled-adiabatic"
        case product_id:
            identifier = product_id
    parts = [params.date.strftime("%Y%m%d"), params.site.id, identifier]
    if isinstance(params, ProductParams) and params.instrument:
        parts.append(str(params.instrument.uuid)[:8])
    elif isinstance(params, ModelParams):
        parts.append(params.model.id)
    return "_".join(parts) + ".nc"


def _process_mwrpy(
    processor: Processor, params: ProductParams, uuid: Uuid, directory: Path
) -> Path:
    if params.instrument is None:
        raise RuntimeError("Instrument is None")
    metadata = processor.client.files(
        site_id=params.site.id,
        date=params.date,
        product_id="mwr-l1c",
        instrument_pid=params.instrument.pid,
    )
    _check_response(metadata, "mwr-l1c")
    l1c_file = processor.storage_api.download_product(metadata[0], directory)

    output_file = directory / "output.nc"
    if params.product.id == "mwr-single":
        fun = (
            generate_mwr_lhumpro
            if params.instrument.instrument_id == "lhumpro"
            else generate_mwr_single
        )
        offsets_previous_date = _get_lwp_offset(processor, params, date_diff=-1)
        offsets_next_date = _get_lwp_offset(processor, params, date_diff=1)
        lwp_offset = (offsets_previous_date[-1], offsets_next_date[0])
        uuid.product = fun(l1c_file, output_file, uuid.volatile, lwp_offset=lwp_offset)
        with netCDF4.Dataset(output_file, "r") as nc:
            offset = nc.variables["lwp_offset"][:]
            valid = offset[~ma.getmaskarray(offset)]
            values: list[float | None] = (
                [None, None] if valid.size == 0 else [float(valid[0]), float(valid[-1])]
            )
            body = {"lwpOffset": values}
            processor.md_api.put_calibration(params.instrument.pid, params.date, body)
    else:
        if params.instrument.instrument_id == "lhumpro":
            raise utils.SkipTaskError("Cannot generate mwr-multi from LHUMPRO")
        uuid.product = generate_mwr_multi(l1c_file, output_file, uuid.volatile)
    return output_file


def _get_lwp_offset(
    processor: Processor, params: ProductParams, date_diff: int
) -> tuple[float | None, float | None]:
    if params.instrument is None:
        return (None, None)
    payload = {
        "date": params.date + datetime.timedelta(days=date_diff),
        "instrumentPid": params.instrument.pid,
        "strictDate": True,
    }
    try:
        res = processor.md_api.get("api/calibration", payload)
        offsets = res.get("data", {}).get("lwpOffset")
        if offsets is not None:
            return offsets
        return (None, None)
    except HTTPError as e:
        return (None, None)


def _process_cpr_simulation(
    processor: Processor, params: ProductParams, uuid: Uuid, directory: Path
) -> Path:
    _check_cpr_date(params)
    metadata = processor.client.files(
        site_id=params.site.id,
        date=params.date,
        product_id="categorize",
    )
    _check_response(metadata, "categorize")
    _check_is_overpass(params)
    categorize_file = processor.storage_api.download_product(metadata[0], directory)
    output_file = directory / "output.nc"
    orbital = Suborbital()
    uuid_str = orbital.simulate_cloudnet(
        str(categorize_file),
        str(output_file),
        mean_wind=6,
        uuid=str(uuid.volatile) if uuid.volatile is not None else None,
    )
    uuid.product = UUID(uuid_str)
    utils.add_global_attributes(output_file)
    return output_file


def _process_cpr_validation(
    processor: Processor, params: ProductParams, uuid: Uuid, directory: Path
) -> Path:
    _check_cpr_date(params)
    cpr_simu_metadata = processor.client.files(
        site_id=params.site.id,
        date=params.date,
        product_id="cpr-simulation",
    )
    _check_response(cpr_simu_metadata, "cpr-simulation")
    cpr_simu_file = processor.storage_api.download_product(
        cpr_simu_metadata[0], directory
    )
    output_file = directory / "output.nc"
    try:
        uuid_str = cloudnet_earthcare(
            params.site.id,
            cpr_simu_file,
            output_file,
            cache_dir=directory
            if processor.md_api.config.is_production
            else Path("/tmp"),
            uuid=uuid.volatile if uuid.volatile is not None else None,
        )
    except MissingEarthCAREDataError:
        raise SkipTaskError("Missing EarthCARE data")
    uuid.product = UUID(uuid_str)
    utils.add_global_attributes(output_file)
    return output_file


def _check_cpr_date(params: ProductParams) -> None:
    earthcare_launch_date = datetime.date(2024, 5, 28)
    if params.date < earthcare_launch_date:
        raise SkipTaskError("CPR products only feasible for dates before 2024-05-28")


def _check_is_overpass(params: ProductParams) -> None:
    distance_km = 200
    files = search(
        product="CPR_NOM_1B",
        date=params.date,
        lat=params.site.latitude,
        lon=params.site.longitude,
        radius=distance_km,
    )
    if not files:
        raise SkipTaskError("No EarthCARE CPR overpass found.")


def _process_epsilon_from_lidar(
    processor: Processor, params: ProductParams, uuid: Uuid, directory: Path
) -> Path:
    if params.instrument is None:
        raise RuntimeError("Instrument is None")

    metadata_stare = processor.client.files(
        site_id=params.site.id,
        date=params.date,
        product_id="doppler-lidar",
        instrument_pid=params.instrument.pid,
    )
    _check_response(metadata_stare, "doppler-lidar")

    metadata_wind = processor.client.files(
        site_id=params.site.id,
        date=params.date,
        product_id="doppler-lidar-wind",
        instrument_pid=params.instrument.pid,
    )
    _check_response(metadata_wind, "doppler-lidar-wind")

    metadata_wind = sorted(
        metadata_wind,
        key=lambda meta: -1
        if meta.instrument is not None
        and params.instrument is not None
        and meta.instrument.pid == params.instrument.pid
        else 1,
    )

    file_lidar, file_wind = processor.storage_api.download_products(
        [metadata_stare[0], metadata_wind[0]], directory
    )

    output_file = directory / "output.nc"
    uuid.product = generate_epsilon_from_lidar(
        file_lidar, file_wind, output_file, uuid.volatile
    )
    return output_file


def _process_categorize(
    processor: Processor, params: ProductParams, uuid: Uuid, directory: Path
) -> Path:
    options = _get_categorize_options(params)
    is_voodoo = params.product.id == "categorize-voodoo"
    meta_records = _get_level1b_metadata_for_categorize(processor, params, is_voodoo)
    paths = processor.storage_api.download_products(meta_records.values(), directory)
    input_files = cast(CategorizeInput, dict(zip(meta_records.keys(), paths)))
    if is_voodoo:
        input_files["lv0_files"], lv0_uuid = _get_input_files_for_voodoo(
            processor, params, directory, meta_records["radar"]
        )
    else:
        lv0_uuid = []
    output_path = directory / "output.nc"
    try:
        uuid.product = generate_categorize(
            input_files, output_path, uuid=uuid.volatile, options=options
        )
        uuid.raw.extend(lv0_uuid)
    except ModelDataError as exc:
        gdas1_meta = processor.get_product(params, product_id="model", model_id="gdas1")
        if not gdas1_meta:
            raise SkipTaskError("Bad model data and no gdas1") from exc
        input_files["model"] = processor.storage_api.download_product(
            gdas1_meta, directory
        )
        uuid.product = generate_categorize(input_files, output_path, uuid=uuid.volatile)
    return output_path


def _get_categorize_options(params: ProductParams) -> dict | None:
    key = "temperature_offset"
    if params.site.id == "schneefernerhaus":
        return {key: -7}
    if params.site.id == "granada":
        return {key: 3}
    return None


def _process_l3(
    processor: Processor, params: ModelParams, uuid: Uuid, directory: Path
) -> Path:
    model_meta = processor.client.files(
        site_id=params.site.id,
        date=params.date,
        product_id="model",
        model_id=params.model.id,
    )
    _check_response(model_meta, "model")

    model_file = processor.storage_api.download_product(model_meta[0], directory)
    l3_prod = params.product.id.split("-")[1]
    source = "categorize" if l3_prod == "cf" else l3_prod

    product_meta = processor.client.files(
        site_id=params.site.id,
        date=params.date,
        product_id=source,
    )
    _check_response(product_meta, source)

    product_file = processor.storage_api.download_product(product_meta[0], directory)
    output_file = directory / "output.nc"
    uuid.product = product_resampling.process_L3_day_product(
        params.model.id,
        l3_prod,
        [model_file],
        product_file,
        output_file,
        uuid=uuid.volatile,
        overwrite=True,
    )
    return output_file


def _process_level2(
    processor: Processor, params: ProductParams, uuid: Uuid, directory: Path
) -> Path:
    if params.product.id == "classification-voodoo":
        cat_file = "categorize-voodoo"
        module_name = "classification"
    else:
        cat_file = "categorize"
        module_name = params.product.id

    metadata = processor.client.files(
        site_id=params.site.id,
        date=params.date,
        product_id=cat_file,
    )
    _check_response(metadata, cat_file)

    categorize_file = processor.storage_api.download_product(metadata[0], directory)
    module = importlib.import_module(f"cloudnetpy.products.{module_name}")
    prod = (
        "classification"
        if params.product.id == "classification-voodoo"
        else params.product.id.replace("-", "_")
    )
    output_file = directory / "output.nc"
    fun = getattr(module, f"generate_{prod}")
    uuid.product = fun(categorize_file, output_file, uuid=uuid.volatile)
    return output_file


def _get_level1b_metadata_for_categorize(
    processor: Processor, params: ProductParams, is_voodoo: bool
) -> dict[str, ProductMetadata]:
    meta_records = {
        "model": processor.get_product(params, product_id="model"),
        "mwr": (
            processor.find_instrument_product(params, "mwr-single")
            or processor.find_instrument_product(
                params, "mwr", fallback=["hatpro", "radiometrics"]
            )
            or processor.find_instrument_product(
                params, "radar", require=["rpg-fmcw-35", "rpg-fmcw-94"]
            )
        ),
        "radar": (
            processor.find_instrument_product(params, "radar", require=["rpg-fmcw-94"])
            if is_voodoo
            else processor.find_instrument_product(
                params,
                "radar",
                fallback=["mira-35", "rpg-fmcw-35", "rpg-fmcw-94", "copernicus"],
                exclude=["mira-10"],
            )
        ),
        "lidar": processor.find_optimal_lidar(params),
        "disdrometer": processor.find_instrument_product(
            params, "disdrometer", fallback=["thies-lnm", "parsivel"]
        ),
    }
    optional_products = ["disdrometer", "mwr"]
    for product, metadata in meta_records.items():
        if product not in optional_products and metadata is None:
            raise SkipTaskError(f"Missing required input product: {product}")
    return {key: value for key, value in meta_records.items() if value is not None}


def _get_input_files_for_voodoo(
    processor: Processor,
    params: ProductParams,
    directory: Path,
    metadata: ProductMetadata,
) -> tuple[list[Path], list[UUID]]:
    assert metadata.instrument is not None
    try:
        (
            full_paths,
            uuids,
        ) = processor.download_instrument(
            site_id=params.site.id,
            date=params.date,
            instrument_id="rpg-fmcw-94",
            instrument_pid=metadata.instrument.pid,
            include_pattern=".LV0",
            directory=directory,
        )
    except RawDataMissingError:
        raise SkipTaskError("Missing rpg-fmcw-94 Level 0 data.")
    return full_paths, uuids


def _update_dvas_metadata(processor: Processor, uuid: UUID) -> None:
    meta = processor.client.file(uuid)
    processor.dvas.upload(meta)


def _check_response(metadata: list[ProductMetadata], product: str) -> None:
    if len(metadata) == 0:
        raise SkipTaskError(f"Missing required input product: {product}")
    if len(metadata) > 1:
        raise RuntimeError("Multiple products found")
