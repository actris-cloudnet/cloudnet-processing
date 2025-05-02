import datetime
import importlib
import logging
import uuid as std_uuid
from pathlib import Path

import netCDF4
from cloudnetpy.categorize import generate_categorize
from cloudnetpy.exceptions import CloudnetException, ModelDataError
from cloudnetpy.model_evaluation.products import product_resampling
from cloudnetpy.products import (
    generate_mwr_lhumpro,
    generate_mwr_multi,
    generate_mwr_single,
)
from cloudnetpy.products.epsilon import generate_epsilon_from_lidar
from orbital_radar import Suborbital
from requests import HTTPError

from processing import utils
from processing.netcdf_comparer import NCDiff, nc_difference
from processing.processor import ModelParams, Processor, ProductParams
from processing.utils import SkipTaskError, Uuid


def process_me(processor: Processor, params: ModelParams, directory: Path):
    uuid = Uuid()
    pid_to_new_file = None
    if existing_product := processor.fetch_product(params):
        if existing_product["volatile"]:
            uuid.volatile = existing_product["uuid"]
            pid_to_new_file = existing_product["pid"]
        filename = existing_product["filename"]
        existing_file = processor.storage_api.download_product(
            existing_product, directory
        )
    else:
        filename = _generate_filename(params)
        existing_file = None

    volatile = not existing_file or uuid.volatile is not None

    try:
        new_file = _process_l3(processor, params, uuid, directory)
    except CloudnetException as err:
        raise utils.SkipTaskError(str(err)) from err

    if not params.product.experimental:
        processor.pid_utils.add_pid_to_file(new_file, pid_to_new_file)

    utils.add_global_attributes(new_file)

    upload = True
    patch = False
    if existing_product and existing_file:
        difference = nc_difference(existing_file, new_file)
        if difference == NCDiff.NONE:
            upload = False
            new_file = existing_file
            uuid.product = existing_product["uuid"]
        elif difference == NCDiff.MINOR:
            # Replace existing file
            patch = True
            if not params.product.experimental:
                processor.pid_utils.add_pid_to_file(new_file, existing_product["pid"])
            with netCDF4.Dataset(new_file, "r+") as nc:
                nc.file_uuid = existing_product["uuid"]
            uuid.product = existing_product["uuid"]

    if upload:
        processor.upload_file(params, new_file, filename, volatile, patch)
    else:
        logging.info("Skipping PUT to data portal, file has not changed")

    processor.create_and_upload_l3_images(
        new_file,
        params.product.id,
        params.model.id,
        std_uuid.UUID(uuid.product),
        filename,
        directory,
    )
    qc_result = processor.upload_quality_report(
        new_file, std_uuid.UUID(uuid.product), params.site, params.product.id
    )
    utils.print_info(uuid, volatile, patch, upload, qc_result)


def process_product(processor: Processor, params: ProductParams, directory: Path):
    uuid = Uuid()
    pid_to_new_file = None
    if existing_product := processor.fetch_product(params):
        if existing_product["volatile"]:
            uuid.volatile = existing_product["uuid"]
        filename = existing_product["filename"]
        existing_file = processor.storage_api.download_product(
            existing_product, directory
        )
    else:
        filename = _generate_filename(params)
        existing_file = None

    volatile = not existing_file or uuid.volatile is not None

    try:
        if params.product.id in ("mwr-single", "mwr-multi"):
            new_file = _process_mwrpy(processor, params, uuid, directory)
        elif params.product.id in ("categorize", "categorize-voodoo"):
            new_file = _process_categorize(processor, params, uuid, directory)
        elif params.product.id == "cpr-simulation":
            new_file = _process_cpr_simulation(processor, params, uuid, directory)
        elif params.product.id == "epsilon-lidar":
            new_file = _process_epsilon_from_lidar(processor, params, uuid, directory)
        else:
            new_file = _process_level2(processor, params, uuid, directory)
    except CloudnetException as err:
        raise utils.SkipTaskError(str(err)) from err

    if not params.product.experimental:
        processor.pid_utils.add_pid_to_file(new_file, pid_to_new_file)

    utils.add_global_attributes(
        new_file, instrument_pid=params.instrument.pid if params.instrument else None
    )

    upload = True
    patch = False
    if existing_product and existing_file:
        difference = nc_difference(existing_file, new_file)
        if difference == NCDiff.NONE:
            upload = False
            new_file = existing_file
            uuid.product = existing_product["uuid"]
        elif difference == NCDiff.MINOR:
            # Replace existing file
            patch = True
            if not params.product.experimental:
                processor.pid_utils.add_pid_to_file(new_file, existing_product["pid"])
            with netCDF4.Dataset(new_file, "r+") as nc:
                nc.file_uuid = existing_product["uuid"]
            uuid.product = existing_product["uuid"]

    if upload:
        processor.upload_file(params, new_file, filename, volatile, patch)
    else:
        logging.info("Skipping PUT to data portal, file has not changed")
    processor.create_and_upload_images(
        new_file,
        params.product.id,
        std_uuid.UUID(uuid.product),
        filename,
        directory,
    )
    qc_result = processor.upload_quality_report(
        new_file, std_uuid.UUID(uuid.product), params.site, params.product.id
    )
    utils.print_info(uuid, volatile, patch, upload, qc_result)
    if processor.md_api.config.is_production:
        _update_dvas_metadata(processor, params)


def _generate_filename(params: ProductParams | ModelParams) -> str:
    match params.product.id:
        case "mwr-single" | "mwr-multi":
            assert isinstance(params, ProductParams) and params.instrument is not None
            identifier = params.product.id.replace("mwr", params.instrument.type)
        case "iwc":
            identifier = "iwc-Z-T-method"
        case "lwc":
            identifier = "lwc-scaled-adiabatic"
        case product_id:
            identifier = product_id
    parts = [params.date.strftime("%Y%m%d"), params.site.id, identifier]
    if isinstance(params, ProductParams) and params.instrument:
        parts.append(params.instrument.uuid[:8])
    elif isinstance(params, ModelParams):
        parts.append(params.model.id)
    return "_".join(parts) + ".nc"


def _process_mwrpy(
    processor: Processor, params: ProductParams, uuid: Uuid, directory: Path
) -> Path:
    if params.instrument is None:
        raise RuntimeError("Instrument is None")
    payload = _get_payload(
        params,
        product_id="mwr-l1c",
        instrument_pid=params.instrument.pid,
    )
    metadata = processor.md_api.get("api/files", payload)
    _check_response(metadata, "mwr-l1c")
    l1c_file = processor.storage_api.download_product(metadata[0], directory)

    output_file = directory / "output.nc"
    if params.product.id == "mwr-single":
        fun = (
            generate_mwr_lhumpro
            if params.instrument.type == "lhumpro"
            else generate_mwr_single
        )
        uuid.product = fun(str(l1c_file), str(output_file), uuid.volatile)
    else:
        if params.instrument.type == "lhumpro":
            raise utils.SkipTaskError("Cannot generate mwr-multi from LHUMPRO")
        uuid.product = generate_mwr_multi(
            str(l1c_file), str(output_file), uuid.volatile
        )
    return output_file


def _process_cpr_simulation(
    processor: Processor, params: ProductParams, uuid: Uuid, directory: Path
):
    earthcare_launch_date = datetime.date(2024, 5, 28)
    if params.date < earthcare_launch_date:
        raise SkipTaskError(
            "CPR simulation is only feasible for dates before 2024-05-28"
        )
    orbital = Suborbital()
    payload = _get_payload(
        params,
        product_id="categorize",
    )
    metadata = processor.md_api.get("api/files", payload)
    _check_response(metadata, "categorize")
    categorize_file = processor.storage_api.download_product(metadata[0], directory)
    output_file = directory / "output.nc"
    uuid.product = orbital.simulate_cloudnet(
        str(categorize_file), str(output_file), mean_wind=6, uuid=uuid.volatile
    )
    utils.add_global_attributes(output_file)
    return output_file


def _process_epsilon_from_lidar(
    processor: Processor, params: ProductParams, uuid: Uuid, directory: Path
) -> Path:
    if params.instrument is None:
        raise RuntimeError("Instrument is None")
    payload_stare = _get_payload(
        params,
        product_id="doppler-lidar",
        instrument_pid=params.instrument.pid,
    )
    metadata_stare = processor.md_api.get("api/files", payload_stare)
    _check_response(metadata_stare, "doppler-lidar")

    payload_wind = _get_payload(
        params,
        product_id="doppler-lidar-wind",
    )
    metadata_wind = processor.md_api.get("api/files", payload_wind)
    if len(metadata_wind) == 0:
        raise SkipTaskError("Missing required input product: doppler-lidar-wind")
    prefer_pid = params.instrument.pid
    metadata_wind = sorted(
        metadata_wind,
        key=lambda meta: -1 if meta["instrument"]["pid"] == prefer_pid else 1,
    )

    file_lidar, file_wind = processor.storage_api.download_products(
        [metadata_stare[0], metadata_wind[0]], directory
    )

    output_file = directory / "output.nc"
    uuid.product = generate_epsilon_from_lidar(
        file_lidar, file_wind, str(output_file), uuid.volatile
    )
    return output_file


def _process_categorize(
    processor: Processor, params: ProductParams, uuid: Uuid, directory: Path
) -> Path:
    options = _get_categorize_options(params)
    is_voodoo = params.product.id == "categorize-voodoo"
    meta_records = _get_level1b_metadata_for_categorize(processor, params, is_voodoo)
    paths = processor.storage_api.download_products(meta_records.values(), directory)
    input_files: dict[str, str | list[str]] = {
        product: str(path) for product, path in zip(meta_records.keys(), paths)
    }
    if is_voodoo:
        input_files["lv0_files"], lv0_uuid = _get_input_files_for_voodoo(
            processor, params, directory
        )
    else:
        lv0_uuid = []
    output_path = directory / "output.nc"
    try:
        uuid.product = generate_categorize(
            input_files, str(output_path), uuid=uuid.volatile, options=options
        )
        uuid.raw.extend(lv0_uuid)
    except ModelDataError as exc:
        payload = _get_payload(params, model_id="gdas1")
        metadata = processor.md_api.get("api/model-files", payload)
        if not metadata:
            raise SkipTaskError("Bad model data and no gdas1") from exc
        input_files["model"] = str(
            processor.storage_api.download_product(metadata[0], directory)
        )
        uuid.product = generate_categorize(
            input_files, str(output_path), uuid=uuid.volatile
        )
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
    payload = _get_payload(params, model_id=params.model.id)
    model_meta = processor.md_api.get("api/model-files", payload)
    _check_response(model_meta, "model")
    model_file = processor.storage_api.download_product(model_meta[0], directory)
    l3_prod = params.product.id.split("-")[1]
    source = "categorize" if l3_prod == "cf" else l3_prod
    payload = _get_payload(params, product_id=source)
    product_meta = processor.md_api.get("api/files", payload)
    _check_response(product_meta, source)
    product_file = processor.storage_api.download_product(product_meta[0], directory)
    output_file = directory / "output.nc"
    uuid.product = product_resampling.process_L3_day_product(
        params.model.id,
        l3_prod,
        [str(model_file)],
        str(product_file),
        str(output_file),
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
    payload = _get_payload(params, product_id=cat_file)
    metadata = processor.md_api.get("api/files", payload)
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
    uuid.product = fun(categorize_file, str(output_file), uuid=uuid.volatile)
    return output_file


def _get_level1b_metadata_for_categorize(
    processor: Processor, params: ProductParams, is_voodoo: bool
) -> dict:
    meta_records = {
        "model": _find_model_product(processor, params),
        "mwr": (
            _find_instrument_product(processor, params, "mwr-single")
            or _find_instrument_product(
                processor, params, "mwr", fallback=["hatpro", "radiometrics"]
            )
            or _find_instrument_product(
                processor, params, "radar", require=["rpg-fmcw-35", "rpg-fmcw-94"]
            )
        ),
        "radar": (
            _find_instrument_product(
                processor, params, "radar", require=["rpg-fmcw-94"]
            )
            if is_voodoo
            else _find_instrument_product(
                processor,
                params,
                "radar",
                fallback=["mira-35", "rpg-fmcw-35", "rpg-fmcw-94", "copernicus"],
                exclude=["mira-10"],
            )
        ),
        "lidar": (
            _find_instrument_product(
                processor,
                params,
                "lidar",
                fallback=["chm15k", "chm15kx", "cl61d", "cl51", "cl31"],
            )
            or _find_instrument_product(processor, params, "doppler-lidar")
        ),
        "disdrometer": _find_instrument_product(
            processor, params, "disdrometer", fallback=["thies-lnm", "parsivel"]
        ),
    }
    optional_products = ["disdrometer", "mwr"]
    for product, metadata in meta_records.items():
        if product not in optional_products and metadata is None:
            raise SkipTaskError(f"Missing required input product: {product}")
    return {key: value for key, value in meta_records.items() if value is not None}


def _find_model_product(processor: Processor, params: ProductParams) -> dict | None:
    payload = _get_payload(params)
    metadata = processor.md_api.get("api/model-files", payload)
    _check_response(metadata, "model")
    return metadata[0]


def _find_instrument_product(
    processor: Processor,
    params: ProductParams,
    product_id: str,
    fallback: list[str] = [],
    require: list[str] = [],
    exclude: list[str] = [],
) -> dict | None:
    """
    Retrieve the most suitable instrument product based on specified parameters.

    Args:
        processor: Processor object used to interact with the metadata API.
        params: Parameters containing site and date information for the query.
        product_id: Identifier for the instrument product.
        fallback: Prioritize instrument types in the specified order if multiple
            products are available.
        require: Same as `fallback` but instrument type must one of the
            explicitly specified ones.
        exclude: Never choose products with these instrument types.

    Returns:
        The metadata of the most suitable instrument product, or `None` if no
        matching metadata is found.
    """
    if require and fallback:
        raise ValueError("Use either require or fallback")
    if require:
        fallback = require

    def file_key(file):
        if (
            nominal_instrument_pid
            and file["instrument"]["pid"] == nominal_instrument_pid
        ):
            return -1
        try:
            return fallback.index(file["instrument"]["instrumentId"])
        except ValueError:
            return 999

    payload = _get_payload(
        params,
        product_id=product_id,
        instrument_id=require,
    )
    metadata = processor.md_api.get("api/files", payload)
    metadata = [
        file for file in metadata if file["instrument"]["instrumentId"] not in exclude
    ]
    if not metadata:
        return None
    nominal_instrument_pid = _get_nominal_instrument_pid(processor, params, product_id)
    return min(metadata, key=file_key)


def _get_nominal_instrument_pid(
    processor: Processor, params: ProductParams, product_id: str
) -> dict | None:
    payload = {
        "site": params.site.id,
        "product": product_id,
        "date": params.date.isoformat(),
    }
    try:
        metadata = processor.md_api.get("api/nominal-instrument", payload)
        return metadata["nominalInstrument"]["pid"]
    except HTTPError as err:
        if err.response.status_code == 404:
            return None
        raise


def _get_input_files_for_voodoo(
    processor: Processor, params: ProductParams, directory: Path
) -> tuple[list[str], list[str]]:
    payload = _get_payload(params, instrument_id="rpg-fmcw-94")
    metadata = processor.md_api.get("upload-metadata", payload)
    unique_pids = [row["instrument"]["pid"] for row in metadata]
    if unique_pids:
        instrument_pid = unique_pids[0]
    else:
        raise SkipTaskError("No rpg-fmcw-94 cloud radar found")
    (
        full_paths,
        uuids,
    ) = processor.download_instrument(
        site_id=params.site.id,
        date=params.date,
        instrument_id="rpg-fmcw-94",
        instrument_pid=instrument_pid,
        include_pattern=".LV0",
        directory=directory,
    )
    full_paths_list = [full_paths] if isinstance(full_paths, str) else full_paths
    return full_paths_list, uuids


def _get_payload(
    params: ProductParams | ModelParams,
    product_id: str | list[str] | None = None,
    instrument_id: str | list[str] | None = None,
    instrument_pid: str | list[str] | None = None,
    model_id: str | None = None,
) -> dict:
    payload = {
        "site": params.site.id,
        "date": params.date.isoformat(),
        "developer": True,
    }
    if product_id is not None:
        payload["product"] = product_id
    if instrument_id is not None:
        payload["instrument"] = instrument_id
    if instrument_pid is not None:
        payload["instrumentPid"] = instrument_pid
    if model_id is not None:
        payload["model"] = model_id
    return payload


def _update_dvas_metadata(processor: Processor, params: ProductParams):
    payload = {
        "site": params.site.id,
        "product": params.product.id,
        "date": params.date.isoformat(),
    }
    if params.instrument:
        payload["instrumentPid"] = params.instrument.pid
    metadata = processor.md_api.get("api/files", payload)
    if metadata:
        processor.dvas.upload(metadata[0])


def _check_response(metadata: list[dict], product: str) -> None:
    if len(metadata) == 0:
        raise SkipTaskError(f"Missing required input product: {product}")
    if len(metadata) > 1:
        raise RuntimeError("Multiple products found")
