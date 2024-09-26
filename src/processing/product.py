import datetime
import importlib
import logging
import uuid as std_uuid
from pathlib import Path

from cloudnetpy.categorize import generate_categorize
from cloudnetpy.exceptions import CloudnetException, ModelDataError
from cloudnetpy.model_evaluation.products import product_resampling
from cloudnetpy.products import generate_mwr_multi, generate_mwr_single
from requests import HTTPError

from processing import utils
from processing.netcdf_comparer import are_identical_nc_files
from processing.processor import ModelParams, Processor, ProductParams
from processing.utils import SkipTaskError, Uuid


def process_me(processor: Processor, params: ModelParams, directory: Path):
    create_new_version = False
    uuid = Uuid()
    if existing_product := processor.fetch_product(params):
        if existing_product["volatile"]:
            uuid.volatile = existing_product["uuid"]
        else:
            create_new_version = True
        filename = existing_product["filename"]
        existing_file = processor.storage_api.download_product(
            existing_product, directory
        )
    else:
        filename = _generate_filename(params)
        existing_file = None

    try:
        new_file = _process_l3(processor, params, uuid, directory)
    except CloudnetException as err:
        raise utils.SkipTaskError(str(err)) from err

    if create_new_version:
        processor.pid_utils.add_pid_to_file(new_file)

    utils.add_global_attributes(new_file)

    if existing_file and are_identical_nc_files(existing_file, new_file):
        raise SkipTaskError("Skipping PUT to data portal, file has not changed")

    processor.upload_file(params, new_file, filename, volatile=not create_new_version)
    processor.create_and_upload_l3_images(
        new_file,
        params.product.id,
        params.model_id,
        std_uuid.UUID(uuid.product),
        filename,
        directory,
    )
    qc_result = processor.upload_quality_report(
        new_file, std_uuid.UUID(uuid.product), params.product.id
    )
    _print_info(uuid, create_new_version, qc_result)


def process_product(processor: Processor, params: ProductParams, directory: Path):
    create_new_version = False
    uuid = Uuid()
    if existing_product := processor.fetch_product(params):
        if existing_product["volatile"]:
            uuid.volatile = existing_product["uuid"]
        else:
            create_new_version = True
        filename = existing_product["filename"]
        existing_file = processor.storage_api.download_product(
            existing_product, directory
        )
    else:
        filename = _generate_filename(params)
        existing_file = None

    try:
        if params.product.id in ("mwr-single", "mwr-multi"):
            new_file = _process_mwrpy(processor, params, uuid, directory)
        elif params.product.id in ("categorize", "categorize-voodoo"):
            new_file = _process_categorize(processor, params, uuid, directory)
        else:
            new_file = _process_level2(processor, params, uuid, directory)
    except CloudnetException as err:
        raise utils.SkipTaskError(str(err)) from err

    if create_new_version or existing_product is None or not existing_product["pid"]:
        volatile_pid = None
    else:
        volatile_pid = existing_product["pid"]
    processor.pid_utils.add_pid_to_file(new_file, pid=volatile_pid)

    utils.add_global_attributes(
        new_file, instrument_pid=params.instrument.pid if params.instrument else None
    )

    if existing_file and are_identical_nc_files(existing_file, new_file):
        raise SkipTaskError("Skipping PUT to data portal, file has not changed")

    processor.upload_file(params, new_file, filename, volatile=not create_new_version)
    processor.create_and_upload_images(
        new_file,
        params.product.id,
        std_uuid.UUID(uuid.product),
        filename,
        directory,
    )
    qc_result = processor.upload_quality_report(
        new_file, std_uuid.UUID(uuid.product), params.product.id
    )
    _print_info(uuid, create_new_version, qc_result)
    if create_new_version and processor.md_api.config.is_production:
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
        parts.append(params.model_id)
    return "_".join(parts) + ".nc"


def _process_mwrpy(
    processor: Processor, params: ProductParams, uuid: Uuid, directory: Path
) -> Path:
    assert params.instrument is not None
    payload = _get_payload(
        site_id=params.site.id,
        date=params.date,
        product_id="mwr-l1c",
        instrument_pid=params.instrument.pid,
    )
    metadata = processor.md_api.get("api/files", payload)
    _check_response(metadata, "mwr-l1c")
    l1c_file = processor.storage_api.download_product(metadata[0], directory)

    output_file = directory / "output.nc"
    if params.product.id == "mwr-single":
        uuid.product = generate_mwr_single(
            str(l1c_file), str(output_file), uuid.volatile
        )
    else:
        uuid.product = generate_mwr_multi(
            str(l1c_file), str(output_file), uuid.volatile
        )
    return output_file


def _process_categorize(
    processor: Processor, params: ProductParams, uuid: Uuid, directory: Path
) -> Path:
    options = _get_categorize_options(params)
    is_voodoo = params.product.id == "categorize-voodoo"
    meta_records = _get_level1b_metadata_for_categorize(processor, params, is_voodoo)
    input_files: dict[str, str | list[str]] = {
        product: str(processor.storage_api.download_product(metadata, directory))
        for product, metadata in meta_records.items()
        if metadata is not None
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
        payload = _get_payload(
            site_id=params.site.id, date=params.date, model_id="gdas1"
        )
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
    payload = _get_payload(
        site_id=params.site.id, date=params.date, model_id=params.model_id
    )
    model_meta = processor.md_api.get("api/model-files", payload)
    _check_response(model_meta, "model")
    model_file = processor.storage_api.download_product(model_meta[0], directory)
    l3_prod = params.product.id.split("-")[1]
    source = "categorize" if l3_prod == "cf" else l3_prod
    payload = _get_payload(site_id=params.site.id, date=params.date, product_id=source)
    product_meta = processor.md_api.get("api/files", payload)
    _check_response(product_meta, source)
    product_file = processor.storage_api.download_product(product_meta[0], directory)
    output_file = directory / "output.nc"
    uuid.product = product_resampling.process_L3_day_product(
        params.model_id,
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
    payload = _get_payload(
        site_id=params.site.id, date=params.date, product_id=cat_file
    )
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
    return meta_records


def _find_model_product(processor: Processor, params: ProductParams) -> dict | None:
    payload = _get_payload(site_id=params.site.id, date=params.date)
    metadata = processor.md_api.get("api/model-files", payload)
    _check_response(metadata, "model")
    return metadata[0]


def _find_instrument_product(
    processor: Processor,
    params: ProductParams,
    product_id: str,
    fallback: list[str] = [],
    require: list[str] = [],
) -> dict | None:
    if require and fallback:
        raise ValueError("Use either require or fallback")
    if require:
        fallback = require

    def file_key(file):
        if nominal_instrument_pid and file["instrumentPid"] == nominal_instrument_pid:
            return -1
        try:
            return fallback.index(file["instrument"]["id"])
        except ValueError:
            return 999

    payload = _get_payload(
        site_id=params.site.id,
        date=params.date,
        product_id=product_id,
        instrument_id=require,
    )
    metadata = processor.md_api.get("api/files", payload)
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
    payload = _get_payload(
        site_id=params.site.id, date=params.date, instrument_id="rpg-fmcw-94"
    )
    metadata = processor.md_api.get("upload-metadata", payload)
    unique_pids = [row["instrumentPid"] for row in metadata]
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
    site_id: str,
    date: datetime.date,
    product_id: str | list[str] | None = None,
    instrument_id: str | list[str] | None = None,
    instrument_pid: str | list[str] | None = None,
    model_id: str | None = None,
) -> dict:
    payload = {
        "site": site_id,
        "date": date.isoformat(),
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


def _print_info(
    uuid: Uuid, create_new_version: bool, qc_result: str | None = None
) -> None:
    action = (
        "Created new version"
        if create_new_version
        else ("Replaced volatile file" if uuid.volatile else "Created volatile file")
    )
    link = utils.build_file_landing_page_url(uuid.product)
    qc_str = f" QC: {qc_result.upper()}" if qc_result is not None else ""
    logging.info(f"{action}: {link}{qc_str}")


def _update_dvas_metadata(processor: Processor, params: ProductParams):
    payload = {
        "site": params.site.id,
        "product": params.product.id,
        "date": params.date.isoformat(),
        "allVersions": "true",
    }
    if params.instrument:
        payload["instrumentPid"] = params.instrument.pid
    metadata = processor.md_api.get("api/files", payload)
    latest_version = metadata[0]
    if any(row["dvasId"] is not None for row in metadata):
        processor.dvas.upload(latest_version)


def _check_response(metadata: list[dict], product: str) -> None:
    if len(metadata) == 0:
        raise SkipTaskError(f"Missing required input product: {product}")
    if len(metadata) > 1:
        raise RuntimeError("Multiple products found")
