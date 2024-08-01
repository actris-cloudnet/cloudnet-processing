import datetime
import importlib
import logging
import uuid as std_uuid
from pathlib import Path

from cloudnetpy.categorize import generate_categorize
from cloudnetpy.exceptions import ModelDataError
from cloudnetpy.products import generate_mwr_multi, generate_mwr_single
from data_processing import utils
from data_processing.processing_tools import Uuid
from data_processing.utils import SkipTaskError

from processing.processor import Processor, ProductParams


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
        filename = generate_filename(params)
        existing_file = None

    if params.product.id in ("mwr-single", "mwr-multi"):
        new_file = process_mwrpy(processor, params, uuid, directory)
    elif params.product.id in ("categorize", "categorize-voodoo"):
        new_file = process_categorize(processor, params, uuid, directory)
    else:
        new_file = process_level2(processor, params, uuid, directory)

    if create_new_version:
        processor.pid_utils.add_pid_to_file(new_file)
    utils.add_global_attributes(
        new_file, instrument_pid=params.instrument.pid if params.instrument else None
    )

    if existing_file and utils.are_identical_nc_files(existing_file, new_file):
        raise SkipTaskError("Skipping PUT to data portal, file has not changed")

    processor.upload_file(params, new_file, filename)
    processor.create_and_upload_images(
        new_file, params.product.id, std_uuid.UUID(uuid.product), filename, directory
    )
    qc_result = processor.upload_quality_report(
        new_file, std_uuid.UUID(uuid.product), params.product.id
    )
    print_info(uuid, create_new_version, qc_result)


def generate_filename(params: ProductParams) -> str:
    match params.product.id:
        case "mwr-single" | "mwr-multi":
            assert params.instrument is not None
            identifier = params.product.id.replace("mwr", params.instrument.type)
        case "iwc":
            identifier = "iwc-Z-T-method"
        case "lwc":
            identifier = "lwc-scaled-adiabatic"
        case product_id:
            identifier = product_id
    parts = [params.date.strftime("%Y%m%d"), params.site.id, identifier]
    if params.instrument:
        parts.append(params.instrument.uuid[:8])
    return "_".join(parts) + ".nc"


def process_mwrpy(
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
    if len(metadata) == 0:
        raise SkipTaskError("Missing required input product: mwr-l1c")
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


def process_categorize(
    processor: Processor, params: ProductParams, uuid: Uuid, directory: Path
) -> Path:
    is_voodoo = params.product.id == "categorize-voodoo"
    meta_records = _get_level1b_metadata_for_categorize(processor, params, is_voodoo)
    input_files: dict[str, str | list[str]] = {
        product: str(processor.storage_api.download_product(metadata, directory))
        for product, metadata in meta_records.items()
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
            input_files, str(output_path), uuid=uuid.volatile
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


def process_level2(
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
    if len(metadata) == 0:
        raise SkipTaskError(f"Missing required input file: {cat_file}")
    if len(metadata) > 1:
        logging.info("API responded with several files")
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
    instrument_order = {
        "mwr": ("hatpro", "radiometrics"),
        "radar": ("mira", "rpg-fmcw-35", "rpg-fmcw-94", "copernicus"),
        "lidar": ("chm15k", "chm15kx", "cl61d", "cl51", "cl31"),
        "disdrometer": ("thies-lnm", "parsivel"),
        "model": "",  # You always get 1 and it's the best one
    }
    meta_records = {}
    for product in instrument_order:
        if product == "model":
            payload = _get_payload(site_id=params.site.id, date=params.date)
            route = "api/model-files"
        else:
            payload = _get_payload(
                site_id=params.site.id, date=params.date, product_id=product
            )
            route = "api/files"
        if is_voodoo and product == "radar":
            payload["instrument"] = "rpg-fmcw-94"
        metadata = processor.md_api.get(route, payload)
        if product == "mwr" and not metadata:
            # Use RPG-FMCW-XX as a fallback MWR
            payload["instrument"] = ["rpg-fmcw-35", "rpg-fmcw-94"]
            metadata = processor.md_api.get("api/files", payload)
        if product == "lidar" and not metadata:
            # Use Doppler lidar as a fallback lidar
            payload["product"] = "doppler-lidar"
            metadata = processor.md_api.get("api/files", payload)
        if product == "disdrometer" and not metadata:
            continue
        if not metadata:
            raise SkipTaskError(f"Missing required input product: {product}")
        meta_records[product] = metadata[0]
        if len(metadata) == 1:
            continue
        found = False
        for preferred_instrument in instrument_order[product]:
            for row in metadata:
                if row["instrument"]["id"] == preferred_instrument and not found:
                    meta_records[product] = row
                    found = True
        logging.info(
            f"Several options for {product}, using {meta_records[product]['instrument']['id']} with PID {meta_records[product]['instrumentPid']}"
        )
    return meta_records


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
    product_id: str | None = None,
    instrument_id: str | None = None,
    instrument_pid: str | None = None,
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


def print_info(
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
