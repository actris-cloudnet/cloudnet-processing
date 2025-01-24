import logging
import uuid
from pathlib import Path

from processing import utils
from processing.harmonizer.model import harmonize_model_file
from processing.netcdf_comparer import NCDiff, nc_difference
from processing.processor import ModelParams, Processor
from processing.utils import MiscError, SkipTaskError

SKIP_MODELS = ("arpege",)


def process_model(processor: Processor, params: ModelParams, directory: Path):
    if params.model_id in SKIP_MODELS:
        msg = f"Processing {params.model_id} not implemented yet"
        raise SkipTaskError(msg)

    upload_meta = processor.get_model_upload(params)
    if not upload_meta:
        msg = "No valid model upload found"
        raise SkipTaskError(msg)

    full_paths, raw_uuids = processor.download_raw_data([upload_meta], directory)
    if n_files := len(full_paths) != 1:
        raise ValueError(f"Found {n_files} files")
    full_path = full_paths[0]

    volatile = True
    if existing_meta := processor.get_model_file(params):
        if not existing_meta["volatile"]:
            logging.warning("Stable model file found.")
            volatile = False
        product_uuid = uuid.UUID(existing_meta["uuid"])
        filename = existing_meta["filename"]
        existing_file = processor.storage_api.download_product(existing_meta, directory)
    else:
        product_uuid = _generate_uuid()

        filename = _generate_filename(params)

    try:
        new_file = directory / "output.nc"
        _harmonize_model(params, full_path, new_file, product_uuid)

        if not existing_meta or not existing_meta["pid"]:
            volatile_pid = None
        else:
            volatile_pid = existing_meta["pid"]
        processor.pid_utils.add_pid_to_file(new_file, pid=volatile_pid)

        if existing_meta and existing_file:
            difference = nc_difference(existing_file, new_file)
            if difference == NCDiff.NONE:
                raise SkipTaskError("Skipping PUT to data portal, file has not changed")

        processor.upload_file(params, new_file, filename, volatile, patch=True)
        if "hidden" in params.site.types:
            logging.info("Skipping plotting for hidden site")
        else:
            processor.create_and_upload_images(
                new_file, "model", product_uuid, filename, directory
            )

        qc_result = processor.upload_quality_report(new_file, product_uuid)
        _print_info(product_uuid, qc_result)
        processor.update_statuses(raw_uuids, "processed")
    except MiscError as err:
        raise SkipTaskError(err.message) from err


def _generate_uuid() -> uuid.UUID:
    return uuid.uuid4()


def _generate_filename(params: ModelParams) -> str:
    parts = [
        params.date.strftime("%Y%m%d"),
        params.site.id,
        params.model_id,
    ]
    return "_".join(parts) + ".nc"


def _harmonize_model(
    params: ModelParams, input_path: Path, output_path: Path, uuid: uuid.UUID
):
    data = {
        "site_name": params.site.id,
        "date": params.date.isoformat(),
        "uuid": str(uuid),
        "full_path": input_path,
        "output_path": output_path,
        "model": params.model_id,
        "instrument": None,
    }
    harmonize_model_file(data)


def _print_info(file_uuid: uuid.UUID, qc_result: str | None = None) -> None:
    link = utils.build_file_landing_page_url(str(file_uuid))
    qc_str = f" QC: {qc_result.upper()}" if qc_result is not None else ""
    logging.info(f"Updated model: {link}{qc_str}")
