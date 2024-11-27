import logging
import uuid
from pathlib import Path

from processing import nc_header_augmenter, utils
from processing.processor import ModelParams, Processor
from processing.utils import MiscError, SkipTaskError

SKIP_MODELS = ("arpege",)


def process_model(processor: Processor, params: ModelParams, directory: Path):
    if params.model_id in SKIP_MODELS:
        logging.warning("Processing %s not implemented yet", params.model_id)
        return

    upload_meta = processor.get_model_upload(params)
    if not upload_meta:
        return
    full_paths, raw_uuids = processor.download_raw_data([upload_meta], directory)
    if n_files := len(full_paths) != 1:
        raise ValueError(f"Found {n_files} files")
    full_path = full_paths[0]

    volatile = True
    if file_meta := processor.get_model_file(params):
        if not file_meta["volatile"]:
            logging.warning("Stable model file found. Replacing...")
            volatile = False
        product_uuid = uuid.UUID(file_meta["uuid"])
        filename = file_meta["filename"]
    else:
        product_uuid = _generate_uuid()

        filename = _generate_filename(params)

    try:
        output_path = directory / "output.nc"
        _harmonize_model(params, full_path, output_path, product_uuid)

        if not file_meta or not file_meta["pid"]:
            volatile_pid = None
        else:
            volatile_pid = file_meta["pid"]
        processor.pid_utils.add_pid_to_file(output_path, pid=volatile_pid)

        processor.upload_file(params, output_path, filename, volatile, patch=True)
        if "hidden" in params.site.types:
            logging.info("Skipping plotting for hidden site")
        else:
            processor.create_and_upload_images(
                output_path, "model", product_uuid, filename, directory
            )
        qc_result = processor.upload_quality_report(output_path, product_uuid)
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
    nc_header_augmenter.harmonize_model_file(data)


def _print_info(file_uuid: uuid.UUID, qc_result: str | None = None) -> None:
    link = utils.build_file_landing_page_url(str(file_uuid))
    qc_str = f" QC: {qc_result.upper()}" if qc_result is not None else ""
    logging.info(f"Updated model: {link}{qc_str}")
