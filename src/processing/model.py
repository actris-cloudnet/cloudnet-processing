import logging
import uuid
from pathlib import Path

from data_processing import nc_header_augmenter
from data_processing.utils import MiscError, SkipTaskError

from processing.processor import ModelParams, Processor


def process_model(processor: Processor, params: ModelParams, directory: Path):
    upload_meta = processor.get_model_upload(params)
    if not upload_meta:
        return
    full_paths, raw_uuids = processor.download_raw_data([upload_meta], directory)
    if n_files := len(full_paths) != 1:
        raise ValueError(f"Found {n_files} files")
    full_path = full_paths[0]

    if file_meta := processor.get_model_file(params):
        if not file_meta["volatile"]:
            logging.warning("Stable model file found. Replacing...")
        product_uuid = uuid.UUID(file_meta["uuid"])
        filename = file_meta["filename"]
    else:
        product_uuid = generate_uuid()
        filename = generate_filename(params)

    try:
        output_path = directory / "output.nc"
        harmonize_model(params, full_path, output_path, product_uuid)
        processor.upload_file(params, output_path, filename)
        if "hidden" in params.site.types:
            logging.info("Skipping plotting for hidden site")
        else:
            processor.create_and_upload_images(
                output_path, "model", product_uuid, filename, directory
            )
        processor.upload_quality_report(output_path, product_uuid)
        processor.update_statuses(raw_uuids, "processed")
    except MiscError as err:
        raise SkipTaskError(err.message) from err


def generate_uuid() -> uuid.UUID:
    return uuid.uuid4()


def generate_filename(params: ModelParams) -> str:
    parts = [
        params.date.strftime("%Y%m%d"),
        params.site.id,
        params.model_id,
    ]
    return "_".join(parts) + ".nc"


def harmonize_model(
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
