import datetime
import logging
import uuid
from pathlib import Path

import housekeeping

from processing import utils
from processing.processor import InstrumentParams, ModelParams, Processor, ProcessParams
from processing.utils import utctoday


def update_plots(processor: Processor, params: ProcessParams, directory: Path) -> None:
    metadata, full_path = _fetch_data(processor, params, directory)
    file_uuid = uuid.UUID(metadata["uuid"])

    if isinstance(params, ModelParams) and "evaluation" in params.product.type:
        processor.create_and_upload_l3_images(
            full_path,
            params.product.id,
            params.model_id,
            file_uuid,
            metadata["filename"],
            directory,
        )
    else:
        processor.create_and_upload_images(
            full_path,
            params.product.id,
            file_uuid,
            metadata["filename"],
            directory,
        )
    url = utils.build_file_landing_page_url(str(file_uuid))
    logging.info(f"Plots updated: {url}/visualizations")


def update_qc(processor: Processor, params: ProcessParams, directory: Path) -> None:
    metadata, full_path = _fetch_data(processor, params, directory)
    file_uuid = uuid.UUID(metadata["uuid"])
    result = processor.upload_quality_report(full_path, file_uuid, params.product.id)
    url = f"{utils.build_file_landing_page_url(str(file_uuid))}/quality"
    logging.info(f"Created quality report: {url} {result.upper()}")


def freeze(processor: Processor, params: ProcessParams, directory: Path) -> None:
    metadata, full_path = _fetch_data(processor, params, directory)
    if metadata["pid"] and not metadata["volatile"]:
        raise utils.SkipTaskError("Product already frozen")
    logging.info(f"Freezing product: {metadata['uuid']}")
    s3key = (
        f"legacy/{metadata['filename']}" if metadata["legacy"] else metadata["filename"]
    )
    if metadata["pid"]:
        existing_pid = metadata["pid"]
        volatile = False
    else:
        existing_pid = None
        volatile = True
    file_uuid, pid, url = processor.pid_utils.add_pid_to_file(
        full_path, pid=existing_pid
    )
    if uuid.UUID(file_uuid) != uuid.UUID(metadata["uuid"]):
        msg = f"File {s3key} UUID mismatch (DB: {metadata['uuid']}, File: {file_uuid})"
        raise ValueError(msg)
    logging.info(f'Minting PID "{pid}" to URL "{url}')
    response_data = processor.storage_api.upload_product(
        full_path, s3key, volatile=volatile
    )
    payload = {
        "uuid": file_uuid,
        "checksum": utils.sha256sum(full_path),
        "volatile": volatile,
        "pid": pid,
        **response_data,
    }
    processor.md_api.post("files", payload)
    if volatile is False:
        processor.storage_api.delete_volatile_product(s3key)
    metadata = processor.md_api.get(f"api/files/{metadata['uuid']}")
    if processor.md_api.config.is_production:
        processor.dvas.upload(metadata)


def upload_to_dvas(processor: Processor, params: ProcessParams) -> None:
    metadata = processor.fetch_product(params)
    if not metadata:
        raise utils.SkipTaskError("Product not found")
    if metadata["dvasId"]:
        raise utils.SkipTaskError("Already uploaded to DVAS")
    processor.dvas.upload(metadata)
    logging.info("Uploaded to DVAS")


# TODO: copy-pasted from instrument.py
def hkd(processor: Processor, params: InstrumentParams) -> None:
    if params.date < utctoday() - datetime.timedelta(days=3):
        logging.info("Skipping housekeeping for old data")
        return
    logging.info("Processing housekeeping data")
    raw_api = utils.RawApi(processor.md_api.config, processor.md_api.session)
    payload = processor._get_payload(
        site=params.site.id, date=params.date, instrument_pid=params.instrument.pid
    )
    records = processor.md_api.get("api/raw-files", payload)
    try:
        with housekeeping.Database() as db:
            for record in records:
                housekeeping.process_record(record, raw_api=raw_api, db=db)
    except housekeeping.HousekeepingException:
        logging.exception("Housekeeping failed")


def _fetch_data(
    processor: Processor, params: ProcessParams, directory: Path
) -> tuple[dict, Path]:
    if isinstance(params, ModelParams) and "evaluation" not in params.product.type:
        metadata = processor.get_model_file(params)
    else:
        metadata = processor.fetch_product(params)
    if not metadata:
        raise utils.SkipTaskError("Product not found")
    full_path = processor.storage_api.download_product(metadata, directory)
    return metadata, full_path
