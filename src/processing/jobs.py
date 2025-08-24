import logging
import uuid
from pathlib import Path

from cloudnet_api_client.containers import ProductMetadata

from processing import utils
from processing.housekeeping_utils import process_housekeeping as hkd
from processing.processor import ModelParams, Processor, ProcessParams

__all__ = ["freeze", "hkd", "update_plots", "update_qc", "upload_to_dvas"]


def update_plots(processor: Processor, params: ProcessParams, directory: Path) -> None:
    metadata, full_path = _fetch_data(processor, params, directory)
    file_uuid = metadata.uuid

    if isinstance(params, ModelParams) and "evaluation" in params.product.type:
        processor.create_and_upload_l3_images(
            full_path,
            params.product.id,
            params.model.id,
            file_uuid,
            metadata.filename,
            directory,
        )
    else:
        processor.create_and_upload_images(
            full_path,
            params.product.id,
            file_uuid,
            metadata.filename,
            directory,
            metadata.legacy,
        )
    url = utils.build_file_landing_page_url(file_uuid)
    logging.info(f"Plots updated: {url}/visualizations")


def update_qc(processor: Processor, params: ProcessParams, directory: Path) -> None:
    metadata, full_path = _fetch_data(processor, params, directory)
    result = processor.upload_quality_report(
        full_path, metadata.uuid, params.site, params.product.id
    )
    url = f"{utils.build_file_landing_page_url(metadata.uuid)}/quality"
    logging.info(f"Created quality report: {url} {result.upper()}")


def freeze(processor: Processor, params: ProcessParams, directory: Path) -> None:
    metadata, full_path = _fetch_data(processor, params, directory)
    if metadata.pid and not metadata.volatile:
        raise utils.SkipTaskError("Product already frozen")
    if params.product.experimental:
        raise utils.SkipTaskError("Product is experimental")
    logging.info(f"Freezing product: {metadata.uuid}")
    s3key = f"legacy/{metadata.filename}" if metadata.legacy else metadata.filename
    if metadata.pid:
        existing_pid = metadata.pid
        volatile = False
    else:
        existing_pid = None
        volatile = True
    file_uuid, pid, url = processor.pid_utils.add_pid_to_file(
        full_path, pid=existing_pid
    )
    if uuid.UUID(file_uuid) != metadata.uuid:
        msg = f"File {s3key} UUID mismatch (DB: {metadata.uuid}, File: {file_uuid})"
        raise ValueError(msg)
    if metadata.volatile and metadata.pid:
        msg = f"Removing volatile status of {url}"
    elif metadata.volatile and not metadata.pid:
        msg = f"Minting PID {pid} to URL {url} and keeping volatile status"
    else:
        msg = f"Minting PID {pid} to URL {url}"
    logging.info(msg)

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
    metadata = processor.client.file(file_uuid)
    if processor.md_api.config.is_production:
        processor.dvas.upload(metadata)


def upload_to_dvas(processor: Processor, params: ProcessParams) -> None:
    metadata = processor.get_product(params)
    if not metadata:
        raise utils.SkipTaskError("Product not found")
    if metadata.dvas_id:
        raise utils.SkipTaskError("Already uploaded to DVAS")
    processor.dvas.upload(metadata)
    logging.info("Uploaded to DVAS")


def _fetch_data(
    processor: Processor, params: ProcessParams, directory: Path
) -> tuple[ProductMetadata, Path]:
    metadata = processor.get_product(params)
    if not metadata:
        raise utils.SkipTaskError("Product not found")
    full_path = processor.storage_api.download_product(metadata, directory)
    return metadata, full_path
