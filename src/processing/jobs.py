import logging
import uuid
from pathlib import Path

from data_processing import utils

from processing.processor import ModelParams, Processor, ProcessParams


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
    if metadata["pid"]:
        raise utils.SkipTaskError("Product already frozen")
    logging.info(f"Freezing product: {metadata['uuid']}")
    _, pid, url = processor.pid_utils.add_pid_to_file(full_path)
    processor.upload_file(params, full_path, metadata["filename"])
    logging.info(f'Minting PID "{pid}" to URL "{url}')


def upload_to_dvas(processor: Processor, params: ProcessParams) -> None:
    metadata = processor.fetch_product(params)
    if not metadata:
        raise utils.SkipTaskError("Product not found")
    if metadata["dvasId"]:
        raise utils.SkipTaskError("Already uploaded to DVAS")
    processor.dvas.upload(metadata)
    logging.info("Uploaded to DVAS")


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
