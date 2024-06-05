import datetime
import logging
import uuid as std_uuid
from pathlib import Path
from typing import Type

import housekeeping
from data_processing import utils
from data_processing.processing_tools import Uuid

from processing import instrument_process
from processing.processor import InstrumentParams, Processor
from processing.utils import utctoday

ProcessClass = Type[instrument_process.ProcessInstrument]


def process_instrument(processor: Processor, params: InstrumentParams, directory: Path):
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

    try:
        new_file = process_file(processor, params, uuid, directory)
    except (utils.RawDataMissingError, NotImplementedError):
        return

    if create_new_version:
        processor.pid_utils.add_pid_to_file(new_file)
    utils.add_global_attributes(new_file, instrument_pid=params.instrument.pid)

    if existing_file and utils.are_identical_nc_files(existing_file, new_file):
        logging.info("Skipping PUT to data portal, file has not changed")
        return

    processor.upload_file(params, new_file, filename)
    processor.create_and_upload_images(
        new_file, params.product_id, std_uuid.UUID(uuid.product), filename, directory
    )
    qc_result = processor.upload_quality_report(
        new_file, std_uuid.UUID(uuid.product), params.product_id
    )
    processor.update_statuses(uuid.raw, "processed")
    print_info(uuid, create_new_version, qc_result)
    process_housekeeping(processor, params)


def generate_filename(params: InstrumentParams) -> str:
    identifier = params.instrument.type
    if params.product_id == "mwr-l1c":
        identifier += "-l1c"
    elif params.instrument.type == "halo-doppler-lidar-calibrated":
        identifier = "halo-doppler-lidar"
    elif params.product_id == "doppler-lidar-wind":
        identifier += "-wind"
    parts = [
        params.date.strftime("%Y%m%d"),
        params.site.id,
        identifier,
        params.instrument.uuid[:8],
    ]
    return "_".join(parts) + ".nc"


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


def process_file(
    processor: Processor, params: InstrumentParams, uuid: Uuid, directory: Path
) -> Path:
    product_camel_case = "".join(
        [part.capitalize() for part in params.product_id.split("-")]
    )
    instrument_snake_case = params.instrument.type.replace("-", "_")
    process_class: ProcessClass = getattr(
        instrument_process, f"Process{product_camel_case}"
    )
    process = process_class(directory, params, uuid, processor)
    getattr(process, f"process_{instrument_snake_case}")()
    return process.output_path


def process_housekeeping(processor: Processor, params: InstrumentParams) -> None:
    if params.date < utctoday() - datetime.timedelta(days=3):
        logging.info("Skipping housekeeping for old data")
        return
    logging.info("Processing housekeeping data")
    raw_api = utils.RawApi(session=utils.make_session())
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
