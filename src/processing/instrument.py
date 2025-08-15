import logging
import uuid as std_uuid
from pathlib import Path
from typing import Type

import netCDF4
from cloudnetpy.exceptions import CloudnetException

from processing import instrument_process, utils
from processing.housekeeping_utils import process_housekeeping
from processing.netcdf_comparer import NCDiff, nc_difference
from processing.processor import InstrumentParams, Processor
from processing.utils import Uuid

ProcessClass = Type[instrument_process.ProcessInstrument]


def process_instrument(processor: Processor, params: InstrumentParams, directory: Path):
    uuid = Uuid()
    pid_to_new_file = None
    if existing_product := processor.fetch_product(params):
        if existing_product.volatile:
            uuid.volatile = str(existing_product.uuid)
            pid_to_new_file = existing_product.pid
        filename = existing_product.filename
        existing_file = processor.storage_api.download_product(
            existing_product, directory
        )
    else:
        filename = _generate_filename(params)
        existing_file = None

    volatile = not existing_file or uuid.volatile is not None

    try:
        new_file = _process_file(processor, params, uuid, directory)
    except utils.RawDataMissingError as err:
        raise utils.SkipTaskError(err.message) from err
    except NotImplementedError as err:
        raise utils.SkipTaskError("Processing not implemented yet") from err
    except CloudnetException as err:
        raise utils.SkipTaskError(str(err)) from err

    if not params.product.experimental:
        processor.pid_utils.add_pid_to_file(new_file, pid_to_new_file)

    utils.add_global_attributes(new_file, params.instrument.pid)

    upload = True
    patch = False
    if existing_product and existing_file:
        difference = nc_difference(existing_file, new_file)
        if difference == NCDiff.NONE:
            upload = False
            new_file = existing_file
            uuid.product = str(existing_product.uuid)
        elif difference == NCDiff.MINOR:
            # Replace existing file
            patch = True
            if not params.product.experimental:
                processor.pid_utils.add_pid_to_file(new_file, existing_product.pid)
            with netCDF4.Dataset(new_file, "r+") as nc:
                nc.file_uuid = str(existing_product.uuid)
            uuid.product = str(existing_product.uuid)

    if upload:
        processor.upload_file(params, new_file, filename, volatile, patch)
    else:
        logging.info("Skipping PUT to data portal, file has not changed")
    processor.create_and_upload_images(
        new_file, params.product.id, std_uuid.UUID(uuid.product), filename, directory
    )
    qc_result = processor.upload_quality_report(
        new_file, std_uuid.UUID(uuid.product), params.site, params.product.id
    )
    processor.update_statuses(uuid.raw, "processed")
    utils.print_info(uuid, volatile, patch, upload, qc_result)
    if processor.md_api.config.is_production:
        process_housekeeping(processor, params)


def _generate_filename(params: InstrumentParams) -> str:
    identifier = params.instrument.type
    if params.product.id == "mwr-l1c":
        identifier += "-l1c"
    elif params.instrument.type == "halo-doppler-lidar-calibrated":
        identifier = "halo-doppler-lidar"
    elif params.product.id == "doppler-lidar-wind":
        identifier += "-wind"
    parts = [
        params.date.strftime("%Y%m%d"),
        params.site.id,
        identifier,
        params.instrument.uuid[:8],
    ]
    return "_".join(parts) + ".nc"


def _process_file(
    processor: Processor, params: InstrumentParams, uuid: Uuid, directory: Path
) -> Path:
    product_camel_case = "".join(
        [part.capitalize() for part in params.product.id.split("-")]
    )
    instrument_snake_case = params.instrument.type.replace("-", "_")
    process_class: ProcessClass = getattr(
        instrument_process, f"Process{product_camel_case}"
    )
    process = process_class(directory, params, uuid, processor)
    getattr(process, f"process_{instrument_snake_case}")()
    return process.output_path
