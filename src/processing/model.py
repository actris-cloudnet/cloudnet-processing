import datetime
import logging
import math
import uuid
from pathlib import Path

import netCDF4
from model_munger.merge import merge_models
from model_munger.model import Location
from model_munger.readers import read_arpege, read_ecmwf_open, read_gdas1

from processing import utils
from processing.harmonizer.model import harmonize_model_file
from processing.netcdf_comparer import NCDiff, nc_difference
from processing.processor import ModelParams, Processor
from processing.utils import MiscError, SkipTaskError

SKIP_MODELS = ()
MODEL_READERS = {
    "arpege": read_arpege,
    "ecmwf-open": read_ecmwf_open,
    "gdas1": read_gdas1,
}


def process_model(processor: Processor, params: ModelParams, directory: Path):
    if params.model.id in SKIP_MODELS:
        msg = f"Processing {params.model.id} not implemented yet"
        raise SkipTaskError(msg)

    if params.model.source_model_id is None and params.model.id in MODEL_READERS:
        msg = (
            f"Model '{params.model.id}' should never be processed, "
            "only used to upload model data! This check would be "
            "redundant if 'model upload' and 'model product' types "
            "were separated in the database."
        )
        raise ValueError(msg)

    if (
        params.model.forecast_start is not None
        and params.model.forecast_end is not None
    ):
        start_offset = math.floor(-params.model.forecast_end / 24)
        end_offset = math.floor((24 - params.model.forecast_start) / 24)
        start_date = params.date + datetime.timedelta(days=start_offset)
        end_date = params.date + datetime.timedelta(days=end_offset)
    else:
        start_date = params.date
        end_date = params.date

    upload_meta = processor.get_model_upload(params, start_date, end_date)
    if not upload_meta:
        msg = "No valid model upload found"
        raise SkipTaskError(msg)

    raw_dir = directory / "raw"
    raw_dir.mkdir()
    full_paths = processor.client.download(upload_meta, raw_dir, progress=False)
    raw_uuids = [raw.uuid for raw in upload_meta]

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
        tmp_path = _process_model(params, full_paths, directory / "temp.nc")
        new_file = directory / "output.nc"
        _harmonize_model(params, tmp_path, new_file, product_uuid)

        if not existing_meta or not existing_meta["pid"]:
            volatile_pid = None
        else:
            volatile_pid = existing_meta["pid"]
        processor.pid_utils.add_pid_to_file(new_file, pid=volatile_pid)

        upload = True
        if existing_meta and existing_file:
            difference = nc_difference(existing_file, new_file)
            if difference == NCDiff.NONE:
                upload = False
                new_file = existing_file

        if upload:
            processor.upload_file(params, new_file, filename, volatile, patch=True)
        else:
            logging.info("Skipping PUT to data portal, file has not changed")
        if "hidden" in params.site.types:
            logging.info("Skipping plotting for hidden site")
        else:
            processor.create_and_upload_images(
                new_file, "model", product_uuid, filename, directory
            )
        qc_result = processor.upload_quality_report(new_file, product_uuid, params.site)
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
        params.model.id,
    ]
    return "_".join(parts) + ".nc"


def _process_model(
    params: ModelParams, input_paths: list[Path], output_path: Path
) -> Path:
    # Skip processing of legacy gdas1 files.
    if params.model.id == "gdas1":
        date_str = params.date.strftime("%Y%m%d")
        munger_files = []
        legacy_files = []
        for input_path in input_paths:
            with netCDF4.Dataset(input_path) as nc:
                if hasattr(nc, "history") and "model-munger" in nc.history:
                    munger_files.append(input_path)
                elif input_path.name.startswith(date_str):
                    legacy_files.append(input_path)
        if legacy_files:
            return legacy_files[0]
        input_paths = munger_files
    # Skip processing of other legacy files.
    if params.model.source_model_id is None:
        if n_files := len(input_paths) != 1:
            raise ValueError(f"Expected a single file but found {n_files} files")
        return input_paths[0]
    reader = MODEL_READERS[params.model.source_model_id]
    location = Location(id=params.site.id, name=params.site.name)
    models = []
    for path in input_paths:
        model = reader(path, location)
        model.screen_time(params.date)
        if (
            params.model.forecast_start is not None
            and params.model.forecast_end is not None
        ):
            model.screen_forecast_time(
                params.model.forecast_start, params.model.forecast_end
            )
        if len(model.data["time"]) > 0:
            models.append(model)
    if len(models) == 0:
        raise SkipTaskError("No valid time steps found")
    merged = merge_models(models)
    merged.write_netcdf(output_path)
    return output_path


def _harmonize_model(
    params: ModelParams, input_path: Path, output_path: Path, uuid: uuid.UUID
):
    data = {
        "site_name": params.site.id,
        "date": params.date.isoformat(),
        "uuid": str(uuid),
        "full_path": input_path,
        "output_path": output_path,
        "model": params.model.id,
        "instrument": None,
    }
    harmonize_model_file(data)


def _print_info(file_uuid: uuid.UUID, qc_result: str | None = None) -> None:
    link = utils.build_file_landing_page_url(str(file_uuid))
    qc_str = f" QC: {qc_result.upper()}" if qc_result is not None else ""
    logging.info(f"Updated model: {link}{qc_str}")
