from collections.abc import Iterable
from pathlib import Path
import tempfile
from monitoring.instrument import Instrument
from monitoring.period import (
    AllPeriod,
    DayPeriod,
    MonthPeriod,
    Period,
    WeekPeriod,
    YearPeriod,
)
from processing.processor import Processor
import doppy
import datetime
import numpy as np
import matplotlib.pyplot as plt
from uuid import uuid4


def process(processor: Processor, instrument: Instrument, site_id: str, period: Period):
    process_system_parameters(processor, instrument, site_id, period)


def process_system_parameters(
    processor: Processor, instrument: Instrument, site_id: str, period: Period
):
    date_start, date_end = period.as_range()
    measurement_date_offset = datetime.timedelta(days=31)
    date_start_off = date_start - measurement_date_offset
    date_end_off = date_end + measurement_date_offset

    monitoring_file_uuid = str(uuid4())

    start_date = period.start_date()
    start_date = str(start_date) if start_date else None

    payload = {
        "uuid": monitoring_file_uuid,
        "startDate": start_date,
        "periodType": str(period),
        "site": site_id,
        "monitoringProduct": "housekeeping",
        "instrumentInfo": instrument.uuid,
    }

    resp = processor.md_api.session.put(
        f"{processor.md_api._url}/api/monitoring-files", json=payload
    )
    if not resp.ok:
        raise ValueError("Failed to create a monitoring file")

    with tempfile.TemporaryDirectory() as tempdir:
        paths, _ = processor.download_instrument(
            site_id=site_id,
            instrument_id=instrument.id,
            directory=Path(tempdir),
            date=(date_start_off, date_end_off),
            instrument_pid=instrument.pid,
            include_pattern=r"system_parameters_.*\.txt",
        )
        if not isinstance(paths, Iterable):
            raise TypeError
        sys_params_list = [doppy.raw.HaloSysParams.from_src(p) for p in paths]
        sys_params = (
            doppy.raw.HaloSysParams.merge(sys_params_list)
            .sorted_by_time()
            .non_strictly_increasing_timesteps_removed()
        )

        time_start = np.datetime64(date_start).astype(sys_params.time.dtype)
        time_end = np.datetime64(date_end + datetime.timedelta(days=1)).astype(
            sys_params.time.dtype
        )
        select = (time_start < sys_params.time) & (sys_params.time < time_end)
        sys_params = sys_params[select]

    with tempfile.TemporaryDirectory() as plotdir:
        img_path = Path(plotdir) / "img.png"
        _plot_internal_temperature(img_path, sys_params)
        plotname = _period_for_plotname(period)
        s3key = f"monitoring/{instrument.id}-{plotname}.png"
        processor.storage_api.upload_image(full_path=img_path, s3key=s3key)
    payload = {
            "s3key": s3key,
            "sourceFile": monitoring_file_uuid,
            "monitoringProductVariable": "internal-temperature",
            "width": 100,
            "height": 100,
            "marginTop": 0,
            "marginRight": 0,
            "marginBottom": 0,
            "marginLeft": 0,
        }
    resp = processor.md_api.session.put(
        f"{processor.md_api._url}/api/monitoring-visualizations", json=payload
    )
    print(resp)


def _period_for_plotname(period: Period) -> str:
    match period:
        case AllPeriod():
            return "All"
        case YearPeriod(start=start, end=end):
            return start.strftime("Year%Y")
        case MonthPeriod(start=start, end=end):
            return start.strftime("Month%Y%B")
        case WeekPeriod(start=start, end=end):
            return f"Week{start.isoformat()}-{end.isoformat()}"
        case DayPeriod(start=start, end=end):
            return f"Day{start.isoformat()}-{end.isoformat()}"
        case _:
            raise ValueError("Unsupported period")


def _plot_internal_temperature(img_path: Path, sys_params: doppy.raw.HaloSysParams):
    fig, ax = plt.subplots()
    ax.scatter(sys_params.time, sys_params.internal_temperature)
    fig.savefig(img_path)
