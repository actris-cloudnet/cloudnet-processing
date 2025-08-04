import datetime
import tempfile
from collections.abc import Iterable
from pathlib import Path

import doppy
import matplotlib.pyplot as plt
import numpy as np
from processing.processor import Processor

from monitoring.instrument import Instrument
from monitoring.monitoring_file import (
    Dimensions,
    MonitoringFile,
    MonitoringVisualization,
)
from monitoring.period import (
    Period,
)


def process(processor: Processor, instrument: Instrument, site_id: str, period: Period):
    process_system_parameters(processor, instrument, site_id, period)


def process_system_parameters(
    processor: Processor, instrument: Instrument, site_id: str, period: Period
):
    monitoring_file = MonitoringFile(
        instrument, site_id, period, "halo-doppler-lidar_housekeeping"
    )
    monitoring_file.put_file(processor.md_api)

    date_start, date_end = period.as_range()
    measurement_date_offset = datetime.timedelta(days=31)
    date_start_off = date_start - measurement_date_offset
    date_end_off = date_end + measurement_date_offset

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

    vars = [
        ("acquisition_card_temperature", "acquisition-card-temperature"),
        ("internal_relative_humidity", "internal-relative-humidity"),
        ("internal_temperature", "internal-temperature"),
        ("platform_pitch_angle", "platform-pitch-angle"),
        ("platform_roll_angle", "platform-roll-angle"),
        ("supply_voltage", "supply-voltage"),
    ]
    for var_name, var_id in vars:
        with tempfile.TemporaryDirectory() as plotdir:
            img_path = Path(plotdir) / "img.png"
            vis = _plot_sys_params_var(var_name, sys_params, img_path, var_id)
            monitoring_file.put_visualization(
                processor.storage_api, processor.md_api, vis
            )


def _plot_sys_params_var(
    var: str, sys_params: doppy.raw.HaloSysParams, img_path: Path, variable_id: str
) -> MonitoringVisualization:
    fig, ax = plt.subplots()
    y = getattr(sys_params, var)
    ax.scatter(sys_params.time, y)
    fig.savefig(img_path)
    return MonitoringVisualization(
        img_path, variable_id, Dimensions(100, 100, 0, 0, 0, 0)
    )
