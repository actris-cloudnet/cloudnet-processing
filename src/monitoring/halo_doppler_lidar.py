import datetime
import tempfile
from collections import Counter
from collections.abc import Iterable
from pathlib import Path

import doppy
import matplotlib.dates
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
from processing.processor import Processor
from processing.utils import RawDataMissingError

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
    # process_system_parameters(processor, instrument, site_id, period)
    process_background(processor, instrument, site_id, period)


def process_background(
    processor: Processor, instrument: Instrument, site_id: str, period: Period
):
    monitoring_file = MonitoringFile(
        instrument, site_id, period, "halo-doppler-lidar_background"
    )
    monitoring_file.put_file(processor.md_api)
    with tempfile.TemporaryDirectory() as tempdir:
        try:
            paths, _ = processor.download_instrument(
                site_id=site_id,
                instrument_id=instrument.id,
                directory=Path(tempdir),
                date=period.as_range(),
                instrument_pid=instrument.pid,
                include_pattern=r"Background_.*\.txt",
            )
        except RawDataMissingError:
            print(
                f"No system paramters files to monitor for site {site_id} [{period.as_range()}]"
            )
            return
        if not isinstance(paths, list):
            paths = [paths]
        bgs = doppy.raw.HaloBg.from_srcs(paths)
    counter = Counter((bg.signal.shape[1] for bg in bgs))
    most_common_ngates = counter.most_common()[0][0]
    bgs = [bg for bg in bgs if bg.signal.shape[1] == most_common_ngates]
    bg = doppy.raw.HaloBg.merge(bgs)
    bg = bg.sorted_by_time().non_strictly_increasing_timesteps_removed()
    date_start, date_end = period.as_range()
    time_start = np.datetime64(date_start).astype(bg.time.dtype)
    time_end = np.datetime64(date_end + datetime.timedelta(days=1)).astype(
        bg.time.dtype
    )
    select = (time_start < bg.time) & (bg.time < time_end)
    bg = bg[select]
    if len(bg.time) == 0:
        print(
            f"No timestamps Background to monitor for site {site_id} [{time_start}, {time_end}]"
        )
        return
    with tempfile.TemporaryDirectory() as plotdir:
        img_path = Path(plotdir) / "img.png"
        vis = _plot_bg(bg, img_path, "background-profile")
        monitoring_file.put_visualization(processor.storage_api, processor.md_api, vis)


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
        try:
            paths, _ = processor.download_instrument(
                site_id=site_id,
                instrument_id=instrument.id,
                directory=Path(tempdir),
                date=(date_start_off, date_end_off),
                instrument_pid=instrument.pid,
                include_pattern=r"system_parameters_.*\.txt",
            )
        except RawDataMissingError:
            print(
                f"No system paramters files to monitor for site {site_id} [{date_start_off}, {date_end_off}]"
            )
            return

        if not isinstance(paths, Iterable):
            raise TypeError
        if not paths:
            print(
                f"No system paramters files to monitor for site {site_id} [{date_start_off}, {date_end_off}]"
            )
            return
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
    if len(sys_params.time) == 0:
        print(
            f"No timestamps System paramters  to monitor for site {site_id} [{time_start}, {time_end}]"
        )
        return

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


def _format_time_axis(ax):
    locator = matplotlib.dates.AutoDateLocator()
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(matplotlib.dates.ConciseDateFormatter(locator))


def _pretty_ax(ax):
    ax.set_facecolor("#f0f0f0")
    ax.grid(True, axis="y", color="white", linestyle="-", linewidth=4)
    ax.set_axisbelow(True)
    ax.tick_params(
        axis="both",
        length=4,
        width=2,
        direction="out",
        pad=10,
    )

    ax.spines["left"].set_position(("outward", 10))
    ax.spines["bottom"].set_position(("outward", 10))
    ax.spines["left"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_visible(False)


def _pretty_ax_2d(ax):
    ax.tick_params(
        axis="both",
        length=8,
        width=2,
        direction="out",
        pad=10,
    )
    ax.spines["left"].set_position(("outward", 15))
    ax.spines["bottom"].set_position(("outward", 15))
    ax.spines["left"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_visible(False)


def _save_fig(fig, path: Path) -> tuple[int, int]:
    aspect = 16 / 9
    dpi = 400
    height_inch = 16
    height_px = height_inch * dpi
    width_px = aspect * height_px
    fig.set_size_inches(width_px / dpi, height_px / dpi)

    font_size = 22
    for ax in fig.axes:
        ax.tick_params(axis="both", labelsize=font_size)
        ax.xaxis.label.set_size(font_size)
        ax.yaxis.label.set_size(font_size)
        ax.title.set_size(font_size + 2)
        ax.xaxis.get_offset_text().set_fontsize(font_size)
        ax.yaxis.get_offset_text().set_fontsize(font_size)

    fig.savefig(path, dpi=dpi, bbox_inches="tight", pad_inches=0)
    return int(width_px), int(height_px)


def _plot_bg(
    bg: doppy.raw.HaloBg, img_path: Path, variable_id: str
) -> MonitoringVisualization:
    fig, ax = plt.subplots()
    vmin, vmax = np.percentile(bg.signal.ravel(), [5, 95])
    cax = ax.pcolormesh(
        bg.time, np.arange(bg.signal.shape[1]), bg.signal.T, vmin=vmin, vmax=vmax
    )
    fig.colorbar(
        cax, ax=ax, orientation="horizontal", shrink=0.5, pad=0.1, aspect=30
    ).outline.set_visible(False)  # type: ignore
    _format_time_axis(ax)
    _pretty_ax_2d(ax)
    width, height = _save_fig(fig, img_path)
    return MonitoringVisualization(
        img_path, variable_id, Dimensions(width, height, 0, 0, 0, 0)
    )


def _plot_sys_params_var(
    var: str, sys_params: doppy.raw.HaloSysParams, img_path: Path, variable_id: str
) -> MonitoringVisualization:
    fig, ax = plt.subplots()
    y = getattr(sys_params, var)
    ax.scatter(sys_params.time, y)
    _format_time_axis(ax)
    _pretty_ax(ax)
    width, height = _save_fig(fig, img_path)
    return MonitoringVisualization(
        img_path, variable_id, Dimensions(width, height, 0, 0, 0, 0)
    )
