import datetime
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory

import matplotlib.pyplot as plt
import numpy as np
from cloudnetpy.plotting.plotting import Dimensions
from doppy.raw import HaloSysParams
from doppy.raw.halo_bg import HaloBg
from doppy.raw.halo_hpl import HaloHpl

from monitoring.monitor_options import MonitorOptions
from monitoring.monitoring_file import MonitoringFile, MonitoringVisualization
from monitoring.period import All, PeriodType
from monitoring.plot_utils import (
    SCATTER_OPTS,
    add_colorbar,
    format_time_axis,
    pretty_ax,
    pretty_ax_2d,
    save_fig,
    scientific_cbar,
    set_xlim_for_period,
)
from monitoring.product import MonitoringProduct, MonitoringVariable
from monitoring.utils import (
    RawFilesDatePayload,
    instrument_uuid_to_pid,
)


def monitor(opts: MonitorOptions) -> None:
    match opts.product.id:
        case "halo-doppler-lidar_housekeeping":
            monitor_housekeeping(opts)
        case "halo-doppler-lidar_background":
            monitor_background(opts)
        case "halo-doppler-lidar_signal":
            monitor_signal(opts)


def monitor_housekeeping(opts: MonitorOptions) -> None:
    pid = instrument_uuid_to_pid(opts.api_client, opts.instrument_uuid)
    date_opts: RawFilesDatePayload = {}
    if not isinstance(opts.period, All):
        start, stop = opts.period.to_interval_padded(days=31)
        date_opts = {"date_from": start, "date_to": stop}

    records = opts.api_client.raw_files(
        site_id=opts.site,
        instrument_pid=pid,
        filename_prefix="system_parameters_",
        filename_suffix=".txt",
        **date_opts,
    )
    if not records:
        raise ValueError(
            f"No raw files for monitoring period {opts.period} {opts.product.id} {opts.site} {pid}"
        )

    with TemporaryDirectory() as tempdir:
        (paths, _uuids) = opts.storage_api.download_raw_data(records, Path(tempdir))
        sys_params_list = [HaloSysParams.from_src(p) for p in paths]
    sys_params = (
        HaloSysParams.merge(sys_params_list)
        .sorted_by_time()
        .non_strictly_increasing_timesteps_removed()
    )
    if not isinstance(opts.period, All):
        start, stop = opts.period.to_interval()
        dtype = sys_params.time.dtype
        start_time = np.datetime64(start).astype(dtype)
        stop_time = np.datetime64(stop + datetime.timedelta(days=1)).astype(dtype)
        select = (start_time <= sys_params.time) & (sys_params.time < stop_time)
        sys_params = sys_params[select]
    if len(sys_params.time) == 0:
        raise ValueError(
            f"No timestamps for monitoring period {opts.period} {opts.product.id} {opts.site} {pid}"
        )

    monitoring_file = MonitoringFile(
        opts.instrument_uuid,
        opts.site,
        opts.period,
        opts.product,
        monitor_housekeeping_plots(sys_params, opts.period, opts.product),
        opts.md_api,
        opts.storage_api,
    )
    monitoring_file.upload()


def monitor_housekeeping_plots(
    sys_params: HaloSysParams, period: PeriodType, product: MonitoringProduct
) -> list[MonitoringVisualization]:
    plots = []
    for variable in product.variables:
        plots.append(plot_housekeeping_variable(sys_params, period, variable))
    return plots


def plot_housekeeping_variable(
    sys_params: HaloSysParams,
    period: PeriodType,
    variable: MonitoringVariable,
) -> MonitoringVisualization:
    fig, ax = plt.subplots()
    y = getattr(sys_params, variable.id.replace("-", "_"))
    ax.scatter(sys_params.time, y, **SCATTER_OPTS)
    set_xlim_for_period(ax, period, sys_params.time, pad=0.025)
    format_time_axis(ax)
    pretty_ax(ax, grid="y")
    fig_ = save_fig(fig)
    vis = MonitoringVisualization(fig_.bytes, variable, Dimensions(fig, [ax]))
    plt.close(fig)
    return vis


def monitor_background(opts: MonitorOptions) -> None:
    pid = instrument_uuid_to_pid(opts.api_client, opts.instrument_uuid)

    date_opts: RawFilesDatePayload = {}
    if not isinstance(opts.period, All):
        start, stop = opts.period.to_interval_padded(days=1)
        date_opts = {"date_from": start, "date_to": stop}

    records = opts.api_client.raw_files(
        site_id=opts.site,
        instrument_pid=pid,
        filename_prefix="Background_",
        filename_suffix=".txt",
        **date_opts,
    )
    if not records:
        raise ValueError(
            f"No raw files for monitoring period {opts.period} {opts.product.id} {opts.site} {pid}"
        )

    with TemporaryDirectory() as tempdir:
        (paths, _uuids) = opts.storage_api.download_raw_data(records, Path(tempdir))
        bgs = HaloBg.from_srcs(paths)
    counter = Counter((bg.signal.shape[1] for bg in bgs))
    most_common_ngates = counter.most_common()[0][0]
    bgs = [bg for bg in bgs if bg.signal.shape[1] == most_common_ngates]
    bg = HaloBg.merge(bgs).sorted_by_time().non_strictly_increasing_timesteps_removed()
    if not isinstance(opts.period, All):
        start, stop = opts.period.to_interval()
        dtype = bg.time.dtype
        start_time = np.datetime64(start).astype(dtype)
        stop_time = np.datetime64(stop + datetime.timedelta(days=1)).astype(dtype)
        select = (start_time <= bg.time) & (bg.time < stop_time)
        bg = bg[select]
    if len(bg.time) == 0:
        raise ValueError(
            f"No timestamps for monitoring period {opts.period} {opts.product.id} {opts.site} {pid}"
        )
    monitoring_file = MonitoringFile(
        opts.instrument_uuid,
        opts.site,
        opts.period,
        opts.product,
        [p for p in monitor_background_plots(bg, opts.period, opts.product) if p],
        opts.md_api,
        opts.storage_api,
    )
    monitoring_file.upload()


def monitor_background_plots(
    bg: HaloBg, period: PeriodType, product: MonitoringProduct
) -> list[MonitoringVisualization]:
    plots = []
    for variable in product.variables:
        plots.append(plot_background_variable(bg, period, variable))
    return plots


def plot_background_variable(
    bg: HaloBg, period: PeriodType, variable: MonitoringVariable
) -> MonitoringVisualization:
    match variable.id:
        case "background-profile":
            return plot_background_profile(bg, period, variable)
        case "background-profile-variance":
            return plot_background_variance(bg, period, variable)
        case "time-averaged-background-profile-range":
            return plot_time_averaged_background_profile(bg, period, variable)
        case _:
            raise NotImplementedError(
                f"Variable '{variable.id}' not implemented for halo-doppler-lidar"
            )


def plot_background_profile(
    bg: HaloBg, period: PeriodType, variable: MonitoringVariable
) -> MonitoringVisualization:
    fig, ax = plt.subplots()
    vmin, vmax = np.percentile(bg.signal.ravel(), [5, 95])
    cax = ax.pcolormesh(
        bg.time, np.arange(bg.signal.shape[1]), bg.signal.T, vmin=vmin, vmax=vmax
    )
    ax.set_ylabel("Range gate")
    add_colorbar(fig, ax, cax, orientation="horizontal", shrink=0.5, pad=0.1, aspect=30)
    set_xlim_for_period(ax, period, bg.time)
    format_time_axis(ax)
    pretty_ax_2d(ax)
    fig_ = save_fig(fig)
    vis = MonitoringVisualization(fig_.bytes, variable, Dimensions(fig, [ax]))
    plt.close(fig)
    return vis


def plot_background_variance(
    bg: HaloBg, period: PeriodType, variable: MonitoringVariable
) -> MonitoringVisualization:
    fig, ax = plt.subplots()
    var = bg.signal.var(axis=1)
    ax.scatter(bg.time, var, **SCATTER_OPTS)
    set_xlim_for_period(ax, period, bg.time)
    format_time_axis(ax)
    pretty_ax(ax, grid="y")
    fig_ = save_fig(fig)
    vis = MonitoringVisualization(fig_.bytes, variable, Dimensions(fig, [ax]))
    plt.close(fig)
    return vis


def plot_time_averaged_background_profile(
    bg: HaloBg, _: PeriodType, variable: MonitoringVariable
) -> MonitoringVisualization:
    fig, ax = plt.subplots()
    mean = bg.signal.mean(axis=0)
    range_ = np.arange(bg.signal.shape[1])
    # Lowest ~3 range gates are often noisy, filter those out from the plot
    # if they deviate from median too much
    med_mean = np.median(mean)
    abs_rel_dif = np.abs(np.abs(mean / med_mean) - 1)
    select = (range_ > 3) | (abs_rel_dif < 0.001)

    ax.scatter(range_[select], mean[select], **SCATTER_OPTS)
    ax.set_xlabel("Range gate")

    pretty_ax(ax, grid="y")
    fig_ = save_fig(fig)
    vis = MonitoringVisualization(fig_.bytes, variable, Dimensions(fig, [ax]))
    plt.close(fig)
    return vis


def monitor_signal(opts: MonitorOptions) -> None:
    pid = instrument_uuid_to_pid(opts.api_client, opts.instrument_uuid)

    date_opts: RawFilesDatePayload = {}
    if not isinstance(opts.period, All):
        start, stop = opts.period.to_interval()
        date_opts = {"date_from": start, "date_to": stop}

    records = opts.api_client.raw_files(
        site_id=opts.site,
        instrument_pid=pid,
        filename_suffix=".hpl",
        **date_opts,
    )

    records = [r for r in records if "cross" not in r.tags]
    if not records:
        raise ValueError(
            f"No raw files for monitoring period {opts.period} {opts.product.id} {opts.site} {pid}"
        )

    with TemporaryDirectory() as tempdir:
        (paths, _uuids) = opts.storage_api.download_raw_data(records, Path(tempdir))
        raws = HaloHpl.from_srcs(paths)

    def is_stare(raw: HaloHpl) -> bool:
        elevations = set(np.round(raw.elevation))
        if len(elevations) != 1:
            return False
        elevation = elevations.pop()
        if np.abs(elevation - 90) > 10:
            return False
        return True

    raws = [r for r in raws if is_stare(r)]
    if not raws:
        raise ValueError(
            f"No raw stare files found for {opts.period} {opts.instrument_uuid} {opts.product.id}"
        )
    counter = Counter((raw.intensity.shape[1] for raw in raws))
    most_common_ngates = counter.most_common()[0][0]
    raws = [raw for raw in raws if raw.intensity.shape[1] == most_common_ngates]
    raw = (
        HaloHpl.merge(raws).sorted_by_time().non_strictly_increasing_timesteps_removed()
    )
    if not isinstance(opts.period, All):
        start, stop = opts.period.to_interval()
        dtype = raw.time.dtype
        start_time = np.datetime64(start).astype(dtype)
        stop_time = np.datetime64(stop + datetime.timedelta(days=1)).astype(dtype)
        select = (start_time <= raw.time) & (raw.time < stop_time)
        raw = raw[select]

    if len(raw.time) == 0:
        raise ValueError(
            f"No timestamps for monitoring period {opts.period} {opts.product.id} {opts.site} {pid}"
        )

    monitoring_file = MonitoringFile(
        opts.instrument_uuid,
        opts.site,
        opts.period,
        opts.product,
        monitor_signal_plots(raw, opts.period, opts.product),
        opts.md_api,
        opts.storage_api,
    )
    monitoring_file.upload()


def monitor_signal_plots(
    raw: HaloHpl, period: PeriodType, product: MonitoringProduct
) -> list[MonitoringVisualization]:
    plots = []
    for variable in product.variables:
        plots.append(plot_signal_variable(raw, period, variable))
    return plots


def plot_signal_variable(
    raw: HaloHpl, period: PeriodType, variable: MonitoringVariable
) -> MonitoringVisualization:
    match variable.id:
        case "radial-velocity-histogram":
            return plot_radial_velocity_histogram(raw, period, variable)
        case "signal-radial-velocity":
            return plot_signal_radial_velocity(raw, period, variable)
        case _:
            raise NotImplementedError(
                f"Variable '{variable.id}' not implemented for halo-doppler-lidar"
            )


def plot_radial_velocity_histogram(
    raw: HaloHpl, _: PeriodType, variable: MonitoringVariable
) -> MonitoringVisualization:
    fig, ax = plt.subplots()
    bins = _compute_radial_velocity_bins(raw)
    ax.hist(raw.radial_velocity.ravel(), bins=bins)
    ax.ticklabel_format(style="scientific", axis="y", scilimits=(0, 0))
    ax.set_xlabel("Radial velocity")
    ax.set_ylabel("Count")
    pretty_ax(ax, grid="both")
    fig_ = save_fig(fig)
    vis = MonitoringVisualization(fig_.bytes, variable, Dimensions(fig, [ax]))
    plt.close(fig)
    return vis


def _compute_radial_velocity_bins(raw: HaloHpl) -> list[float] | int:
    v_uniq = np.unique(np.round(raw.radial_velocity.ravel(), decimals=5))
    if len(v_uniq) < 100 or len(v_uniq) > 3000:
        return 100
    midpoints = (v_uniq[:-1] + v_uniq[1:]) / 2
    left_gap = v_uniq[1] - v_uniq[0]
    right_gap = v_uniq[-1] - v_uniq[-2]
    left_edge = v_uniq[0] - left_gap / 2
    right_edge = v_uniq[-1] + right_gap / 2
    bins = np.concatenate(([left_edge], midpoints, [right_edge]))
    return bins.tolist()


def plot_signal_radial_velocity(
    raw: HaloHpl, _: PeriodType, variable: MonitoringVariable
) -> MonitoringVisualization:
    fig, ax = plt.subplots()
    vmin, vmax = np.percentile(raw.intensity.ravel(), [2, 95])
    select = (vmin < raw.intensity) & (raw.intensity < vmax)
    cax = ax.hexbin(raw.intensity[select].ravel(), raw.radial_velocity[select].ravel())
    cbar = add_colorbar(fig, ax, cax)
    scientific_cbar(cbar)
    ax.set_xlabel("Intensity")
    ax.set_ylabel("Radial velocity")
    pretty_ax(ax)
    fig_ = save_fig(fig)
    vis = MonitoringVisualization(fig_.bytes, variable, Dimensions(fig, [ax]))
    plt.close(fig)
    return vis
