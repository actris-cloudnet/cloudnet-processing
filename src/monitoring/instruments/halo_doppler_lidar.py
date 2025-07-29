import datetime
from collections import Counter
from tempfile import TemporaryDirectory

import matplotlib.pyplot as plt
import numpy as np
from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import Instrument, Site
from doppy.raw import HaloBg, HaloHpl, HaloSysParams

from monitoring.monitoring_file import (
    Dimensions,
    MonitoringFile,
    MonitoringVisualization,
)
from monitoring.period import Period, PeriodWithRange
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
from monitoring.utils import range_from_period


def monitor(
    client: APIClient,
    site: Site,
    instrument: Instrument,
    period: Period,
    product: MonitoringProduct,
) -> None:
    match product.id:
        case "halo-doppler-lidar_housekeeping":
            monitor_housekeeping(client, site, instrument, period, product)
        case "halo-doppler-lidar_background":
            monitor_background(client, site, instrument, period, product)
        case "halo-doppler-lidar_signal":
            monitor_signal(client, site, instrument, period, product)

        case _:
            raise NotImplementedError(
                f"Monitoring product '{product.id}' not implemented for '{instrument.instrument_id}'"
            )


def monitor_housekeeping(
    client: APIClient,
    site: Site,
    instrument: Instrument,
    period: Period,
    product: MonitoringProduct,
) -> None:
    start, end = range_from_period(period)

    start -= datetime.timedelta(days=32)
    end += datetime.timedelta(days=32)
    raw_files = client.raw_files(
        site_id=site.id,
        instrument_pid=instrument.pid,
        date_from=start,
        date_to=end,
        filename_prefix="system_parameters_",
        filename_suffix=".txt",
    )
    if not raw_files:
        raise ValueError(
            f"Not raw files found for {period} {instrument.name} {product.id}"
        )
    with TemporaryDirectory() as tempdir:
        paths = client.download(raw_files, tempdir, progress=False)
        sys_params_list = [HaloSysParams.from_src(p) for p in paths]
    sys_params = (
        HaloSysParams.merge(sys_params_list)
        .sorted_by_time()
        .non_strictly_increasing_timesteps_removed()
    )
    if isinstance(period, PeriodWithRange):
        start_time = np.datetime64(period.start_date).astype(sys_params.time.dtype)
        end_time = np.datetime64(period.end_date + datetime.timedelta(days=1)).astype(
            sys_params.time.dtype
        )
        select = (start_time <= sys_params.time) & (sys_params.time < end_time)
        sys_params = sys_params[select]

    monitoring_file = MonitoringFile(
        instrument,
        site,
        period,
        product,
        monitor_housekeeping_plots(sys_params, period, product),
    )
    monitoring_file.upload()


def monitor_housekeeping_plots(
    sys_params: HaloSysParams, period: Period, product: MonitoringProduct
) -> list[MonitoringVisualization]:
    plots = []
    for variable in product.variables:
        plots.append(plot_housekeeping_variable(sys_params, period, variable))
    return plots


def plot_housekeeping_variable(
    sys_params: HaloSysParams,
    period: Period,
    variable: MonitoringVariable,
) -> MonitoringVisualization:
    fig, ax = plt.subplots()
    y = getattr(sys_params, variable.id.replace("-", "_"))
    ax.scatter(sys_params.time, y, **SCATTER_OPTS)
    set_xlim_for_period(ax, period, sys_params.time, pad=0.025)
    format_time_axis(ax)
    pretty_ax(ax, grid="y")
    fig_ = save_fig(fig)
    return MonitoringVisualization(
        fig_.bytes, variable, Dimensions(fig_.width, fig_.height)
    )


def monitor_background(
    client: APIClient,
    site: Site,
    instrument: Instrument,
    period: Period,
    product: MonitoringProduct,
) -> None:
    start, end = range_from_period(period)
    start -= datetime.timedelta(days=1)
    end += datetime.timedelta(days=1)
    raw_files = client.raw_files(
        site_id=site.id,
        instrument_pid=instrument.pid,
        date_from=start,
        date_to=end,
        filename_prefix="Background_",
        filename_suffix=".txt",
    )
    raw_files = [r for r in raw_files if "cross" not in r.tags]
    if not raw_files:
        raise ValueError(
            f"Not raw files found for {period} {instrument.name} {product.id}"
        )
    with TemporaryDirectory() as tempdir:
        paths = client.download(raw_files, tempdir, progress=False)
        bgs = HaloBg.from_srcs(paths)
    counter = Counter((bg.signal.shape[1] for bg in bgs))
    most_common_ngates = counter.most_common()[0][0]
    bgs = [bg for bg in bgs if bg.signal.shape[1] == most_common_ngates]
    bg = HaloBg.merge(bgs).sorted_by_time().non_strictly_increasing_timesteps_removed()

    if isinstance(period, PeriodWithRange):
        start_time = np.datetime64(period.start_date).astype(bg.time.dtype)
        end_time = np.datetime64(period.end_date + datetime.timedelta(days=1)).astype(
            bg.time.dtype
        )
        select = (start_time <= bg.time) & (bg.time < end_time)
        bg = bg[select]

    monitoring_file = MonitoringFile(
        instrument,
        site,
        period,
        product,
        [p for p in monitor_background_plots(bg, period, product) if p],
    )
    monitoring_file.upload()


def monitor_background_plots(
    bg: HaloBg, period: Period, product: MonitoringProduct
) -> list[MonitoringVisualization]:
    plots = []
    for variable in product.variables:
        plots.append(plot_background_variable(bg, period, variable))
    return plots


def plot_background_variable(
    bg: HaloBg, period: Period, variable: MonitoringVariable
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
    bg: HaloBg, period: Period, variable: MonitoringVariable
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
    return MonitoringVisualization(
        fig_.bytes, variable, Dimensions(fig_.width, fig_.height)
    )


def plot_background_variance(
    bg: HaloBg, period: Period, variable: MonitoringVariable
) -> MonitoringVisualization:
    fig, ax = plt.subplots()
    var = bg.signal.var(axis=1)
    ax.scatter(bg.time, var, **SCATTER_OPTS)
    set_xlim_for_period(ax, period, bg.time)
    format_time_axis(ax)
    pretty_ax(ax, grid="y")
    fig_ = save_fig(fig)
    return MonitoringVisualization(
        fig_.bytes, variable, Dimensions(fig_.width, fig_.height)
    )


def plot_time_averaged_background_profile(
    bg: HaloBg, _: Period, variable: MonitoringVariable
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
    return MonitoringVisualization(
        fig_.bytes, variable, Dimensions(fig_.width, fig_.height)
    )


def monitor_signal(
    client: APIClient,
    site: Site,
    instrument: Instrument,
    period: Period,
    product: MonitoringProduct,
) -> None:
    start, end = range_from_period(period)

    start -= datetime.timedelta(days=1)
    end += datetime.timedelta(days=1)
    raw_files = client.raw_files(
        site_id=site.id,
        instrument_pid=instrument.pid,
        date_from=start,
        date_to=end,
        filename_suffix=".hpl",
    )
    raw_files = [r for r in raw_files if "cross" not in r.tags]
    if not raw_files:
        raise ValueError(
            f"No raw files found for {period} {instrument.name} {product.id}"
        )
    with TemporaryDirectory() as tempdir:
        paths = client.download(raw_files, tempdir, progress=False)
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
            f"No raw stare files found for {period} {instrument.name} {product.id}"
        )
    counter = Counter((raw.intensity.shape[1] for raw in raws))
    most_common_ngates = counter.most_common()[0][0]
    raws = [raw for raw in raws if raw.intensity.shape[1] == most_common_ngates]
    raw = (
        HaloHpl.merge(raws).sorted_by_time().non_strictly_increasing_timesteps_removed()
    )
    if isinstance(period, PeriodWithRange):
        start_time = np.datetime64(period.start_date).astype(raw.time.dtype)
        end_time = np.datetime64(period.end_date + datetime.timedelta(days=1)).astype(
            raw.time.dtype
        )
        select = (start_time <= raw.time) & (raw.time < end_time)
        raw = raw[select]

    monitoring_file = MonitoringFile(
        instrument,
        site,
        period,
        product,
        monitor_signal_plots(raw, period, product),
    )
    monitoring_file.upload()


def monitor_signal_plots(
    raw: HaloHpl, period: Period, product: MonitoringProduct
) -> list[MonitoringVisualization]:
    plots = []
    for variable in product.variables:
        plots.append(plot_signal_variable(raw, period, variable))
    return plots


def plot_signal_variable(
    raw: HaloHpl, period: Period, variable: MonitoringVariable
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
    raw: HaloHpl, _: Period, variable: MonitoringVariable
) -> MonitoringVisualization:
    fig, ax = plt.subplots()
    ax.hist(raw.radial_velocity.ravel(), bins=400)
    ax.ticklabel_format(style="scientific", axis="y", scilimits=(0, 0))
    ax.set_xlabel("Radial velocity")
    ax.set_ylabel("Count")
    pretty_ax(ax, grid="both")
    fig_ = save_fig(fig)
    return MonitoringVisualization(
        fig_.bytes, variable, Dimensions(fig_.width, fig_.height)
    )


def plot_signal_radial_velocity(
    raw: HaloHpl, _: Period, variable: MonitoringVariable
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
    return MonitoringVisualization(
        fig_.bytes, variable, Dimensions(fig_.width, fig_.height)
    )
