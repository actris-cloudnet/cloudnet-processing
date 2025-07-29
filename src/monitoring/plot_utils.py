import datetime
import io
from dataclasses import dataclass
from typing import Literal, TypedDict

import matplotlib.dates
import matplotlib.ticker
import numpy as np
from matplotlib.axes import Axes
from matplotlib.collections import PolyCollection, QuadMesh
from matplotlib.colorbar import Colorbar
from matplotlib.figure import Figure
from numpy.typing import NDArray

from monitoring.period import Period, PeriodWithRange

FONT_SIZE = 30
DPI = 400
HEIGHT_INCH = 16
ASPECT = 16 / 9


class ScatterOpts(TypedDict):
    s: int
    c: str


SCATTER_OPTS: ScatterOpts = {"s": 50, "c": "#2D9AE0"}


@dataclass
class Fig:
    bytes: bytes
    width: int
    height: int


def format_time_axis(ax: Axes) -> None:
    locator = matplotlib.dates.AutoDateLocator()
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(matplotlib.dates.ConciseDateFormatter(locator))


def pretty_ax(ax: Axes, grid: Literal["x", "y", "both"] | None = None) -> None:
    ax.set_facecolor("#f0f0f0")
    if grid is not None:
        ax.grid(True, axis=grid, color="white", linestyle="-", linewidth=4)
    ax.set_axisbelow(True)
    ax.tick_params(
        axis="both",
        length=12,
        width=3,
        direction="out",
        pad=15,
    )
    ax.xaxis.labelpad = 20
    ax.yaxis.labelpad = 20

    ax.spines["left"].set_position(("outward", 10))
    ax.spines["bottom"].set_position(("outward", 10))
    ax.spines["left"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_visible(False)


def scientific_cbar(cbar: Colorbar) -> None:
    formatter = matplotlib.ticker.ScalarFormatter()
    formatter.set_scientific(True)
    formatter.set_powerlimits((-1, 1))
    cbar.ax.yaxis.set_major_formatter(formatter)
    offset_text = cbar.ax.yaxis.get_offset_text()
    offset_text.set_verticalalignment("bottom")
    offset_text.set_horizontalalignment("right")


def add_colorbar(
    fig: Figure,
    ax: Axes,
    cax: PolyCollection | QuadMesh,
    shrink: float = 1,
    pad: float = 0.02,
    aspect: float = 20,
    orientation: Literal["vertical", "horizontal"] = "vertical",
) -> Colorbar:
    cbar = fig.colorbar(
        cax, ax=ax, orientation=orientation, shrink=shrink, pad=pad, aspect=aspect
    )
    cbar.outline.set_visible(False)  # type: ignore
    return cbar


def pretty_ax_2d(ax: Axes) -> None:
    ax.set_facecolor("#f0f0f0")
    ax.grid(True, axis="y", color="white", linestyle="-", linewidth=4)
    ax.set_axisbelow(True)
    ax.tick_params(
        axis="both",
        length=12,
        width=3,
        direction="out",
        pad=15,
    )
    ax.spines["left"].set_position(("outward", 15))
    ax.spines["bottom"].set_position(("outward", 15))
    ax.spines["left"].set_visible(False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["bottom"].set_visible(False)


def set_xlim_for_period(
    ax: Axes, period: Period, time: NDArray[np.datetime64], pad: float = 0
) -> None:
    if isinstance(period, PeriodWithRange):
        start_lim = np.datetime64(period.start_date).astype(time.dtype)
        end_lim = np.datetime64(period.end_date + datetime.timedelta(days=1)).astype(
            time.dtype
        )
        delta = (end_lim - start_lim) * pad
        ax.set_xlim(start_lim - delta, end_lim + delta)  # type: ignore[arg-type]


def save_fig(fig: Figure) -> Fig:
    aspect = ASPECT
    dpi = DPI
    height_inch = HEIGHT_INCH
    height_px = height_inch * dpi
    width_px = aspect * height_px
    fig.set_size_inches(width_px / dpi, height_px / dpi)

    font_size = FONT_SIZE
    for ax in fig.axes:
        ax.tick_params(axis="both", labelsize=font_size)
        ax.xaxis.label.set_fontsize(font_size)
        ax.yaxis.label.set_fontsize(font_size)
        ax.title.set_fontsize(font_size + 2)
        ax.xaxis.get_offset_text().set_fontsize(font_size)
        ax.yaxis.get_offset_text().set_fontsize(font_size)

    buf = io.BytesIO()
    bbox_extra_artists = [ax.yaxis.label for ax in fig.axes]
    fig.savefig(
        buf,
        format="png",
        dpi=dpi,
        bbox_inches="tight",
        pad_inches=0,
        bbox_extra_artists=bbox_extra_artists,
    )
    buf.seek(0)
    return Fig(buf.read(), int(width_px), int(height_px))
