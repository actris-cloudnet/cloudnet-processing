from dataclasses import dataclass
from typing import Literal

import numpy as np
import numpy.ma as ma
import numpy.typing as npt
from cloudnet_api_client.containers import Site
from cloudnetpy.utils import interpolate_2D_along_y


class MissingEarthCAREDataError(Exception):
    pass


def calc_distances(
    latitudes: npt.NDArray, longitudes: npt.NDArray, site: Site
) -> np.ndarray:
    assert site.latitude is not None
    assert site.longitude is not None
    R = 6371.0
    lat1 = np.radians(latitudes)
    lon1 = np.radians(longitudes)
    lat2 = np.radians(site.latitude)
    lon2 = np.radians(site.longitude)
    dlon = lon1 - lon2
    dlat = lat1 - lat2
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c


@dataclass
class BasicData:
    lat: npt.NDArray
    lon: npt.NDArray
    data: npt.NDArray


@dataclass
class Data:
    time: npt.NDArray
    height: npt.NDArray
    echo: ma.MaskedArray
    velocity: ma.MaskedArray
    lat: npt.NDArray | None = None
    lon: npt.NDArray | None = None
    distances: npt.NDArray | None = None
    baseline: str = ""
    filename: str = ""

    def to_regular_height_grid(self) -> None:
        target_height = np.linspace(-1, 12, 200)
        for attr in ("echo", "velocity"):
            data = getattr(self, attr)
            if data.mask.all():
                raise MissingEarthCAREDataError("All data is masked.")
            if self.height.ndim == 2:
                interpolated_data = ma.masked_all((data.shape[0], len(target_height)))
                for i in range(data.shape[0]):
                    source_height = self.height[i]
                    z = data[i]
                    interpolated_data[i] = interpolate_masked_1d(
                        source_height, z, target_height, method="nearest"
                    )
            else:
                interpolated_data = interpolate_2D_along_y(
                    self.height, data, target_height
                )

            setattr(self, attr, interpolated_data)

        self.height = target_height

    def filter(self) -> None:
        """Applies a simple noise filter to the echo and velocity data."""
        n_top_gates = 20
        far = self.echo[:, -n_top_gates:].compressed()
        median = np.median(far)
        mad = np.median(np.abs(far - median))
        sigma = 1.4826 * mad if mad > 0 else np.std(far)
        threshold = max(median + 2 * sigma, -35)
        mask = self.echo < threshold
        self.echo.mask = mask
        self.velocity.mask = mask

    def screen_time(self, ind: list[int] | npt.NDArray[np.integer]) -> None:
        """Keeps only the selected time indices in all relevant attributes."""
        attributes = ["time", "echo", "velocity", "lat", "lon", "distances"]
        if self.height.shape == self.echo.shape:
            attributes.append("height")
        for attr in attributes:
            value = getattr(self, attr)
            if value is not None:
                setattr(self, attr, value[ind])

    def mask_ground_echo(self) -> None:
        """Masks out near-ground echoes below 2 m."""
        mask = self.height < 2
        self.echo.mask = mask
        self.velocity.mask = mask


def interpolate_masked_1d(
    x: np.ndarray,
    y: ma.MaskedArray,
    x_new: np.ndarray,
    method: Literal["linear", "nearest"] = "linear",
) -> ma.MaskedArray:
    """
    Interpolate a masked 1D array while preserving the mask.

    Args:
        x: 1D array of source coordinates.
        y: 1D masked array of data values.
        x_new: 1D array of target coordinates.
        method: 'linear' or 'nearest' interpolation.

    Returns:
        Masked array of interpolated values (same mask behavior as input).
    """
    if y.ndim != 1:
        raise ValueError("Input 'y' must be 1D.")
    if x.shape != y.shape:
        raise ValueError("x and y must have the same shape.")

    valid_mask = (~y.mask) & np.isfinite(x) & np.isfinite(y)
    if not np.any(valid_mask):
        return ma.masked_all_like(x_new)

    x_valid = x[valid_mask]
    y_valid = y[valid_mask]

    if method == "linear":
        y_interp = np.interp(x_new, x_valid, y_valid, left=np.nan, right=np.nan)
    elif method == "nearest":
        idx = np.searchsorted(x_valid, x_new)
        idx = np.clip(idx, 1, len(x_valid) - 1)
        left = x_valid[idx - 1]
        right = x_valid[idx]
        choose_right = np.abs(x_new - right) < np.abs(x_new - left)
        nearest_idx = np.where(choose_right, idx, idx - 1)
        y_interp = y_valid[nearest_idx]
    else:
        raise ValueError("method must be 'linear' or 'nearest'")

    mask = (x_new < x_valid.min()) | (x_new > x_valid.max()) | np.isnan(y_interp)
    return ma.MaskedArray(y_interp, mask=mask)


def time_to_faction_hour(time: npt.NDArray) -> npt.NDArray:
    return np.array(
        [
            t.hour + t.minute / 60 + t.second / 3600 + t.microsecond / 3_600_000_000
            for t in time
        ],
        dtype=np.float64,
    )
