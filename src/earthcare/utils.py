import datetime
import glob
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import cftime
import netCDF4
import numpy as np
import numpy.ma as ma
import numpy.typing as npt
from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import Site
from cloudnetpy.utils import interpolate_2D_along_y
from earthcare_downloader import download, search

from processing.utils import utcnow


class MissingEarthCAREDataError(Exception):
    pass


@dataclass
class BasicData:
    lat: npt.NDArray
    lon: npt.NDArray
    data: npt.NDArray


@dataclass
class Data:
    data: dict[str, ma.MaskedArray]
    time: npt.NDArray
    height: npt.NDArray
    lat: npt.NDArray | None = None
    lon: npt.NDArray | None = None
    distances: npt.NDArray | None = None
    baseline: str = ""
    filename: str = ""
    full_path: Path | None = None

    def to_regular_height_grid(self) -> None:
        target_height = np.linspace(-1, 12, 200)
        for key, array in self.data.items():
            if array.mask.all():
                raise MissingEarthCAREDataError("All data is masked.")
            if self.height.ndim == 2:
                interpolated_data = ma.masked_all((array.shape[0], len(target_height)))
                for i in range(array.shape[0]):
                    source_height = self.height[i]
                    z = array[i]
                    interpolated_data[i] = _interpolate_masked_1d(
                        source_height, z, target_height, method="nearest"
                    )
            else:
                interpolated_data = interpolate_2D_along_y(
                    self.height, array, target_height
                )
            self.data[key] = interpolated_data
        self.height = target_height

    def screen_time(self, ind: list[int] | npt.NDArray[np.integer]) -> None:
        attributes = ["time", "lat", "lon", "distances"]
        first_array = next(iter(self.data.values()))
        if self.height.shape == first_array.shape:
            attributes.append("height")
        for attr in attributes:
            value = getattr(self, attr)
            if value is not None:
                setattr(self, attr, value[ind])
        for key in self.data:
            self.data[key] = self.data[key][ind]

    def filter(self) -> None:
        """Applies a simple noise filter to the echo and velocity data."""
        n_top_gates = 20
        far = self.data["echo"][:, -n_top_gates:].compressed()
        median = np.median(far)
        mad = np.median(np.abs(far - median))
        sigma = 1.4826 * mad if mad > 0 else np.std(far)
        threshold = max(median + 2 * sigma, -35)
        mask = self.data["echo"] < threshold
        self.data["echo"].mask = mask
        self.data["velocity"].mask = mask


@dataclass
class CloudnetData:
    data: dict[str, ma.MaskedArray]
    time: npt.NDArray
    height: npt.NDArray
    time_pydate: npt.NDArray
    file_uuid: str
    model: str
    history: str
    date: datetime.date

    def map_time_indices(self) -> None:
        # Map 30s timestamps to distance-axis, which is sparser
        first_data_array = next(iter(self.data.values()))
        idx = np.linspace(0, len(self.time) - 1, len(first_data_array)).astype(int)
        self.time = self.time[idx]
        self.time_pydate = self.time_pydate[idx]

    def get_overpass_indices(
        self, target_time: datetime.datetime, n_profiles: int
    ) -> npt.NDArray[np.integer]:
        # Ad-hoc method for "matching" Cloudnet data to EarthCARE overpass time
        closest_ind = int(np.argmin(np.abs(self.time_pydate - target_time)))
        ind0 = max(0, closest_ind - n_profiles)
        ind1 = min(len(self.time), closest_ind + n_profiles)
        return np.arange(ind0, ind1)


class Fetcher:
    def __init__(
        self,
        site_id: str,
        date: datetime.date,
        distance: float,
    ) -> None:
        self.site_id = site_id
        self.date = date
        self.distance = distance
        self._client = APIClient()
        self.site = self._client.site(site_id)

    def fetch_ec(self, product: str, directory: Path) -> tuple[Path, str]:
        ec_file = search(
            product=product,
            lat=self.site.latitude,
            lon=self.site.longitude,
            date=self.date,
            radius=self.distance,
        )
        if not ec_file:
            msg = f"No EarthCARE {product} data for {self.site.id} on {self.date}"
            raise MissingEarthCAREDataError(msg)
        if len(ec_file) > 1:
            warnings.warn(
                f"{len(ec_file)} EarthCARE {product} files found for "
                f"{self.site.id} on {self.date}. Using the first one."
            )
            ec_file = ec_file[:1]
        baseline = ec_file[0].baseline
        local_files = glob.glob(str(directory / "*.h5"))
        for local_file in local_files:
            local_file_path = Path(local_file)
            identifier = ec_file[0].filename.split(".")[0]
            if identifier in local_file_path.name:
                return local_file_path, baseline
        paths = download(ec_file, output_path=directory, unzip=True)
        for path in paths:
            if path.suffix == ".h5":
                return path, baseline
        raise RuntimeError("Problem finding EarthCARE file.")

    def get_distances(
        self,
        lat: npt.NDArray,
        lon: npt.NDArray,
    ) -> tuple[list[int], npt.NDArray[np.floating]]:
        distances = calc_distances(lat, lon, self.site)
        return np.where(distances < self.distance)[0].tolist(), distances


def read_cloudnet_file(
    fname: Path,
    keymap: dict[str, str],
    height_name: str,
) -> CloudnetData:
    with netCDF4.Dataset(fname, "r") as nc:
        time = nc["time"]
        return CloudnetData(
            data={k: nc[v][:] for k, v in keymap.items()},
            height=nc[height_name][:] / 1000,
            time=time[:],
            time_pydate=cftime.num2pydate(time[:], units=time.units),
            file_uuid=nc.file_uuid,
            model=nc.source,
            history=nc.history,
            date=datetime.date(int(nc.year), int(nc.month), int(nc.day)),
        )


def read_cpr_geo(
    fname: Path,
) -> tuple[npt.NDArray, npt.NDArray, npt.NDArray, npt.NDArray]:
    with netCDF4.Dataset(fname, "r") as f:
        g = f["ScienceData"]
        if "Geo" in g.groups:
            g = g["Geo"]
            time = g["profileTime"]
            height = g["binHeight"][:]
        else:
            time = g["time"]
            height = g["height"][:]
        lat = g["latitude"][:]
        lon = g["longitude"][:]
        height = height / 1000
        timestamps = cftime.num2pydate(time[:], time.units)
    return lat, lon, np.flip(height, axis=1), timestamps


def _interpolate_masked_1d(
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


def time_to_fraction_hour(time: npt.NDArray) -> npt.NDArray:
    return np.array(
        [
            t.hour + t.minute / 60 + t.second / 3600 + t.microsecond / 3_600_000_000
            for t in time
        ],
        dtype=np.float64,
    )


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


def make_history(history: str, product: str) -> str:
    timestamp = utcnow().strftime("%Y-%m-%d %H:%M:%S +00:00")
    new_entry = f"{timestamp} - {product} file created."
    return f"{new_entry}\n{history}"
