import datetime
import glob
import warnings
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

import cftime
import netCDF4
import numpy as np
import numpy.typing as npt
from cloudnet_api_client import APIClient
from cloudnetpy.utils import lin2db
from earthcare_downloader import download, search
from earthcare_downloader.version import __version__ as __version__
from numpy import ma

from processing.utils import utcnow

from . import utils
from .utils import BasicData, Data, MissingEarthCAREDataError

FILE_PATH = Path(__file__).resolve().parent


warnings.filterwarnings("ignore", message=".*valid_range not used*")


@dataclass
class SimuData:
    velocity: ma.MaskedArray
    echo: ma.MaskedArray
    height: npt.NDArray
    time: npt.NDArray
    time_pydate: npt.NDArray
    file_uuid: str
    model: str
    history: str
    date: datetime.date

    def map_time_indices(self) -> None:
        # Map 30s timestamps to distance-axis, which is sparser
        idx = np.linspace(0, len(self.time) - 1, len(self.velocity)).astype(int)
        self.time = self.time[idx]
        self.time_pydate = self.time_pydate[idx]

    def get_overpass_indices(
        self, target_time: datetime.datetime, n_profiles: int = 10
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
        distances = utils.calc_distances(lat, lon, self.site)
        return np.where(distances < self.distance)[0].tolist(), distances


def _read_cpr_geo(
    fname: Path,
) -> tuple[npt.NDArray, npt.NDArray, npt.NDArray, npt.NDArray]:
    with netCDF4.Dataset(fname, "r") as f:
        g = f["ScienceData"]["Geo"]
        lat = g["latitude"][:]
        lon = g["longitude"][:]
        height = g["binHeight"][:] / 1000
        time = g["profileTime"][:]
        timestamps = cftime.num2pydate(time, g["profileTime"].units)
    return lat, lon, np.flip(height, axis=1), timestamps


def _read_msi_data(
    fname: Path,
) -> tuple[ma.MaskedArray, ma.MaskedArray, ma.MaskedArray]:
    with netCDF4.Dataset(fname, "r") as f:
        g = f["ScienceData"]
        lat = g["latitude"][:]
        lon = g["longitude"][:]
        data = g["cloud_top_height"][:]
    return lat, lon, data


def cloudnet_earthcare(
    site_id: str,
    cpr_simu_path: Path,
    output_file: Path,
    cache_dir: Path = Path("/tmp"),
    distance: float = 15,
    uuid: UUID | None = None,
) -> str:
    # Synthetic CPR data
    simu = _read_simulation_file(cpr_simu_path)
    simu.map_time_indices()
    synthetic = Data(simu.time, simu.height, simu.echo, simu.velocity)

    # Earthcare CPR_NOM_L1B data
    fetcher = Fetcher(site_id, simu.date, distance)
    cpr_path, baseline = fetcher.fetch_ec("CPR_NOM_1B", cache_dir)
    lat, lon, height, time = _read_cpr_geo(cpr_path)
    ind, distances = fetcher.get_distances(lat, lon)
    if not ind:
        raise MissingEarthCAREDataError(
            f"No EarthCARE CPR_NOM_1B data within {distance} km "
            f"from site {site_id} on {simu.date}"
        )
    velocity, echo = _read_ec_data(cpr_path)
    ec = Data(
        time=time,
        height=height,
        echo=echo,
        velocity=velocity,
        lat=lat,
        lon=lon,
        distances=distances,
        baseline=baseline,
        filename=cpr_path.name,
    )
    ec.screen_time(ind)
    ec.to_regular_height_grid()
    ec.filter()

    overpass_indices = simu.get_overpass_indices(ec.time[0], n_profiles=10)
    synthetic.screen_time(overpass_indices)
    synthetic.to_regular_height_grid()

    ec.time = utils.time_to_faction_hour(ec.time)

    # Earthcare MSI_COP_2A data (optional)
    try:
        msi_path, _ = fetcher.fetch_ec("MSI_COP_2A", cache_dir)
        msi = _get_msi_data(msi_path, fetcher, distance=100)
    except MissingEarthCAREDataError:
        msi = BasicData(
            lat=np.array([]),
            lon=np.array([]),
            data=np.array([]),
        )
    return _save_results(output_file, ec, synthetic, fetcher, simu, msi, uuid)


def _get_msi_data(msi_path: Path, fetcher: Fetcher, distance: float) -> BasicData:
    lat, lon, data = _read_msi_data(msi_path)
    mean_lats = np.mean(lat, axis=1)
    mean_lons = np.mean(lon, axis=1)
    distances = utils.calc_distances(mean_lats, mean_lons, fetcher.site)
    ind = distances < distance
    lat = lat[ind].flatten()
    lon = lon[ind].flatten()
    data = data[ind].flatten()
    valid_ind = ~lat.mask & ~lon.mask
    lat = lat[valid_ind]
    lon = lon[valid_ind]
    data = data[valid_ind]
    return BasicData(lat, lon, data)


def _save_results(
    output_file: Path,
    ec: Data,
    synthetic: Data,
    fetcher: Fetcher,
    simu: SimuData,
    msi: BasicData,
    uuid: UUID | None,
) -> str:
    EARTHCARE = "EarthCARE"
    MSI = "Multispectral Imager (MSI)"
    CPR = "Cloud Profiling Radar (CPR)"
    ESA_LICENSE = "ESA's Earth Observation Terms and Conditions"
    CLOUDNET_LICENCE = "CC-BY-4.0"
    file_uuid = str(uuid) if uuid is not None else str(uuid4())
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with netCDF4.Dataset(output_file, "w", format="NETCDF4_CLASSIC") as nc:
        # Dimensions
        nc.createDimension("cpr", len(ec.time))
        nc.createDimension("time", len(synthetic.time))
        nc.createDimension("height", len(synthetic.height))
        nc.createDimension("msi", len(msi.lat))

        # Variables
        fill_value = netCDF4.default_fillvals["f4"]
        nc.createVariable("time_cpr", "f8", ("cpr",))
        nc.createVariable("time", "f4", ("time",))
        nc.createVariable("height", "f4", ("height",))
        nc.createVariable("echo_cpr", "f4", ("cpr", "height"), fill_value=fill_value)
        nc.createVariable("ze_sat", "f4", ("time", "height"), fill_value=fill_value)
        nc.createVariable("v_cpr", "f4", ("cpr", "height"), fill_value=fill_value)
        nc.createVariable(
            "vm_sat_folded", "f4", ("time", "height"), fill_value=fill_value
        )
        nc.createVariable("latitude", "f4", ("time",))
        nc.createVariable("longitude", "f4", ("time",))
        nc.createVariable("latitude_cpr", "f4", ("cpr",))
        nc.createVariable("longitude_cpr", "f4", ("cpr",))
        nc.createVariable("distance", "f4", ("cpr",))
        nc.createVariable("altitude", "f4", ("time",))
        nc.createVariable("latitude_msi", "f4", ("msi",))
        nc.createVariable("longitude_msi", "f4", ("msi",))
        nc.createVariable("cloud_top_height", "f4", ("msi",), fill_value=fill_value)

        time_units = f"hours since {fetcher.date.strftime('%Y-%m-%d')} 00:00:00 +00:00"

        var = nc.variables["latitude_msi"]
        var[:] = msi.lat
        var.long_name = "Latitude of MSI ground track"
        var.standard_name = "latitude"
        var.units = "degree_north"
        var.source = f"{EARTHCARE} {MSI}"
        var.license = ESA_LICENSE

        var = nc.variables["longitude_msi"]
        var[:] = msi.lon
        var.long_name = "Longitude of MSI ground track"
        var.standard_name = "longitude"
        var.units = "degree_east"
        var.source = f"{EARTHCARE} {MSI}"
        var.license = ESA_LICENSE

        var = nc.variables["cloud_top_height"]
        var[:] = msi.data
        var.long_name = "Cloud top height"
        var.units = "m"
        var.source = f"{EARTHCARE} {MSI}"
        var.license = ESA_LICENSE

        var = nc.variables["latitude_cpr"]
        var[:] = ec.lat  # type: ignore
        var.long_name = "Latitude of CPR ground track"
        var.standard_name = "latitude"
        var.units = "degree_north"
        var.source = f"{EARTHCARE} {CPR}"
        var.license = ESA_LICENSE

        var = nc.variables["longitude_cpr"]
        var[:] = ec.lon  # type: ignore
        var.long_name = "Longitude of CPR ground track"
        var.standard_name = "longitude"
        var.units = "degree_east"
        var.source = f"{EARTHCARE} {CPR}"
        var.license = ESA_LICENSE

        var = nc.variables["distance"]
        var[:] = ec.distances  # type: ignore
        var.long_name = "Distance between site and CPR ground track"
        var.units = "km"

        var = nc.variables["time_cpr"]
        var[:] = ec.time
        var.long_name = "Time UTC"
        var.units = time_units
        var.standard_name = "time"
        var.calendar = "standard"
        var.source = f"{EARTHCARE} {CPR}"
        var.license = ESA_LICENSE

        CPR_COMMENT = (
            f"Noise-screened {EARTHCARE} CPR_NOM_1B data "
            f"within {fetcher.distance} km from the site."
        )

        var = nc.variables["echo_cpr"]
        var[:] = ec.echo
        var.long_name = "Radar reflectivity factor"
        var.units = "dBZ"
        var.source = f"{EARTHCARE} {CPR}"
        var.comment = CPR_COMMENT
        var.license = ESA_LICENSE

        var = nc.variables["v_cpr"]
        var[:] = ec.velocity
        var.long_name = "Doppler velocity"
        var.units = "m s-1"
        var.source = f"{EARTHCARE} {CPR}"
        var.comment = CPR_COMMENT
        var.license = ESA_LICENSE

        orbital_radar_comment = (
            "Cloudnet data convolved to CPR resolution using orbital-radar software."
        )

        var = nc.variables["ze_sat"]
        var[:] = synthetic.echo
        var.long_name = "Convolved and integrated radar reflectivity factor"
        var.units = "dBZ"
        var.source = simu.model
        var.comment = orbital_radar_comment
        var.license = CLOUDNET_LICENCE

        var = nc.variables["vm_sat_folded"]
        var[:] = synthetic.velocity
        var.long_name = (
            "Doppler velocity with noise, satellite motion error, and folding"
        )
        var.units = "m s-1"
        var.source = simu.model
        var.comment = orbital_radar_comment
        var.license = CLOUDNET_LICENCE

        var = nc.variables["time"]
        var[:] = synthetic.time
        var.long_name = "Time UTC"
        var.units = time_units
        var.standard_name = "time"
        var.calendar = "standard"
        var.source = simu.model

        var = nc.variables["latitude"]
        var[:] = fetcher.site.latitude if fetcher.site.latitude else np.nan
        var.long_name = "Latitude of site"
        var.standard_name = "latitude"
        var.units = "degree_north"
        var.comment = "Latitude of the ground-based measurement site."

        var = nc.variables["longitude"]
        var[:] = fetcher.site.longitude if fetcher.site.longitude else np.nan
        var.long_name = "Longitude of site"
        var.standard_name = "longitude"
        var.units = "degree_east"
        var.comment = "Longitude of the ground-based measurement site."

        var = nc.variables["altitude"]
        var[:] = fetcher.site.altitude if fetcher.site.altitude else np.nan
        var.long_name = "Altitude of site"
        var.standard_name = "altitude"
        var.units = "m"
        var.comment = "Altitude of the ground-based measurement site."

        var = nc.variables["height"]
        var[:] = synthetic.height * 1000
        var.long_name = "Height above mean sea level"
        var.standard_name = "height_above_mean_sea_level"
        var.units = "m"
        var.source = simu.model

        # Global attributes
        nc.cloudnet_file_type = "cpr-validation"
        nc.Conventions = "CF-1.8"
        nc.file_uuid = file_uuid
        nc.title = f"CPR validation from {fetcher.site.human_readable_name}"
        nc.year = fetcher.date.strftime("%Y")
        nc.month = fetcher.date.strftime("%m")
        nc.day = fetcher.date.strftime("%d")
        nc.location = fetcher.site.human_readable_name
        nc.history = _make_history(simu.history)
        nc.source = f"{simu.model}\n{EARTHCARE} {CPR}\n{EARTHCARE} {MSI}"
        nc.references = "https://doi.org/10.5194/gmd-18-101-2025"
        nc.source_file_uuids = simu.file_uuid
        nc.cpr_l1b_baseline = ec.baseline
        nc.cpr_l1b_filename = ec.filename
        nc.earthcare_downloader_version = __version__
    return file_uuid


def _make_history(history: str) -> str:
    timestamp = utcnow().strftime("%Y-%m-%d %H:%M:%S +00:00")
    new_entry = f"{timestamp} - cpr-validation file created."
    return f"{new_entry}\n{history}"


def _read_ec_data(fname: Path) -> tuple[ma.MaskedArray, ma.MaskedArray]:
    with netCDF4.Dataset(fname, "r") as nc:
        velocity = nc["ScienceData"]["Data"]["dopplerVelocity"][:]
        echo = lin2db(nc["ScienceData"]["Data"]["radarReflectivityFactor"][:])
    return ma.masked_array(np.flip(velocity, axis=1)), ma.masked_array(
        np.flip(echo, axis=1)
    )


def _read_simulation_file(
    fname: Path,
) -> SimuData:
    with netCDF4.Dataset(fname, "r") as nc:
        time = nc["time"]
        return SimuData(
            velocity=nc["vm_sat_folded"][:],
            echo=nc["ze_sat"][:],
            height=nc["height_sat"][:] / 1000,
            time=time[:],
            time_pydate=cftime.num2pydate(time[:], units=time.units),
            file_uuid=nc.file_uuid,
            model=nc.source,
            history=nc.history,
            date=datetime.date(int(nc.year), int(nc.month), int(nc.day)),
        )
