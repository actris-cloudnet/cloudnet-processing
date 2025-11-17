import warnings
from pathlib import Path
from uuid import UUID, uuid4

import netCDF4
import numpy as np
from earthcare_downloader.version import __version__ as __version__
from numpy import ma

from . import utils
from .utils import CloudnetData, Data, Fetcher, MissingEarthCAREDataError

FILE_PATH = Path(__file__).resolve().parent


warnings.filterwarnings("ignore", message=".*valid_range not used*")


def cloudnet_vs_ec_classification(
    site_id: str,
    classification_file: Path,
    output_file: Path,
    cache_dir: Path = Path("/tmp"),
    distance: float = 30,
    uuid: UUID | None = None,
) -> str:
    # Cloudnet target classification
    cnet = utils.read_cloudnet_file(
        classification_file,
        keymap={"classification": "target_classification"},
        height_name="height",
    )
    cnet.map_time_indices()

    synthetic = Data(
        time=cnet.time,
        height=cnet.height,
        data={**cnet.data},
        full_path=classification_file,
    )

    # Earthcare CPR_TC__2A data
    fetcher = Fetcher(site_id, cnet.date, distance)
    cpr_path, baseline = fetcher.fetch_ec("CPR_TC__2A", cache_dir)
    lat, lon, height, time = utils.read_cpr_geo(cpr_path)
    ind, distances = fetcher.get_distances(lat, lon)
    if not ind:
        raise MissingEarthCAREDataError(
            f"No EarthCARE CPR_NOM_1B data within {distance} km "
            f"from site {site_id} on {cnet.date}"
        )

    ec = Data(
        time=time,
        height=height,
        data={"classification": _read_ec_data(cpr_path)},
        lat=lat,
        lon=lon,
        distances=distances,
        baseline=baseline,
        filename=cpr_path.name,
        full_path=cpr_path,
    )
    ec.screen_time(ind)
    ec.to_regular_height_grid()

    overpass_indices = cnet.get_overpass_indices(ec.time[0], n_profiles=40)
    synthetic.screen_time(overpass_indices)

    synthetic.to_regular_height_grid()

    ec.time = utils.time_to_fraction_hour(ec.time)

    return _save_results(output_file, ec, synthetic, fetcher, cnet, uuid)


def _read_ec_data(fname: Path) -> ma.MaskedArray:
    with netCDF4.Dataset(fname, "r") as nc:
        classification = nc["ScienceData"]["hydrometeor_classification"][:]
        return ma.masked_array(np.flip(classification, axis=1))


def _save_results(
    output_file: Path,
    ec: Data,
    synthetic: Data,
    fetcher: Fetcher,
    simu: CloudnetData,
    uuid: UUID | None,
) -> str:
    EARTHCARE = "EarthCARE"
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

        # Variables
        fill_value_int = netCDF4.default_fillvals["i4"]
        nc.createVariable("time_cpr", "f8", ("cpr",), zlib=True)
        nc.createVariable("time", "f4", ("time",), zlib=True)
        nc.createVariable("height", "f4", ("height",), zlib=True)
        nc.createVariable(
            "target_classification_cpr",
            "i4",
            ("cpr", "height"),
            fill_value=fill_value_int,
            zlib=True,
        )
        nc.createVariable(
            "target_classification",
            "i4",
            ("time", "height"),
            fill_value=fill_value_int,
            zlib=True,
        )
        nc.createVariable("latitude", "f4", ("time",), zlib=True)
        nc.createVariable("longitude", "f4", ("time",), zlib=True)
        nc.createVariable("latitude_cpr", "f4", ("cpr",), zlib=True)
        nc.createVariable("longitude_cpr", "f4", ("cpr",), zlib=True)
        nc.createVariable("distance", "f4", ("cpr",), zlib=True)
        nc.createVariable("altitude", "f4", ("time",), zlib=True)

        time_units = f"hours since {fetcher.date.strftime('%Y-%m-%d')} 00:00:00 +00:00"

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

        var = nc.variables["target_classification_cpr"]
        var[:] = ec.data["classification"]
        var.long_name = "CPR target classification"
        var.units = "1"
        var.source = f"{EARTHCARE} {CPR}"
        var.comment = (
            f"{EARTHCARE} CPR_TC__2A hydrometeor classification data "
            f"within {fetcher.distance} km from the site."
        )
        var.license = ESA_LICENSE
        assert ec.full_path is not None
        with netCDF4.Dataset(ec.full_path, "r") as src:
            defn = src["ScienceData"]["hydrometeor_classification"].definition
        var.definition = _get_definition_string(defn)

        var = nc.variables["target_classification"]
        var[:] = synthetic.data["classification"]
        var.long_name = "Target classification"
        var.units = "1"
        var.license = CLOUDNET_LICENCE
        assert synthetic.full_path is not None
        with netCDF4.Dataset(synthetic.full_path, "r") as src:
            defn = src["target_classification"].definition
        var.definition = defn
        time_in_minutes = len(synthetic.time) * 30 / 60
        var.comment = f"A {time_in_minutes:.1f}-minute segment of Cloudnet data centered on the EarthCARE overpass."

        var = nc.variables["time"]
        var[:] = synthetic.time
        var.long_name = "Time UTC"
        var.units = time_units
        var.standard_name = "time"
        var.calendar = "standard"

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

        # Global attributes
        nc.cloudnet_file_type = "cpr-tc-validation"
        nc.Conventions = "CF-1.8"
        nc.file_uuid = file_uuid
        nc.title = f"CPR target classification validation from {fetcher.site.human_readable_name}"
        nc.year = fetcher.date.strftime("%Y")
        nc.month = fetcher.date.strftime("%m")
        nc.day = fetcher.date.strftime("%d")
        nc.location = fetcher.site.human_readable_name
        nc.history = utils.make_history(simu.history, product="cpr-tc-validation")
        nc.source = f"{simu.model}\n{EARTHCARE}"
        nc.source_file_uuids = simu.file_uuid
        nc.cpr_2a_baseline = ec.baseline
        nc.cpr_2a_filename = ec.filename
        nc.earthcare_downloader_version = __version__
    return file_uuid


def _get_definition_string(defn: str) -> str:
    data = []
    parts = defn.split("=")
    digit = int(parts[0])
    for part in parts[1:]:
        comma_index = part.rfind(",")
        if comma_index != -1:
            part = part[:comma_index]
        data.append(f"Value {digit}: {part.strip()}")
        digit += 1
    return "\n" + f"\n".join(data)
