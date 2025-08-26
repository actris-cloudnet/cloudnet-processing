import shutil
from tempfile import NamedTemporaryFile

import netCDF4

from processing.harmonizer import core
from processing.utils import MiscError


def harmonize_model_file(data: dict) -> str:
    """Harmonizes model netCDF file."""
    if "output_path" not in data:
        temp_file = NamedTemporaryFile()
    with (
        netCDF4.Dataset(data["full_path"], "r") as nc_raw,
        netCDF4.Dataset(
            data["output_path"] if "output_path" in data else temp_file.name,
            "w",
            format="NETCDF4_CLASSIC",
        ) as nc,
    ):
        model = ModelNc(nc_raw, nc, data)
        model.copy_file_contents()
        model.harmonize_attribute("units", ("latitude", "longitude", "altitude"))
        uuid = model.add_uuid()
        model.add_global_model_attributes()
        model.check_time_dimension()
        model.add_date()
        model.add_history("model")
    if "output_path" not in data:
        shutil.copy(temp_file.name, data["full_path"])
    return uuid


class ModelNc(core.Level1Nc):
    def check_time_dimension(self) -> None:
        """Checks time dimension."""
        resolutions = {"gdas1": 24 // 3, "ecmwf-open": 24 // 3}
        n_steps = len(self.nc.dimensions["time"])
        n_steps_expected = resolutions.get(self.data["model"], 24)
        if n_steps < n_steps_expected:
            raise MiscError(
                f"Incomplete model file: expected at least {n_steps_expected} but found {n_steps}"
            )
        if n_steps > n_steps_expected + 1:
            raise MiscError(
                f"Too many steps in model file: expected at most {n_steps_expected+1} but found {n_steps}"
            )

    def add_date(self) -> None:
        """Adds date in correct format."""
        date_string = self.nc.variables["time"].units
        date = date_string.split()[2]
        self.nc.year, self.nc.month, self.nc.day = date.split("-")

    def add_global_model_attributes(self) -> None:
        """Adds required global attributes."""
        self.nc.cloudnet_file_type = "model"
        self.nc.Conventions = "CF-1.8"
