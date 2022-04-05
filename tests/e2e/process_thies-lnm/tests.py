from os import path

import netCDF4
import pytest

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestProcessing:
    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params["full_path"]
        self.nc = netCDF4.Dataset(self.full_path)
        yield
        self.nc.close()

    def test_attributes(self):
        assert self.nc.year == "2021"
        assert self.nc.month == "09"
        assert self.nc.day == "15"
        assert self.nc.cloudnet_file_type == "disdrometer"
        assert self.nc.Conventions == "CF-1.8"
        assert hasattr(self.nc, "pid") is False
        assert hasattr(self.nc, "cloudnetpy_version")
        assert hasattr(self.nc, "cloudnet_processing_version")

    def test_time_is_sorted(self):
        time = self.nc.variables["time"][:]
        assert len(time) == 120
        for ind, t in enumerate(time[:-1]):
            assert time[ind + 1] > t
