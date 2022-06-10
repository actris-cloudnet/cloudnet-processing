import os

import netCDF4
import pytest
from cloudnetpy.utils import seconds2hours

YEAR = 2020
N_VALID_FILES = 3
N_TIME_IN_SINGLE_FILE = 10


class TestCHM15kConcatenation:
    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self._full_path = params["full_path"]

    def test_file_arrived(self):
        assert os.path.isfile(self._full_path)

    def test_time_vector_is_correct(self):
        nc = netCDF4.Dataset(self._full_path)
        time = nc.variables["time"][:]
        assert len(time) == N_VALID_FILES * N_TIME_IN_SINGLE_FILE
        fraction_hour = seconds2hours(time)
        assert min(fraction_hour) > 0
        assert max(fraction_hour) < 24
        nc.close()
