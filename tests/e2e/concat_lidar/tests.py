import glob
import pytest
import netCDF4
from cloudnetpy.utils import seconds2hours

YEAR = 2020
N_VALID_FILES = 2
N_TIME_IN_SINGLE_FILE = 10


class TestDefaultProcessing:

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self._path = params['input']
        self._files = glob.glob(f'{self._path}/{YEAR}/*.nc')

    def test_file_arrived(self):
        assert len(self._files) == 1

    def test_time_vector_is_correct(self):
        nc = netCDF4.Dataset(self._files[0])
        time = nc.variables['time'][:]
        assert len(time) == N_VALID_FILES * N_TIME_IN_SINGLE_FILE
        fraction_hour = seconds2hours(time)
        assert min(fraction_hour) > 0
        assert max(fraction_hour) < 24
        nc.close()


class TestOutput:

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self._path = params['output']

    def test_file_arrived(self, params):
        files = glob.glob(f'{self._path}/{YEAR}/*.nc')
        assert len(files) == 1
