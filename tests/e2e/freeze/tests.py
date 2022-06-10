import netCDF4
import pytest


class TestFreeze:
    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params["full_path"]

    def test_is_correct_pid(self):
        nc = netCDF4.Dataset(self.full_path)
        assert nc.pid == "https://hdl.handle.net/21.T12995/1.be8154c1a6aa4f44"
        nc.close()
