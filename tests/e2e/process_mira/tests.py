from os import path

import netCDF4
import pytest
from test_utils.utils import count_strings, read_log_file

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestMIRAProcessing:
    product = "radar"
    instrument = "mira"
    n_img = 4

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params["full_path"]

    def test_that_does_not_call_pid_api(self):
        f = open(f"{SCRIPT_PATH}/pid.log")
        data = f.readlines()
        assert len(data) == 0

    def test_attributes(self):
        nc = netCDF4.Dataset(self.full_path)
        assert nc.year == "2021"
        assert nc.month == "01"
        assert nc.day == "27"
        assert nc.cloudnet_file_type == self.product
        assert nc.Conventions == "CF-1.8"
        assert nc.source == "METEK MIRA-35"
        assert nc.references == "https://doi.org/10.21105/joss.02123"
        assert hasattr(nc, "pid") is False
        assert hasattr(nc, "cloudnetpy_version")
        assert hasattr(nc, "cloudnet_processing_version")
        nc.close()

    def test_data_values(self):
        nc = netCDF4.Dataset(self.full_path)
        assert (nc.variables["latitude"][:] - 50.906) < 0.01
        assert (nc.variables["longitude"][:] - 6.407) < 0.01
        assert (nc.variables["altitude"][:] - 108) < 0.01
        nc.close()

    def test_that_calls_metadata_api(self):
        data = read_log_file(SCRIPT_PATH)
        n_raw_files = 2
        n_gets = 6  # instrument checks (2) + product check (1) + mira raw (1) + previous product (1) + hkd(1)
        n_puts = 2 + self.n_img
        n_posts = n_raw_files
        n_check_images = 1

        assert len(data) == n_gets + n_puts + n_posts + n_check_images

        prefix = "?dateFrom=2021-01-27&dateTo=2021-01-27&site=juelich&developer=True&"

        # Check product status
        assert (
            f'"GET /api/files{prefix}instrumentPid=http%3A%2F%2Fpid.test%2F3.abcabcabcmira&product=radar&showLegacy=True HTTP/1.1" 200 -'
        ) in "\n".join(data)

        # Two instrument API calls...

        # GET MIRA raw data
        assert (
            f'"GET /upload-metadata{prefix}instrumentPid=http%3A%2F%2Fpid.test%2F3.abcabcabcmira&status%5B%5D=uploaded&status%5B%5D=processed HTTP/1.1" 200'
            in "\n".join(data)
        )

        # PUT file
        assert (
            '"PUT /files/20210127_juelich_mira_abcabcab.nc HTTP/1.1" 201 -'
            in "\n".join(data)
        )

        # PUT images
        img_put = '"PUT /visualizations/20210127_juelich_mira_abcabcab-'
        assert count_strings(data, img_put) == self.n_img

        # POST metadata
        file_put = '"POST /upload-metadata HTTP/1.1" 200 -'
        assert count_strings(data, file_put) == n_raw_files
