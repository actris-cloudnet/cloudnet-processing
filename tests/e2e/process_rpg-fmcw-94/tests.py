from os import path

import netCDF4
import pytest
from test_utils.utils import count_strings, read_log_file

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestRPGFMCW94Processing:
    product = "radar"
    instrument = "rpg-fmcw-94"

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params["full_path"]

    def test_that_does_not_call_pid_api(self):
        f = open(f"{SCRIPT_PATH}/pid.log")
        data = f.readlines()
        assert len(data) == 0

    def test_attributes(self):
        nc = netCDF4.Dataset(self.full_path)
        assert nc.year == "2020"
        assert nc.month == "10"
        assert nc.day == "22"
        assert nc.cloudnet_file_type == self.product
        assert nc.Conventions == "CF-1.8"
        assert hasattr(nc, "pid") is False
        assert hasattr(nc, "cloudnetpy_version")
        assert hasattr(nc, "cloudnet_processing_version")
        nc.close()

    def test_that_calls_metadata_api(self):
        data = read_log_file(SCRIPT_PATH)
        n_raw_files = 2
        n_gets = 6  # product check (1) + instrument checks (2)  + rpg-fmcw-94 raw (1) + previous product (1) + hkd(1)
        n_img = 7
        n_puts = 2 + n_img
        n_posts = n_raw_files
        n_check_images = 1

        assert len(data) == n_gets + n_puts + n_posts + n_check_images

        # Check product status
        assert (
            '"GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest&developer=True&instrumentPid=http%3A%2F%2Fpid.test%2F3.abcabcabcrpg&product=radar&showLegacy=True HTTP/1.1" 200 -'
            in "\n".join(data)
        )

        # GET RPG raw data
        assert (
            '"GET /upload-metadata?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest&developer=True&instrumentPid=http%3A%2F%2Fpid.test%2F3.abcabcabcrpg&status%5B%5D=uploaded&status%5B%5D=processed HTTP/1.1" 200 -'
            in "\n".join(data)
        )

        # PUT file
        assert (
            '"PUT /files/20201022_bucharest_rpg-fmcw-94_abcabcab.nc HTTP/1.1" 201 -'
            in "\n".join(data)
        )

        # PUT images
        img_put = '"PUT /visualizations/20201022_bucharest_rpg-fmcw-94_abcabcab-'
        assert count_strings(data, img_put) == n_img

        # POST metadata
        file_post = '"POST /upload-metadata HTTP/1.1" 200 -'
        assert count_strings(data, file_post) == n_raw_files
