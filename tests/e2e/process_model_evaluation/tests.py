from os import path

import netCDF4
import pytest

from test_utils.utils import count_strings, read_log_file

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestModelEvaluationProcessing:

    n_img = 4

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params["full_path"]

    def test_that_has_correct_attributes(self):
        nc = netCDF4.Dataset(self.full_path)
        assert nc.year == "2020"
        assert nc.month == "10"
        assert nc.day == "22"
        assert nc.title == f"Downsampled Cf of ecmwf from Bucharest"
        assert nc.cloudnet_file_type == "cf_ecmwf"
        assert nc.Conventions == "CF-1.8"
        assert (
            ", ".join(sorted(nc.source_file_uuids.split(", ")))
            == "0d4fbbd985c3451c989b4f26e044c0ed, d963776b33844dc7b979d4c31d84a86b"
        )
        nc.close()

    def test_that_calls_metadata_api(self):
        data = read_log_file(SCRIPT_PATH)
        n_gets = 4
        n_puts = 1
        assert len(data) == n_gets + n_puts + self.n_img

        # Check product status
        assert (
            "GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest"
            "&developer=True&product=l3-cf&showLegacy=True HTTP/1.1" in data[0]
        )

        # GET input model
        assert (
            '"GET /api/model-files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest'
            '&developer=True&model=ecmwf HTTP/1.1" 200 -' in data[1]
        )

        # GET input product
        assert (
            '"GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest'
            '&developer=True&product=categorize HTTP/1.1" 200 -' in data[2]
        )

        # Check product timestamp
        assert (
            "GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest&developer=True&product=l3-cf"
            " HTTP/1.1" in data[3]
        )

        # PUT file
        assert "PUT /files/20201022_bucharest_l3-cf_downsampled_ecmwf.nc" in data[4]

        # PUT images
        img_put = '"PUT /visualizations/20201022_bucharest_l3-cf_downsampled'
        assert count_strings(data, img_put) == self.n_img
