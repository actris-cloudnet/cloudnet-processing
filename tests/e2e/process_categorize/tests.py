from os import path

import netCDF4
import pytest

from data_processing import utils
from test_utils.utils import count_strings, read_log_file

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestCategorizeProcessing:

    product = "categorize"
    n_img = len(utils.get_fields_for_plot(product)[0])

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params["full_path"]

    def test_that_has_correct_attributes(self):
        nc = netCDF4.Dataset(self.full_path)
        assert nc.year == "2020"
        assert nc.month == "10"
        assert nc.day == "22"
        assert hasattr(nc, "pid") is True
        assert nc.cloudnet_file_type == self.product
        assert nc.Conventions == "CF-1.8"
        assert hasattr(nc, "cloudnetpy_version")
        assert hasattr(nc, "cloudnet_processing_version")
        for uuid in (
            "2d485fa6d3af40ca9c93612a0abf0430",
            "38a41d8f-f688-4196-8b88-2b401f433fed",
            "0d4fbbd9-85c3-451c-989b-4f26e044c0ed",
        ):
            assert uuid.replace("-", "") in nc.source_file_uuids
        nc.close()

    def test_that_calls_pid_api(self):
        f = open(f"{SCRIPT_PATH}/pid.log")
        data = f.readlines()
        assert len(data) == 1
        assert 'POST /pid/ HTTP/1.1" 200 -' in data[0]

    def test_that_calls_metadata_api(self):
        data = read_log_file(SCRIPT_PATH)
        n_gets = len(utils.get_product_types(level="1b"))
        n_puts = 2
        n_checks_for_updated_at = 1
        assert len(data) == n_gets + self.n_img + n_puts + n_checks_for_updated_at

        # Check product status
        assert (
            '"GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest'
            '&developer=True&product=categorize&showLegacy=True HTTP/1.1" 200 -' in data[0]
        )

        # GET input files
        sub_str = "dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest&developer=True"
        for prod in ("radar", "lidar", "mwr"):
            assert (
                count_strings(data, f'"GET /api/files?{sub_str}&product={prod} HTTP/1.1" 200 -')
                == 1
            )
        assert count_strings(data, f'"GET /api/model-files?{sub_str} HTTP/1.1" 200 -') == 1

        # PUT file
        assert '"PUT /files/20201022_bucharest_categorize.nc HTTP/1.1" 201 -' in data[6]

        # PUT images
        img_put = '"PUT /visualizations/20201022_bucharest_categorize-'
        assert count_strings(data, img_put) == self.n_img
