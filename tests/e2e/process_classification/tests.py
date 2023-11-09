from os import path

import netCDF4
import pytest
from data_processing import utils
from test_utils.utils import count_strings, read_log_file

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestClassificationProcessing:
    product = "classification"
    n_img = len(utils.get_fields_for_plot(product)[0])

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params["full_path"]

    def test_that_has_correct_attributes(self):
        nc = netCDF4.Dataset(self.full_path)
        assert nc.year == "2020"
        assert nc.month == "10"
        assert nc.day == "22"
        assert nc.title == f"{self.product.capitalize()} products from Bucharest"
        assert nc.cloudnet_file_type == self.product
        assert nc.Conventions == "CF-1.8"
        assert nc.source_file_uuids == "d963776b33844dc7b979d4c31d84a86b"
        assert hasattr(nc, "cloudnetpy_version")
        assert hasattr(nc, "cloudnet_processing_version")
        nc.close()

    def test_that_calls_metadata_api(self):
        data = read_log_file(SCRIPT_PATH)

        # Check product status
        assert (
            '"GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest'
            '&developer=True&product=classification&showLegacy=True HTTP/1.1" 200 -'
            in "\n".join(data)
        )

        # GET input file
        assert (
            '"GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest'
            '&developer=True&product=categorize HTTP/1.1" 200 -' in "\n".join(data)
        )

        # PUT file
        assert (
            '"PUT /files/20201022_bucharest_classification.nc HTTP/1.1" 201 -'
            in "\n".join(data)
        )

        # PUT images
        img_put = '"PUT /visualizations/20201022_bucharest_classification-'
        assert count_strings(data, img_put) == self.n_img
