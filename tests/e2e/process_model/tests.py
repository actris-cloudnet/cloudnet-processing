from os import path

import netCDF4
import pytest

from data_processing import utils
from test_utils.utils import count_strings, read_log_file

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestModelProcessing:

    product = "model"
    images = utils.get_fields_for_plot(product)[0]

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params["full_path"]

    def test_that_has_correct_attributes(self):
        nc = netCDF4.Dataset(self.full_path)
        assert nc.year == "2020"
        assert nc.month == "10"
        assert nc.day == "22"
        assert hasattr(nc, "pid") is False
        assert nc.title == "ECMWF single-site output over Bucharest"
        assert nc.cloudnet_file_type == self.product
        assert "netcdf4" in nc.file_format.lower()
        nc.close()

    def test_that_calls_metadata_api(self):
        data = read_log_file(SCRIPT_PATH)

        n_gets = 3
        n_file_puts = 2
        n_img_puts = len(self.images)
        n_metadata_posts = 1

        assert len(data) == n_gets + n_file_puts + n_img_puts + n_metadata_posts

        # Check existing (not-processed) model metadata for the whole period
        assert '"GET /upload-model-metadata?site=bucharest&status=uploaded' in data[0]

        # Check product status
        assert (
            '"GET /api/model-files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest'
            '&developer=True&model=ecmwf HTTP/1.1" 200 -' in data[1]
        )

        # GET certain day / model
        assert (
            '"GET /upload-model-metadata?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest'
            '&developer=True&model=ecmwf&status%5B%5D=uploaded&status%5B%5D=processed HTTP/1.1" 200 -'
            in data[2]
        )

        # PUT file
        assert '"PUT /files/20201022_bucharest_ecmwf.nc HTTP/1.1" 201 -' in data[3]

        # POST metadata
        assert '"POST /upload-metadata HTTP/1.1" 200 -' in data[4]

        # PUT images
        img_put = '"PUT /visualizations/20201022_bucharest_ecmwf-'
        assert count_strings(data, img_put) == n_img_puts
