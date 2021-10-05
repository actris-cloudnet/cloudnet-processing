from os import path
import pytest
import netCDF4
from data_processing import utils
from test_utils.utils import count_strings

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestClassificationProcessing:

    product = 'classification'
    n_img = len(utils.get_fields_for_plot(product)[0])

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params['full_path']

    def test_that_has_correct_attributes(self):
        nc = netCDF4.Dataset(self.full_path)
        assert nc.year == '2020'
        assert nc.month == '10'
        assert nc.day == '22'
        assert nc.title == f'{self.product.capitalize()} file from Bucharest'
        assert nc.cloudnet_file_type == self.product
        assert nc.Conventions == 'CF-1.7'
        assert nc.source_file_uuids == 'd963776b33844dc7b979d4c31d84a86b'
        nc.close()

    def test_that_calls_metadata_api(self):
        f = open(f'{SCRIPT_PATH}/md.log')
        data = f.readlines()
        n_gets = 3
        n_puts = 2
        assert len(data) == n_gets + n_puts + self.n_img

        # Check product status
        assert '"GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest' \
               '&developer=True&product=classification&showLegacy=True HTTP/1.1" 200 -' in data[0]

        # GET input file
        assert '"GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest' \
               '&developer=True&product=categorize HTTP/1.1" 200 -' in data[1]

        # PUT file
        assert '"PUT /files/20201022_bucharest_classification.nc HTTP/1.1" 201 -' in data[3]

        # PUT images
        img_put = '"PUT /visualizations/20201022_bucharest_classification-'
        assert count_strings(data, img_put) == self.n_img
