from os import path
import pytest
import netCDF4
from data_processing import utils

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestClassificationProcessing:

    product = 'classification'
    images = utils.get_fields_for_plot(product)[0]

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.output = params['output']
        self.full_path = params['full_path']

    def test_that_reports_volatile_file_creation(self):
        assert 'Created: Volatile file' in self.output

    @pytest.mark.reprocess
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
        n_img = len(self.images)
        n_gets = 2
        n_puts = 1
        assert len(data) == n_gets + n_puts + n_img
        suffix = 'dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest&developer=True'
        assert f'GET /api/files?{suffix}&showLegacy=True HTTP/1.1" 200' in data[0]
        for row in data[1:n_gets]:
            assert f'GET /api/files?{suffix} HTTP/1.1" 200' in row
        assert f'PUT /files/20201022_bucharest_{self.product}.nc' in data[n_gets]
        for row in data[n_gets+1:]:
            assert f'PUT /visualizations/20201022_bucharest_{self.product}' in row
