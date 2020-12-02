import pytest
import netCDF4
from os import path
from data_processing import utils

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestCategorizeProcessing:

    product = 'categorize'
    images = utils.get_fields_for_plot(product)[0]

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params['full_path']

    @pytest.mark.reprocess
    def test_that_has_correct_attributes(self):
        nc = netCDF4.Dataset(self.full_path)
        assert nc.year == '2020'
        assert nc.month == '10'
        assert nc.day == '22'
        assert hasattr(nc, 'pid') is True
        assert nc.title == f'{self.product.capitalize()} file from Bucharest'
        assert nc.cloudnet_file_type == self.product
        assert nc.Conventions == 'CF-1.7'
        for uuid in ('a50161be-6ecd-4ae6-ad0e-26bb739af752', '38a41d8f-f688-4196-8b88-2b401f433fed',
                     '0d4fbbd9-85c3-451c-989b-4f26e044c0ed'):
            assert uuid.replace('-', '') in nc.source_file_uuids
        nc.close()

    def test_that_calls_pid_api(self):
        f = open(f'{SCRIPT_PATH}/pid.log')
        data = f.readlines()
        assert len(data) == 1
        assert 'POST /pid/ HTTP/1.1" 200 -' in data[0]

    def test_that_calls_metadata_api(self):
        f = open(f'{SCRIPT_PATH}/md.log')
        data = f.readlines()
        n_img = len(self.images)
        n_gets = len(utils.get_product_types(level=1)) + 1
        n_puts = 1
        assert len(data) == n_gets + n_img + n_puts
        suffix = 'dateFrom=2020-10-22&dateTo=2020-10-22&location=bucharest'
        for row in data[:n_gets]:
            assert f'GET /api/files?{suffix} HTTP/1.1" 200' in row
        assert f'PUT /files/20201022_bucharest_{self.product}.nc HTTP/1.1" 201' in data[n_gets]
        for row in data[n_gets+1:]:
            assert f'PUT /visualizations/20201022_bucharest_{self.product}' in row
