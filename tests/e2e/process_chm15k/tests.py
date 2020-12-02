import netCDF4
from os import path
from data_processing import utils
import pytest

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestChm15kProcessing:

    product = 'lidar'
    instrument = 'chm15k'
    images = utils.get_fields_for_plot(product)[0]

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.output = params['output']
        self.full_path = params['full_path']
        print(self.output)

    @pytest.mark.first_run
    def test_that_refuses_to_process_without_reprocess_flag(self):
        assert 'Existing freezed file and no "reprocess" flag' in self.output

    @pytest.mark.first_run
    def test_that_calls_metadata_api_only_once(self):
        f = open(f'{SCRIPT_PATH}/md.log')
        data = f.readlines()
        assert len(data) == 1
        suffix = 'dateFrom=2020-10-22&dateTo=2020-10-22&location=bucharest'
        assert f'GET /api/files?{suffix}' in data[0]

    @pytest.mark.first_run
    def test_that_does_not_call_pid_api(self):
        f = open(f'{SCRIPT_PATH}/pid.log')
        data = f.readlines()
        assert len(data) == 0

    @pytest.mark.reprocess
    def test_that_reports_successful_processing(self):
        assert 'Created: New version' in self.output

    @pytest.mark.reprocess
    def test_attributes(self):
        nc = netCDF4.Dataset(self.full_path)
        assert nc.year == '2020'
        assert nc.month == '10'
        assert nc.day == '22'
        assert nc.title == f'Ceilometer file from Bucharest'
        assert nc.cloudnet_file_type == self.product
        assert nc.Conventions == 'CF-1.7'
        assert hasattr(nc, 'pid') is True
        nc.close()

    @pytest.mark.reprocess
    def test_that_calls_pid_api(self):
        f = open(f'{SCRIPT_PATH}/pid.log')
        data = f.readlines()
        assert len(data) == 1
        assert 'POST /pid/ HTTP/1.1" 200 -' in data[0]

    @pytest.mark.reprocess
    def test_that_calls_metadata_api(self):
        f = open(f'{SCRIPT_PATH}/md.log')
        data = f.readlines()
        n_img = len(self.images)
        n_gets = 3
        n_puts = 1
        n_posts = 3
        assert len(data) == n_gets + n_puts + n_img + n_posts
        suffix = 'dateFrom=2020-10-22&dateTo=2020-10-22&location=bucharest'
        assert f'GET /api/files?{suffix}' in data[1]
        assert f'GET /upload-metadata?{suffix}' in data[2]
        assert f'PUT /files/20201022_bucharest_{self.instrument}.nc HTTP/1.1" 201' in data[3]
        for row in data[4:4+n_img]:
            assert f'PUT /visualizations/20201022_bucharest_{self.instrument}' in row
        for row in data[4+n_img:]:
            assert f'POST /upload-metadata HTTP/1.1" 200' in row

