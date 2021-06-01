import netCDF4
from os import path
from data_processing import utils
from test_utils.utils import count_strings
import pytest

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestChm15kProcessing:

    product = 'lidar'
    instrument = 'chm15k'
    n_img = len(utils.get_fields_for_plot(product)[0])

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.output = params['output']
        self.full_path = params['full_path']

    @pytest.mark.first_run
    def test_that_refuses_to_process_without_reprocess_flag(self):
        assert 'Existing freezed file and no "reprocess" flag' in self.output

    @pytest.mark.first_run
    def test_that_calls_metadata_api_only_once(self):
        f = open(f'{SCRIPT_PATH}/md.log')
        data = f.readlines()
        assert len(data) == 1
        assert '"GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest&developer=True' \
               '&product=lidar&showLegacy=True HTTP/1.1" 200 -' in data[0]

    @pytest.mark.first_run
    def test_that_does_not_call_pid_api(self):
        f = open(f'{SCRIPT_PATH}/pid.log')
        data = f.readlines()
        assert len(data) == 0

#    @pytest.mark.reprocess
#    def test_that_reports_successful_processing(self):
#        assert 'Created: New version' in self.output

    @pytest.mark.reprocess
    def test_attributes(self):
        nc = netCDF4.Dataset(self.full_path)
        assert nc.year == "2020"
        assert nc.month == "10"
        assert nc.day == "22"
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

        n_raw_files = 3

        n_gets = 2
        n_puts = 1 + self.n_img
        n_posts = n_raw_files

        assert len(data) == n_gets + n_puts + n_posts

        # Check product status
        assert '"GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest&developer=True' \
               '&product=lidar&showLegacy=True HTTP/1.1" 200 -' in data[0]

        # GET raw data
        assert '"GET /upload-metadata?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest' \
               '&developer=True&instrument=chm15k HTTP/1.1" 200 -' in data[1]

        # PUT file
        assert '"PUT /files/20201022_bucharest_chm15k.nc HTTP/1.1"' in data[2]

        # PUT images
        img_put = '"PUT /visualizations/20201022_bucharest_chm15k-'
        assert count_strings(data, img_put) == self.n_img

        # POST metadata
        file_put = '"POST /upload-metadata HTTP/1.1" 200 -'
        assert count_strings(data, file_put) == n_raw_files
