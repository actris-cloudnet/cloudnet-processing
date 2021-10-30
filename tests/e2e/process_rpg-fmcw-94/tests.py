import netCDF4
from os import path
from data_processing import utils
from test_utils.utils import count_strings
import pytest

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestRPGFMCW94Processing:

    product = 'radar'
    instrument = 'rpg-fmcw-94'
    n_img = len(utils.get_fields_for_plot(product)[0])

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params['full_path']

    def test_that_does_not_call_pid_api(self):
        f = open(f'{SCRIPT_PATH}/pid.log')
        data = f.readlines()
        assert len(data) == 0

    def test_attributes(self):
        nc = netCDF4.Dataset(self.full_path)
        assert nc.year == '2020'
        assert nc.month == '10'
        assert nc.day == '22'
        assert nc.title == f'{self.product.capitalize()} file from Bucharest'
        assert nc.cloudnet_file_type == self.product
        assert nc.Conventions == 'CF-1.8'
        assert hasattr(nc, 'pid') is False
        nc.close()

    def test_that_calls_metadata_api(self):
        f = open(f'{SCRIPT_PATH}/md.log')
        data = f.readlines()
        n_raw_files = 2
        n_gets = 4  # product check (1) + instrument checks (2)  + rpg-fmcw-94 raw (1)
        n_puts = 2 + self.n_img
        n_posts = n_raw_files

        assert len(data) == n_gets + n_puts + n_posts

        # Check product status
        assert '"GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest' \
               '&developer=True&product=radar&showLegacy=True HTTP/1.1" 200 -' in data[0]

        # GET RPG raw data
        assert '"GET /upload-metadata?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest' \
               '&developer=True&instrument=rpg-fmcw-94&status%5B%5D=uploaded&status%5B%5D=processed HTTP/1.1" 200 -' in data[3]

        # PUT file
        assert '"PUT /files/20201022_bucharest_rpg-fmcw-94.nc HTTP/1.1" 201 -' in data[4]

        # PUT images
        img_put = '"PUT /visualizations/20201022_bucharest_rpg-fmcw-94-'
        assert count_strings(data, img_put) == self.n_img

        # POST metadata
        file_post = '"POST /upload-metadata HTTP/1.1" 200 -'
        assert count_strings(data, file_post) == n_raw_files
