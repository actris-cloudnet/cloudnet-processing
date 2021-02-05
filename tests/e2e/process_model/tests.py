from os import path
import pytest
import netCDF4
from data_processing import utils

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestModelProcessing:

    product = 'model'
    instrument = 'ecmwf'
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
        assert hasattr(nc, 'pid') is False
        assert nc.title == f'{self.product.capitalize()} file from Bucharest'
        assert nc.cloudnet_file_type == self.product
        assert 'netcdf4' in nc.file_format.lower()
        nc.close()

    def test_that_calls_metadata_api(self):
        f = open(f'{SCRIPT_PATH}/md.log')
        data = f.readlines()

        n_valid_metadata = 2

        n_upload_gets = 1 + n_valid_metadata  # +1 because of initial check for valid models
        n_file_puts = n_valid_metadata
        n_metadata_posts = n_valid_metadata
        n_img_puts = len(self.images) * n_valid_metadata - 1  # -1 because of gdas1
        n_api_files_gets = n_valid_metadata

        assert len(data) == (n_upload_gets + n_file_puts + n_metadata_posts
                             + n_img_puts + n_api_files_gets)

        def count_strings(string: str, n_expected: int):
            n = 0
            for row in data:
                if string in row:
                    n += 1
            assert n == n_expected

        s = '"GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest&developer=True&showLegacy=True&model='
        count_strings(s, n_valid_metadata)

        s = '"PUT /visualizations/20201022_bucharest_'
        count_strings(s, n_img_puts)

        s = '"POST /upload-metadata HTTP/1.1" 200'
        count_strings(s, n_metadata_posts)

        s = '"GET /upload-metadata?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest&developer=True HTTP/1.1" 200'
        count_strings(s, n_upload_gets - 1)

        s = '"PUT /files/20201022_bucharest_'
        count_strings(s, n_file_puts)
