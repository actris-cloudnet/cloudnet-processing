import netCDF4
from os import path
import pytest
from test_utils.utils import parse_args, count_strings, read_log_file

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class Test:

    instrument = 'cl61d'

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params['full_path']
        self.site, self.date, self.product = parse_args(params['args'])
        self.date_short = self.date.replace('-', '')

    def test_product_file(self):
        nc = netCDF4.Dataset(self.full_path)
        assert len(nc.variables['time']) == 4*12
        nc.close()

    def test_metadata_api_calls(self):
        data = read_log_file(SCRIPT_PATH)

        n_raw_files = 2

        fix = f'dateFrom={self.date}&dateTo={self.date}&site={self.site}&developer=True'
        instru = f'&instrument={self.instrument}'

        assert f'GET /api/files?{fix}&product={self.product}&showLegacy=True' in data[0]

        # Detect uploaded instruments
        assert f'GET /api/instruments' in data[1]
        assert f'GET /upload-metadata?{fix}' in data[2]

        # Try to find daily raw file
        assert f'GET /upload-metadata?{fix}{instru}&status%5B%5D=uploaded&status%5B%5D=processed' \
               in data[3]

        # It exist, get uploaded raw files and append
        assert f'GET /upload-metadata?{fix}{instru}&status=uploaded' in data[4]

        # Submit updated daily raw file
        assert f'POST /upload/metadata' in data[5]
        assert f'PUT /upload/data' in data[6]

        # GET calibration
        assert "GET /api/calibration" in data[7]

        # PUT product file
        assert f'PUT /files/{self.date_short}_{self.site}_{self.instrument}.nc' in data[8]

        # Update status of raw files
        file_put = '"POST /upload-metadata HTTP/1.1" 200 -'
        assert count_strings(data, file_put) == n_raw_files

        # Submit QC report
        assert f'PUT /quality/' in data[-1]
