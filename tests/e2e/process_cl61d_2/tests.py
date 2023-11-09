from os import path

import netCDF4
import pytest
from test_utils.utils import count_strings, parse_args, read_log_file

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class Test:
    instrument = "cl61d"

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params["full_path"]
        self.site, self.date, self.product = parse_args(params["args"])
        self.date_short = self.date.replace("-", "")

    def test_product_file(self):
        nc = netCDF4.Dataset(self.full_path)
        assert len(nc.variables["time"]) == 4 * 12
        nc.close()

    def test_metadata_api_calls(self):
        data = read_log_file(SCRIPT_PATH)

        n_raw_files = 4

        # GET calibration
        assert "GET /api/calibration" in "\n".join(data)

        # PUT product file
        assert (
            f"PUT /files/{self.date_short}_{self.site}_{self.instrument}.nc"
            in "\n".join(data)
        )

        # Update status of raw files
        file_put = '"POST /upload-metadata HTTP/1.1" 200 -'
        assert count_strings(data, file_put) == n_raw_files

        # Submit QC report
        assert "PUT /quality/" in data[-1]
