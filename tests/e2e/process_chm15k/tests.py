from os import path

import netCDF4
import pytest
from test_utils.utils import count_strings, read_log_file

SCRIPT_PATH = path.dirname(path.realpath(__file__))


class TestChm15kProcessing:
    product = "lidar"
    instrument = "chm15k"

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.full_path = params["full_path"]

    @pytest.mark.first_run
    def test_that_refuses_to_process_without_reprocess_flag(self):
        with pytest.raises(OSError):
            netCDF4.Dataset(self.full_path)

    @pytest.mark.first_run
    def test_that_calls_metadata_api_only_once(self):
        data = read_log_file(SCRIPT_PATH)
        assert len(data) == 3
        # assert (
        #    '"GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest&developer=True'
        #    '&product=lidar&showLegacy=True HTTP/1.1" 200 -' in data[1]
        # )

    @pytest.mark.first_run
    def test_that_does_not_call_pid_api(self):
        f = open(f"{SCRIPT_PATH}/pid.log")
        data = f.readlines()
        assert len(data) == 0

    @pytest.mark.reprocess
    def test_attributes(self):
        nc = netCDF4.Dataset(self.full_path)
        assert nc.year == "2020"
        assert nc.month == "10"
        assert nc.day == "22"
        assert nc.cloudnet_file_type == self.product
        assert hasattr(nc, "pid") is True
        assert hasattr(nc, "cloudnetpy_version")
        assert hasattr(nc, "cloudnet_processing_version")
        nc.close()

    @pytest.mark.reprocess
    def test_that_calls_pid_api(self):
        f = open(f"{SCRIPT_PATH}/pid.log")
        data = f.readlines()
        assert len(data) == 1
        assert 'POST /pid/ HTTP/1.1" 200 -' in data[0]

    @pytest.mark.reprocess
    def test_that_calls_metadata_api(self):
        data = read_log_file(SCRIPT_PATH)

        n_img = 2
        n_raw_files = 3

        # Check product status
        assert (
            '"GET /api/files?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest&developer=True&instrumentPid=http%3A%2F%2Fpid.test%2F3.abcabcabcchm15k&product=lidar&showLegacy=True HTTP/1.1" 200 -'
            in "\n".join(data)
        )

        # GET raw data
        assert (
            '"GET /upload-metadata?dateFrom=2020-10-22&dateTo=2020-10-22&site=bucharest&developer=True&instrumentPid=http%3A%2F%2Fpid.test%2F3.abcabcabcchm15k&status%5B%5D=uploaded&status%5B%5D=processed HTTP/1.1" 200 -'
            in "\n".join(data)
        )

        # GET calibration
        assert (
            '"GET /api/calibration?instrumentPid=http%3A%2F%2Fpid.test%2F3.abcabcabcchm15k&date=2020-10-22 HTTP/1.1" 200 -'
            in "\n".join(data)
        )

        # PUT file
        assert '"PUT /files/20201022_bucharest_chm15k.nc HTTP/1.1" 201 -' in "\n".join(
            data
        )

        # PUT images
        img_put = '"PUT /visualizations/20201022_bucharest_chm15k'
        assert count_strings(data, img_put) == n_img

        # POST metadata
        file_put = '"POST /upload-metadata HTTP/1.1" 200 -'
        assert count_strings(data, file_put) == n_raw_files
