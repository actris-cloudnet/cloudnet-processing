import os
import pytest


class TestLidarConcatenation:

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.site = params['site']
        self.input = params['input']
        self.output = params['output']
        self.date = params['date'].replace('-', '')
        self.year = self.date[:4]

    def test_that_lidar_files_are_concatenated(self):
        lidar_file = '/'.join((self.input, self.site, 'uncalibrated/chm15k',
                               self.year, f"chm15k_{self.date}.nc"))
        assert os.path.isfile(lidar_file)

