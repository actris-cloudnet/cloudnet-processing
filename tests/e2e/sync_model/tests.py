import os
import glob
import pytest
import netCDF4

N_FILES_EXPECTED = 2


class TestSyncModel:

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.site = params['site']
        self.input = params['input']
        self.output = params['output']
        self._files = glob.glob(f'{self.output}/**/*.nc', recursive=True)

    def test_files_are_synced(self):
        assert len(self._files) == N_FILES_EXPECTED

    def test_files_contain_correct_attributes(self):
        for file in self._files:
            nc = netCDF4.Dataset(file)
            for attr in ('file_uuid', 'cloudnet_file_type'):
                assert hasattr(nc, attr)
            assert nc.cloudnet_file_type == 'model'


def test_that_puts_all_files_to_metadata_server():
    script_path = os.path.dirname(os.path.realpath(__file__))
    with open(f'{script_path}/md.log', 'r') as file:
        assert file.read().count('PUT') == N_FILES_EXPECTED
