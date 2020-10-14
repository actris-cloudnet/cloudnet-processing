import os

script_path = os.path.dirname(os.path.realpath(__file__))

FILEPATH = f'{script_path}/../../data/api_files/granada/calibrated/ecmwf/2020/20200405_granada_ecmwf.nc'


class TestDataSubmission:

    def test_that_PUTs_to_metadata_service(self):
        n_files = 1
        with open(f'{script_path}/md.log', 'r') as file:
            assert file.read().count('PUT') == n_files

    def test_saves_files(self):
        assert os.path.isfile(FILEPATH)
        assert os.stat(FILEPATH).st_size == 501460

