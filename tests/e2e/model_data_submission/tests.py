import os

script_path = os.path.dirname(os.path.realpath(__file__))

FILEPATH = f'tests/data/api_files/granada/calibrated/ecmwf/2020/20200405_granada_ecmwf.nc'
FILESIZE = 501460


class TestDataSubmission:

    def test_that_PUTs_model_files_only_to_metadata_service(self):
        n_files = 1
        with open(f'{script_path}/md.log', 'r') as file:
            assert file.read().count('PUT') == n_files

    def test_that_saves_files(self):
        assert os.path.isfile(FILEPATH)
        assert os.stat(FILEPATH).st_size == FILESIZE

    def test_that_creates_symlinks(self):
        link = 'tests/data/freeze/model/20200405_granada_ecmwf.nc'
        assert os.path.islink(link)
        assert os.stat(link).st_size == FILESIZE
