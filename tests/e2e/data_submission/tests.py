import os

script_path = os.path.dirname(os.path.realpath(__file__))

pid = 'https://hdl.handle.net/21.T12995/1.be8154c1a6aa4f44'
expected_str = '<attribute name="pid" value="https://hdl.handle.net/21.T12995/1.be8154c1a6aa4f44" />'


class TestDataSubmission:

    def test_that_PUTs_to_metadata_service(self):
        n_files = 3
        with open(f'{script_path}/md.log', 'r') as file:
            assert file.read().count('PUT') == n_files

    def test_that_POSTs_to_metadata_service(self):
        n_files = 3
        with open(f'{script_path}/md.log', 'r') as file:
            assert file.read().count('POST') == n_files

    def test_saves_files(self):
        dirs = ['chm15k', 'ecmwf', 'rpg-fmcw-94']
        files = ['chm15k_20200405-1ed679842745d1b180.nc',
                 '20200405_granada_ecmwf-8ea4732a1234f66c37.nc',
                 '200405_020000_P06_ZEN-539969323e9a8cfb60.LV1']
        sizes = [11357662, 501460, 4282656]
        for (dir, file, size) in zip(dirs, files, sizes):
            path = f'tests/data/api_files/granada/{dir}/2020/04/05/{file}'
            assert os.path.isfile(path)
            assert os.stat(path).st_size == size
