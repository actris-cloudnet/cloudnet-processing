import os
import subprocess

import pytest


script_path = os.path.dirname(os.path.realpath(__file__))

pid = 'https://hdl.handle.net/21.T12995/1.be8154c1a6aa4f44'
expected_str = '<attribute name="pid" value="https://hdl.handle.net/21.T12995/1.be8154c1a6aa4f44" />'


class TestFreeze:

    @pytest.fixture(autouse=True)
    def _fetch_params(self, params):
        self.output = params['output']

    def test_that_calls_pid_service(self):
        n_files = 5
        with open(f'{script_path}/pid.log', 'r') as file:
            assert file.read().count('POST') == n_files

    def test_that_calls_metadata_service(self):
        n_files = 5
        with open(f'{script_path}/md.log', 'r') as file:
            file_str = file.read()
            assert file_str.count('GET') == 1
            assert file_str.count('PUT') == n_files

    def test_that_generated_files_have_correct_pids(self):
        files_with_pids = [
            '20200513_bucharest_chm15k.nc',
            '20200513_granada_rpg-fmcw-94.nc',
            '20200514_bucharest_ecmwf.nc',
            '20200514_granada_ecmwf.nc',
            '20200514_norunda_ecmwf.nc'
        ]

        for file in files_with_pids:
            ncdump_out = subprocess.check_output(['ncdump', '-xh', f'{self.output}/{file}'])
            assert expected_str in str(ncdump_out)

    def test_that_nonfreezable_files_have_no_pids(self):
        files_with_no_pids = [
            '20200513_bucharest_ecmwf.nc',
            '20200513_granada_ecmwf.nc',
            '20200513_norunda_ecmwf.nc'
        ]

        for file in files_with_no_pids:
            ncdump_out = subprocess.check_output(['ncdump', '-xh', f'{self.output}/{file}'])
            assert expected_str not in str(ncdump_out)
