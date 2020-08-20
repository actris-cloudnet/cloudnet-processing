import os
import subprocess

import pytest


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
