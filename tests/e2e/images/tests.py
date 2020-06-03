import os

script_path = os.path.dirname(os.path.realpath(__file__))

def test_that_PUTs_all_files_to_metadata_server():
    n_files = 18
    with open(f'{script_path}/md.log', 'r') as file:
        assert file.read().count('PUT') == n_files
