import os

N_FILES_EXPECTED = 9

def test_that_puts_all_files_to_metadata_server():
    script_path = os.path.dirname(os.path.realpath(__file__))
    with open(f'{script_path}/md.log', 'r') as file:
        assert file.read().count('PUT') == N_FILES_EXPECTED
