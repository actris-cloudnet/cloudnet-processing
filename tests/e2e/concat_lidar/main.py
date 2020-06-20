#!/usr/bin/env python3
import subprocess
import pytest
from test_utils.utils import remove_files, remove_dir

YEAR = 2020

def main():

    def _get_result_path(path_in):
        return f'{path_in}/2020/'

    # Test default output path
    data_path = 'tests/data/input/bucharest/uncalibrated/chm15k/'
    result_path = _get_result_path(data_path)
    remove_files(result_path)
    subprocess.check_call(['python3', 'scripts/concat-lidar.py', data_path])
    pytest.main(['-v', 'tests/e2e/concat_lidar/tests.py',
                 '-k', 'TestDefaultProcessing',
                 '--input', data_path])
    remove_files(result_path)

    # Test user-defined output path
    output = 'tests/data/output/'
    result_path = _get_result_path(output)
    remove_dir(result_path)
    subprocess.check_call(['python3', 'scripts/concat-lidar.py', data_path,
                           f'--output={output}'])
    pytest.main(['-v', 'tests/e2e/concat_lidar/tests.py',
                 '-k', 'TestOutput',
                 '--output', output])
    remove_dir(result_path)


if __name__ == "__main__":
    main()
