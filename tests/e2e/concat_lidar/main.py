#!/usr/bin/env python3
import pytest
import glob
from data_processing.concat_wrapper import concat_chm15k_files
from tempfile import NamedTemporaryFile


def main():
    fpath = 'tests/data/raw/chm15k/'
    temp_file = NamedTemporaryFile()
    chm15k_files = glob.glob(f'{fpath}*nc',)
    concat_chm15k_files(chm15k_files, '2020-10-22', temp_file.name)
    pytest.main(['-v', 'tests/e2e/concat_lidar/lidar_tests.py',
                 '--full_path', temp_file.name])


if __name__ == "__main__":
    main()
