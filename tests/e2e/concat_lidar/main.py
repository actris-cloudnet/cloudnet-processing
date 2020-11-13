#!/usr/bin/env python3
import os
import pytest
import glob
from data_processing.concat_lib import concat_chm15k_files


def main():

    fpath = 'tests/data/input/bucharest/uncalibrated/chm15k/2020/04/02/'

    output_file = f'{fpath}concatenated_chm15k_file.nc'
    if os.path.isfile(output_file):
        os.remove(output_file)

    chm15k_files = glob.glob(f'{fpath}*nc',)

    concat_chm15k_files(chm15k_files, '2020-04-02', output_file)

    pytest.main(['-v', 'tests/e2e/concat_lidar/tests.py',
                 '--output', output_file])


if __name__ == "__main__":
    main()
