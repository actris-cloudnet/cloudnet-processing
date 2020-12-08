import numpy as np
import netCDF4
import pytest
from data_processing import concat_lib as lib


def test__validate_date_attributes(nc_file):
    nc = netCDF4.Dataset(nc_file)
    assert lib._validate_date_attributes(nc, ['2020', '05', '20'])


def test_get_dtype():
    data_int = np.array([1, 2, 3], dtype=int)
    data_float = np.array([1, 2, 3], dtype=float)
    assert lib._get_dtype('time', data_int) == 'f8'
    assert lib._get_dtype('time', data_float) == 'f8'
    assert lib._get_dtype('foo', data_int) == 'i4'
    assert lib._get_dtype('foo', data_float) == 'f4'


@pytest.mark.parametrize("size, result", [
    (np.zeros((10, 5)), ('time', 'range')),
    (np.zeros((5, 10)), ('range', 'time')),
    (np.zeros((5, )), ('range', )),
    (np.zeros((10, )), ('time', )),
    (np.zeros((100, )), ('time', )),
    (np.zeros((3, 1000)), ('other', 'time')),
])
def test_get_dim(size, result, nc_file):
    f = netCDF4.Dataset(nc_file)
    assert lib._get_dim(f, size) == result
