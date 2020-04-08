import numpy as np
from numpy.testing import assert_array_equal
import netCDF4
import pytest


lib = __import__('operational-processing').concat_lib


def test_get_files_by_suffix():
    files = ['abc.nc', 'ggg.txt', 'sfsdfs.nc', 'bbbb.ncn', 'ffffnc']
    expected = ['abc.nc', 'sfsdfs.nc']
    assert_array_equal(lib.get_files_by_suffix(files), expected)


def test_get_dtype():
    data_int = np.array([1, 2, 3], dtype=int)
    data_float = np.array([1, 2, 3], dtype=float)
    assert lib.get_dtype('time', data_int) == 'f8'
    assert lib.get_dtype('time', data_float) == 'f8'
    assert lib.get_dtype('foo', data_int) == 'i4'
    assert lib.get_dtype('foo', data_float) == 'f4'


def test_get_full_input_path():
    root = '/foo/bar/'
    date = ('2020', '01', '15')
    assert lib.get_full_input_path(root, date) == '/foo/bar//2020/01/15'


def test_get_default_range():
    assert lib.get_default_range('year') == [2000, 2020]
    assert lib.get_default_range('month') == [1, 12]
    assert lib.get_default_range('day') == [1, 31]


def test_get_good_dirs(tmpdir):
    for y in ['2016', '2017', '2019', '2020', '2021', 'kissa']:
        tmpdir.mkdir(y)
    res = lib.get_good_dirs(str(tmpdir), [2017, 2020])
    for y in ['2017', '2019', '2020']:
        assert y in res


def test_get_files_for_day(tmpdir):
    fpath = str(tmpdir)
    tmpdir.mkdir('aaa_234.nc')
    tmpdir.mkdir('aaa_534.nc')
    tmpdir.mkdir('aaa_123.nc')
    res = lib.get_files_for_day(fpath)
    assert_array_equal(res, ['/'.join((fpath, 'aaa_123.nc')),
                             '/'.join((fpath, 'aaa_234.nc')),
                             '/'.join((fpath, 'aaa_534.nc'))])


def test_get_dim(nc_file):
    f = netCDF4.Dataset(nc_file)
    assert lib.get_dim(f, np.zeros((10, 5))) == ('time', 'range')
    assert lib.get_dim(f, np.zeros((5, 10))) == ('range', 'time')
    assert lib.get_dim(f, np.zeros((5, ))) == ('range', )
    assert lib.get_dim(f, np.zeros((10, ))) == ('time', )
    assert lib.get_dim(f, np.zeros((100,))) == ('time', )
    assert lib.get_dim(f, np.zeros((3, 1000))) == ('other', 'time')


@pytest.mark.parametrize("input, result", [
    ('/foo/bar/Y2020/M01/D01/file.nc', ('2020', '01', '01')),
    ('/foo/bar/2020/01/01/file.nc', ('2020', '01', '01')),
    ('/foo/bar/2020/M12/01/file.nc', ('2020', '12', '01')),
    ('/foo/bar/Y2020/M01/01/file.nc', ('2020', '01', '01')),
    ('/foo/bar/Y2020/M13/01/file.nc', None),
    ('/foo/bar/Y2020/M01/D32/file.nc', None),
    ('/foo/bar/Y0000/M01/01/file.nc', None),
    ('/foo/bar/sfksdlsf20200101asdfadf.nc', ('2020', '01', '01')),
    ('/foo/bar/2020/20201231.nc', ('2020', '12', '31')),
])
def test_find_date(input, result):
    assert lib.find_date(input) == result
