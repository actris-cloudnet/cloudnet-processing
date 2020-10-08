import os
import datetime
from pathlib import Path
from collections import namedtuple
import tempfile
import pytest
import data_processing.utils as utils

test_file_path = Path(__file__).parent.absolute()


def test_read_site_info():
    site = 'bucharest'
    site_info = utils.read_site_info(site)
    assert site_info['id'] == site
    assert site_info['name'] == 'Bucharest'


def test_find_file(tmpdir):
    fpath = str(tmpdir)
    tmpdir.mkdir('aaa_234.nc')
    tmpdir.mkdir('aaa_534.nc')
    tmpdir.mkdir('aaa_123.nc')
    assert utils.find_file(fpath, '*53*') == '/'.join((fpath, 'aaa_534.nc'))
    assert utils.find_file(fpath, 'aaa_2*') == '/'.join((fpath, 'aaa_234.nc'))
    assert utils.find_file(fpath, '*3.nc') == '/'.join((fpath, 'aaa_123.nc'))
    with pytest.raises(FileNotFoundError):
        utils.find_file(fpath, '*xyz')


def test_date_string_to_date():
    date = '2020-01-01'
    res = utils.date_string_to_date(date)
    assert isinstance(res, datetime.date)
    assert str(res) == date


@pytest.mark.parametrize("n, input_date, result", [
    (0, '2020-05-20', '2020-05-20'),
    (5, '2020-05-20', '2020-05-15'),
    (1, '2020-01-01', '2019-12-31'),
    (-1, '2020-01-10', '2020-01-11'),
])
def test_get_date_from_past(n, input_date, result):
    assert utils.get_date_from_past(n, input_date) == result


@pytest.mark.parametrize("filename, hash_sum, result", [
    ('abc.nc', 'xyz', 'abc-xyz.nc'),
    ('abc', 'xyz', 'abc-xyz'),
    ('xyz', 'aaaaaaaaaaaaaaaabbccdd', 'xyz-aaaaaaaaaaaaaaaabb'),
])
def test_add_hash(filename, hash_sum, result):
    assert utils.add_hash_to_filename(filename, hash_sum) == result


def test_get_plottable_variables_info():
    res = utils.get_plottable_variables_info('lidar')
    expected = {'lidar-beta': ['Attenuated backscatter coefficient', 0],
                'lidar-beta_raw': ['Raw attenuated backscatter coefficient', 1]}
    assert res == expected


def test_read_conf_1():
    Args = namedtuple('args', ['config_dir'])
    args = Args(config_dir=f"{test_file_path}/../../config")
    conf = utils.read_conf(args)
    assert conf['main']['METADATASERVER']['url'] == 'http://localhost:3000/'


def test_read_conf_2():
    Args = namedtuple('args', ['config_dir', 'site'])
    args = Args(config_dir=f"{test_file_path}/../../config", site=['bucharest'])
    conf = utils.read_conf(args)
    assert conf['site']['INSTRUMENTS']['lidar'] == 'chm15k'


def test_read_conf_3():
    Args = namedtuple('args', ['config_dir', 'input', 'output'])
    args = Args(config_dir=f"{test_file_path}/../../config", input='/my/input', output='/my/output')
    conf = utils.read_conf(args)
    assert conf['main']['PATH']['input'] == '/my/input'
    assert conf['main']['PATH']['output'] == '/my/output'


def test_read_main_conf():
    Args = namedtuple('args', 'config_dir')
    args = Args(config_dir=f"{test_file_path}/../../config")
    conf = utils.read_main_conf(args)
    assert 'received_api_files' in conf['PATH']


@pytest.mark.parametrize("pattern, result", [
    ('19800112*.nc', 2),
    ('19800113*.nc', 1),
    ('19800114*.nc', 0),
    ('19800112*.txt', 1),
    ('19800113*.txt', 0),
])
def test_list_files(pattern, result):
    f1 = tempfile.NamedTemporaryFile(prefix='19800112', suffix='.nc')
    f2 = tempfile.NamedTemporaryFile(prefix='19800112', suffix='.nc')
    f3 = tempfile.NamedTemporaryFile(prefix='19800112', suffix='.txt')
    f4 = tempfile.NamedTemporaryFile(prefix='19800113', suffix='.nc')
    folder = os.path.dirname(f1.name)
    assert len(utils.list_files(folder, pattern)) == result


def test_add_uuid_to_filename():
    temp_dir = 'tests/data/temp/'
    filename = 'kukkuu.nc'
    os.makedirs(temp_dir, exist_ok=True)
    filename = os.path.join(temp_dir, filename)
    open(filename, 'a').close()
    uuid = 'abcdefgh'
    new_filename = utils.add_uuid_to_filename(uuid, filename)
    os.remove(new_filename)
    os.rmdir(temp_dir)
    assert new_filename == f"{filename[:-3]}_abcd.nc"


def test_is_volatile_file(nc_file, nc_file_with_pid):
    assert utils.is_volatile_file(nc_file) is True
    assert utils.is_volatile_file(nc_file_with_pid) is False


def test_replace_path():
    filename = '/foo/bar/filu.nc'
    new_path = '/uusi/polku'
    assert utils.replace_path(filename, new_path) == '/uusi/polku/filu.nc'


def test_sha256sum(nc_file):
    hash_sum = utils.sha256sum(nc_file)
    assert isinstance(hash_sum, str)
    assert len(hash_sum) == 64
