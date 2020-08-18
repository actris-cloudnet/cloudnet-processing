import datetime
from pathlib import Path
from collections import namedtuple
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
