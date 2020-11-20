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


def test_read_main_conf():
    Args = namedtuple('args', 'config_dir')
    args = Args(config_dir=f"{test_file_path}/../../config")
    conf = utils.read_main_conf(args)
    assert 'received_api_files' in conf['PATH']


def test_sha256sum(nc_file):
    hash_sum = utils.sha256sum(nc_file)
    assert isinstance(hash_sum, str)
    assert len(hash_sum) == 64


def test_md5sum():
    file = 'tests/data/input/bucharest/uncalibrated/chm15k/2020/04/02/00100_A202004022120_CHM170137.nc'
    hash_sum = utils.md5sum(file)
    assert hash_sum == '0c76528228514824b7975c80a911e2f4'


def test_sha256sum2():
    file = 'tests/data/input/bucharest/uncalibrated/chm15k/2020/04/02/00100_A202004022120_CHM170137.nc'
    hash_sum = utils.sha256sum(file)
    assert hash_sum == 'd0b67250568e7a9c0948d50553b5d56be183500d4289627bbfe65b0c2a3316a1'
