import datetime
import pytest

from operational_processing import utils

"""
This needs altocumulus.fmi.fi to be visible outside FMI
def test_read_site_info():
    site = 'bucharest'
    site_info = utils.read_site_info(site)
    assert site_info['id'] == site
    assert site_info['name'] == 'Bucharest'
"""


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
])
def test_get_date_from_past(n, input_date, result):
    assert utils.get_date_from_past(n, input_date) == result
