import datetime
from pathlib import Path
from collections import namedtuple
import pytest
import data_processing.utils as utils

test_file_path = Path(__file__).parent.absolute()


@pytest.mark.parametrize("key, value", [
    ('product', 'classification'),
    ('site', 'bucharest'),
    ('measurementDate', '2020-11-21'),
    ('format', 'HDF5 (NetCDF4)'),
    ('checksum', '48e006f769a9352a42bf41beac449eae62aea545f4d3ba46bffd35759d8982ca'),
    ('volatile', True),
    ('uuid', '2a211fc97e86489c9745e8027f86053a'),
    ('pid', ''),
    ('cloudnetpyVersion', '1.3.2'),
    ('version', 'abc'),
    ('size', 120931)
])
def test_create_product_payload(key, value):
    storage_response = {'version': 'abc',
                        'size': 120931}
    full_path = 'tests/data/products/20201121_bucharest_classification.nc'
    payload = utils.create_product_put_payload(full_path, storage_response)
    assert key in payload
    assert payload[key] == value


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


@pytest.mark.parametrize("instrument, file_type", [
    ('hatpro', 'mwr'),
    ('mira', 'radar'),
    ('chm15k', 'lidar'),
    ('parsivel', 'disdrometer'),
])
def test_get_level1b_type(instrument, file_type):
    assert utils.get_level1b_type(instrument) == file_type


def test_is_volatile_file():
    file = 'tests/data/products/20201121_bucharest_classification.nc'
    assert utils.is_volatile_file(file) is True


def test_get_product_bucket():
    assert utils.get_product_bucket(True) == 'cloudnet-product-volatile'
    assert utils.get_product_bucket(False) == 'cloudnet-product'


def test_get_product_types():
    l1_types = ['lidar', 'model', 'mwr', 'radar']
    l2_types = ['classification', 'drizzle', 'iwc', 'lwc']
    assert utils.get_product_types(level='1b') == l1_types
    assert utils.get_product_types(level='2') == l2_types
    assert utils.get_product_types() == l1_types + ['categorize'] + l2_types


class TestHash:

    file = 'tests/data/products/20201121_bucharest_classification.nc'

    def test_md5sum(self):
        hash_sum = utils.md5sum(self.file)
        assert hash_sum == 'c81d7834d7189facbc5f63416fe5b3da'

    def test_sha256sum2(self):
        hash_sum = utils.sha256sum(self.file)
        assert hash_sum == '48e006f769a9352a42bf41beac449eae62aea545f4d3ba46bffd35759d8982ca'


class TestsCreateProductPutPayload:

    storage_response = {'size': 66, 'version': 'abc'}

    def test_with_legacy_file(self):
        file = 'tests/data/products/legacy_classification.nc'
        payload = utils.create_product_put_payload(file, self.storage_response,
                                                   product='classification',
                                                   site='schneefernerhaus',
                                                   date_str='2020-07-06')
        assert payload['measurementDate'] == '2020-07-06'
        assert payload['format'] == 'NetCDF3'
        assert payload['pid'] == ''
        assert len(payload['cloudnetpyVersion']) == 0
        assert payload['volatile'] is True
        assert payload['site'] == 'schneefernerhaus'
        assert payload['product'] == 'classification'

    def test_with_cloudnetpy_file(self):
        file = 'tests/data/products/20201022_bucharest_categorize.nc'
        payload = utils.create_product_put_payload(file, self.storage_response)
        assert payload['measurementDate'] == '2020-10-22'
        assert payload['format'] == 'HDF5 (NetCDF4)'
        assert payload['volatile'] is True
        assert payload['site'] == 'bucharest'
        assert payload['product'] == 'categorize'
        assert len(payload['cloudnetpyVersion']) == 5


@pytest.mark.parametrize("filename, identifier", [
    ('20201022_bucharest_gdas1.nc', 'gdas1'),
    ('20201022_bucharest_ecmwf.nc', 'ecmwf'),
    ('20200101_potenza_icon-iglo-12-23.nc', 'icon-iglo-12-23')
])
def test_get_model_identifier(filename, identifier):
    assert utils.get_model_identifier(filename) == identifier


@pytest.mark.parametrize("dtime, zone, expected", [
    ('2021-03-15 15:00:00', 'Europe/Stockholm', '2021-03-15 14:00:00'),
    ('2021-06-15 15:00:00', 'Europe/Stockholm', '2021-06-15 13:00:00'),
    ('2021-06-15 00:00:01', 'Europe/Helsinki', '2021-06-14 21:00:01'),
])
def test_datetime_to_utc(dtime, zone, expected):
    assert utils.datetime_to_utc(dtime, zone) == expected
