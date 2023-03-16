import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

import netCDF4
import numpy as np
import pytest
from numpy import ma

import data_processing.utils as utils

test_file_path = Path(__file__).parent.absolute()


@pytest.mark.parametrize(
    "key, value",
    [
        ("product", "classification"),
        ("site", "bucharest"),
        ("measurementDate", "2020-11-21"),
        ("format", "HDF5 (NetCDF4)"),
        (
            "checksum",
            "48e006f769a9352a42bf41beac449eae62aea545f4d3ba46bffd35759d8982ca",
        ),
        ("volatile", True),
        ("uuid", "2a211fc97e86489c9745e8027f86053a"),
        ("pid", ""),
        ("cloudnetpyVersion", "1.3.2"),
        ("version", "abc"),
        ("size", 120931),
    ],
)
def test_create_product_payload(key, value):
    storage_response = {"version": "abc", "size": 120931}
    full_path = "tests/data/products/20201121_bucharest_classification.nc"
    payload = utils.create_product_put_payload(full_path, storage_response)
    assert key in payload
    assert payload[key] == value


def test_read_site_info():
    site = "bucharest"
    site_info = utils.read_site_info(site)
    assert site_info["id"] == site
    assert site_info["name"] == "Bucharest"


def test_date_string_to_date():
    date = "2020-01-01"
    res = utils.date_string_to_date(date)
    assert isinstance(res, datetime.date)
    assert str(res) == date


@pytest.mark.parametrize(
    "n, input_date, result",
    [
        (0, "2020-05-20", "2020-05-20"),
        (5, "2020-05-20", "2020-05-15"),
        (1, "2020-01-01", "2019-12-31"),
        (-1, "2020-01-10", "2020-01-11"),
    ],
)
def test_get_date_from_past(n, input_date, result):
    assert utils.get_date_from_past(n, input_date) == result


def test_get_plottable_variables_info():
    res = utils.get_plottable_variables_info("lidar")
    expected = {
        "lidar-beta": ["Attenuated backscatter coefficient", 0],
        "lidar-beta_raw": ["Raw attenuated backscatter coefficient", 1],
        "lidar-depolarisation": ["Lidar depolarisation", 2],
        "lidar-depolarisation_raw": ["Raw depolarisation", 3],
        "lidar-beta_1064": ["Attenuated backscatter coefficient at 1064 nm", 4],
        "lidar-beta_532": ["Attenuated backscatter coefficient at 532 nm", 5],
        "lidar-beta_355": ["Attenuated backscatter coefficient at 355 nm", 6],
        "lidar-depolarisation_532": ["Lidar depolarisation at 532 nm", 7],
        "lidar-depolarisation_355": ["Lidar depolarisation at 355 nm", 8],
    }
    assert res == expected


@pytest.mark.parametrize(
    "instrument, file_type",
    [
        ("hatpro", "mwr"),
        ("mira", "radar"),
        ("chm15k", "lidar"),
        ("parsivel", "disdrometer"),
    ],
)
def test_get_level1b_type(instrument, file_type):
    assert utils.get_level1b_type(instrument) == file_type


def test_is_volatile_file():
    file = "tests/data/products/20201121_bucharest_classification.nc"
    assert utils.is_volatile_file(file) is True


def test_get_product_bucket():
    assert utils.get_product_bucket(True) == "cloudnet-product-volatile"
    assert utils.get_product_bucket(False) == "cloudnet-product"


@pytest.mark.parametrize(
    "level, expected",
    [
        ("1b", ["lidar", "model", "mwr", "radar", "disdrometer", "weather-station"]),
        ("1c", ["categorize", "categorize-voodoo"]),
        (
            "2",
            [
                "classification",
                "classification-voodoo",
                "drizzle",
                "iwc",
                "lwc",
                "der",
                "ier",
            ],
        ),
        (
            None,
            [
                "lidar",
                "model",
                "mwr",
                "radar",
                "disdrometer",
                "weather-station",
                "categorize",
                "categorize-voodoo",
                "classification",
                "classification-voodoo",
                "drizzle",
                "iwc",
                "lwc",
                "der",
                "ier",
                "l3-cf",
                "l3-iwc",
                "l3-lwc",
            ],
        ),
    ],
)
def test_get_product_types(level, expected):
    result = utils.get_product_types(level=level)
    assert set(result) == set(expected)


class TestHash:
    file = "tests/data/products/20201121_bucharest_classification.nc"

    def test_md5sum(self):
        hash_sum = utils.md5sum(self.file)
        assert hash_sum == "c81d7834d7189facbc5f63416fe5b3da"

    def test_sha256sum2(self):
        hash_sum = utils.sha256sum(self.file)
        assert (
            hash_sum
            == "48e006f769a9352a42bf41beac449eae62aea545f4d3ba46bffd35759d8982ca"
        )


class TestsCreateProductPutPayload:
    storage_response = {"size": 66, "version": "abc"}

    def test_with_legacy_file(self):
        file = "tests/data/products/legacy_classification.nc"
        payload = utils.create_product_put_payload(
            file,
            self.storage_response,
            product="classification",
            site="schneefernerhaus",
            date_str="2020-07-06",
        )
        assert payload["measurementDate"] == "2020-07-06"
        assert payload["format"] == "NetCDF3"
        assert payload["pid"] == ""
        assert len(payload["cloudnetpyVersion"]) == 0
        assert payload["volatile"] is True
        assert payload["site"] == "schneefernerhaus"
        assert payload["product"] == "classification"

    def test_with_cloudnetpy_file(self):
        file = "tests/data/products/20201022_bucharest_categorize.nc"
        payload = utils.create_product_put_payload(file, self.storage_response)
        assert payload["measurementDate"] == "2020-10-22"
        assert payload["format"] == "HDF5 (NetCDF4)"
        assert payload["volatile"] is True
        assert payload["site"] == "bucharest"
        assert payload["product"] == "categorize"
        assert len(payload["cloudnetpyVersion"]) == 5


@pytest.mark.parametrize(
    "filename, identifier",
    [
        ("20201022_bucharest_gdas1.nc", "gdas1"),
        ("20201022_bucharest_ecmwf.nc", "ecmwf"),
        ("20200101_potenza_icon-iglo-12-23.nc", "icon-iglo-12-23"),
    ],
)
def test_get_model_identifier(filename, identifier):
    assert utils.get_model_identifier(filename) == identifier


def test_are_identical_nc_files():
    fname1 = "tests/data/misc/20180703_granada_classification_old.nc"
    fname2 = "tests/data/misc/20180703_granada_classification.nc"
    fname3 = "tests/data/misc/20180703_granada_classification_reprocessed.nc"
    assert utils.are_identical_nc_files(fname1, fname2) is False
    assert utils.are_identical_nc_files(fname2, fname3) is True


@pytest.mark.parametrize(
    "array1, array2, expected",
    [
        ([1, 2, 3], [1, 2, 3], True),
        ([1, 2, 3], [1, 2, 4], False),
        (np.array([1, 2, 3]), np.array([1, 2, 3]), True),
        (np.array([1, 2, 3]), np.array([1, 2, 4]), False),
        (ma.array([1, 2, 3]), ma.array([1, 2, 3]), True),
        (ma.array([1, 2, 3]), ma.array([1, 2, 4]), False),
        (
            ma.array([1.0, 2.0, 3.0], mask=[0, 0, 0]),
            ma.array([1.0, 2.0, 3.0], mask=[0, 0, 0]),
            True,
        ),
        (
            ma.array([1.0, 2.0, 3.0], mask=[0, 0, 0]),
            ma.array([1.0, 2.0, 3.0], mask=[0, 0, 1]),
            False,
        ),
    ],
)
def test_compare_variables(array1, array2, expected: bool):
    with (
        netCDF4.Dataset(NamedTemporaryFile(), "w", format="NETCDF4_CLASSIC") as nc1,
        netCDF4.Dataset(NamedTemporaryFile(), "w", format="NETCDF4_CLASSIC") as nc2,
    ):
        nc1.createDimension("time", 3)
        nc2.createDimension("time", 3)
        var1 = nc1.createVariable("time", "f8", ("time",))
        var2 = nc2.createVariable("time", "f8", ("time",))
        var1[:] = array1
        var2[:] = array2
        if expected:
            utils._compare_variables(nc1, nc2)
        else:
            with pytest.raises(Exception):
                utils._compare_variables(nc1, nc2)
