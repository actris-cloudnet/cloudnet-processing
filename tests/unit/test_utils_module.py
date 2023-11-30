import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

import data_processing.utils as utils
import netCDF4
import numpy as np
import pytest
from numpy import ma

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
        (
            "software",
            {
                "cloudnetpy": "1.3.2",
                "cloudnet-processing": utils.get_data_processing_version(),
            },
        ),
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
        assert payload["software"] == {
            "cloudnet-processing": utils.get_data_processing_version()
        }
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
        assert payload["software"] == {
            "cloudnetpy": "1.4.0",
            "cloudnet-processing": utils.get_data_processing_version(),
        }


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


def test_are_identical_nc_files_real_data():
    fname1 = "tests/data/misc/20180703_granada_classification_old.nc"
    fname2 = "tests/data/misc/20180703_granada_classification.nc"
    fname3 = "tests/data/misc/20180703_granada_classification_reprocessed.nc"
    assert utils.are_identical_nc_files(fname1, fname2) is False
    assert utils.are_identical_nc_files(fname2, fname3) is True


@pytest.mark.parametrize(
    "data1, kwargs1, ncattrs1, data2, kwargs2, ncattrs2, expected",
    [
        (np.array([1, 2, 3]), {}, {}, np.array([1, 2, 3]), {}, {}, True),
        (np.array([1, 2, 3]), {}, {}, np.array([1, 2, 4]), {}, {}, False),
        (ma.masked_array([1, 2, 3]), {}, {}, ma.masked_array([1, 2, 3]), {}, {}, True),
        (ma.masked_array([1, 2, 3]), {}, {}, ma.masked_array([1, 2, 4]), {}, {}, False),
        (np.array([1.0]), {}, {}, np.array([1.1]), {}, {}, False),
        (np.array([1.0]), {}, {}, np.array([1.0000001]), {}, {}, True),
        (np.array([1]), {}, {}, np.array([1, 2]), {}, {}, False),
        (
            np.array([1, 99]),
            {"fill_value": 99},
            {},
            np.array([1, 99]),
            {"fill_value": 99},
            {},
            True,
        ),
        (
            np.array([1, 99]),
            {"fill_value": 99},
            {},
            np.array([2, 99]),
            {"fill_value": 99},
            {},
            False,
        ),
        (
            np.array([1, np.nan]),
            {"fill_value": np.nan},
            {},
            np.array([1, np.nan]),
            {"fill_value": np.nan},
            {},
            True,
        ),
        (
            ma.masked_array([1, 2], mask=[0, 1]),
            {"fill_value": 99},
            {},
            ma.masked_array([1, 3], mask=[0, 1]),
            {"fill_value": 999},
            {},
            True,
        ),
        (
            ma.masked_array([1, 2], mask=[1, 1]),
            {"fill_value": 99},
            {},
            ma.masked_array([1, 3], mask=[1, 1]),
            {"fill_value": 999},
            {},
            True,
        ),
        (
            ma.masked_array([23, 23], mask=[1, 1]),
            {},
            {},
            ma.masked_array([1, 3], mask=[1, 1]),
            {},
            {},
            True,
        ),
        (
            np.array([23, 23]),
            {},
            {},
            ma.masked_array([23, 23], mask=[1, 0]),
            {},
            {},
            False,
        ),
        (
            np.array([23, 23]),
            {},
            {},
            ma.masked_array([23, 23], mask=[0, 0]),
            {},
            {},
            True,
        ),
        (
            np.array([23, 23]),
            {},
            {},
            ma.masked_array([23, 23], mask=[1, 1]),
            {},
            {},
            False,
        ),
        (np.array([1]), {}, {}, np.array([1.0]), {}, {}, False),
        (
            ma.masked_array([1.0, 2.0, 3.0], mask=[0, 0, 0]),
            {},
            {},
            ma.masked_array([1.0, 2.0, 3.0], mask=[0, 0, 0]),
            {},
            {},
            True,
        ),
        (
            ma.masked_array([1.0, 2.0, 3.0], mask=[0, 0, 1]),
            {},
            {},
            ma.masked_array([1.0, 2.0, 4.0], mask=[0, 0, 1]),
            {},
            {},
            True,
        ),
        (
            ma.masked_array([1.0, 2.0, 3.0], mask=[0, 0, 0]),
            {},
            {},
            ma.masked_array([1.0, 2.0, 3.0], mask=[0, 0, 1]),
            {},
            {},
            False,
        ),
        (
            ma.masked_array([1.0, 2.0, 3.0], mask=[0, 1, 0]),
            {},
            {},
            ma.masked_array([1.0, 2.0, 3.0], mask=[0, 0, 1]),
            {},
            {},
            False,
        ),
        (np.array([1], dtype="i2"), {}, {}, np.array([1], dtype="i4"), {}, {}, False),
        (
            np.array([1.0]),
            {},
            {"units": "m"},
            np.array([1.0]),
            {},
            {"units": "cm"},
            False,
        ),
        (np.array([np.nan]), {}, {}, np.array([np.nan]), {}, {}, True),
    ],
)
def test_are_identical_nc_files_generated_data(
    data1, kwargs1, ncattrs1, data2, kwargs2, ncattrs2, expected, tmp_path
):
    fname1 = tmp_path / "old.nc"
    fname2 = tmp_path / "new.nc"
    for fname, data, kwargs, ncattrs in zip(
        (fname1, fname2), (data1, data2), (kwargs1, kwargs2), (ncattrs1, ncattrs2)
    ):
        with netCDF4.Dataset(fname, "w") as nc:
            nc.createDimension("time")
            time = nc.createVariable("time", data.dtype, ("time",), **kwargs)
            for attr, value in ncattrs.items():
                setattr(time, attr, value)
            time[:] = data
        if "fill_value" in kwargs:
            with netCDF4.Dataset(fname, "r") as nc:
                fv1 = nc["time"]._FillValue
                fv2 = kwargs["fill_value"]
                assert fv1 == fv2 or (np.isnan(fv1) and np.isnan(fv2))
    assert utils.are_identical_nc_files(fname1, fname2) == expected


@pytest.mark.parametrize(
    "attrs1, attrs2, expected",
    [
        ({"file_uuid": "kissa"}, {"file_uuid": "koira"}, True),
        (
            {"source_file_uuids": "kissa, hiiri"},
            {"source_file_uuids": "hiiri, kissa"},
            True,
        ),
        (
            {"source_file_uuids": "kissa, hiiri"},
            {"source_file_uuids": "kissa, koira"},
            False,
        ),
        ({"history": "processed yesterday"}, {"history": "processed today"}, True),
        ({"hilavitkutin_version": "3.1"}, {"hilavitkutin_version": "3.14"}, True),
    ],
)
def test_are_identical_nc_files_global_attributes(tmp_path, attrs1, attrs2, expected):
    fname1 = tmp_path / "old.nc"
    fname2 = tmp_path / "new.nc"
    for fname, attrs in zip((fname1, fname2), (attrs1, attrs2)):
        with netCDF4.Dataset(fname, "w") as nc:
            for attr, value in attrs.items():
                setattr(nc, attr, value)
    assert utils.are_identical_nc_files(fname1, fname2) == expected


@pytest.mark.parametrize(
    "array1, array2, expected",
    [
        ([1, 2, 3], [1, 2, 3], True),
        ([1, 2, 3], [1, 2, 4], False),
        (np.array([1, 2, 3]), np.array([1, 2, 3]), True),
        (np.array([1, 2, 3]), np.array([1, 2, 4]), False),
        (ma.masked_array([1, 2, 3]), ma.masked_array([1, 2, 3]), True),
        (ma.masked_array([1, 2, 3]), ma.masked_array([1, 2, 4]), False),
        (
            ma.masked_array([1.0, 2.0, 3.0], mask=[0, 0, 0]),
            ma.masked_array([1.0, 2.0, 3.0], mask=[0, 0, 0]),
            True,
        ),
        (
            ma.masked_array([1.0, 2.0, 3.0], mask=[0, 0, 0]),
            ma.masked_array([1.0, 2.0, 3.0], mask=[0, 0, 1]),
            False,
        ),
    ],
)
def test_compare_variables(array1, array2, expected: bool):
    with NamedTemporaryFile() as temp1, NamedTemporaryFile() as temp2:
        with (
            netCDF4.Dataset(temp1, "w", format="NETCDF4_CLASSIC") as nc1,
            netCDF4.Dataset(temp2, "w", format="NETCDF4_CLASSIC") as nc2,
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
