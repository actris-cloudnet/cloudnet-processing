from pathlib import Path

import netCDF4
import numpy as np
import numpy.typing as npt
import pytest
from cloudnet_api_client.utils import md5sum, sha256sum
from numpy import ma

from processing import netcdf_comparer
from processing.netcdf_comparer import NCDiff
from processing.storage_api import _get_product_bucket

test_file_path = Path(__file__).parent.absolute()


def test_get_product_bucket() -> None:
    assert _get_product_bucket(True) == "cloudnet-product-volatile"
    assert _get_product_bucket(False) == "cloudnet-product"


class TestHash:
    file = "tests/data/20201121_bucharest_classification.nc"

    def test_md5sum(self) -> None:
        hash_sum = md5sum(self.file)
        assert hash_sum == "c81d7834d7189facbc5f63416fe5b3da"

    def test_sha256sum2(self) -> None:
        hash_sum = sha256sum(self.file)
        assert (
            hash_sum
            == "48e006f769a9352a42bf41beac449eae62aea545f4d3ba46bffd35759d8982ca"
        )


def test_are_identical_nc_files_real_data() -> None:
    fname1 = "tests/data/20180703_granada_classification_old.nc"
    fname2 = "tests/data/20180703_granada_classification.nc"
    fname3 = "tests/data/20180703_granada_classification_reprocessed.nc"
    assert netcdf_comparer.nc_difference(Path(fname1), Path(fname2)) == NCDiff.MAJOR
    assert netcdf_comparer.nc_difference(Path(fname2), Path(fname3)) == NCDiff.NONE


@pytest.mark.parametrize(
    "data1, kwargs1, ncattrs1, data2, kwargs2, ncattrs2, expected",
    [
        (np.array([1, 2, 3]), {}, {}, np.array([1, 2, 3]), {}, {}, NCDiff.NONE),
        (np.array([1, 2, 3]), {}, {}, np.array([1, 2, 4]), {}, {}, NCDiff.MAJOR),
        (
            ma.masked_array([1, 2, 3]),
            {},
            {},
            ma.masked_array([1, 2, 3]),
            {},
            {},
            NCDiff.NONE,
        ),
        (
            ma.masked_array([1, 2, 3]),
            {},
            {},
            ma.masked_array([1, 2, 4]),
            {},
            {},
            NCDiff.MAJOR,
        ),
        (np.array([1.0]), {}, {}, np.array([1.1]), {}, {}, NCDiff.MAJOR),
        (np.array([1.0]), {}, {}, np.array([0.9991]), {}, {}, NCDiff.NONE),
        (np.array([-1.0]), {}, {}, np.array([-0.9991]), {}, {}, NCDiff.NONE),
        (np.array([1.0]), {}, {}, np.array([1.05]), {}, {}, NCDiff.MAJOR),
        (np.array([1.0]), {}, {}, np.array([1.04]), {}, {}, NCDiff.MINOR),
        (np.array([-1.0]), {}, {}, np.array([-1.04]), {}, {}, NCDiff.MINOR),
        (np.array([-1.0]), {}, {}, np.array([-1.05]), {}, {}, NCDiff.MAJOR),
        (np.array([1.0]), {}, {}, np.array([1.1]), {}, {}, NCDiff.MAJOR),
        (np.array([1]), {}, {}, np.array([1, 2]), {}, {}, NCDiff.MAJOR),
        (
            np.array([1, 2]),
            {},
            {},
            np.array([1, 2]),
            {"fill_value": 2},
            {},
            NCDiff.MAJOR,
        ),
        (
            np.array([1, 2]),
            {"fill_value": 2},
            {},
            np.array([1, 2]),
            {},
            {},
            NCDiff.MAJOR,
        ),
        (
            np.array([1, 2]),
            {"fill_value": 2},
            {},
            np.array([1, 3]),
            {"fill_value": 3},
            {},
            NCDiff.NONE,
        ),
        (
            np.array([1, 99]),
            {"fill_value": 99},
            {},
            np.array([1, 99]),
            {"fill_value": 99},
            {},
            NCDiff.NONE,
        ),
        (
            np.array([1, 99]),
            {"fill_value": 99},
            {},
            np.array([2, 99]),
            {"fill_value": 99},
            {},
            NCDiff.MAJOR,
        ),
        (
            np.array([1, np.nan]),
            {"fill_value": np.nan},
            {},
            np.array([1, np.nan]),
            {"fill_value": np.nan},
            {},
            NCDiff.NONE,
        ),
        (
            ma.masked_array([1, 2], mask=[False, True]),
            {"fill_value": 99},
            {},
            ma.masked_array([1, 3], mask=[False, True]),
            {"fill_value": 999},
            {},
            NCDiff.NONE,
        ),
        (
            ma.masked_array([1, 2], mask=[True, True]),
            {"fill_value": 99},
            {},
            ma.masked_array([1, 3], mask=[True, True]),
            {"fill_value": 999},
            {},
            NCDiff.NONE,
        ),
        (
            ma.masked_array([23, 23], mask=[True, True]),
            {},
            {},
            ma.masked_array([1, 3], mask=[True, True]),
            {},
            {},
            NCDiff.NONE,
        ),
        (
            np.array([23, 23]),
            {},
            {},
            ma.masked_array([23, 23], mask=[True, False]),
            {},
            {},
            NCDiff.MAJOR,
        ),
        (
            np.array([23, 23]),
            {},
            {},
            ma.masked_array([23, 23], mask=[False, False]),
            {},
            {},
            NCDiff.NONE,
        ),
        (
            np.array([23, 23]),
            {},
            {},
            ma.masked_array([23, 23], mask=[True, True]),
            {},
            {},
            NCDiff.MAJOR,
        ),
        (
            ma.masked_array([1.0, 2.0, 3.0], mask=[False, False, False]),
            {},
            {},
            ma.masked_array([1.0, 2.0, 3.0], mask=[False, False, False]),
            {},
            {},
            NCDiff.NONE,
        ),
        (
            ma.masked_array([1.0, 2.0, 3.0], mask=[False, False, True]),
            {},
            {},
            ma.masked_array([1.0, 2.0, 4.0], mask=[False, False, True]),
            {},
            {},
            NCDiff.NONE,
        ),
        (
            ma.masked_array([1.0, 2.0, 3.0], mask=[False, False, False]),
            {},
            {},
            ma.masked_array([1.0, 2.0, 3.0], mask=[False, False, True]),
            {},
            {},
            NCDiff.MAJOR,
        ),
        (
            ma.masked_array([1.0, 2.0, 3.0], mask=[False, True, False]),
            {},
            {},
            ma.masked_array([1.0, 2.0, 3.0], mask=[False, False, True]),
            {},
            {},
            NCDiff.MAJOR,
        ),
        (
            np.array([1.0]),
            {},
            {"units": "m"},
            np.array([1.0]),
            {},
            {"units": "cm"},
            NCDiff.MAJOR,
        ),
        (np.array([np.nan]), {}, {}, np.array([np.nan]), {}, {}, NCDiff.NONE),
    ],
)
def test_are_identical_nc_files_generated_data(
    data1: npt.NDArray,
    kwargs1: dict,
    ncattrs1: dict,
    data2: npt.NDArray,
    kwargs2: dict,
    ncattrs2: dict,
    expected: NCDiff,
    tmp_path: Path,
) -> None:
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
    assert netcdf_comparer.nc_difference(fname1, fname2) == expected


@pytest.mark.parametrize(
    "attrs1, attrs2, expected",
    [
        ({"file_uuid": "kissa"}, {"file_uuid": "koira"}, NCDiff.NONE),
        (
            {"source_file_uuids": "kissa, hiiri"},
            {"source_file_uuids": "hiiri, kissa"},
            NCDiff.NONE,
        ),
        (
            {"source_file_uuids": "kissa, hiiri"},
            {"source_file_uuids": "kissa, koira"},
            NCDiff.MINOR,
        ),
        (
            {"history": "processed yesterday"},
            {"history": "processed today"},
            NCDiff.NONE,
        ),
        (
            {"hilavitkutin_version": "3.1"},
            {"hilavitkutin_version": "3.14"},
            NCDiff.NONE,
        ),
        (
            {"kissa1": "kaneli", "kissa2": "jaffa"},
            {"kissa1": "kaneli"},
            NCDiff.MAJOR,
        ),
        (
            {"kissa1": "kaneli"},
            {"kissa1": "kaneli", "kissa2": "jaffa"},
            NCDiff.MINOR,
        ),
        (
            {"highscore": np.array([10, 22, 54])},
            {"highscore": np.array([10, 22, 54])},
            NCDiff.NONE,
        ),
        (
            {"highscore": np.array([10, 22, 54])},
            {"highscore": np.array([22, 54, 60])},
            NCDiff.MINOR,
        ),
        (
            {"highscore": np.array([22, 54])},
            {"highscore": np.array([22, 54, 60])},
            NCDiff.MINOR,
        ),
        (
            {"highscore": np.array([10, 22, 54])},
            {"highscore": np.array([54, 60])},
            NCDiff.MINOR,
        ),
        (
            {"highscore": 54},
            {"highscore": np.array([22, 54, 60])},
            NCDiff.MINOR,
        ),
        (
            {"highscore": np.array([10, 22, 54])},
            {"highscore": 60},
            NCDiff.MINOR,
        ),
    ],
)
def test_are_identical_nc_files_global_attributes(
    tmp_path: Path, attrs1: dict, attrs2: dict, expected: NCDiff
) -> None:
    fname1 = tmp_path / "old.nc"
    fname2 = tmp_path / "new.nc"
    for fname, attrs in zip((fname1, fname2), (attrs1, attrs2)):
        with netCDF4.Dataset(fname, "w") as nc:
            for attr, value in attrs.items():
                setattr(nc, attr, value)
    assert netcdf_comparer.nc_difference(fname1, fname2) == expected


@pytest.mark.parametrize(
    "array1, array2, expected",
    [
        ([1, 2, 3], [1, 2, 3], NCDiff.NONE),
        ([1, 2, 3], [1, 2, 4], NCDiff.MAJOR),
        (np.array([1, 2, 3]), np.array([1, 2, 3]), NCDiff.NONE),
        (np.array([1, 2, 3]), np.array([1, 2, 4]), NCDiff.MAJOR),
        (ma.masked_array([1, 2, 3]), ma.masked_array([1, 2, 3]), NCDiff.NONE),
        (ma.masked_array([1, 2, 3]), ma.masked_array([1, 2, 4]), NCDiff.MAJOR),
        (
            ma.masked_array([1.0, 2.0, 3.0], mask=[False, False, False]),
            ma.masked_array([1.0, 2.0, 3.0], mask=[False, False, False]),
            NCDiff.NONE,
        ),
        (
            ma.masked_array([1.0, 2.0, 3.0], mask=[False, False, False]),
            ma.masked_array([1.0, 2.0, 3.0], mask=[False, False, True]),
            NCDiff.MAJOR,
        ),
    ],
)
def test_compare_variables(
    array1: npt.ArrayLike, array2: npt.ArrayLike, expected: NCDiff, tmp_path: Path
) -> None:
    temp1 = tmp_path / "file1.nc"
    temp2 = tmp_path / "file2.nc"
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
    assert netcdf_comparer.nc_difference(temp1, temp2) == expected


def test_missing_variable_in_new_file(tmp_path: Path) -> None:
    old_file = tmp_path / "file1.nc"
    new_file = tmp_path / "file2.nc"
    with (
        netCDF4.Dataset(old_file, "w", format="NETCDF4_CLASSIC") as nc1,
        netCDF4.Dataset(new_file, "w", format="NETCDF4_CLASSIC") as nc2,
    ):
        nc1.createDimension("time", 3)
        nc2.createDimension("time", 3)
        nc1.createVariable("time", "f8", ("time",))
        nc1.createVariable("kissa", "f8", ("time",))
        nc2.createVariable("time", "f8", ("time",))
    assert netcdf_comparer.nc_difference(old_file, new_file) == NCDiff.MAJOR


def test_missing_variable_in_old_file(tmp_path: Path) -> None:
    old_file = tmp_path / "file1.nc"
    new_file = tmp_path / "file2.nc"
    with (
        netCDF4.Dataset(old_file, "w", format="NETCDF4_CLASSIC") as nc1,
        netCDF4.Dataset(new_file, "w", format="NETCDF4_CLASSIC") as nc2,
    ):
        nc1.createDimension("time", 3)
        nc2.createDimension("time", 3)
        nc1.createVariable("time", "f8", ("time",))
        nc2.createVariable("kissa", "f8", ("time",))
        nc2.createVariable("time", "f8", ("time",))
    assert netcdf_comparer.nc_difference(old_file, new_file) == NCDiff.MINOR


@pytest.mark.parametrize(
    "dtype_old, dtype_new, expected",
    [
        ("f8", "f4", NCDiff.MINOR),
        ("f8", "f8", NCDiff.NONE),
        ("i4", "i2", NCDiff.MINOR),
        ("f4", "i4", NCDiff.MINOR),
    ],
)
def test_compare_variable_dtypes(
    dtype_old: str, dtype_new: str, expected: NCDiff, tmp_path: Path
) -> None:
    old_file = tmp_path / "file1.nc"
    new_file = tmp_path / "file2.nc"
    with (
        netCDF4.Dataset(old_file, "w", format="NETCDF4_CLASSIC") as nc1,
        netCDF4.Dataset(new_file, "w", format="NETCDF4_CLASSIC") as nc2,
    ):
        nc1.createDimension("time", 3)
        nc2.createDimension("time", 3)
        nc1.createVariable("time", "f8", ("time",))
        nc2.createVariable("time", "f8", ("time",))
        nc1.createVariable("kissa", dtype_old, ("time",))
        nc2.createVariable("kissa", dtype_new, ("time",))
    assert netcdf_comparer.nc_difference(old_file, new_file) == expected


@pytest.mark.parametrize(
    "change, expected",
    [
        (1e-10, NCDiff.NONE),
        (20, NCDiff.MINOR),
        (50, NCDiff.MINOR),
        (10000, NCDiff.MAJOR),
    ],
)
def test_compare_variable_values_added(
    change: float, expected: NCDiff, tmp_path: Path
) -> None:
    old_file = tmp_path / "file1.nc"
    new_file = tmp_path / "file2.nc"
    len_data = 100_000
    data = np.random.rand(len_data)
    data[0] = 0.1
    with (
        netCDF4.Dataset(old_file, "w", format="NETCDF4_CLASSIC") as nc1,
        netCDF4.Dataset(new_file, "w", format="NETCDF4_CLASSIC") as nc2,
    ):
        nc1.createDimension("time", len_data)
        nc2.createDimension("time", len_data)
        nc1.createVariable("time", "f8", ("time",))
        nc2.createVariable("time", "f8", ("time",))
        nc1.createVariable("kissa", "f4", ("time",))
        nc2.createVariable("kissa", "f4", ("time",))
        nc1["time"][:] = data

        data2 = data.copy()
        data2[0] = data2[0] + change
        nc2["time"][:] = data2

    assert netcdf_comparer.nc_difference(old_file, new_file) == expected


@pytest.mark.parametrize(
    "change, expected",
    [
        (0.1, NCDiff.MAJOR),
        (0.999, NCDiff.MINOR),
        (0.999999999999, NCDiff.NONE),
        (1.0, NCDiff.NONE),
        (1.000000000001, NCDiff.NONE),
        (1.002, NCDiff.MINOR),
        (2, NCDiff.MAJOR),
    ],
)
def test_compare_variable_values_multiplied(
    change: float, expected: NCDiff, tmp_path: Path
) -> None:
    old_file = tmp_path / "file1.nc"
    new_file = tmp_path / "file2.nc"
    len_data = 10_000
    data = np.random.rand(len_data)
    with (
        netCDF4.Dataset(old_file, "w", format="NETCDF4_CLASSIC") as nc1,
        netCDF4.Dataset(new_file, "w", format="NETCDF4_CLASSIC") as nc2,
    ):
        nc1.createDimension("time", len_data)
        nc2.createDimension("time", len_data)
        nc1.createVariable("time", "f8", ("time",))
        nc2.createVariable("time", "f8", ("time",))
        nc1.createVariable("kissa", "f4", ("time",))
        nc2.createVariable("kissa", "f4", ("time",))
        nc1["time"][:] = data
        nc2["time"][:] = change * data

    assert netcdf_comparer.nc_difference(old_file, new_file) == expected


def test_missing_units_in_new_variable(tmp_path: Path) -> None:
    old_file = tmp_path / "file1.nc"
    new_file = tmp_path / "file2.nc"
    with (
        netCDF4.Dataset(old_file, "w", format="NETCDF4_CLASSIC") as nc1,
        netCDF4.Dataset(new_file, "w", format="NETCDF4_CLASSIC") as nc2,
    ):
        nc1.createDimension("time", 3)
        nc2.createDimension("time", 3)
        var = nc1.createVariable("time", "f8", ("time",))
        var.units = "s"
        nc2.createVariable("time", "f8", ("time",))
    assert netcdf_comparer.nc_difference(old_file, new_file) == NCDiff.MAJOR


@pytest.mark.parametrize(
    "n_masked, expected",
    [
        (0, NCDiff.NONE),
        (1, NCDiff.MINOR),
        (5, NCDiff.MINOR),
        (10, NCDiff.MINOR),
        (100, NCDiff.MAJOR),
        (500, NCDiff.MAJOR),
        (1000, NCDiff.MAJOR),
    ],
)
def test_compare_masks(n_masked: int, expected: NCDiff, tmp_path: Path) -> None:
    temp1 = tmp_path / "file1.nc"
    temp2 = tmp_path / "file2.nc"
    with (
        netCDF4.Dataset(temp1, "w", format="NETCDF4_CLASSIC") as nc1,
        netCDF4.Dataset(temp2, "w", format="NETCDF4_CLASSIC") as nc2,
    ):
        array1 = np.zeros(100_000)
        array2 = ma.array(array1)
        array2[:n_masked] = ma.masked
        nc1.createDimension("time", len(array1))
        nc2.createDimension("time", len(array2))
        var1 = nc1.createVariable("time", "f8", ("time",))
        var2 = nc2.createVariable("time", "f8", ("time",))
        var1[:] = array1
        var2[:] = array2
    assert netcdf_comparer.nc_difference(temp1, temp2) == expected


@pytest.mark.parametrize(
    "data1, dims1, data2, dims2, expected",
    [
        (np.array([1, 2, 3]), ("time",), np.array([1, 2, 3]), ("time",), NCDiff.NONE),
        (np.array([1, 2, 3]), ("time",), np.array([3, 2, 1]), ("time",), NCDiff.MAJOR),
        (np.array([1, 2, 3]), ("time",), np.array([1, 2, 3]), ("range",), NCDiff.MAJOR),
        (
            np.array([1]),
            (),
            np.array([1, 1, 1]),
            ("time",),
            NCDiff.MINOR,
        ),
        (np.array([1, 1, 1]), ("time",), np.array([1]), (), NCDiff.MINOR),
        (
            np.array([1]),
            (),
            np.array([2, 2, 2]),
            ("time",),
            NCDiff.MAJOR,
        ),
        (
            np.array([2]),
            (),
            np.array([1, 1, 1]),
            ("time",),
            NCDiff.MAJOR,
        ),
        (np.array([1, 1, 1]), ("time",), np.array([2]), (), NCDiff.MAJOR),
        (np.array([2, 2, 2]), ("time",), np.array([1]), (), NCDiff.MAJOR),
        (
            np.array([1]),
            (),
            np.array([[1, 1], [1, 1], [1, 1]]),
            ("time", "range"),
            NCDiff.MINOR,
        ),
    ],
)
def test_different_dimensions(
    data1: npt.NDArray,
    dims1: tuple[str, ...],
    data2: npt.NDArray,
    dims2: tuple[str, ...],
    expected: NCDiff,
    tmp_path: Path,
) -> None:
    fname1 = tmp_path / "old.nc"
    fname2 = tmp_path / "new.nc"
    for fname, data, dims in zip((fname1, fname2), (data1, data2), (dims1, dims2)):
        with netCDF4.Dataset(fname, "w") as nc:
            nc.createDimension("time")
            nc.createDimension("range")
            var = nc.createVariable("data", "f4", dims)
            var[:] = data
    assert netcdf_comparer.nc_difference(fname1, fname2) == expected


def test_compression(tmp_path: Path) -> None:
    old_file = tmp_path / "file1.nc"
    new_file = tmp_path / "file2.nc"
    with (
        netCDF4.Dataset(old_file, "w", format="NETCDF4_CLASSIC") as nc1,
        netCDF4.Dataset(new_file, "w", format="NETCDF4_CLASSIC") as nc2,
    ):
        nc1.createDimension("time", 3)
        nc2.createDimension("time", 3)
        nc1.createVariable("time", "f8", ("time",), zlib=False)
        nc2.createVariable("time", "f8", ("time",), zlib=True)
    assert netcdf_comparer.nc_difference(old_file, new_file) == NCDiff.MINOR
