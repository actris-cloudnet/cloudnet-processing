import logging
from enum import Enum
from os import PathLike

import netCDF4
import numpy as np
import numpy.ma as ma


class NCDiff(Enum):
    MAJOR = "major"  # new version
    MINOR = "minor"  # patch old file
    NONE = "none"  # do nothing


def nc_difference(old_file: PathLike | str, new_file: PathLike | str) -> NCDiff:
    with netCDF4.Dataset(old_file, "r") as old, netCDF4.Dataset(new_file, "r") as new:
        try:
            _compare_dimensions(old, new)
            _check_old_variables_exist(old, new)
            _check_old_global_attributes_exist(old, new)
            _compare_variables(old, new, ignore=("beta_smooth",))
        except AssertionError as err:
            logging.debug(err)
            return NCDiff.MAJOR
        try:
            _compare_global_attributes(old, new)
            _compare_variable_attributes(old, new)
            _check_for_new_variables(old, new)
            _check_for_new_global_attributes(old, new)
        except AssertionError as err:
            logging.debug(err)
            return NCDiff.MINOR
    return NCDiff.NONE


def _compare_dimensions(old: netCDF4.Dataset, new: netCDF4.Dataset):
    dims1 = old.dimensions.keys()
    dims2 = new.dimensions.keys()
    assert (
        len(set(dims1) ^ set(dims2)) == 0
    ), f"different dimensions: {dims1} vs {dims2}"
    for dim in old.dimensions:
        value1 = len(old.dimensions[dim])
        value2 = len(new.dimensions[dim])
        assert value1 == value2, _log("dimensions", dim, value1, value2)


def _compare_global_attributes(old: netCDF4.Dataset, new: netCDF4.Dataset):
    old_attributes = [a for a in old.ncattrs() if not _skip_compare_global_attribute(a)]
    for name in old_attributes:
        value1 = getattr(old, name)
        value2 = getattr(new, name)
        if name == "source_file_uuids":
            value1 = value1.split(", ")
            value2 = value2.split(", ")
            value1.sort()
            value2.sort()
        assert value1 == value2, _log("global attributes", name, value1, value2)


def _skip_compare_global_attribute(name: str) -> bool:
    return name in ("history", "file_uuid", "pid") or name.endswith("_version")


def _check_old_global_attributes_exist(old: netCDF4.Dataset, new: netCDF4.Dataset):
    for attr in old.ncattrs():
        assert attr in new.ncattrs(), f"missing global attribute: {attr}"


def _check_for_new_global_attributes(old: netCDF4.Dataset, new: netCDF4.Dataset):
    for attr in new.ncattrs():
        assert attr in old.ncattrs(), f"new global attribute: {attr}"


def _check_old_variables_exist(old: netCDF4.Dataset, new: netCDF4.Dataset):
    for var in old.variables:
        assert var in new.variables, f"missing variable: {var}"


def _check_for_new_variables(old: netCDF4.Dataset, new: netCDF4.Dataset):
    for var in new.variables:
        assert var in old.variables, f"new variable: {var}"


def _compare_variables(old: netCDF4.Dataset, new: netCDF4.Dataset, ignore: tuple = ()):
    for name in old.variables:
        if name in ignore:
            continue
        value1 = old.variables[name][:]
        value2 = new.variables[name][:]
        # np.allclose does not seem to work if all values are masked
        if (
            isinstance(value1, ma.MaskedArray)
            and isinstance(value2, ma.MaskedArray)
            and value1.mask.all()
            and value2.mask.all()
        ):
            return
        assert value1.shape == value2.shape, _log(
            "shapes", name, value1.shape, value2.shape
        )
        assert np.allclose(value1, value2, rtol=1e-4, equal_nan=True), _log(
            "variable values", name, value1, value2
        )
        if isinstance(value1, ma.MaskedArray) and isinstance(value2, ma.MaskedArray):
            assert np.array_equal(
                value1.mask,
                value2.mask,
            ), _log("variable masks", name, value1.mask, value2.mask)
        for attr in ("dtype", "dimensions"):
            value1 = getattr(old.variables[name], attr)
            value2 = getattr(new.variables[name], attr)
            assert value1 == value2, _log(f"variable {attr}", name, value1, value2)


def _compare_variable_attributes(old: netCDF4.Dataset, new: netCDF4.Dataset):
    for name in old.variables:
        attrs1 = set(old.variables[name].ncattrs())
        attrs2 = set(new.variables[name].ncattrs())
        assert len(attrs1 ^ attrs2) == 0, _log(
            "variable attributes", name, attrs1, attrs2
        )
        for attr in attrs1:
            value1 = getattr(old.variables[name], attr)
            value2 = getattr(new.variables[name], attr)
            assert type(value1) == type(value2), _log(
                "variable attribute types",
                f"{name} - {attr}",
                type(value1),
                type(value2),
            )
            # Allow the value of fill value to change.
            if attr == "_FillValue":
                continue
            if isinstance(value1, np.ndarray):
                assert np.array_equal(
                    value1,
                    value2,
                ), _log("variable attribute values", f"{name} - {attr}", value1, value2)
            else:
                assert value1 == value2, _log(
                    "variable attribute values", f"{name} - {attr}", value1, value2
                )


def _log(text: str, var_name: str, value1, value2) -> str:
    return f"{text} differ in {var_name}: {value1} vs. {value2}"
