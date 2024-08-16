import logging
from os import PathLike

import netCDF4
import numpy as np
import numpy.ma as ma


def are_identical_nc_files(
    filename1: PathLike | str, filename2: PathLike | str
) -> bool:
    with netCDF4.Dataset(filename1, "r") as nc1, netCDF4.Dataset(filename2, "r") as nc2:
        try:
            _compare_dimensions(nc1, nc2)
            _compare_global_attributes(nc1, nc2)
            _compare_variables(nc1, nc2, ignore=("beta_smooth",))
            _compare_variable_attributes(nc1, nc2)
        except AssertionError as err:
            logging.debug(err)
            return False
    return True


def _compare_dimensions(nc1: netCDF4.Dataset, nc2: netCDF4.Dataset):
    dims1 = nc1.dimensions.keys()
    dims2 = nc2.dimensions.keys()
    assert (
        len(set(dims1) ^ set(dims2)) == 0
    ), f"different dimensions: {dims1} vs {dims2}"
    for dim in nc1.dimensions:
        value1 = len(nc1.dimensions[dim])
        value2 = len(nc2.dimensions[dim])
        assert value1 == value2, _log("dimensions", dim, value1, value2)


def _skip_compare_global_attribute(name: str) -> bool:
    return name in ("history", "file_uuid", "pid") or name.endswith("_version")


def _compare_global_attributes(nc1: netCDF4.Dataset, nc2: netCDF4.Dataset):
    l1 = [a for a in nc1.ncattrs() if not _skip_compare_global_attribute(a)]
    l2 = [a for a in nc2.ncattrs() if not _skip_compare_global_attribute(a)]
    assert len(set(l1) ^ set(l2)) == 0, f"different global attributes: {l1} vs. {l2}"
    for name in l1:
        value1 = getattr(nc1, name)
        value2 = getattr(nc2, name)
        if name == "source_file_uuids":
            value1 = value1.split(", ")
            value2 = value2.split(", ")
            value1.sort()
            value2.sort()
        assert value1 == value2, _log("global attributes", name, value1, value2)


def _compare_variables(nc1: netCDF4.Dataset, nc2: netCDF4.Dataset, ignore: tuple = ()):
    vars1 = nc1.variables.keys()
    vars2 = nc2.variables.keys()
    assert (
        len(set(vars1) ^ set(vars2)) == 0
    ), f"different variables: {vars1} vs. {vars2}"
    for name in vars1:
        if name in ignore:
            continue
        value1 = nc1.variables[name][:]
        value2 = nc2.variables[name][:]
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
            value1 = getattr(nc1.variables[name], attr)
            value2 = getattr(nc2.variables[name], attr)
            assert value1 == value2, _log(f"variable {attr}", name, value1, value2)


def _compare_variable_attributes(nc1: netCDF4.Dataset, nc2: netCDF4.Dataset):
    for name in nc1.variables:
        attrs1 = set(nc1.variables[name].ncattrs())
        attrs2 = set(nc2.variables[name].ncattrs())
        assert len(attrs1 ^ attrs2) == 0, _log(
            "variable attributes", name, attrs1, attrs2
        )
        for attr in attrs1:
            value1 = getattr(nc1.variables[name], attr)
            value2 = getattr(nc2.variables[name], attr)
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
