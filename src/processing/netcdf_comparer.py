import logging
from enum import Enum
from os import PathLike
from typing import Tuple

import netCDF4
import numpy as np
import numpy.ma as ma


class NCDiff(Enum):
    MAJOR = "major"  # new version
    MINOR = "minor"  # patch old file
    NONE = "none"  # do nothing


class NetCDFComparator:
    def __init__(
        self,
        old_file: PathLike | str,
        new_file: PathLike | str,
        ignore_vars: Tuple[str] = ("beta_smooth",),
    ):
        self.old_file = old_file
        self.new_file = new_file
        self.ignore_vars = ignore_vars

    def compare(self) -> NCDiff:
        with netCDF4.Dataset(self.old_file, "r") as old, netCDF4.Dataset(
            self.new_file, "r"
        ) as new:
            self.old = old
            self.new = new

            major_checks = [
                self._compare_dimensions,
                self._check_old_variables_exist,
                self._check_old_global_attributes_exist,
                self._compare_variable_shapes,
                self._compare_critical_variable_attributes,
            ]

            for check in major_checks:
                if not check():
                    return NCDiff.MAJOR

            mask_diff = self._compare_variable_masks()
            if mask_diff in (NCDiff.MAJOR, NCDiff.MINOR):
                return mask_diff

            var_diff = self._compare_variable_values()
            if var_diff in (NCDiff.MAJOR, NCDiff.MINOR):
                return var_diff

            minor_checks = [
                self._compare_global_attributes,
                self._compare_variable_attributes,
                self._compare_variable_dtypes,
                self._check_for_new_variables,
                self._check_for_new_global_attributes,
            ]

            for check in minor_checks:
                if not check():
                    return NCDiff.MINOR

        return NCDiff.NONE

    def _compare_dimensions(self) -> bool:
        dims_old = set(self.old.dimensions.keys())
        dims_new = set(self.new.dimensions.keys())
        if dims_old != dims_new:
            logging.info(f"Different dimensions: {dims_old} vs {dims_new}")
            return False

        for dim in dims_old:
            len_old = len(self.old.dimensions[dim])
            len_new = len(self.new.dimensions[dim])
            if len_old != len_new:
                logging.info(
                    f"Dimension '{dim}' lengths differ: {len_old} vs {len_new}"
                )
                return False
        return True

    def _compare_global_attributes(self) -> bool:
        skip_attrs = ("history", "file_uuid", "pid")
        old_attrs = {
            a: getattr(self.old, a) for a in self.old.ncattrs() if a not in skip_attrs
        }
        new_attrs = {
            a: getattr(self.new, a) for a in self.new.ncattrs() if a not in skip_attrs
        }

        if old_attrs.keys() != new_attrs.keys():
            logging.info(
                f"Global attributes differ: {old_attrs.keys()} vs {new_attrs.keys()}"
            )
            return False

        for attr in old_attrs:
            val_old = old_attrs[attr]
            val_new = new_attrs[attr]
            if attr == "source_file_uuids":
                val_old = sorted(val_old.split(", "))
                val_new = sorted(val_new.split(", "))
            if val_old != val_new:
                logging.info(
                    f"Global attribute '{attr}' differs: {val_old} vs {val_new}"
                )
                return False
        return True

    def _check_old_global_attributes_exist(self) -> bool:
        old_attrs = set(self.old.ncattrs())
        new_attrs = set(self.new.ncattrs())
        missing_attrs = old_attrs - new_attrs
        if missing_attrs:
            logging.info(f"Missing global attributes in new file: {missing_attrs}")
            return False
        return True

    def _check_for_new_global_attributes(self) -> bool:
        old_attrs = set(self.old.ncattrs())
        new_attrs = set(self.new.ncattrs())
        new_attrs_found = new_attrs - old_attrs
        if new_attrs_found:
            logging.info(f"New global attributes in new file: {new_attrs_found}")
            return False
        return True

    def _check_old_variables_exist(self) -> bool:
        old_vars = set(self.old.variables.keys())
        new_vars = set(self.new.variables.keys())
        missing_vars = old_vars - new_vars
        if missing_vars:
            logging.info(f"Missing variables in new file: {missing_vars}")
            return False
        return True

    def _check_for_new_variables(self) -> bool:
        old_vars = set(self.old.variables.keys())
        new_vars = set(self.new.variables.keys())
        new_vars_found = new_vars - old_vars
        if new_vars_found:
            logging.info(f"New variables in new file: {new_vars_found}")
            return False
        return True

    def _compare_variable_shapes(self) -> bool:
        for var in self.old.variables:
            shape_old = self.old.variables[var].shape
            shape_new = self.new.variables[var].shape
            if shape_old != shape_new:
                logging.info(
                    f"Variable '{var}' shapes differ: {shape_old} vs {shape_new}"
                )
                return False
        return True

    def _compare_variable_values(self) -> NCDiff:
        for var in self.old.variables:
            if var in self.ignore_vars:
                continue
            val_old = ma.masked_invalid(self.old.variables[var][:])
            val_new = ma.masked_invalid(self.new.variables[var][:])
            mae = np.mean(np.abs(val_old - val_new))
            if mae >= 0.1:
                logging.info(f"Variable '{var}' has major differences (MAE={mae})")
                return NCDiff.MAJOR
            elif mae >= 1e-12:
                logging.info(f"Variable '{var}' has minor differences (MAE={mae})")
                return NCDiff.MINOR
        return NCDiff.NONE

    def _compare_variable_masks(self) -> NCDiff:
        for var in self.old.variables:
            if var in self.ignore_vars:
                continue
            val_old = self.old.variables[var][:]
            val_new = self.new.variables[var][:]
            mask_old = ma.getmaskarray(val_old)
            mask_new = ma.getmaskarray(val_new)
            same_percentage = (
                100 * np.count_nonzero(mask_old == mask_new) / mask_old.size
            )
            if same_percentage <= 95:
                logging.info(
                    f"Variable '{var}' masks have major differences ({same_percentage}%)"
                )
                return NCDiff.MAJOR
            if same_percentage <= 99:
                logging.info(
                    f"Variable '{var}' masks have minor differences ({same_percentage}%)"
                )
                return NCDiff.MINOR
        return NCDiff.NONE

    def _compare_critical_variable_attributes(self) -> bool:
        for var in self.old.variables:
            if var in self.ignore_vars:
                continue
            # Compare dimensions
            dims_old = self.old.variables[var].dimensions
            dims_new = self.new.variables[var].dimensions
            if dims_old != dims_new:
                logging.info(
                    f"Variable '{var}' dimensions differ: {dims_old} vs {dims_new}"
                )
                return False
            # Compare units if present
            units_old = getattr(self.old.variables[var], "units", None)
            units_new = getattr(self.new.variables[var], "units", None)
            if units_old != units_new:
                logging.info(
                    f"Variable '{var}' units differ: {units_old} vs {units_new}"
                )
                return False
        return True

    def _compare_variable_dtypes(self) -> bool:
        for var in self.old.variables:
            dtype_old = self.old.variables[var].dtype
            dtype_new = self.new.variables[var].dtype
            if dtype_old != dtype_new:
                logging.info(
                    f"Variable '{var}' data types differ: {dtype_old} vs {dtype_new}"
                )
                return False
        return True

    def _compare_variable_attributes(self) -> bool:
        for var in self.old.variables:
            attrs_old = set(self.old.variables[var].ncattrs())
            attrs_new = set(self.new.variables[var].ncattrs())
            if attrs_old != attrs_new:
                logging.info(
                    f"Variable '{var}' attributes differ: {attrs_old} vs {attrs_new}"
                )
                return False
            for attr in attrs_old:
                val_old = getattr(self.old.variables[var], attr)
                val_new = getattr(self.new.variables[var], attr)
                if attr == "_FillValue":
                    continue
                if isinstance(val_old, np.ndarray):
                    if not np.array_equal(val_old, val_new):
                        logging.info(
                            f"Variable '{var}' attribute '{attr}' values differ"
                        )
                        return False
                else:
                    if val_old != val_new:
                        logging.info(
                            f"Variable '{var}' attribute '{attr}' values differ: {val_old} vs {val_new}"
                        )
                        return False
        return True


def nc_difference(old_file: PathLike | str, new_file: PathLike | str) -> NCDiff:
    comparator = NetCDFComparator(old_file, new_file)
    return comparator.compare()
