import logging
from enum import Enum
from pathlib import Path
from typing import Tuple, cast

import netCDF4
import numpy as np
import numpy.ma as ma
import numpy.typing as npt


class NCDiff(Enum):
    MAJOR = "major"  # new version
    MINOR = "minor"  # patch old file
    NONE = "none"  # do nothing


class NetCDFComparator:
    def __init__(
        self,
        old_file: Path,
        new_file: Path,
        ignore_vars: Tuple[str, ...] = (
            "beta_smooth",
            "ze_sat_noise",
            "vm_sat_noise",
            "vm_sat_folded",
            "folding_flag",
            "nyquist_velocity",
        ),
    ) -> None:
        self.old_file = old_file
        self.new_file = new_file
        self.ignore_vars = ignore_vars

    def compare(self) -> NCDiff:
        with (
            netCDF4.Dataset(self.old_file, "r") as old,
            netCDF4.Dataset(self.new_file, "r") as new,
        ):
            self.old = old
            self.new = new

            major_checks = [
                self._check_old_variables_exist,
                self._check_old_global_attributes_exist,
                self._compare_variable_units,
            ]

            for check in major_checks:
                if not check():
                    return NCDiff.MAJOR

            var_diff = self._compare_variable_data()
            if var_diff != NCDiff.NONE:
                return var_diff

            minor_checks = [
                self._compare_global_attributes,
                self._compare_variable_attributes,
                self._compare_variable_dtypes,
                self._compare_variable_filters,
                self._check_for_new_variables,
                self._check_for_new_global_attributes,
            ]

            for check in minor_checks:
                if not check():
                    return NCDiff.MINOR

        return NCDiff.NONE

    def _compare_global_attributes(self) -> bool:
        old_attrs = {
            a: getattr(self.old, a)
            for a in self.old.ncattrs()
            if not self._skip_compare_global_attribute(a)
        }
        new_attrs = {
            a: getattr(self.new, a)
            for a in self.new.ncattrs()
            if not self._skip_compare_global_attribute(a)
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
            val_equal = (
                np.array_equal(val_old, val_new)
                if isinstance(val_old, np.ndarray) or isinstance(val_new, np.ndarray)
                else val_old == val_new
            )
            if not val_equal:
                logging.info(
                    f"Global attribute '{attr}' differs: {val_old} vs {val_new}"
                )
                return False
        return True

    @staticmethod
    def _skip_compare_global_attribute(name: str) -> bool:
        return name in ("history", "file_uuid", "pid") or name.endswith("_version")

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

    def _compare_variable_data(self) -> NCDiff:
        for var in self.old.variables:
            if var in self.ignore_vars:
                continue

            old_var = self.old.variables[var]
            new_var = self.new.variables[var]

            if (
                old_var.dimensions == new_var.dimensions
                and old_var.shape != new_var.shape
            ):
                logging.info(
                    f"Variable '{var}' shapes differ: {old_var.shape} vs {new_var.shape}"
                )
                return NCDiff.MAJOR

            smaller_var, larger_var = sorted(
                [old_var, new_var], key=lambda variable: len(variable.shape)
            )

            # For now, only broadcasting scalar to array (of any shape) is
            # supported. More complex cases (e.g. 1d to 2d array) could be
            # handled in the future.
            if (
                len(smaller_var.dimensions) != 0
                and smaller_var.dimensions != larger_var.dimensions
            ):
                logging.info(
                    f"Variable '{var}' has incompatible dimensions: {old_var.dimensions} vs {new_var.dimensions}"
                )
                return NCDiff.MAJOR

            try:
                orig_data = ma.getdata(smaller_var[:])
                orig_mask = ma.getmaskarray(smaller_var[:])
                broad_data = np.broadcast_to(orig_data, larger_var.shape)
                broad_mask = np.broadcast_to(orig_mask, larger_var.shape)
                data_smaller = ma.array(broad_data, mask=broad_mask)
                data_larger = larger_var[:]
            except ValueError:
                logging.info(
                    f"Cannot broadcast variable '{var}' from {smaller_var.shape} to {larger_var.shape}"
                )
                return NCDiff.MAJOR

            mask_diff = self._compare_variable_masks(var, data_smaller, data_larger)
            if mask_diff != NCDiff.NONE:
                return mask_diff

            var_diff = self._compare_variable_values(var, data_smaller, data_larger)
            if var_diff != NCDiff.NONE:
                return var_diff

            # If dimensions don't match and there are no major differences,
            # consider this always as a minor difference.
            if smaller_var.dimensions != larger_var.dimensions:
                return NCDiff.MINOR

        return NCDiff.NONE

    def _compare_variable_values(
        self, var: str, val_old: npt.NDArray, val_new: npt.NDArray
    ) -> NCDiff:
        val_old = ma.masked_invalid(val_old)
        val_new = ma.masked_invalid(val_new)

        epsilon = 1e-12
        val_old_nonzero = ma.where(ma.abs(val_old) < epsilon, epsilon, val_old)
        percentage_error = ma.abs(val_new - val_old) / ma.abs(val_old_nonzero) * 100.0
        percentage_error = ma.masked_invalid(percentage_error)
        mape = ma.mean(percentage_error)

        major_threshold = 5
        minor_threshold = 0.1

        if mape >= major_threshold:
            logging.info(f"Variable '{var}' has major differences (MAPE={mape:g})")
            return NCDiff.MAJOR
        elif mape >= minor_threshold:
            logging.info(f"Variable '{var}' has minor differences (MAPE={mape:g})")
            return NCDiff.MINOR

        return NCDiff.NONE

    def _compare_variable_masks(
        self, var: str, val_old: npt.NDArray, val_new: npt.NDArray
    ) -> NCDiff:
        mask_old = ma.getmaskarray(val_old)
        mask_new = ma.getmaskarray(val_new)
        same_percentage = 100 * np.count_nonzero(mask_old == mask_new) / mask_old.size
        if same_percentage <= 99.9:
            logging.info(
                f"Variable '{var}' masks have major differences (matches {same_percentage:.2f} %)"
            )
            return NCDiff.MAJOR
        if same_percentage <= 99.9999:
            logging.info(
                f"Variable '{var}' masks have minor differences (matches {same_percentage:.2f} %)"
            )
            return NCDiff.MINOR
        return NCDiff.NONE

    def _compare_variable_units(self) -> bool:
        for var in self.old.variables:
            if var in self.ignore_vars:
                continue
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

    def _compare_variable_filters(self) -> bool:
        for var in self.old.variables:
            filters_old = cast(dict, self.old.variables[var].filters())
            filters_new = cast(dict, self.new.variables[var].filters())
            if filters_old != filters_new:
                diff_old = {
                    key: filters_old[key]
                    for key in filters_old
                    if key not in filters_new or filters_old[key] != filters_new[key]
                }
                diff_new = {
                    key: filters_new[key]
                    for key in filters_new
                    if key not in filters_old or filters_new[key] != filters_old[key]
                }
                logging.info(
                    f"Variable '{var}' filters differ: {diff_old} vs {diff_new}"
                )
                return False
        return True


def nc_difference(old_file: Path, new_file: Path) -> NCDiff:
    comparator = NetCDFComparator(old_file, new_file)
    return comparator.compare()
