import re

import cftime
import netCDF4
import numpy as np
import numpy.typing as npt


def decode_bits(data: np.ndarray, format: list[tuple[str, int]]) -> dict[str, np.ndarray]:
    """
    Decode array of bit fields into decimals starting from the least-significant
    bit.

    Args:
        data: Array of bit fields.
        format: tuple with name and bit size for each field. Names prefixed with
                underscore will be skipped.

    Returns:
        dictionary from name to decoded values.
    """
    bits = data.copy()
    output = {}
    for name, size in format:
        if not name.startswith("_"):
            output[name] = bits & (2**size - 1)
        bits >>= size
    return output


def cftime2datetime64(time: netCDF4.Variable) -> npt.NDArray:
    units = _fix_invalid_cftime_units(time.units)
    return cftime.num2pydate(time[:], units=units).astype("datetime64")


def _fix_invalid_cftime_units(unit: str) -> str:
    match_ = re.match(
        r"^(\w+) since (\d{1,2})\.(\d{1,2})\.(\d{4}), (\d{1,2}):(\d{1,2}):(\d{1,2})$", unit
    )
    if match_:
        _unit = match_.group(1)
        day = match_.group(2).zfill(2)
        month = match_.group(3).zfill(2)
        year = match_.group(4)
        hour = match_.group(5).zfill(2)
        minute = match_.group(6).zfill(2)
        sec = match_.group(7).zfill(2)
        new_unit = f"{_unit} since {year}-{month}-{day} {hour}:{minute}:{sec}"
        return new_unit
    return unit
