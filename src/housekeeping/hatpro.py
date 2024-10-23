from io import SEEK_END
from os import PathLike
from typing import BinaryIO

import netCDF4
import numpy as np
import numpy.typing as npt

from .utils import cftime2datetime64, decode_bits

TIME_REF_LOCAL = 0
TIME_REF_UTC = 1


def _read_from_file(
    file: BinaryIO, fields: list[tuple[str, str]], count: int = 1
) -> dict:
    arr = np.fromfile(file, np.dtype(fields), count)
    if (read := len(arr)) != count:
        raise IOError(f"Read {read} of {count} records from file")
    if count == 1:
        arr = arr[0]
    return {field: arr[field] for field, _type in fields}


def _check_eof(file: BinaryIO):
    current_offset = file.tell()
    file.seek(0, SEEK_END)
    end_offset = file.tell()
    if current_offset != end_offset:
        raise IOError(f"{end_offset - current_offset} unread bytes")


class HatproHkdError(Exception):
    pass


class HatproHkd:
    """HATPRO *.HKD (Housekeeping Data) binary file reader. Byte order is
    assumed to be little endian.

    References:
        Radiometer Physics (2014): Instrument Operation and Software Guide
        Operation Principles and Software Description for RPG standard single
        polarization radiometers (G5 series).
        https://www.radiometer-physics.de/download/PDF/Radiometers/HATPRO/RPG_MWR_STD_Software_Manual%20G5.pdf
    """

    header: dict
    data: dict

    def __init__(self, filename: str | bytes | PathLike):
        with open(filename, "rb") as file:
            self._read_header(file)
            self._read_data(file)
            _check_eof(file)

    def _read_header(self, file: BinaryIO):
        self.header = _read_from_file(
            file,
            [
                ("HKDCode", "<i4"),
                ("N", "<i4"),
                ("HKDTimeRef", "<i4"),
                ("HKDSelect", "<i4"),
            ],
        )
        if self.header["HKDCode"] != 837854832:
            raise HatproHkdError(f'Unknown file signature: {self.header["HKDCode"]}')
        if self.header["HKDTimeRef"] != TIME_REF_UTC:
            raise HatproHkdError("Only UTC time reference is supported")

    def _read_data(self, file: BinaryIO):
        fields = [("T", "<i4"), ("Alarm", "b")]
        if self.header["HKDSelect"] & 0x1:
            # According to the file format description, coordinates are stored
            # as degrees and minutes packed in decimal numbers. In practice,
            # however, the coordinates seem to be stored simply as decimal
            # degrees.
            fields.append(("Longitude", "<f"))
            fields.append(("Latitude", "<f"))
        if self.header["HKDSelect"] & 0x2:
            fields.append(("T0", "<f"))
            fields.append(("T1", "<f"))
            fields.append(("T2", "<f"))
            fields.append(("T3", "<f"))
        if self.header["HKDSelect"] & 0x4:
            fields.append(("Stab0", "<f"))
            fields.append(("Stab1", "<f"))
        if self.header["HKDSelect"] & 0x8:
            fields.append(("Flash", "<i4"))
        if self.header["HKDSelect"] & 0x10:
            fields.append(("Quality", "<i4"))
        if self.header["HKDSelect"] & 0x20:
            fields.append(("Status", "<i4"))
        self.data = _read_from_file(file, fields, self.header["N"])
        self.data["T"] = np.datetime64("2001-01-01") + self.data["T"].astype(
            "timedelta64[s]"
        )
        self.data |= decode_quality_flags(self.data["Quality"])
        self.data |= decode_status_flags(self.data["Status"])


class HatproHkdNc:
    def __init__(self, filename: str | PathLike):
        with netCDF4.Dataset(filename) as nc:
            time_ref = nc.variables.get("time_reference")
            if time_ref and time_ref[0] != TIME_REF_UTC:
                raise HatproHkdError("Only UTC time reference is supported")
            self.data = {var: nc[var][:] for var in nc.variables.keys()}
            self.data["time"] = cftime2datetime64(nc.variables["time"])
            if quality_flags := nc.variables.get("quality_flags"):
                self.data |= decode_quality_flags(quality_flags[:])
            if status_flags := nc.variables.get("status_flags"):
                self.data |= decode_status_flags(status_flags[:])


def decode_quality_flags(data: npt.NDArray) -> dict[str, npt.NDArray]:
    return decode_bits(
        data,
        [
            ("lwp_quality_level", 2),
            ("lwp_quality_reason", 2),
            ("iwv_quality_level", 2),
            ("iwv_quality_reason", 2),
            ("dly_quality_level", 2),
            ("dly_quality_reason", 2),
            ("hpc_quality_level", 2),
            ("hpc_quality_reason", 2),
            ("tpc_quality_level", 2),
            ("tpc_quality_reason", 2),
            ("tpb_quality_level", 2),
            ("tpb_quality_reason", 2),
            ("sta_quality_level", 2),
            ("sta_quality_reason", 2),
            ("lpr_quality_level", 2),
            ("lpr_quality_reason", 2),
        ],
    )


def decode_status_flags(data: npt.NDArray) -> dict[str, npt.NDArray]:
    return decode_bits(
        data,
        [
            ("humidity_profiler_channel1_status", 1),
            ("humidity_profiler_channel2_status", 1),
            ("humidity_profiler_channel3_status", 1),
            ("humidity_profiler_channel4_status", 1),
            ("humidity_profiler_channel5_status", 1),
            ("humidity_profiler_channel6_status", 1),
            ("humidity_profiler_channel7_status", 1),
            ("_unused1", 1),
            ("temperature_profiler_channel1_status", 1),
            ("temperature_profiler_channel2_status", 1),
            ("temperature_profiler_channel3_status", 1),
            ("temperature_profiler_channel4_status", 1),
            ("temperature_profiler_channel5_status", 1),
            ("temperature_profiler_channel6_status", 1),
            ("temperature_profiler_channel7_status", 1),
            ("_unused2", 1),
            ("rain_status", 1),
            ("dew_blower_speed_status", 1),
            ("boundary_layer_mode_status", 1),
            ("sky_tipping_calibration_status", 1),
            ("gain_calibration_status", 1),
            ("noise_calibration_status", 1),
            ("humidity_profiler_noise_diode_status", 1),
            ("temperature_profiler_noise_diode_status", 1),
            ("humidity_profiler_stability_status", 2),
            ("temperature_profiler_stability_status", 2),
            ("power_failure_status", 1),
            ("ambient_target_stability_status", 1),
            ("noise_diode_status", 1),
        ],
    )
