from io import SEEK_END
from os import PathLike
from typing import BinaryIO, List, Tuple, Union

import numpy as np


def _read_from_file(file: BinaryIO, fields: List[Tuple[str, str]], count: int = 1) -> dict:
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

    def __init__(self, filename: Union[str, bytes, PathLike]):
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
        if self.header["HKDTimeRef"] != 1:
            raise HatproHkdError("Only UTC time reference is supported")

    def _read_data(self, file: BinaryIO):
        fields = [("T", "<i4"), ("Alarm", "c")]
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
        self.data["T"] = np.datetime64("2001-01-01") + self.data["T"].astype("timedelta64[s]")
