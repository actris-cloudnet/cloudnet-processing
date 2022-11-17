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


def _decode_bits(data, format: List[Tuple[str, int]]) -> dict:
    bits = data.copy()
    output = {}
    for name, size in format:
        if not name.startswith("_"):
            output[name] = bits & (2**size - 1)
        bits >>= size
    return output


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
        self.data["T"] = np.datetime64("2001-01-01") + self.data["T"].astype("timedelta64[s]")
        self.data.update(
            _decode_bits(
                self.data["Quality"],
                [
                    ("QFLWP1", 2),
                    ("QFLWP2", 2),
                    ("QFIWV1", 2),
                    ("QFIWV2", 2),
                    ("QFDLY1", 2),
                    ("QFDLY2", 2),
                    ("QFHPC1", 2),
                    ("QFHPC2", 2),
                    ("QFTPC1", 2),
                    ("QFTPC2", 2),
                    ("QFTPB1", 2),
                    ("QFTPB2", 2),
                    ("QFSTA1", 2),
                    ("QFSTA2", 2),
                    ("QFLPR1", 2),
                    ("QFLPR2", 2),
                ],
            )
        )
        self.data.update(
            _decode_bits(
                self.data["Status"],
                [
                    ("HPCh1", 1),
                    ("HPCh2", 1),
                    ("HPCh3", 1),
                    ("HPCh4", 1),
                    ("HPCh5", 1),
                    ("HPCh6", 1),
                    ("HPCh7", 1),
                    ("_unused1", 1),
                    ("TPCh1", 1),
                    ("TPCh2", 1),
                    ("TPCh3", 1),
                    ("TPCh4", 1),
                    ("TPCh5", 1),
                    ("TPCh6", 1),
                    ("TPCh7", 1),
                    ("_unused2", 1),
                    ("RF", 1),
                    ("DB", 1),
                    ("BLM", 1),
                    ("SCa", 1),
                    ("GCa", 1),
                    ("NCa", 1),
                    ("ND1", 1),
                    ("ND2", 1),
                    ("R1St", 2),
                    ("R2St", 2),
                    ("PF", 1),
                    ("TarSt", 1),
                    ("NDSt", 1),
                ],
            )
        )
