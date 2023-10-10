from pathlib import Path

import numpy as np
import pytest

from housekeeping.housekeeping import get_reader
from housekeeping.utils import decode_bits


@pytest.mark.parametrize(
    "filename,instrument_id,measurement_date",
    [
        (
            "tests/data/raw/chm15k/00100_A202010212350_CHM170137.nc",
            "chm15k",
            "2020-10-21",
        ),
        (
            "tests/data/raw/chm15k/00100_A202010220835_CHM170137.nc",
            "chm15k",
            "2020-10-22",
        ),
        (
            "tests/data/raw/chm15k/00100_A202010221205_CHM170137.nc",
            "chm15k",
            "2020-10-22",
        ),
        (
            "tests/data/raw/chm15k/00100_A202010221900_CHM170137.nc",
            "chm15k",
            "2020-10-22",
        ),
        (
            "tests/data/raw/rpg-fmcw-94/201022_070003_P06_ZEN.LV1",
            "rpg-fmcw-94",
            "2020-10-22",
        ),
        (
            "tests/data/raw/rpg-fmcw-94/201022_100001_P06_ZEN.LV1",
            "rpg-fmcw-94",
            "2020-10-22",
        ),
        (
            "tests/data/raw/rpg-fmcw-94/201023_160000_P06_ZEN.LV1",
            "rpg-fmcw-94",
            "2020-10-23",
        ),
    ],
)
def test_something(filename, instrument_id, measurement_date):
    path = Path(filename)
    metadata = {
        "filename": path.name,
        "instrumentId": instrument_id,
        "instrumentPid": None,
        "measurementDate": measurement_date,
        "siteId": "hyytiala",
    }
    reader = get_reader(metadata)
    assert reader is not None
    points = reader(path.read_bytes(), metadata)
    assert len(points) > 0


def test_decode_bits():
    values = decode_bits(np.array([0b101010]), [("A", 4), ("_B", 1), ("C", 1)])
    assert values == {
        "A": np.array([0b1010]),
        "C": np.array([0b1]),
    }
