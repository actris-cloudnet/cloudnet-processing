import pathlib

import numpy as np
import pytest

from housekeeping.housekeeping import get_reader
from housekeeping.utils import decode_bits


@pytest.mark.parametrize("instrument_id", ["chm15k", "rpg-fmcw-94"])
def test_something(instrument_id):
    src_dir = pathlib.Path(f"./tests/data/raw/{instrument_id}")
    for path in src_dir.iterdir():
        reader = get_reader(
            {
                "instrumentId": instrument_id,
                "filename": path.name,
            }
        )
        df = reader(path.read_bytes())
        assert not df.empty


def test_decode_bits():
    values = decode_bits(np.array([0b101010]), [("A", 4), ("_B", 1), ("C", 1)])
    assert values == {
        "A": np.array([0b1010]),
        "C": np.array([0b1]),
    }
