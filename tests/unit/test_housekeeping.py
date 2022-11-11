import pathlib

import pytest

from housekeeping import get_config
from housekeeping.housekeeping import _nc2df, _rpg2df


@pytest.mark.parametrize("instrument_id", ["chm15k", "rpg-fmcw-94", "hatpro"])
def test_something(instrument_id):
    src_dir = pathlib.Path(f"./tests/data/raw/{instrument_id}")
    if instrument_id == "rpg-fmcw-94":
        cfg = get_config(cfg_id=instrument_id)
        fun2df = _rpg2df
    elif instrument_id == "hatpro":
        cfg = {"vars": ["air_temperature", "azimuth_angle", "ele"]}
        fun2df = _nc2df
    else:
        cfg = get_config(cfg_id=instrument_id)
        fun2df = _nc2df
    assert "vars" in cfg

    for p in src_dir.iterdir():
        df = fun2df(p, cfg)
        assert not df.empty
