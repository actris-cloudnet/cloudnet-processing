import datetime
import uuid
from pathlib import Path

import numpy as np
import pytest
from cloudnet_api_client.containers import Instrument, RawMetadata, Site
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
def test_something(filename: str, instrument_id: str, measurement_date: str):
    path = Path(filename)
    instrument = Instrument(
        uuid=uuid.uuid4(),
        pid="some_pid",
        instrument_id=instrument_id,
        model="model",
        type="type",
        owners=("owner1", "owner2"),
        name="name",
        serial_number=None,
    )
    site = Site(
        id="hyytiala",
        human_readable_name="Hyytiala",
        latitude=61.847,
        longitude=24.288,
        altitude=100,
        country="Finland",
        country_code="FI",
        type=frozenset(("cloudnet",)),
        station_name="Hyytiala",
        dvas_id=None,
        country_subdivision_code=None,
        gaw=None,
        actris_id=None,
    )

    metadata = RawMetadata(
        filename=path.name,
        instrument=instrument,
        measurement_date=datetime.date.fromisoformat(measurement_date),
        site=site,
        tags=frozenset(),
        created_at=datetime.datetime.now(),
        updated_at=datetime.datetime.now(),
        download_url="<download_url>",
        status="uploaded",
        checksum="<checksum>",
        uuid=uuid.uuid4(),
        size=233,
    )
    reader = get_reader(metadata)
    assert reader is not None
    points = reader(path, metadata)
    assert len(points) > 0


def test_decode_bits():
    values = decode_bits(np.array([0b101010]), [("A", 4), ("_B", 1), ("C", 1)])
    assert values == {
        "A": np.array([0b1010]),
        "C": np.array([0b1]),
    }
