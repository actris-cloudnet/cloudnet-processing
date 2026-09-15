import datetime

import pytest
from cloudnet_api_client.containers import Site

from processing.dvas import parse_compliance


def _site(labelling_status: str | None) -> Site:
    return Site(
        id="hyytiala",
        human_readable_name="Hyytiälä",
        station_name=None,
        latitude=61.8,
        longitude=24.3,
        altitude=174,
        dvas_id="1",
        actris_id=1,
        country="Finland",
        country_code="FI",
        country_subdivision_code=None,
        type=frozenset({"cloudnet"}),
        gaw=None,
        labelling_status=labelling_status,  # type: ignore[arg-type]
    )


@pytest.mark.parametrize(
    "status, expected",
    [
        ("labelled", "ACTRIS labelled"),
        ("initially-accepted", "ACTRIS compliant"),
        ("planned", "ACTRIS associated"),
        (None, "ACTRIS associated"),
    ],
)
def test_parse_compliance(status: str | None, expected: str) -> None:
    assert parse_compliance(_site(status), datetime.date(2024, 1, 1)) == expected


@pytest.mark.parametrize("status", ["labelled", "initially-accepted", None])
def test_parse_compliance_legacy(status: str | None) -> None:
    assert (
        parse_compliance(_site(status), datetime.date(2023, 4, 24)) == "ACTRIS legacy"
    )
