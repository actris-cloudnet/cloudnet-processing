import datetime
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import netCDF4
import pytest
from cloudnet_api_client.containers import Site

from processing import product
from processing.processor import ProductParams
from processing.utils import MiscError, SkipTaskError

SITE = Site(
    id="arm-sgp",
    human_readable_name="ARM SGP",
    station_name=None,
    latitude=36.6,
    longitude=-97.5,
    altitude=320,
    dvas_id=None,
    actris_id=None,
    country="USA",
    country_code="US",
    country_subdivision_code=None,
    type=frozenset({"arm"}),
    gaw=None,
)


def _params() -> ProductParams:
    prod = MagicMock()
    prod.id = "categorize"
    return ProductParams(
        site=SITE, date=datetime.date(2022, 6, 1), product=prod, instrument=None
    )


MODEL_UUID = uuid.UUID("569198e3-3805-4b0b-b3a4-4b41a353e3f1")


def _processor(tmp_path: Path) -> MagicMock:
    processor = MagicMock()
    model_meta = MagicMock()
    model_meta.uuid = MODEL_UUID
    processor.get_product.return_value = model_meta
    processor.storage_api.download_product.return_value = tmp_path / "model.nc"
    return processor


@pytest.fixture(autouse=True)
def _arm_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ARM_USER", "user")
    monkeypatch.setenv("ARM_TOKEN", "token")


def test_arm_input_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    raw = {"radar": [Path("kazr.nc")], "lidar": [Path("ceil.nc")]}
    l1b = {"radar": str(tmp_path / "radar.nc"), "lidar": str(tmp_path / "lidar.nc")}
    fetch = MagicMock(return_value=raw)
    convert = MagicMock(return_value=l1b)
    monkeypatch.setattr(product.arm, "fetch_files", fetch)
    monkeypatch.setattr(product.arm, "convert_to_l1b", convert)
    files, model_uuid = product._get_arm_input_files_for_categorize(
        _processor(tmp_path), _params(), tmp_path
    )
    assert files == {
        "model": tmp_path / "model.nc",
        "radar": tmp_path / "radar.nc",
        "lidar": tmp_path / "lidar.nc",
    }
    assert model_uuid == MODEL_UUID
    assert fetch.call_args.args[:2] == ("arm-sgp", datetime.date(2022, 6, 1))
    assert convert.call_args.args[4]["name"] == "ARM SGP"
    assert convert.call_args.kwargs["calibration"] == {"radar": {"Zh_offset": None}}


def test_arm_zh_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    table = {"x": {"2020-01-01": 1.0, "2021-01-01": 2.0}}
    monkeypatch.setattr(product, "ARM_ZH_OFFSETS", table)
    assert product._get_arm_zh_offset("x", datetime.date(2019, 12, 31)) is None
    assert product._get_arm_zh_offset("x", datetime.date(2020, 1, 1)) == 1.0
    assert product._get_arm_zh_offset("x", datetime.date(2021, 6, 1)) == 2.0
    assert product._get_arm_zh_offset("y", datetime.date(2021, 6, 1)) is None


def test_arm_missing_radar(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(product.arm, "fetch_files", MagicMock(return_value={}))
    monkeypatch.setattr(product.arm, "convert_to_l1b", MagicMock(return_value={}))
    with pytest.raises(SkipTaskError, match="radar"):
        product._get_arm_input_files_for_categorize(
            _processor(tmp_path), _params(), tmp_path
        )


def test_arm_missing_model(tmp_path: Path) -> None:
    processor = _processor(tmp_path)
    processor.get_product.return_value = None
    with pytest.raises(SkipTaskError, match="model"):
        product._get_arm_input_files_for_categorize(processor, _params(), tmp_path)


def test_arm_missing_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("ARM_USER")
    with pytest.raises(MiscError, match="credentials"):
        product._get_arm_input_files_for_categorize(
            _processor(tmp_path), _params(), tmp_path
        )


def test_keep_portal_source_uuids(tmp_path: Path) -> None:
    cat = tmp_path / "cat.nc"
    with netCDF4.Dataset(cat, "w") as nc:
        nc.source_file_uuids = f"radar-uuid, {MODEL_UUID}, lidar-uuid"
    product._keep_portal_source_uuids(cat, MODEL_UUID)
    with netCDF4.Dataset(cat) as nc:
        assert nc.source_file_uuids == str(MODEL_UUID)
