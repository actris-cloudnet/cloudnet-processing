import json
import re
import sys

import requests
import requests_mock
from data_processing.config import Config
from data_processing.subcmds import process_model

sys.path.append("scripts/")
cloudnet = __import__("cloudnet")

adapter = requests_mock.Adapter()
session = requests.Session()
session.mount("http://", adapter)
mock_addr = "http://test/"

args = cloudnet._parse_args(["-s=bucharest", "model"])  # Initialize default arguments

config = Config(
    {
        "DATAPORTAL_PUBLIC_URL": "http://dataportal.test",
        "DATAPORTAL_URL": mock_addr,
        "STORAGE_SERVICE_URL": "foo",
        "STORAGE_SERVICE_USER": "foo",
        "STORAGE_SERVICE_PASSWORD": "foo",
        "PID_SERVICE_URL": "foo",
        "PID_SERVICE_TEST_ENV": "foo",
        "FREEZE_AFTER_DAYS": "3",
        "FREEZE_MODEL_AFTER_DAYS": "4",
        "DVAS_PORTAL_URL": "http://dvas.test",
        "DVAS_ACCESS_TOKEN": "test",
        "DVAS_USERNAME": "test",
        "DVAS_PASSWORD": "test",
    }
)

resp = """
[
  {
    "uuid": "42d523cc-764f-4334-aefc-35a9ca71f342",
    "volatile": "False",
    "filename": "20210812_bucharest_ecmwf.nc"
  }
]
"""

METADATA = {
    "model": {"id": "ecmwf"},
    "uuid": "3ab72e38-69dc-49c2-9fdb-0f9698c386ca",
    "filename:": "20210812_bucharest_ecmwf.nc",
}


def test_upload_with_freezed_product():
    get_url = f"{mock_addr}api/model-files(.*?)"
    adapter.register_uri("GET", re.compile(get_url), json=json.loads(resp))
    adapter.register_uri("POST", f"{mock_addr}upload-metadata", text="OK")
    adapter.register_uri("POST", f"{mock_addr}files", text="OK")

    process = process_model.ProcessModel(args, config, metadata_session=session)
    res, _ = process.fetch_volatile_model_uuid(METADATA)
    assert res == "42d523cc-764f-4334-aefc-35a9ca71f342"  # Gives the stable file uuid
    assert process._create_new_version is False


def test_upload_with_freezed_product_reprocess():
    get_url = f"{mock_addr}api/model-files(.*?)"
    adapter.register_uri("GET", re.compile(get_url), json=json.loads(resp))
    adapter.register_uri("POST", f"{mock_addr}upload-metadata", text="OK")
    args.reprocess = True
    process = process_model.ProcessModel(args, config, metadata_session=session)
    res, _ = process.fetch_volatile_model_uuid(METADATA)
    assert res == "42d523cc-764f-4334-aefc-35a9ca71f342"  # Gives the stable file uuid
    assert process._create_new_version is False


def test_upload_with_no_product():
    resp = "[]"
    get_url = f"{mock_addr}api/model-files(.*?)"
    adapter.register_uri("GET", re.compile(get_url), json=json.loads(resp))
    process = process_model.ProcessModel(args, config, metadata_session=session)
    res, _ = process.fetch_volatile_model_uuid(METADATA)
    assert res is None
    assert process._create_new_version is False


def test_upload_with_volatile_product():
    uuid = "42d523cc-764f-4334-aefc-35a9ca71f342"
    resp = f'[{{"uuid": "{uuid}", "volatile": "True", "filename": "20210812_bucharest_ecmwf.nc"}}]'
    get_url = f"{mock_addr}api/model-files(.*?)"
    adapter.register_uri("GET", re.compile(get_url), json=json.loads(resp))
    process = process_model.ProcessModel(args, config, metadata_session=session)
    res, _ = process.fetch_volatile_model_uuid(METADATA)
    assert res == uuid
    assert process._create_new_version is False
