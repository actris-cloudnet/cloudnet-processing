import json
import re
from argparse import Namespace
from unittest import TestCase
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import requests_mock

from data_processing import metadata_api

adapter = requests_mock.Adapter()
session = requests.Session()
session.mount("http://", adapter)
mock_addr = "http://test/"
config = {
    "DATAPORTAL_URL": mock_addr,
    "FREEZE_AFTER_DAYS": 2,
    "FREEZE_MODEL_AFTER_DAYS": 3,
}

files_response = """
[
  {
    "uuid": "42d523cc-764f-4334-aefc-35a9ca71f342",
    "title": "Radar file from Granada",
    "measurementDate": "2020-05-13",
    "history": "2020-05-15 05:00:26 - radar file created",
    "publicity": "public",
    "cloudnetpyVersion": "1.0.9",
    "releasedAt": "2020-05-15T05:00:27.538Z",
    "filename": "20200513_granada_rpg-fmcw-94.nc",
    "checksum": "33727ffe97e96baef0e4c6708a14ceb3b18fbe32ebeef28efe8703434fcceaaa",
    "size": 19883377,
    "format": "HDF5 (NetCDF4)",
    "site": {
      "id": "granada",
      "humanReadableName": "Granada",
      "latitude": 37.164,
      "longitude": -3.605,
      "altitude": 680,
      "gaw": "UGR",
      "country": "Spain",
      "isTestSite": false
    },
    "product": {
      "id": "radar",
      "humanReadableName": "Radar",
      "level": "1"
    },
    "volatile": true,
    "url": "https://altocumulus.fmi.fi/download/20200513_granada_rpg-fmcw-94.nc"
  }
]
"""


class TestMetadataApi:

    payload = {
        "product": "model",
        "site": "bucharest",
        "measurementDate": "2020-10-21",
        "format": "NetCDF3",
        "checksum": "23cf03694943fbcd1cce9ff45ea05242fae68cda06f127668963bb99d0b2de33",
        "volatile": True,
        "uuid": "b09174d204e04ffbb6d284c150bb7271",
        "pid": "",
        "history": "Some arbitrary history",
        "cloudnetpyVersion": "",
        "version": "",
        "size": 501524,
    }

    def test_put_metadata(self):
        adapter.register_uri("PUT", f"{mock_addr}files/s3key", text="resp")
        md_api = metadata_api.MetadataApi(config, session)
        res = md_api.put("files", "s3key", self.payload)
        assert res.text == "resp"

    def test_raises_error_on_failed_request(self):
        adapter.register_uri("PUT", f"{mock_addr}files/s3key_fail", status_code=500)
        md_api = metadata_api.MetadataApi(config, session)
        with pytest.raises(requests.exceptions.HTTPError):
            md_api.put("files", "s3key_fail", self.payload)

    @pytest.mark.parametrize(
        "args, expected_params",
        [
            (
                {
                    "products": None,
                    "site": None,
                    "date": None,
                    "start": None,
                    "stop": None,
                    "force": False,
                },
                {
                    "files": {
                        "volatile": ["True"],
                        "showLegacy": ["True"],
                        "releasedBefore": True,
                    },
                    "model-files": {
                        "volatile": ["True"],
                        "allModels": ["True"],
                        "releasedBefore": True,
                    },
                },
            ),
            (
                {
                    "products": ["categorize", "radar", "model"],
                    "site": None,
                    "date": None,
                    "start": None,
                    "stop": None,
                    "force": False,
                },
                {
                    "files": {
                        "product": ["categorize", "radar"],
                        "volatile": ["True"],
                        "showLegacy": ["True"],
                        "releasedBefore": True,
                    },
                    "model-files": {
                        "volatile": ["True"],
                        "allModels": ["True"],
                        "releasedBefore": True,
                    },
                },
            ),
            (
                {
                    "products": None,
                    "site": None,
                    "date": None,
                    "start": None,
                    "stop": None,
                    "force": True,
                },
                {
                    "files": {
                        "volatile": ["True"],
                        "showLegacy": ["True"],
                        "releasedBefore": False,
                    },
                    "model-files": {
                        "volatile": ["True"],
                        "allModels": ["True"],
                        "releasedBefore": False,
                    },
                },
            ),
            (
                {
                    "products": None,
                    "site": "bucharest",
                    "date": "2021-01-01",
                    "start": None,
                    "stop": None,
                    "force": True,
                },
                {
                    "files": {
                        "site": ["bucharest"],
                        "date": ["2021-01-01"],
                        "volatile": ["True"],
                        "showLegacy": ["True"],
                        "releasedBefore": False,
                    },
                    "model-files": {
                        "site": ["bucharest"],
                        "date": ["2021-01-01"],
                        "volatile": ["True"],
                        "allModels": ["True"],
                        "releasedBefore": False,
                    },
                },
            ),
            (
                {
                    "products": None,
                    "site": "bucharest",
                    "date": None,
                    "start": "2021-01-01",
                    "stop": "2021-07-01",
                    "force": False,
                },
                {
                    "files": {
                        "site": ["bucharest"],
                        "dateFrom": ["2021-01-01"],
                        "dateTo": ["2021-07-01"],
                        "volatile": ["True"],
                        "showLegacy": ["True"],
                        "releasedBefore": True,
                    },
                    "model-files": {
                        "site": ["bucharest"],
                        "dateFrom": ["2021-01-01"],
                        "dateTo": ["2021-07-01"],
                        "volatile": ["True"],
                        "allModels": ["True"],
                        "releasedBefore": True,
                    },
                },
            ),
        ],
    )
    def test_calls_files_with_proper_params_and_parses_response_correctly(
        self, args, expected_params
    ):
        args = Namespace(**args)

        def make_custom_matcher(route: str):
            def custom_matcher(req: requests.Request):
                params = parse_qs(urlparse(req.url).query)
                if expected_params[route][
                    "releasedBefore"
                ]:  # We're expecting a releasedBefore
                    assert "releasedBefore" in params
                    del params["releasedBefore"]
                del expected_params[route]["releasedBefore"]
                TestCase().assertDictEqual(expected_params[route], params)
                return True

            return custom_matcher

        for route in expected_params.keys():
            adapter.register_uri(
                "GET",
                re.compile(f"{mock_addr}api/{route}(.*?)"),
                additional_matcher=make_custom_matcher(route),
                json=json.loads(files_response),
            )
        md_api = metadata_api.MetadataApi(config, session)
        md_api.find_files_to_freeze(args)
