import sys
import re
import json
from collections import namedtuple
import requests
import requests_mock
import importlib
import pytest
from data_processing.utils import MiscError
sys.path.append('scripts/')
process_model = importlib.import_module('process-model')

adapter = requests_mock.Adapter()
session = requests.Session()
session.mount('http://', adapter)
mock_addr = 'http://test/'

args = namedtuple('args', 'site reprocess')
args.site = 'bucharest'
args.reprocess = False

config = {
    'DATAPORTAL_URL': mock_addr,
    'STORAGE_SERVICE_URL': 'foo',
    'STORAGE_SERVICE_USER': 'foo',
    'STORAGE_SERVICE_PASSWORD': 'foo',
    'PID_SERVICE_URL': 'foo',
    'PID_SERVICE_TEST_ENV': 'foo'
}

resp = '''
[
  {
    "uuid": "42d523cc-764f-4334-aefc-35a9ca71f342",
    "volatile": "False",
    "filename": "20210812_bucharest_ecmwf.nc"
  }
]
'''

raw_uuid = "3ab72e38-69dc-49c2-9fdb-0f9698c386ca"


def test_upload_with_freezed_product():
    get_url = f'{mock_addr}api/model-files(.*?)'
    adapter.register_uri('GET', re.compile(get_url),  json=json.loads(resp))
    adapter.register_uri('POST', f'{mock_addr}upload-metadata',  text='OK')

    process = process_model.ProcessModel(args, config, metadata_session=session)
    with pytest.raises(MiscError):
        process.fetch_volatile_uuid('ecmwf', raw_uuid)


def test_upload_with_freezed_product_reprocess():
    get_url = f'{mock_addr}api/model-files(.*?)'
    adapter.register_uri('GET', re.compile(get_url),  json=json.loads(resp))
    adapter.register_uri('POST', f'{mock_addr}upload-metadata',  text='OK')
    args.reprocess = True
    process = process_model.ProcessModel(args, config, metadata_session=session)
    res = process.fetch_volatile_uuid('ecmwf', raw_uuid)
    assert res is None
    assert process._create_new_version is True


def test_upload_with_no_product():
    resp = '[]'
    get_url = f'{mock_addr}api/model-files(.*?)'
    adapter.register_uri('GET', re.compile(get_url),  json=json.loads(resp))
    process = process_model.ProcessModel(args, config, metadata_session=session)
    res = process.fetch_volatile_uuid('ecmwf', raw_uuid)
    assert res is None
    assert process._create_new_version is False


def test_upload_with_volatile_product():
    uuid = "42d523cc-764f-4334-aefc-35a9ca71f342"
    resp = f'[{{"uuid": "{uuid}", "volatile": "True"}}]'
    get_url = f'{mock_addr}api/model-files(.*?)'
    adapter.register_uri('GET', re.compile(get_url),  json=json.loads(resp))
    process = process_model.ProcessModel(args, config, metadata_session=session)
    res = process.fetch_volatile_uuid('ecmwf', raw_uuid)
    assert res == uuid
    assert process._create_new_version is False