import argparse
from tempfile import SpooledTemporaryFile

import pytest
import requests
import requests_mock
from fastapi import HTTPException, UploadFile
from data_processing import data_submission_api, utils as process_utils

adapter = requests_mock.Adapter()
session = requests.Session()
session.mount('http://', adapter)
mock_addr = 'http://test/'

meta = {'hashSum': '12345678901234567890', 'measurementDate': '2019-01-01', 'instrument': 'kukko',
        'filename': 'file', 'site': 'espoo'}

args = argparse.Namespace(config_dir='tests/data/config')
config = process_utils.read_main_conf(args)
config['METADATASERVER']['url'] = mock_addr
ds_api = data_submission_api.DataSubmissionApi(config, session)
md_id = '123456789012345678'
md_url = f'{mock_addr}metadata/{md_id}'


class TestMetadataApi:

    def test_put_metadata_succeeds_on_http_201(self):
        adapter.register_uri('PUT', md_url, additional_matcher=is_correct_meta, status_code=201)
        ds_api.put_metadata(md_url, meta)

    def test_put_metadata_fails_on_http_200(self):
        adapter.register_uri('PUT', md_url, status_code=200)
        with pytest.raises(HTTPException):
            ds_api.put_metadata(md_url, meta)

    def test_update_metadata_status_to_processed(self):
        adapter.register_uri('POST', md_url, additional_matcher=is_correct_post, status_code=200)
        ds_api.update_metadata_status_to_processed(md_url)

    def test_check_hash(self):
        file = UploadFile('asd')
        file.file = SpooledTemporaryFile()
        file.file.write(bytes('asd', 'utf-8'))

        with pytest.raises(HTTPException):
            data_submission_api.check_hash(meta['hashSum'], file)

        file2 = UploadFile('asd')
        file2.file = SpooledTemporaryFile()
        file2.file.write(bytes('asd', 'utf-8'))
        meta2 = meta.copy()
        meta2['hashSum'] = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
        data_submission_api.check_hash(meta2['hashSum'], file2)

    def test_construct_url_from_meta(self):
        assert ds_api.create_url(meta) == md_url


def is_correct_meta(request):
    return request.json() == meta


def is_correct_post(request):
    return request.json() == {'status': 'uploaded'}
