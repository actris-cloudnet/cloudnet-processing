import requests
import requests_mock
from data_processing.storage_api import StorageApi
from tempfile import TemporaryDirectory
import os

adapter = requests_mock.Adapter()
session = requests.Session()
session.mount('http://', adapter)
mock_addr = 'http://test/'

config = {
    'STORAGE-SERVICE': {
        'url': mock_addr,
        'username': 'test',
        'password': 'test'
    },
}


class TestStorageApi:

    temp_dir = TemporaryDirectory()

    def test_download_product(self):
        filename = '20201121_bucharest_classification.nc'
        metadata = {
            'volatile': True,
            'filename': filename
        }
        file = open(f'tests/data/products/{filename}', 'rb')
        url = f'{mock_addr}cloudnet-product-volatile/{filename}'
        adapter.register_uri('GET', url, body=file)
        storage_api = StorageApi(config, session)
        full_path = storage_api.download_product(metadata, self.temp_dir.name)
        assert os.path.isfile(full_path)
        assert full_path == f'{self.temp_dir.name}/{filename}'
        file.close()

    def test_download_raw_files(self):
        filename = '00100_A202010092355_CHM170137.nc'
        s3key = 'ur_a_nus'
        metadata = [
            {
                's3key': s3key,
                'filename': filename
            },
        ]
        file = open(f'tests/data/raw/chm15k/{filename}', 'rb')
        url = f'{mock_addr}cloudnet-upload/{s3key}'
        adapter.register_uri('GET', url, body=file)
        storage_api = StorageApi(config, session)
        full_paths = storage_api.download_raw_files(metadata, self.temp_dir.name)
        assert os.path.isfile(full_paths[0])
        assert full_paths[0] == f'{self.temp_dir.name}/{filename}'
        file.close()

    def test_upload_product(self):
        s3key = '20201121_bucharest_classification.nc'
        full_path = f'tests/data/products/{s3key}'
        res = {
            "size": 667,
            "version": ""
        }
        url = f'{mock_addr}cloudnet-product-volatile/{s3key}'
        adapter.register_uri('PUT', url, json=res)
        storage_api = StorageApi(config, session)
        data = storage_api.upload_product(full_path, s3key)
        assert data == res
