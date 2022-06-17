import os
from tempfile import TemporaryDirectory

from data_processing.storage_api import StorageApi
from test_utils import utils as utils

session, adapter, mock_addr = utils.init_test_session()

config = {
    "STORAGE_SERVICE_URL": mock_addr,
    "STORAGE_SERVICE_USER": "test",
    "STORAGE_SERVICE_PASSWORD": "test",
}


class TestStorageApi:

    temp_dir = TemporaryDirectory()

    def test_download_product(self):
        filename = "20201121_bucharest_classification.nc"
        metadata = {"volatile": True, "filename": filename}
        file = open(f"tests/data/products/{filename}", "rb")
        url = f"{mock_addr}cloudnet-product-volatile/{filename}"
        adapter.register_uri("GET", url, body=file)
        storage_api = StorageApi(config, session)
        full_path = storage_api.download_product(metadata, self.temp_dir.name)
        assert os.path.isfile(full_path)
        assert full_path == f"{self.temp_dir.name}/{filename}"
        file.close()

    def test_download_raw_files(self):
        filename = "00100_A202010221205_CHM170137.nc"
        s3path = "/ur/a/nus"
        metadata = [
            {"uuid": "1234123", "s3path": s3path, "filename": filename},
        ]
        file = open(f"tests/data/raw/chm15k/{filename}", "rb")
        url = f"{mock_addr}{s3path[1:]}"
        adapter.register_uri("GET", url, body=file)
        storage_api = StorageApi(config, session)
        full_paths, uuids, _ = storage_api.download_raw_data(metadata, self.temp_dir.name)
        assert os.path.isfile(full_paths[0])
        assert full_paths[0] == f"{self.temp_dir.name}/{filename}"
        file.close()

    def test_upload_stable_product(self):
        s3key = "20201022_bucharest_ecmwf.nc"
        full_path = f"tests/data/products/{s3key}"
        res = {"size": 667, "version": "abc"}
        url = f"{mock_addr}cloudnet-product/{s3key}"
        adapter.register_uri("PUT", url, json=res)
        storage_api = StorageApi(config, session)
        data = storage_api.upload_product(full_path, s3key)
        assert data == res

    def test_upload_volatile_product(self):
        s3key = "20201121_bucharest_classification.nc"
        full_path = f"tests/data/products/{s3key}"
        res = {"size": 667, "version": ""}
        url = f"{mock_addr}cloudnet-product-volatile/{s3key}"
        adapter.register_uri("PUT", url, json=res)
        storage_api = StorageApi(config, session)
        data = storage_api.upload_product(full_path, s3key)
        assert data == res
