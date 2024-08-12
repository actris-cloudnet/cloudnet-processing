from pathlib import Path
from tempfile import TemporaryDirectory

from data_processing.config import Config
from data_processing.storage_api import StorageApi
from test_utils import utils as utils

session, adapter, mock_addr = utils.init_test_session()

config = Config(
    {
        "STORAGE_SERVICE_URL": mock_addr,
        "STORAGE_SERVICE_USER": "test",
        "STORAGE_SERVICE_PASSWORD": "test",
        "DATAPORTAL_PUBLIC_URL": "http://dataportal.test",
        "DATAPORTAL_URL": "http://backend.test",
        "PID_SERVICE_URL": "http://pid.test/",
        "PID_SERVICE_TEST_ENV": "true",
        "FREEZE_AFTER_DAYS": "3",
        "FREEZE_MODEL_AFTER_DAYS": "4",
        "DVAS_PORTAL_URL": "http://dvas.test",
        "DVAS_ACCESS_TOKEN": "test",
        "DVAS_USERNAME": "test",
        "DVAS_PASSWORD": "test",
    }
)


class TestStorageApi:
    temp_dir = TemporaryDirectory()

    def test_download_product(self):
        filename = "20201121_bucharest_classification.nc"
        metadata = {
            "volatile": True,
            "filename": filename,
            "size": "120931",
            "checksum": "48e006f769a9352a42bf41beac449eae62aea545f4d3ba46bffd35759d8982ca",
        }
        with open(f"tests/data/products/{filename}", "rb") as file:
            url = f"{mock_addr}cloudnet-product-volatile/{filename}"
            adapter.register_uri("GET", url, body=file)
            storage_api = StorageApi(config, session)
            full_path = storage_api.download_product(metadata, self.temp_dir.name)
            assert full_path.is_file()
            assert Path.samefile(full_path, Path(f"{self.temp_dir.name}/{filename}"))

    def test_download_raw_files(self):
        filename = "00100_A202010221205_CHM170137.nc"
        s3key = f"ur/a/nus/{filename}"
        metadata = [
            {
                "uuid": "09614b3a-484b-43e7-bfa4-c286eb851244",
                "s3key": s3key,
                "filename": filename,
                "size": "53764",
                "checksum": "2c80eae7adce951ab80b3557004388a6",
            },
        ]
        with open(f"tests/data/raw/chm15k/{filename}", "rb") as file:
            url = f"{mock_addr}cloudnet-upload/{s3key}"
            adapter.register_uri("GET", url, body=file)
            storage_api = StorageApi(config, session)
            full_paths, uuids = storage_api.download_raw_data(
                metadata, self.temp_dir.name
            )
            assert full_paths[0].is_file()
            assert Path.samefile(
                full_paths[0], Path(f"{self.temp_dir.name}/{filename}")
            )

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
