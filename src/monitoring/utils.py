from typing import TypedDict

from cloudnet_api_client import APIClient
from cloudnet_api_client.client import DateParam

from processing.config import Config
from processing.metadata_api import MetadataApi
from processing.storage_api import StorageApi
from processing.utils import make_session


def instrument_uuid_to_pid(client: APIClient, uuid: str) -> str:
    res = client._get(f"instrument-pids/{uuid}")
    if len(res) == 0:
        raise ValueError(f"Could not find pid for uuid '{uuid}'")
    if len(res) > 1:
        raise ValueError(f"Pid for uuid '{uuid}' is not unique")
    return res[0]["pid"]


def get_api_client() -> APIClient:
    config = Config()
    client = APIClient(base_url=f"{config.dataportal_url}/api")
    return client


def get_storage_api() -> StorageApi:
    config = Config()
    session = make_session()
    return StorageApi(config, session)


def get_md_api() -> MetadataApi:
    config = Config()
    session = make_session()
    return MetadataApi(config, session)


class RawFilesDatePayload(TypedDict, total=False):
    date_from: DateParam
    date_to: DateParam


class RawFilesPayload(TypedDict, total=False):
    site_id: str
    date_from: DateParam
    date_to: DateParam
    instrument_id: str
    instrument_pid: str
    filename_prefix: str
    filename_suffix: str
