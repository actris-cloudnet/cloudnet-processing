import datetime
import hashlib
from dataclasses import dataclass
from pathlib import Path

import pytest
import requests
from cloudnet_api_client import APIClient
from processing import utils
from processing.config import Config
from processing.dvas import Dvas
from processing.instrument import process_instrument
from processing.metadata_api import MetadataApi
from processing.pid_utils import PidUtils
from processing.processor import Instrument, InstrumentParams, Processor
from processing.storage_api import StorageApi

DATA_PATH = Path(__file__).parent / "data"
CONFIG = Config()


@pytest.fixture(scope="session")
def processor():
    session = utils.make_session()
    md_api = MetadataApi(CONFIG, session)
    storage_api = StorageApi(CONFIG, session)
    pid_utils = PidUtils(CONFIG, session)
    dvas = Dvas(CONFIG, md_api)
    client = APIClient(base_url=f"{CONFIG.dataportal_url}/api/", session=session)
    return Processor(md_api, storage_api, pid_utils, dvas, client)


@dataclass
class Meta:
    filename: str
    site: str
    date: str
    uuid: str
    product: str


meta_list = [
    Meta(
        filename="pluvio2_jue_20250811.nc",
        site="juelich",
        date="2025-08-11",
        uuid="49ca09de-ca9a-4e3e-9258-9c91ed5683f8",
        product="rain-gauge",
    ),
    Meta(
        filename="20220818_disdrometer.nc",
        site="leipzig",
        date="2022-08-18",
        uuid="922bd0a8-c7f3-4064-a6a3-f4aa2291414f",
        product="disdrometer",
    ),
]


@pytest.mark.parametrize("meta", meta_list)
def test_instrument_processing(processor: Processor, meta: Meta, tmp_path):
    instrument = processor.get_instrument(meta.uuid)
    _submit_file(meta, instrument)
    date = datetime.date.fromisoformat(meta.date)
    site = processor.get_site(meta.site, date)

    print(f"Processing {meta.product}")
    instru_params = InstrumentParams(
        site=site,
        date=date,
        product=processor.get_product(meta.product),
        instrument=instrument,
    )
    process_instrument(processor, instru_params, tmp_path)
    file_meta = processor.client.metadata(
        site_id=site.id, date=date, product=meta.product
    )
    assert len(file_meta) == 1


def _submit_file(meta: Meta, instrument: Instrument) -> None:
    auth = ("admin", "admin")
    file_path = DATA_PATH / meta.filename

    with open(file_path, "rb") as f:
        checksum = hashlib.md5(f.read()).hexdigest()

    metadata = {
        "filename": meta.filename,
        "checksum": checksum,
        "site": meta.site,
        "instrument": instrument.instrument_id,
        "measurementDate": meta.date,
        "instrumentPid": instrument.pid,
    }

    res = requests.post(
        f"{CONFIG.dataportal_url}/upload/metadata/", json=metadata, auth=auth
    )
    if res.status_code == 409:
        return
    res.raise_for_status()

    with open(file_path, "rb") as f:
        res = requests.put(
            f"{CONFIG.dataportal_url}/upload/data/{checksum}", data=f, auth=auth
        )
        res.raise_for_status()
