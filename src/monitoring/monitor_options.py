from dataclasses import dataclass

from cloudnet_api_client.client import APIClient

from monitoring.period import PeriodType
from monitoring.product import MonitoringProduct
from processing.metadata_api import MetadataApi
from processing.storage_api import StorageApi


@dataclass
class MonitorOptions:
    period: PeriodType
    product: MonitoringProduct
    site: str
    instrument_uuid: str
    api_client: APIClient
    storage_api: StorageApi
    md_api: MetadataApi
