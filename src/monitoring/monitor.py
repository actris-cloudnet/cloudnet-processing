import logging

from monitoring.instrument.halo_doppler_lidar import (
    monitor as monitor_halo,
)
from monitoring.period import PeriodType
from monitoring.product import MonitoringProduct
from monitoring.utils import get_api_client, get_storage_api


def monitor(
    period: PeriodType, product: MonitoringProduct, site: str, instrument_uuid: str
) -> None:
    api_client = get_api_client()
    storage_api = get_storage_api()
    if product.id.startswith("halo-doppler-lidar"):
        monitor_halo(period, product, site, instrument_uuid, api_client, storage_api)
        logging.info(f"Monitored {period!r} {product} {site}")
    else:
        raise ValueError(f"Unexpected product: '{product}'")
