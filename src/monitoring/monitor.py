import logging

from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import Instrument, Site

from monitoring.instruments import halo_doppler_lidar
from monitoring.period import Period
from monitoring.product import MonitoringProduct
from processing.config import Config


class C:
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"
    MAGENTA = "\033[35m"
    YELLOW = "\033[33m"
    BOLD = "\033[1m"


def monitor(
    site: Site, instrument: Instrument, period: Period, product: MonitoringProduct
) -> None:
    config = Config()
    client = APIClient(f"{config.dataportal_url}/api/")
    msg = (
        f"Monitoring: {C.RED}{site.human_readable_name} "
        f"{C.BLUE}{instrument.name}({instrument.uuid}) "
        f"{C.YELLOW}{period} {C.MAGENTA}{product}{C.RESET}"
    )
    logging.info(msg)
    match instrument.instrument_id:
        case "halo-doppler-lidar":
            halo_doppler_lidar.monitor(client, site, instrument, period, product)
