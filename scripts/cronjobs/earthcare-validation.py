#!/usr/bin/env python3

import logging
import traceback
from datetime import timedelta

from cloudnet_api_client import APIClient
from earthcare_downloader import search
from requests import Session

from processing import utils
from processing.config import Config

logging.basicConfig(level=logging.INFO)

PRODUCTS = ("cpr-validation", "cpr-tc-validation")
SEARCH_RADIUS_KM = 200


def main() -> None:
    config = Config()
    session = utils.make_session()
    try:
        _process(config, session)
    except Exception as err:
        logging.exception("Fatal error in cronjob")
        utils.send_slack_alert(
            config,
            err,
            source="ec-cronjob",
            log=traceback.format_exc(),
        )


def _process(config: Config, session: Session) -> None:
    yesterday = utils.utctoday() - timedelta(days=1)
    client = APIClient()
    sites = client.sites("cloudnet")
    logging.info(f"Checking {len(sites)} sites for EarthCARE overpasses on {yesterday}")
    for site in sites:
        files = search(
            product="CPR_NOM_1B",
            lat=site.latitude,
            lon=site.longitude,
            radius=SEARCH_RADIUS_KM,
            date=yesterday,
        )
        if not files:
            continue
        logging.info(
            f"Found {len(files)} EarthCARE overpass(es) for {site.id} on {yesterday}"
        )
        for product in PRODUCTS:
            _publish_task(config, session, site.id, product, str(yesterday))


def _publish_task(
    config: Config,
    session: Session,
    site_id: str,
    product_id: str,
    date: str,
) -> None:
    task = {
        "type": "process",
        "siteId": site_id,
        "productId": product_id,
        "measurementDate": date,
        "scheduledAt": utils.utcnow().isoformat(),
        "priority": 100,
    }
    logging.info(f"Publish task: {task}")
    res = session.post(
        f"{config.dataportal_url}/api/queue/publish",
        json=task,
        auth=config.data_submission_auth,
    )
    res.raise_for_status()


if __name__ == "__main__":
    main()
