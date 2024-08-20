#!/usr/bin/env python3

import logging
from datetime import timedelta

from processing import utils
from processing.config import Config
from processing.metadata_api import MetadataApi
from requests import Session

logging.basicConfig(level=logging.INFO)


def main():
    config = utils.read_main_conf()
    session = utils.make_session()
    md_api = MetadataApi(config, session)
    files = _find_yesterdays_files(md_api)
    for file in files:
        _publish_qc_task(config, session, file)


def _find_yesterdays_files(md_api: MetadataApi) -> list:
    files_payload = {"date": utils.utctoday() - timedelta(days=1)}
    regular_files = md_api.get("api/files", files_payload)
    logging.info(
        f"Found {len(regular_files)} regular {'file' if len(regular_files) == 1 else 'files'} to check"
    )

    models_payload = {**files_payload, "allModels": True}
    model_files = md_api.get("api/model-files", models_payload)
    logging.info(
        f"Found {len(model_files)} model {'file' if len(model_files) == 1 else 'files'} to check"
    )

    return regular_files + model_files


def _publish_qc_task(config: Config, session: Session, file: dict):
    task = {
        "type": "qc",
        "siteId": file["site"]["id"],
        "productId": file["product"]["id"],
        "measurementDate": file["measurementDate"],
        "scheduledAt": utils.utcnow().isoformat(),
        "priority": 100,
    }
    if "modelId" in file:
        task["modelId"] = file["modelId"]
    if "instrumentInfoUuid" in file:
        task["instrumentInfoUuid"] = file["instrumentInfoUuid"]
    logging.info(f"Publish task: {task}")
    res = session.post(
        f"{config.dataportal_url}/api/queue/publish",
        json=task,
        auth=config.data_submission_auth,
    )
    res.raise_for_status()


if __name__ == "__main__":
    main()
