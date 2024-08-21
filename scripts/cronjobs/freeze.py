#!/usr/bin/env python3

import logging
import traceback
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
    try:
        files = _find_files_to_freeze(config, md_api)
        for file in files:
            _publish_freeze_task(config, session, file)
    except Exception as err:
        logging.exception("Fatal error in cronjob")
        utils.send_slack_alert(
            config, err, source="freeze-cronjob", log=traceback.format_exc()
        )


def _find_files_to_freeze(config: Config, md_api: MetadataApi) -> list:
    files_payload = _get_freeze_payload(config.freeze_after_days)
    regular_files = md_api.get("api/files", files_payload)
    regular_files = [
        file for file in regular_files if _is_freezable(md_api, file["uuid"])
    ]
    logging.info(
        f"Found {len(regular_files)} regular {'file' if len(regular_files) == 1 else 'files'} to freeze"
    )

    models_payload = {
        "allModels": True,
        **_get_freeze_payload(config.freeze_model_after_days),
    }
    model_files = md_api.get("api/model-files", models_payload)
    logging.info(
        f"Found {len(model_files)} model {'file' if len(model_files) == 1 else 'files'} to freeze"
    )

    return regular_files + model_files


def _get_freeze_payload(freeze_after_days: int) -> dict:
    updated_before = utils.utcnow() - timedelta(days=freeze_after_days)
    return {"volatile": True, "releasedBefore": updated_before.isoformat()}


def _is_freezable(md_api: MetadataApi, file_uuid: str, depth: int = 0):
    file = md_api.get(f"api/files/{file_uuid}")
    return (
        (depth == 0 or not file["volatile"])
        and not file["product"]["experimental"]
        and all(
            _is_freezable(md_api, src_uuid, depth + 1)
            for src_uuid in file.get("sourceFileIds", [])
        )
    )


def _publish_freeze_task(config: Config, session: Session, file: dict):
    task = {
        "type": "freeze",
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
