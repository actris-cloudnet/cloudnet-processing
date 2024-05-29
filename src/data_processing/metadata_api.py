"""Metadata API for Cloudnet files."""
import logging
import uuid
from argparse import Namespace
from datetime import datetime, timedelta, timezone

import requests

from data_processing import utils
from data_processing.config import Config


class MetadataApi:
    """Class handling connection between Cloudnet files and database."""

    def __init__(
        self, config: Config, session: requests.Session = utils.make_session()
    ):
        self.config = config
        self.session = session
        self._url = config.dataportal_url
        self._auth = config.data_submission_auth

    def get(self, end_point: str, payload: dict | None = None, json: bool = True):
        """Get Cloudnet metadata."""
        url = f"{self._url}/{end_point}"
        res = self.session.get(url, params=payload)
        res.raise_for_status()
        if json:
            return res.json()
        return res

    def post(
        self, end_point: str, payload: dict, auth: tuple[str, str] | None = None
    ) -> requests.Response:
        """Update upload / product metadata."""
        url = f"{self._url}/{end_point}"
        res = self.session.post(url, json=payload, auth=auth)
        res.raise_for_status()
        return res

    def put(self, end_point: str, resource: str, payload: dict) -> requests.Response:
        """PUT metadata to Cloudnet data portal."""
        url = f"{self._url}/{end_point}/{resource}"
        res = self.session.put(url, json=payload)
        res.raise_for_status()
        return res

    def put_file(
        self, end_point: str, resource: str, full_path: str, auth: tuple[str, str]
    ) -> requests.Response:
        """PUT file to Cloudnet data portal."""
        url = f"{self._url}/{end_point}/{resource}"
        res = self.session.put(url, data=open(full_path, "rb"), auth=auth)
        res.raise_for_status()
        return res

    def delete(self, end_point: str, params: dict | None = None) -> requests.Response:
        """Delete Cloudnet metadata."""
        url = f"{self._url}/{end_point}"
        res = self.session.delete(url, auth=self._auth, params=params)
        res.raise_for_status()
        return res

    def put_images(self, img_metadata: list, product_uuid: str | uuid.UUID):
        for data in img_metadata:
            payload = {
                "sourceFileId": str(product_uuid),
                "variableId": data["variable_id"],
                "dimensions": data["dimensions"],
            }
            self.put("visualizations", data["s3key"], payload)

    def upload_instrument_file(
        self, base, instrument: str, filename: str, instrument_pid: str | None = None
    ):
        checksum = utils.md5sum(base.daily_file.name)
        metadata = {
            "filename": filename,
            "checksum": checksum,
            "instrument": instrument,
            "measurementDate": base.date_str,
            "site": base.site,
            "instrumentPid": instrument_pid,
        }
        try:
            self.post("upload/metadata", metadata, auth=self._auth)
        except requests.exceptions.HTTPError as err:
            logging.info(err)
            return
        self.put_file("upload/data", checksum, base.daily_file.name, self._auth)

    def find_files_to_freeze(self, args: Namespace) -> list:
        """Find volatile files released before certain time limit."""
        freeze_model_files = not args.products or "model" in args.products
        products = _get_regular_products(args)
        common_payload = _get_common_payload(args)

        # Regular files
        files_payload = {
            **common_payload,
            **{"product": products},
            **{"showLegacy": True},
            **self._get_freeze_payload(self.config.freeze_after_days, args),
        }
        regular_files = self.get("api/files", files_payload)

        # Model files
        model_files = []
        if freeze_model_files:
            models_payload = {
                **common_payload,
                **{"allModels": True},
                **self._get_freeze_payload(self.config.freeze_model_after_days, args),
            }
            model_files = self.get("api/model-files", models_payload)

        all_files = regular_files + model_files

        if args.experimental is False:
            all_files = [f for f in all_files if f["product"]["experimental"] is False]

        return all_files

    def update_dvas_info(self, uuid: uuid.UUID, timestamp: str, dvas_id: int):
        payload = {"uuid": uuid, "dvasUpdatedAt": timestamp, "dvasId": dvas_id}
        self.post("files", payload)

    def clean_dvas_info(self, uuid: uuid.UUID):
        payload = {"uuid": uuid, "dvasUpdatedAt": None, "dvasId": None}
        self.post("files", payload)

    def _get_freeze_payload(self, freeze_after_days, args: Namespace) -> dict:
        if args.force:
            logging.warning(
                f"Overriding config option ({freeze_after_days} days). Also recently changed files may be freezed."
            )
            updated_before = None
        else:
            updated_before = (
                datetime.now(timezone.utc) - timedelta(days=freeze_after_days)
            ).isoformat()
        return {"volatile": True, "releasedBefore": updated_before}

    def find_product_metadata(self, args: Namespace, legacy_files: bool = True) -> list:
        common_payload = _get_common_payload(args)
        products = _get_regular_products(args)
        files = []
        if products is not None and len(products) > 0:
            files_payload = {
                "showLegacy": legacy_files,
                **common_payload,
                "product": products,
            }
            files += self.get("api/files", files_payload)
        if "model" in args.products:
            model_files_payload = {
                **common_payload,
            }
            files += self.get("api/model-files", model_files_payload)
        files.sort(key=lambda x: datetime.strptime(x["measurementDate"], "%Y-%m-%d"))
        return files

    def get_qc_version(self, uuid: str) -> dict | None:
        try:
            data = self.get(f"api/quality/{uuid}")
            return {"result": data["errorLevel"], "qc_version": data["qcVersion"]}
        except requests.exceptions.HTTPError:
            return None


def _get_common_payload(args: Namespace) -> dict:
    if args.date is not None:
        payload = {"date": args.date}
    else:
        payload = {"dateFrom": args.start, "dateTo": args.stop}
    payload["site"] = args.site
    return payload


def _get_regular_products(args: Namespace) -> list | None:
    if args.products:
        return [prod for prod in args.products if prod != "model"]
    return None
