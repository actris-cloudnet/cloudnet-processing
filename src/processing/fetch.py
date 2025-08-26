import concurrent.futures
import datetime
import hashlib
import logging
import os
import sys
from argparse import Namespace
from base64 import b64encode
from collections.abc import Callable
from pathlib import Path

import requests
from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import ExtendedProduct, Product, Site
from cloudnet_api_client.utils import md5sum

DATAPORTAL_URL = os.environ["DATAPORTAL_URL"].rstrip("/")
DATAPORTAL_AUTH = ("admin", "admin")
STORAGE_SERVICE_URL = os.environ["STORAGE_SERVICE_URL"].rstrip("/")
STORAGE_SERVICE_AUTH = (
    os.environ["STORAGE_SERVICE_USER"],
    os.environ["STORAGE_SERVICE_PASSWORD"],
)
DOWNLOAD_DIR = Path("download")


if sys.stdout.isatty() and not os.getenv("NO_COLOR"):
    GREEN = "\033[92m"
    BOLD = "\033[1m"
    RESET = "\033[0m"
    YELLOW = "\033[93m"
else:
    GREEN = ""
    BOLD = ""
    RESET = ""
    YELLOW = ""


class Fetcher:
    api_url = "https://cloudnet.fmi.fi/api"

    def __init__(
        self,
        product: Product | ExtendedProduct,
        site: Site,
        date: datetime.date,
        args: Namespace,
    ):
        self.product = product
        self.args = args
        self.payload = {"site": site.id, "date": date.isoformat()}

    def get_raw_instrument_metadata(self) -> list:
        url = f"{self.api_url}/raw-files"
        if (
            isinstance(self.product, ExtendedProduct)
            and self.product.source_instrument_ids
            and not self.args.instruments
        ):
            instruments = self.product.source_instrument_ids
        else:
            instruments = self.args.instruments
        payload = {
            **self.payload,
            "status": ["uploaded", "processed"],
            "instrument": instruments,
        }
        if self.args.instrument_pids:
            payload["instrumentPid"] = self.args.instrument_pids
        res = requests.get(url=url, params=payload)
        res.raise_for_status()
        metadata = res.json()
        if self.args.all:
            return metadata
        return [m for m in metadata if not m["filename"].lower().endswith(".lv0")]

    def get_raw_model_metadata(self) -> list:
        url = f"{self.api_url}/raw-model-files"
        payload = {
            **self.payload,
            "status": ["uploaded", "processed"],
            "allModels": True,
            "model": self.args.models,
        }
        res = requests.get(url=url, params=payload)
        res.raise_for_status()
        return res.json()

    def get_product_metadata(self) -> list:
        url = f"{self.api_url}/files"
        payload = {
            **self.payload,
            "product": self.product.id,
            "showLegacy": "true",
        }
        if self.args.instruments:
            payload["instrument"] = self.args.instruments
        if self.args.instrument_pids:
            payload["instrumentPid"] = self.args.instrument_pids
        res = requests.get(url=url, params=payload)
        res.raise_for_status()
        return res.json()

    def get_model_metadata(self) -> list:
        url = f"{self.api_url}/model-files"
        payload = {
            **self.payload,
        }
        if self.args.models:
            payload["model"] = self.args.models
        res = requests.get(url=url, params=payload)
        res.raise_for_status()
        return res.json()


def fetch(
    product: Product, site: Site, date: datetime.date, args: Namespace, client=APIClient
) -> None:
    is_production = os.environ.get("PID_SERVICE_TEST_ENV", "false").lower() != "true"
    if is_production:
        logging.warning("Running in production, not fetching anything.")
        return

    if args.uuids:
        args.instrument_pids = [client.instrument(i).pid for i in args.uuids]
    else:
        args.instrument_pids = None

    fetcher = Fetcher(product, site, date, args)

    if args.raw:
        if "instrument" in product.type:
            print(f"\n{BOLD}Raw instrument files for {date}:{RESET}\n")
            meta = fetcher.get_raw_instrument_metadata()
            if not meta:
                print(f"{YELLOW}No files found.{RESET}")
            _process_metadata(_submit_upload, meta)
            _fetch_calibration(meta)
        elif args.models:
            print(f"\n{BOLD}Raw model files for {date}:{RESET}\n")
            meta = fetcher.get_raw_model_metadata()
            if not meta:
                print(f"{YELLOW}No files found.{RESET}")
            _process_metadata(_submit_upload, meta)
        elif not (args.instruments or args.models):
            print(f"\n{BOLD}Raw instrument files for {date}:{RESET}\n")
            meta = fetcher.get_raw_instrument_metadata()
            if not meta:
                print(f"{YELLOW}No files found.{RESET}")
            _process_metadata(_submit_upload, meta)
            _fetch_calibration(meta)
            print(f"\n{BOLD}Raw model files for {date}:{RESET}\n")
            meta = fetcher.get_raw_model_metadata()
            if not meta:
                print(f"{YELLOW}No files found.{RESET}")
            _process_metadata(_submit_upload, meta)
    elif product.id == "model":
        meta = fetcher.get_model_metadata()
        _process_metadata(_submit_file, meta, show_progress=False)
    else:
        meta = fetcher.get_product_metadata()
        _process_metadata(_submit_file, meta, show_progress=False)


def _process_metadata(
    submitter: Callable[[Path, dict], str],
    upload_metadata: list,
    show_progress: bool = True,
):
    def process_row(row: dict):
        filename = _download_file(row)
        return submitter(filename, row)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filename = {
            executor.submit(process_row, row): row["filename"]
            for row in upload_metadata
        }
        completed = 0
        for future in concurrent.futures.as_completed(future_to_filename):
            completed += 1
            filename = future_to_filename[future]
            if show_progress:
                info = f"{completed}/{len(upload_metadata)} {filename}"
            else:
                info = filename
            try:
                response = future.result()
                if "File already uploaded" in response:
                    response = "File already uploaded"
                response = (
                    f"{GREEN}{response}{RESET}"
                    if response in ("OK", "Created")
                    else f"{YELLOW}{response}{RESET}"
                )
                print(f"{info} {response}")
            except Exception as exc:
                print(f"{info} {exc}")


def _download_file(row: dict) -> Path:
    res = requests.get(row["downloadUrl"])
    res.raise_for_status()
    if "product" in row:
        subdir = "product-" + row["product"]["id"]
    elif "model" in row:
        subdir = "model-" + row["model"]["id"]
    elif "instrument" in row:
        subdir = (
            row["instrument"]["instrumentId"]
            + "-"
            + row["instrument"]["pid"].split(".")[-1][:8]
        )
        if row["tags"]:
            subdir += "-" + "-".join(sorted(row["tags"]))
    else:
        raise ValueError("Row does not contain product, model or instrument.")
    outdir = DOWNLOAD_DIR / subdir
    outdir.mkdir(exist_ok=True, parents=True)
    filename = outdir / row["filename"]
    filename.write_bytes(res.content)
    return filename


def _submit_upload(filename: Path, row: dict) -> str:
    metadata = {
        "filename": row["filename"],
        "checksum": row["checksum"],
        "site": row["site"]["id"],
        "measurementDate": row["measurementDate"],
    }
    if "tags" in row:
        metadata["tags"] = row["tags"]
    if "instrument" in row:
        metadata["instrument"] = row["instrument"]["instrumentId"]
        metadata["instrumentPid"] = row["instrument"]["pid"]
        end_point = "upload"
    elif "model" in row:
        metadata["model"] = row["model"]["id"]
        end_point = "model-upload"
    else:
        raise ValueError("Row does not contain instrument or model.")
    url = f"{DATAPORTAL_URL}/{end_point}/"
    res = requests.post(f"{url}metadata", json=metadata, auth=DATAPORTAL_AUTH)
    if res.status_code == 200:
        with filename.open("rb") as f:
            res = requests.put(
                f'{url}data/{metadata["checksum"]}', data=f, auth=DATAPORTAL_AUTH
            )
            res.raise_for_status()
    elif res.status_code != 409:
        error = res.content.decode("utf-8", errors="replace")
        raise Exception(f"{res.status_code} {res.reason}: {error}")
    return res.text


def _submit_file(filename: Path, row: dict) -> str:
    bucket = "cloudnet-product-volatile" if row["volatile"] else "cloudnet-product"
    if row["legacy"]:
        bucket = f"{bucket}/legacy"
    ss_url = f"{STORAGE_SERVICE_URL}/{bucket}/{row['filename']}"
    ss_body = filename.read_bytes()
    ss_headers = {"Content-MD5": md5sum(filename, is_base64=True)}
    ss_res = requests.put(
        ss_url, ss_body, auth=STORAGE_SERVICE_AUTH, headers=ss_headers
    )
    ss_res.raise_for_status()
    ss_data = ss_res.json()
    assert int(ss_data["size"]) == int(row["size"]), "Invalid size"

    suffix = f"legacy/{row['filename']}" if row["legacy"] else f"{row['filename']}"
    dp_url = f"{DATAPORTAL_URL}/files/{suffix}"

    dp_body = {
        **row,
        "version": ss_data["version"] if "version" in ss_data else "",
        "sourceFileIds": [],
    }
    if row.get("instrument") is not None:
        dp_body["instrumentPid"] = dp_body["instrument"]["pid"]
        del dp_body["instrument"]
    elif row.get("model") is not None:
        dp_body["model"] = dp_body["model"]["id"]

    dp_res = requests.put(dp_url, json=dp_body)
    dp_res.raise_for_status()

    viz_url = f"https://cloudnet.fmi.fi/api/visualizations/{row['uuid']}"
    viz_res = requests.get(viz_url)
    viz_data = viz_res.json()
    for viz in viz_data["visualizations"]:
        img_url = f"https://cloudnet.fmi.fi/api/download/image/{viz['s3key']}"
        img_res = requests.get(img_url)
        img_data = img_res.content

        ss_url = f"{STORAGE_SERVICE_URL}/cloudnet-img/{viz['s3key']}"
        ss_headers = {"Content-MD5": b64encode(hashlib.md5(img_data).digest()).decode()}
        ss_res = requests.put(
            ss_url, img_data, auth=STORAGE_SERVICE_AUTH, headers=ss_headers
        )
        ss_res.raise_for_status()

        img_payload = {
            "sourceFileId": row["uuid"],
            "variableId": viz["productVariable"]["id"],
            "dimensions": viz["dimensions"],
        }
        img_url = f"{DATAPORTAL_URL}/visualizations/{viz['s3key']}"
        img_res = requests.put(img_url, json=img_payload)
        img_res.raise_for_status()

    try:
        qc_url = f"https://cloudnet.fmi.fi/api/quality/{row['uuid']}"
        qc_res = requests.get(qc_url)
        qc_res.raise_for_status()
        qc_data = qc_res.json()

        qc_url = f"{DATAPORTAL_URL}/quality/{row['uuid']}"
        qc_payload = {
            "timestamp": qc_data["timestamp"],
            "qcVersion": qc_data["qcVersion"],
            "tests": qc_data["testReports"],
        }
        qc_res = requests.put(qc_url, json=qc_payload)
        qc_res.raise_for_status()
    except requests.HTTPError:
        pass

    return "OK"


def _fetch_calibration(upload_metadata: list):
    first = True
    processed_pid_dates: set[tuple] = set()
    for upload in upload_metadata:
        if (
            upload["instrument"]["pid"],
            upload["measurementDate"],
        ) in processed_pid_dates:
            continue
        params = {
            "instrumentPid": upload["instrument"]["pid"],
            "date": upload["measurementDate"],
        }
        res = requests.get("https://cloudnet.fmi.fi/api/calibration", params=params)
        if res.status_code == 404:
            continue
        if first:
            print(f"\n{BOLD}Calibration:{RESET}\n")
            first = False
        print(upload["instrument"]["pid"], upload["measurementDate"])
        res.raise_for_status()
        res = requests.put(
            f"{DATAPORTAL_URL}/api/calibration",
            params=params,
            json=res.json()["data"],
            auth=("admin", "admin"),
        )
        res.raise_for_status()
        processed_pid_dates.add(
            (upload["instrument"]["pid"], upload["measurementDate"])
        )
