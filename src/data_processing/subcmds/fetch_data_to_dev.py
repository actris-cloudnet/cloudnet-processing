import argparse
import concurrent.futures
import datetime
import hashlib
import logging
import os
import sys
from base64 import b64encode
from collections.abc import Callable
from pathlib import Path

import requests

from data_processing import utils

DATAPORTAL_URL = os.environ["DATAPORTAL_URL"].rstrip("/")
DATAPORTAL_AUTH = ("admin", "admin")
STORAGE_SERVICE_URL = os.environ["STORAGE_SERVICE_URL"].rstrip("/")
STORAGE_SERVICE_AUTH = (
    os.environ["STORAGE_SERVICE_USER"],
    os.environ["STORAGE_SERVICE_PASSWORD"],
)
DOWNLOAD_DIR = Path("download")


def main(args: argparse.Namespace):
    is_production = os.environ.get("PID_SERVICE_TEST_ENV", "false").lower() != "true"
    if is_production:
        logging.warning("Running in production, not fetching anything.")
        return

    params = {"site": args.site}
    if args.date:
        params["start"] = args.date
        params["stop"] = args.date
    elif args.start or args.stop:
        params["start"] = args.start
        params["stop"] = args.stop
    else:
        print("Please specify --date, --start or --stop", file=sys.stderr)
        sys.exit(1)
    params["start"] = datetime.date.fromisoformat(params["start"])
    params["stop"] = datetime.date.fromisoformat(params["stop"])

    if not args.products_specified and args.instruments is None and args.models is None:
        print("Please specify --products, --instruments or --models", file=sys.stderr)
        sys.exit(1)

    DOWNLOAD_DIR.mkdir(exist_ok=True)

    if args.products_specified:
        file_metadata = _get_file_metadata(**params, products=args.products)
        if "model" in args.products:
            model_metadata = _get_model_metadata(**params)
            file_metadata.extend(model_metadata)
        if file_metadata:
            print("\nProduct files:\n")
        _process_metadata(_submit_file, file_metadata, args)

    if args.instruments:
        upload_metadata = _get_upload_metadata(
            "raw-files",
            **params,
            instruments=args.instruments if args.instruments != ["all"] else None,
        )
        if args.include is not None:
            upload_metadata = utils.include_records_with_pattern_in_filename(
                upload_metadata, args.include
            )
        if args.exclude is not None:
            upload_metadata = utils.exclude_records_with_pattern_in_filename(
                upload_metadata, args.exclude
            )

        if upload_metadata:
            print("\nMeasurement files:\n")
            _process_metadata(_submit_upload, upload_metadata, args)
            _fetch_calibration(upload_metadata)

    if args.models:
        upload_metadata = _get_upload_metadata(
            "raw-model-files",
            **params,
            models=args.models if args.models != ["all"] else None,
        )
        if upload_metadata:
            print("\nModel files:\n")
            _process_metadata(_submit_upload, upload_metadata, args)


def _get_upload_metadata(
    end_point: str,
    site: str,
    start: datetime.date | None,
    stop: datetime.date | None,
    instruments: list[str] | None = None,
    models: list[str] | None = None,
) -> list:
    url = f"https://cloudnet.fmi.fi/api/{end_point}"
    payload = {
        "site": site,
        "dateFrom": start.isoformat() if start else None,
        "dateTo": stop.isoformat() if stop else None,
        "instrument": instruments,
        "model": models,
        "status": ["uploaded", "processed"],
    }
    res = requests.get(url=url, params=payload)
    res.raise_for_status()
    metadata = res.json()
    return metadata


def _get_file_metadata(
    site: str,
    start: datetime.date | None,
    stop: datetime.date | None,
    products: list[str] | None = None,
) -> list:
    url = "https://cloudnet.fmi.fi/api/files"
    payload = {
        "site": site,
        "dateFrom": start.isoformat() if start else None,
        "dateTo": stop.isoformat() if stop else None,
        "product": products,
        "showLegacy": True,
    }
    res = requests.get(url=url, params=payload)
    res.raise_for_status()
    metadata = res.json()
    return metadata


def _get_model_metadata(
    site: str,
    start: datetime.date | None,
    stop: datetime.date | None,
) -> list:
    url = "https://cloudnet.fmi.fi/api/model-files"
    payload = {
        "site": site,
        "dateFrom": start.isoformat() if start else None,
        "dateTo": stop.isoformat() if stop else None,
    }
    res = requests.get(url=url, params=payload)
    res.raise_for_status()
    metadata = res.json()
    return metadata


def _process_metadata(
    submitter: Callable[[Path, dict], str],
    upload_metadata: list,
    args: argparse.Namespace,
):
    def process_row(row: dict, args: argparse.Namespace):
        filename = _download_file(row)
        response = submitter(filename, row)
        if not args.save:
            filename.unlink()
        return response

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filename = {
            executor.submit(process_row, row, args): row["filename"]
            for row in upload_metadata
        }
        completed = 0
        for future in concurrent.futures.as_completed(future_to_filename):
            completed += 1
            filename = future_to_filename[future]
            info = f"{completed}/{len(upload_metadata)} {filename}"
            try:
                response = future.result()
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
    elif "instrumentPid" in row and row["instrumentPid"] is not None:
        subdir = row["instrument"]["id"] + "-" + row["instrumentPid"].split(".")[-1][:8]
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
        metadata["instrument"] = row["instrument"]["id"]
        metadata["instrumentPid"] = row["instrumentPid"]
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
    ss_headers = {"Content-MD5": utils.md5sum(filename, is_base64=True)}
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
        dp_body["instrument"] = dp_body["instrument"]["id"]
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

    return "OK"


def _fetch_calibration(upload_metadata: list):
    first = True
    processed_pid_dates: set[tuple] = set()
    for upload in upload_metadata:
        if (upload["instrumentPid"], upload["measurementDate"]) in processed_pid_dates:
            continue
        params = {
            "instrumentPid": upload["instrumentPid"],
            "date": upload["measurementDate"],
        }
        res = requests.get("https://cloudnet.fmi.fi/api/calibration", params=params)
        if res.status_code == 404:
            continue
        if first:
            print("\nCalibration:\n")
            first = False
        print(upload["instrumentPid"], upload["measurementDate"])
        res.raise_for_status()
        res = requests.put(
            f"{DATAPORTAL_URL}/api/calibration",
            params=params,
            json=res.json()["data"],
            auth=("admin", "admin"),
        )
        res.raise_for_status()
        processed_pid_dates.add((upload["instrumentPid"], upload["measurementDate"]))


def add_arguments(subparser):
    fetch_parser = subparser.add_parser("fetch", help="Fetch raw data to dev.")
    fetch_parser.add_argument(
        "-i",
        "--instruments",
        type=lambda s: s.split(","),
        help="Instrument types, e.g. cl51,hatpro",
        nargs="?",
        const="all",
    )
    fetch_parser.add_argument(
        "-m",
        "--models",
        type=lambda s: s.split(","),
        help="Model types, e.g. ecmwf",
        nargs="?",
        const="all",
    )
    fetch_parser.add_argument(
        "--include", help="Instrument file regex include pattern", type=str
    )
    fetch_parser.add_argument(
        "--exclude",
        help="Instrument file regex exclude pattern",
        type=str,
        default=".lv0$",
    )
    fetch_parser.add_argument(
        "--save",
        action="store_true",
        default=False,
        help="Also save instrument files to download/",
    )
    return subparser
