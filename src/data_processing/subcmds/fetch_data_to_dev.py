import argparse
import concurrent.futures
import datetime
import logging
import os
import sys
from pathlib import Path

import requests

from data_processing import utils

DATAPORTAL_URL = os.environ["DATAPORTAL_URL"].rstrip("/")
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

    DOWNLOAD_DIR.mkdir(exist_ok=True)

    instruments = (
        [i for i in args.instruments if i != "model"]
        if args.instruments is not None
        else None
    )
    if instruments is None or len(instruments) > 0:
        upload_metadata = _get_metadata("raw-files", **params, instruments=instruments)
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
            _process_metadata(upload_metadata, args)
            print("\nCalibration:\n")
            _fetch_calibration(upload_metadata)

    if args.instruments is None or "model" in args.instruments:
        upload_metadata = _get_metadata("raw-model-files", **params)
        if upload_metadata:
            print("\nModel files:\n")
            _process_metadata(upload_metadata, args)


def _get_metadata(
    end_point: str,
    site: str,
    start: datetime.date | None,
    stop: datetime.date | None,
    instruments: list[str] | None = None,
) -> list:
    url = f"https://cloudnet.fmi.fi/api/{end_point}"
    payload = {
        "site": site,
        "dateFrom": start.isoformat() if start else None,
        "dateTo": stop.isoformat() if stop else None,
        "instrument": instruments,
        "status": ["uploaded", "processed"],
    }
    res = requests.get(url=url, params=payload)
    res.raise_for_status()
    metadata = res.json()
    return metadata


def _process_metadata(upload_metadata: list, args: argparse.Namespace):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filename = {
            executor.submit(_process_row, row, args): row["filename"]
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


def _process_row(row: dict, args: argparse.Namespace):
    filename = _download_file(row)
    response = _submit_to_local_ss(filename, row)
    if not args.save:
        filename.unlink()
    return response


def _download_file(row: dict) -> Path:
    res = requests.get(row["downloadUrl"])
    res.raise_for_status()
    if "instrumentPid" in row:
        subdir = row["instrument"]["id"] + "-" + row["instrumentPid"].split(".")[-1][:8]
    elif "model" in row:
        subdir = "model-" + row["model"]["id"]
    else:
        raise ValueError("Row does not contain instrument or model.")
    outdir = DOWNLOAD_DIR / subdir
    outdir.mkdir(exist_ok=True, parents=True)
    filename = outdir / row["filename"]
    filename.write_bytes(res.content)
    return filename


def _submit_to_local_ss(filename: Path, row: dict):
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
    auth = ("admin", "admin")
    url = f"{DATAPORTAL_URL}/{end_point}/"
    res = requests.post(f"{url}metadata", json=metadata, auth=auth)
    if res.status_code == 200:
        with filename.open("rb") as f:
            res = requests.put(f'{url}data/{metadata["checksum"]}', data=f, auth=auth)
            res.raise_for_status()
    elif res.status_code != 409:
        error = res.content.decode("utf-8", errors="replace")
        raise Exception(f"{res.status_code} {res.reason}: {error}")
    return res.text


def _fetch_calibration(upload_metadata: list):
    processed_pids = set()
    for upload in upload_metadata:
        if upload["instrumentPid"] in processed_pids:
            continue
        processed_pids.add(upload["instrumentPid"])
        params = {
            "instrumentPid": upload["instrumentPid"],
            "date": upload["measurementDate"],
        }
        res = requests.get("https://cloudnet.fmi.fi/api/calibration", params=params)
        if res.status_code == 404:
            continue
        print(upload["instrumentPid"])
        res.raise_for_status()
        res = requests.put(
            f"{DATAPORTAL_URL}/api/calibration",
            params=params,
            json=res.json()["data"],
            auth=("admin", "admin"),
        )
        res.raise_for_status()


def add_arguments(subparser):
    fetch_parser = subparser.add_parser("fetch", help="Fetch raw data to dev.")
    fetch_parser.add_argument(
        "-i",
        "--instruments",
        type=lambda s: s.split(","),
        help="Instrument types, e.g. cl51,hatpro",
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
