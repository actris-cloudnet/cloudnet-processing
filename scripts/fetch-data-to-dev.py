#!/usr/bin/env python3
import argparse
import datetime
import os
import sys

import requests


def main(args: argparse.Namespace):
    params = {"site": args.site, "instruments": args.instruments}
    if args.date and (args.start or args.stop):
        print("Using --date with --start or --stop is not allowed", file=sys.stderr)
        sys.exit(1)
    if args.date:
        params["start"] = args.date
        params["stop"] = args.date
    elif args.start or args.stop:
        params["start"] = args.start
        params["stop"] = args.stop
    else:
        print("Please specify --date, --start or --stop", file=sys.stderr)
        sys.exit(1)
    if args.extension:
        params["extension"] = args.extension

    upload_metadata = _get_metadata("raw-files", **params)
    if upload_metadata:
        print("\nMeasurement files:\n")
    for i, row in enumerate(upload_metadata, start=1):
        info = f'{i}/{len(upload_metadata)} {row["filename"]}'
        print(info, end="\r")
        metadata = {
            key: row[key]
            for key in (
                "filename",
                "checksum",
                "instrument",
                "instrumentPid",
                "measurementDate",
            )
        }
        metadata["instrument"]["auxiliary"] = False
        metadata["site"] = params["site"]
        filename = _download_file(row)
        _submit_to_local_ss("upload", filename, metadata, info)
        if not args.save:
            os.remove(filename)

    if params["instruments"] is not None and "model" not in params["instruments"]:
        return
    del params["instruments"]
    upload_metadata = _get_metadata("raw-model-files", **params)
    if upload_metadata:
        print("\nModel files:\n")
    for i, row in enumerate(upload_metadata, start=1):
        info = f'{i}/{len(upload_metadata)} {row["filename"]}'
        print(info, end="\r")
        metadata = {key: row[key] for key in ("filename", "checksum", "measurementDate")}
        metadata["site"] = params["site"]
        metadata["model"] = row["model"]["id"]
        filename = _download_file(row)
        _submit_to_local_ss("model-upload", filename, metadata, info)
        if not args.save:
            os.remove(filename)


def _get_metadata(
    end_point: str,
    site: str,
    start: datetime.date | None,
    stop: datetime.date | None,
    instruments: list[str] = [],
    extension: str | None = None,
) -> list:
    url = f"https://cloudnet.fmi.fi/api/{end_point}"
    payload = {
        "site": site,
        "dateFrom": start.isoformat() if start else None,
        "dateTo": stop.isoformat() if stop else None,
        "instrument": instruments,
    }
    metadata = requests.get(url=url, params=payload).json()
    if extension:
        extension = extension.lower()
        metadata = [row for row in metadata if row["filename"].lower().endswith(extension)]
    metadata = [row for row in metadata if not row["filename"].lower().endswith(".lv0")]
    return metadata


def _download_file(row: dict):
    dir_name = "download/"
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)
    res = requests.get(row["downloadUrl"])
    filename = f'{dir_name}{row["filename"]}'
    with open(filename, "wb") as f:
        f.write(res.content)
    return filename


def _submit_to_local_ss(end_point: str, filename, metadata: dict, info: str):
    auth = ("admin", "admin")
    url = f"http://localhost:3000/{end_point}/"
    res = requests.post(f"{url}metadata", json=metadata, auth=auth)
    if res.status_code != 200:
        print(f"{info} {res.text}", flush=True)
    else:
        res = requests.put(
            f'{url}data/{metadata["checksum"]}', data=open(filename, "rb"), auth=auth
        )
        print(f"{info} {res.text}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("site", type=str)
    parser.add_argument(
        "-d",
        "--date",
        type=datetime.date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="Single date",
    )
    parser.add_argument(
        "--start",
        type=datetime.date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="Starting date",
    )
    parser.add_argument(
        "--stop",
        type=datetime.date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="Stopping date",
    )
    parser.add_argument("-i", "--instruments", type=lambda s: s.split(","), help="Instrument types")
    parser.add_argument(
        "-e", "--extension", help="Instrument file extension, e.g., -e=.LV1", type=str
    )
    parser.add_argument(
        "-s",
        "--save",
        action="store_true",
        default=False,
        help="Also save instrument files to download/",
    )
    args = parser.parse_args()
    main(args)
