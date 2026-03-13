#!/usr/bin/env python3

import argparse
import datetime
import hashlib
import sys
from pathlib import Path

import requests

BACKEND_URL = "http://localhost:3000"
USERNAME = "admin"
PASSWORD = "admin"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=Path)
    parser.add_argument("-s", "--site", required=True)
    parser.add_argument("-i", "--instrument")
    parser.add_argument("--pid")
    parser.add_argument("-m", "--model")
    parser.add_argument("-d", "--date", required=True, type=datetime.date.fromisoformat)
    args = parser.parse_args()

    if not args.model and not (args.instrument or args.pid):
        parser.error("specify --model or --instrument and --pid")
    if (args.instrument or args.pid) and not (args.instrument and args.pid):
        parser.error("both --instrument and --pid are required")
    if args.model and args.instrument:
        parser.error("specify either --model or --instrument")

    md5_hash = hashlib.md5()
    with open(args.filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)

    checksum = md5_hash.hexdigest()

    metadata = {
        "filename": args.filename.name,
        "checksum": checksum,
        "measurementDate": args.date.isoformat(),
        "site": args.site,
    }
    if args.instrument:
        metadata["instrument"] = args.instrument
        metadata["instrumentPid"] = args.pid
        endpoint = "upload"
    else:
        metadata["model"] = args.model
        endpoint = "model-upload"

    print(f"Uploading {args.filename.name}", end="\t")
    res = requests.post(
        f"{BACKEND_URL}/{endpoint}/metadata/", json=metadata, auth=(USERNAME, PASSWORD)
    )

    if res.status_code != 200:
        print(res.text)
        sys.exit(1)

    with open(args.filename, "rb") as f:
        res = requests.put(
            f"{BACKEND_URL}/{endpoint}/data/{checksum}",
            data=f,
            auth=(USERNAME, PASSWORD),
        )
    print(res.text)
    if not res.ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
