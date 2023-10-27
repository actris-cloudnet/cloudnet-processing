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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=Path)
    parser.add_argument("-s", "--site", required=True)
    parser.add_argument("-i", "--instrument", required=True)
    parser.add_argument("-d", "--date", required=True, type=datetime.date.fromisoformat)
    parser.add_argument("--pid", required=True)
    args = parser.parse_args()

    md5_hash = hashlib.md5()
    with open(args.filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)

    checksum = md5_hash.hexdigest()

    metadata = {
        "filename": args.filename.name,
        "checksum": checksum,
        "measurementDate": args.date.isoformat(),
        "instrument": args.instrument,
        "instrumentPid": args.pid,
        "site": args.site,
    }

    print(f"Uploading {args.filename.name}", end="\t")
    res = requests.post(
        f"{BACKEND_URL}/upload/metadata/", json=metadata, auth=(USERNAME, PASSWORD)
    )

    if res.status_code != 200:
        print(res.text)
        sys.exit(1)

    with open(args.filename, "rb") as f:
        res = requests.put(
            f"{BACKEND_URL}/upload/data/{checksum}", data=f, auth=(USERNAME, PASSWORD)
        )
    print(res.text)
    if not res.ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
