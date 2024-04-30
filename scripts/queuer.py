#!/usr/bin/env python3

import argparse
import datetime
import re
import sys
from collections.abc import Callable
from typing import NamedTuple, TypeVar
from uuid import UUID

import requests

# API_URL = "https://cloudnet.fmi.fi/api"
API_URL = "http://localhost:3000/api"
API_AUTH = ("admin", "admin")


class Task(NamedTuple):
    date: datetime.date
    product_id: str
    site_id: str
    instrument_uuid: UUID


def main():
    args = parse_args()

    res = requests.get(f"{API_URL}/instrument-pids/")
    res.raise_for_status()
    instrument_uuid_to_pid = {UUID(item["uuid"]): item["pid"] for item in res.json()}

    print("Query metadata...")
    tasks = set()
    payload = {"dateFrom": args.start.isoformat(), "dateTo": args.stop.isoformat()}
    if args.sites:
        payload["site[]"] = args.sites
    if args.types:
        payload["instrument[]"] = args.types
    if args.instruments:
        payload["instrumentPid[]"] = [
            instrument_uuid_to_pid[uuid] for uuid in args.instruments
        ]
    res = requests.get(f"{API_URL}/raw-files", params=payload)
    res.raise_for_status()
    for item in res.json():
        for product in args.products:
            task = Task(
                date=datetime.date.fromisoformat(item["measurementDate"]),
                product_id=product,
                site_id=item["site"]["id"],
                instrument_uuid=UUID(item["instrumentInfo"]["uuid"]),
            )
            tasks.add(task)

    if not tasks:
        print("No tasks!")
    else:
        for i, task in enumerate(tasks):
            print(f"Publish task ({i+1}/{len(tasks)}):", task)
            res = requests.post(
                f"{API_URL}/queue/publish",
                json={
                    "type": "process",
                    "siteId": task.site_id,
                    "productId": task.product_id,
                    "instrumentInfoUuid": str(task.instrument_uuid),
                    "measurementDate": task.date.isoformat(),
                },
                auth=API_AUTH,
            )
            res.raise_for_status()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Cloudnet processing queue administration utility.",
        epilog="Please wait for your turn!",
    )
    parser.add_argument(
        "-s",
        "--sites",
        help="Sites to process data from, e.g. hyytiala.",
        type=list_parser(str),
    )
    parser.add_argument(
        "-p",
        "--products",
        help="Products to be processed, e.g., radar,lidar,mwr.",
        type=list_parser(str),
        required=True,
    )
    instrument_group = parser.add_mutually_exclusive_group(required=True)
    instrument_group.add_argument(
        "-t",
        "--types",
        help="Instrument types to be processed, e.g., mira,chm15k,hatpro.",
        type=list_parser(str),
    )
    instrument_group.add_argument(
        "-i",
        "--instruments",
        help="Instrument UUIDs to be processed.",
        type=list_parser(UUID),
    )
    parser.add_argument(
        "--start",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Starting date. Default is five days ago.",
    )
    parser.add_argument(
        "--stop",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Stopping date. Default is current day.",
    )
    parser.add_argument(
        "-d",
        "--date",
        type=parse_date,
        metavar="YYYY-MM-DD",
        help="Date to be processed.",
    )

    args = parser.parse_args()

    if args.date and (args.start or args.stop):
        print("Cannot use --date with --start and --stop", file=sys.stderr)
        sys.exit(1)
    if args.date:
        args.start = args.date
        args.stop = args.date
    else:
        if not args.start:
            args.start = utctoday() - datetime.timedelta(days=5)
        if not args.stop:
            args.stop = utctoday()
        if args.start > args.stop:
            print("--start should be before --stop", file=sys.stderr)
            sys.exit(1)
    del args.date

    return args


T = TypeVar("T")


def list_parser(type: Callable[[str], T]) -> Callable[[str], list[T]]:
    return lambda value: [type(x) for x in value.split(",")]


def parse_date(value: str) -> datetime.date:
    if value == "today":
        return utctoday()
    if value == "yesterday":
        return utctoday() - datetime.timedelta(days=1)
    if match := re.fullmatch("(\d+)d", value):
        return utctoday() - datetime.timedelta(days=int(match[1]))
    return datetime.date.fromisoformat(value)


def utctoday() -> datetime.date:
    return datetime.datetime.now(datetime.timezone.utc).date()


if __name__ == "__main__":
    main()
