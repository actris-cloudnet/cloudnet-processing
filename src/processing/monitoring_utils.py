import argparse
import logging

from monitoring.monitor import monitor
from monitoring.period import (
    DayPeriod,
    MonthPeriod,
    Period,
    WeekPeriod,
    YearPeriod,
)

from processing import utils


def main():
    logging.basicConfig(level=logging.INFO)
    args = _get_args()
    period = Period.from_strings(args.period, args.start)
    monitor(args.instrument_pid, period)


def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("instrument_pid", help="Instrument PID")
    parser.add_argument(
        "period",
        choices=Period.types(),
    )
    parser.add_argument(
        "--start",
        help="Period start date in YYYY-mm-dd format (for periods other than all)",
    )
    return parser.parse_args()


def _build_instrument(pid: str) -> dict:
    instruments = [
        i
        for i in utils.get_from_data_portal_api("/api/instrument-pids")
        if i["pid"] == pid
    ]
    if not instruments:
        raise argparse.ArgumentTypeError(f"Invalid pid: {pid}")
    return instruments[0]


def _get_sites(pid: str, period: Period):
    payload = {"instrumentPid": pid}
    if isinstance(period, (YearPeriod, MonthPeriod, WeekPeriod, DayPeriod)):
        payload["dateFrom"] = str(period.start)
        payload["dateTo"] = str(period.end)
    records = utils.get_from_data_portal_api("/api/raw-files", payload)
    sites = {r["siteId"] for r in records}
    return list(sites)


if __name__ == "__main__":
    main()
