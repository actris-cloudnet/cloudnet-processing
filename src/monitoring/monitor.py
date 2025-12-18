from pprint import pprint
from cloudnet_api_client import APIClient
from processing.config import Config

from monitoring.options import (
    AllOptions,
    DayOptions,
    MonthOptions,
    Options,
    WeekOptions,
    YearOptions,
)


def monitor(opts: Options, config: Config, client: APIClient) -> None:
    match opts:
        case AllOptions():
            monitor_all(opts)
        case YearOptions():
            monitor_year(opts)
        case MonthOptions():
            monitor_month(opts)
        case WeekOptions():
            monitor_week(opts)
        case DayOptions():
            monitor_day(opts)


def monitor_all(opts: AllOptions):
    pprint(opts)


def monitor_year(opts: YearOptions):
    pprint(opts)


def monitor_month(opts: MonthOptions):
    pprint(opts)


def monitor_week(opts: WeekOptions):
    pprint(opts)


def monitor_day(opts: DayOptions):
    pprint(opts)
