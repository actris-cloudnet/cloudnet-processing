from pprint import pprint
from monitoring.period import PeriodWithRangeType
from monitoring.period import All
from monitoring.period import Year
from monitoring.period import Day
import argparse
import itertools
from argparse import ArgumentParser, Namespace
from typing import Callable, Iterable, TypeVar

from monitoring.config import CONFIG
from monitoring.period import Month, Week
from monitoring.monitor import monitor

T = TypeVar("T")


def main() -> None:
    args = _get_args()
    match args.cmd:
        case "day":
            _monitor(args, Day, args.date, "day")
        case "week":
            _monitor(args, Week, args.week, "week")
        case "month":
            _monitor(args, Month, args.month, "month")
        case "year":
            _monitor(args, Year, args.year, "year")
        case "all":
            _monitor_all(args)


def _monitor(
    args: Namespace,
    period_cls: PeriodWithRangeType,
    explicit_periods: list[T] | None,
    period_key,
):
    periods = _resolve_periods(
        args.start, args.stop, explicit_periods, period_cls.now, period_cls.range
    )
    products = _resolve_products(period_key, args.product, args.instrument)
    for period, (instrument, product) in itertools.product(periods, products):
        monitor(period, instrument, product)


def _monitor_all(
    args: Namespace,
):
    products = _resolve_products("all", args.product, args.instrument)
    for instrument, product in products:
        monitor(All(), instrument, product)


def _monitor_day(args: Namespace) -> None:
    periods = _resolve_periods(args.start, args.stop, args.date, Day.now, Day.range)
    products = _resolve_products("day", args.product, args.instrument)
    for period, (instrument, product) in itertools.product(periods, products):
        monitor(period, instrument, product)


def _monitor_week(args: Namespace) -> None:
    periods = _resolve_periods(args.start, args.stop, args.week, Week.now, Week.range)
    products = _resolve_products("week", args.product, args.instrument)
    for period, (instrument, product) in itertools.product(periods, products):
        monitor(period, instrument, product)


def _monitor_month(args: Namespace) -> None:
    periods = _resolve_periods(
        args.start, args.stop, args.month, Month.now, Month.range
    )
    products = _resolve_products("month", args.product, args.instrument)
    for period, (instrument, product) in itertools.product(periods, products):
        monitor(period, instrument, product)


def _monitor_year(args: Namespace) -> None:
    periods = _resolve_periods(args.start, args.stop, args.year, Year.now, Year.range)
    products = _resolve_products("month", args.product, args.instrument)
    for period, (instrument, product) in itertools.product(periods, products):
        monitor(period, instrument, product)


def _monitor_all(args: Namespace) -> None:
    periods = [All()]
    products = _resolve_products("month", args.product, args.instrument)
    for period, (instrument, product) in itertools.product(periods, products):
        monitor(period, instrument, product)


def _resolve_products(
    period: str, products: list[str] | None, instruments: list[str] | None
) -> list[tuple[str, str]]:
    config = CONFIG
    product_tuples = []
    for inst, inst_opts in config.items():
        for prod, period_list in inst_opts.items():
            for p in period_list:
                product_tuples.append((inst, prod, p))
    products_filtered = [
        (inst, prod) for inst, prod, p in product_tuples if p == period
    ]
    if products:
        products_filtered = [
            (inst, prod) for inst, prod in products_filtered if prod in products
        ]
    if instruments:
        products_filtered = [
            (inst, prod) for inst, prod in products_filtered if inst in instruments
        ]
    return products_filtered


def _get_args() -> Namespace:
    parser = ArgumentParser()
    subp = parser.add_subparsers(dest="cmd", required=True)
    _build_day_args(subp.add_parser("day"))
    _build_week_args(subp.add_parser("week"))
    _build_month_args(subp.add_parser("month"))
    _build_year_args(subp.add_parser("year"))
    _build_all_args(subp.add_parser("all"))
    return parser.parse_args()


def _build_day_args(parser: ArgumentParser) -> None:
    parser.add_argument("--start", type=_parse_day)
    parser.add_argument("--stop", type=_parse_day)
    parser.add_argument("--date", type=_parse_day, nargs="*")
    _common(parser)


def _build_week_args(parser: ArgumentParser) -> None:
    parser.add_argument("--start", type=_parse_week)
    parser.add_argument("--stop", type=_parse_week)
    parser.add_argument("--week", type=_parse_week, nargs="*")
    _common(parser)


def _build_month_args(parser: ArgumentParser) -> None:
    parser.add_argument("--start", type=_parse_month)
    parser.add_argument("--stop", type=_parse_month)
    parser.add_argument("--month", type=_parse_month, nargs="*")
    _common(parser)


def _build_year_args(parser: ArgumentParser) -> None:
    parser.add_argument("--start", type=_parse_year)
    parser.add_argument("--stop", type=_parse_year)
    parser.add_argument("--year", type=_parse_year, nargs="*")
    _common(parser)


def _build_all_args(parser: ArgumentParser) -> None:
    _common(parser)


def _common(parser: ArgumentParser) -> None:
    parser.add_argument("--product", nargs="*")
    parser.add_argument("--instrument", nargs="*")


def _parse_year(year_str: str) -> Year:
    try:
        return Year.from_str(year_str)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid year format: '{year_str}'. Expected format: YYYY"
        )


def _parse_month(month_str: str) -> Month:
    try:
        return Month.from_str(month_str)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid month format: '{month_str}'. Expected format: YYYY-MM"
        )


def _parse_week(week_str: str) -> Week:
    try:
        return Week.from_str(week_str)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid week format: '{week_str}'. Expected format: YYYY-VV"
        )


def _parse_day(date_str: str) -> Day:
    try:
        return Day.from_str(date_str)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date: '{date_str}'. Expected format: YYYY-MM-DD"
        )


def _resolve_periods(
    start: T | None,
    stop: T | None,
    explicit: list[T] | None,
    default_factory: Callable[[], T],
    range_func: Callable[[T, T], Iterable[T]],
) -> list[T]:
    if not start and not stop and not explicit:
        return [default_factory()]
    if not start and not stop and explicit is not None:
        return sorted(list(set(explicit)))
    if start is None:
        raise ValueError
    if stop is None:
        stop = default_factory()
    range_ = list(range_func(start, stop))
    combined = set(range_)
    if explicit:
        combined.update(explicit)
    return sorted(list(combined))


if __name__ == "__main__":
    main()
