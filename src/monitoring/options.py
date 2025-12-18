from typing import Generator
from argparse import Namespace
import datetime
from dataclasses import dataclass


@dataclass
class CommonOptions:
    sites: list[str]
    instruments: list[str]
    products: list[str]


@dataclass
class AllOptions:
    common: CommonOptions


@dataclass
class YearOptions:
    common: CommonOptions
    periods: list[int]


@dataclass(order=True, frozen=True)
class Month:
    year: int
    month: int

    @classmethod
    def range(cls, start: "Month", stop: "Month") -> Generator["Month"]:
        start_months = start.year * 12 + start.month - 1
        stop_months = stop.year * 12 + stop.month - 1
        for month in range(start_months, stop_months + 1):
            y, m = divmod(month, 12)
            yield (cls(y, m + 1))


@dataclass
class MonthOptions:
    common: CommonOptions
    periods: list[Month]


@dataclass(order=True, frozen=True)
class Week:
    year: int
    week: int

    @classmethod
    def range(cls, start: "Week", stop: "Week") -> Generator["Week"]:
        current_date = datetime.date.fromisocalendar(start.year, start.week, 1)
        stop_date = datetime.date.fromisocalendar(stop.year, stop.week, 1)
        while current_date <= stop_date:
            iso_year, iso_week, _ = current_date.isocalendar()
            yield cls(iso_year, iso_week)
            current_date += datetime.timedelta(weeks=1)


@dataclass
class WeekOptions:
    common: CommonOptions
    periods: list[Week]


@dataclass
class DayOptions:
    common: CommonOptions
    periods: list[datetime.date]


Options = AllOptions | YearOptions | MonthOptions | WeekOptions | DayOptions


def build_opts(args: Namespace):
    common = CommonOptions(
        sites=args.site or [],
        instruments=args.instrument or [],
        products=args.product or [],
    )
    raw_periods = args.period or []
    match args.period_type:
        case "all":
            return AllOptions(common)
        case "year":
            periods = [_year(x) for x in raw_periods]
            start = _year(args.start) if args.start else None
            stop = _year(args.stop) if args.stop else None
            periods_combined = _year_list(start, stop, periods)
            return YearOptions(common, periods_combined)
        case "month":
            periods = [_month(x) for x in raw_periods]
            start = _month(args.start) if args.start else None
            stop = _month(args.stop) if args.stop else None
            periods_combined = _month_list(start, stop, periods)
            return MonthOptions(common, periods_combined)
        case "week":
            periods = [_week(x) for x in raw_periods]
            start = _week(args.start) if args.start else None
            stop = _week(args.stop) if args.stop else None
            periods_combined = _week_list(start, stop, periods)
            return WeekOptions(common, periods_combined)
        case "day":
            periods = [_day(x) for x in raw_periods]
            start = _day(args.start) if args.start else None
            stop = _day(args.stop) if args.stop else None
            periods_combined = _day_list(start, stop, periods)
            return DayOptions(common, periods_combined)
        case _:
            raise ValueError(f"Unexpected period {args.period_type}")


def _year_list(start: int | None, stop: int | None, periods: list[int]) -> list[int]:
    if start is None and stop is None and not periods:
        return [datetime.date.today().year]
    if start is None and stop is None and periods:
        return list(set(periods))
    if start is None:
        raise ValueError("start must be defined")
    if stop is None:
        stop = datetime.date.today().year
    return sorted(list(set(list(range(start, stop + 1)) + periods)))


def _month_list(
    start: Month | None, stop: Month | None, periods: list[Month]
) -> list[Month]:
    today = datetime.date.today()
    if start is None and stop is None and not periods:
        return [Month(today.year, today.month)]
    if start is None and stop is None and periods:
        return list(set(periods))
    if start is None:
        raise ValueError("start must be defined")
    if stop is None:
        stop = Month(today.year, today.month)
    months = sorted(list(set(list(Month.range(start, stop)) + periods)))
    return months


def _week_list(
    start: Week | None, stop: Week | None, periods: list[Week]
) -> list[Week]:
    today = datetime.date.today()
    cal = today.isocalendar()
    if start is None and stop is None and not periods:
        return [Week(cal.year, cal.week)]
    if start is None and stop is None and periods:
        return list(set(periods))
    if start is None:
        raise ValueError("start must be defined")
    if stop is None:
        stop = Week(cal.year, cal.week)
    weeks = sorted(list(set(list(Week.range(start, stop)) + periods)))
    return weeks


def _day_list(
    start: datetime.date | None,
    stop: datetime.date | None,
    periods: list[datetime.date],
) -> list[datetime.date]:
    today = datetime.date.today()
    if start is None and stop is None and not periods:
        return [today]
    if start is None and stop is None and periods:
        return list(set(periods))
    if start is None:
        raise ValueError("start must be defined")
    if stop is None:
        stop = today
    day_range = []
    if stop >= start:
        delta_days = (stop - start).days
        day_range = [start + datetime.timedelta(days=i) for i in range(delta_days + 1)]
    days = sorted(list(set(day_range + periods)))

    return days


def _year(x: str) -> int:
    date = datetime.date.fromisoformat(x + "-01-01")
    return date.year


def _month(x: str) -> Month:
    date = datetime.date.fromisoformat(x + "-01")
    return Month(date.year, date.month)


def _week(x: str) -> Week:
    date = datetime.datetime.strptime(x + "_1", "%G-w%V_%u")
    cal = date.isocalendar()
    return Week(cal.year, cal.week)


def _day(x: str) -> datetime.date:
    return datetime.date.fromisoformat(x)
