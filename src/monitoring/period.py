import calendar
import datetime
from dataclasses import dataclass
from typing import Literal, cast


class AllPeriod:
    period = "all"

    def __repr__(self) -> str:
        return "AllPeriod"

    def __str__(self) -> str:
        return "all"


ALL_PERIOD = AllPeriod()

PeriodWithRangeType = Literal["year", "month", "week", "day"]


@dataclass
class PeriodWithRange:
    period: PeriodWithRangeType
    start_date: datetime.date

    def __repr__(self) -> str:
        return f"{self.period.capitalize()}({self.start_date})"

    @property
    def end_date(self) -> datetime.date:
        start = self.start_date
        match self.period:
            case "year":
                return datetime.date(start.year, 12, 31)
            case "month":
                last_day = calendar.monthrange(start.year, start.month)[1]
                return datetime.date(start.year, start.month, last_day)
            case "week":
                return start + datetime.timedelta(days=6)
            case "day":
                return start

    def as_interval(self) -> tuple[datetime.date, datetime.date]:
        return (self.start_date, self.end_date)


Period = AllPeriod | PeriodWithRange


def period_from_str(s: str, normalise: bool = True) -> Period:
    s = s.lower().strip()

    if s == "all":
        return ALL_PERIOD
    try:
        period_type, start_str = s.split(":")
    except ValueError as err:
        raise ValueError(
            f"Invalid period format '{s}'. Expected format: "
            "'year|month|week|day:YYYY-MM-DD' or 'all'."
        ) from err
    if period_type not in ("year", "month", "week", "day"):
        raise ValueError(
            f"Invalid period type '{period_type}'. "
            "Expected period types: all|month|week|day"
        )
    period_literal = cast(PeriodWithRangeType, period_type)
    try:
        start_date = datetime.date.fromisoformat(start_str)
    except ValueError as err:
        raise ValueError(
            f"Invalid start date format: '{start_str}'. Expected format: 'YYYY-MM-DD'"
        ) from err
    period = PeriodWithRange(period=period_literal, start_date=start_date)
    if normalise:
        return to_normalised_period(period)
    return period


def to_normalised_period(period: PeriodWithRange) -> PeriodWithRange:
    func = {
        "year": normalise_year,
        "month": normalise_month,
        "week": normalise_week,
        "day": lambda d: d,
    }
    return PeriodWithRange(period.period, func[period.period](period.start_date))


def normalise_year(d: datetime.date) -> datetime.date:
    return datetime.date(d.year, 1, 1)


def normalise_month(d: datetime.date) -> datetime.date:
    return datetime.date(d.year, d.month, 1)


def normalise_week(d: datetime.date) -> datetime.date:
    return d - datetime.timedelta(days=d.weekday())
