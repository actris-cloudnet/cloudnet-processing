from __future__ import annotations

import calendar
import datetime
from dataclasses import dataclass
from typing import Generator, Iterable, Literal, Protocol, Self, runtime_checkable

PeriodName = Literal["day", "week", "month", "year", "all"]


@runtime_checkable
class PeriodProtocol(Protocol):
    @classmethod
    def now(cls) -> Self: ...

    @classmethod
    def range(cls, start: Self, stop: Self) -> Iterable[Self]: ...

    @classmethod
    def to_str(cls) -> PeriodName: ...

    def to_interval(self) -> tuple[datetime.date, datetime.date]: ...
    def to_interval_padded(self, days: int) -> tuple[datetime.date, datetime.date]: ...

    def start(self) -> datetime.date: ...


@dataclass(order=True, frozen=True)
class Day:
    date: datetime.date

    @classmethod
    def from_str(cls, date_str: str) -> Day:
        return cls(datetime.date.fromisoformat(date_str))

    @classmethod
    def to_str(cls) -> Literal["day"]:
        return "day"

    @classmethod
    def range(cls, start: Day, stop: Day) -> Generator[Day, None, None]:
        current = start.date
        while current <= stop.date:
            yield Day(current)
            current += datetime.timedelta(days=1)

    @classmethod
    def now(cls) -> Day:
        return cls(datetime.date.today())

    def start(self) -> datetime.date:
        return self.date

    def to_interval(self) -> tuple[datetime.date, datetime.date]:
        return self.date, self.date

    def to_interval_padded(self, days: int) -> tuple[datetime.date, datetime.date]:
        return pad_interval(self.to_interval(), days)

    def __repr__(self) -> str:
        return f"Day({self.date})"


@dataclass(order=True, frozen=True)
class Week:
    year: int
    week: int

    @classmethod
    def from_str(cls, week_str: str) -> Week:
        dt = datetime.datetime.strptime(week_str + "-1", "%G-%V-%u")
        cal = dt.isocalendar()
        return cls(cal.year, cal.week)

    @classmethod
    def to_str(cls) -> Literal["week"]:
        return "week"

    @classmethod
    def range(cls, start: Week, stop: Week) -> Generator[Week, None, None]:
        current_date = datetime.date.fromisocalendar(start.year, start.week, 1)
        stop_date = datetime.date.fromisocalendar(stop.year, stop.week, 1)
        while current_date <= stop_date:
            iso_year, iso_week, _ = current_date.isocalendar()
            yield cls(iso_year, iso_week)
            current_date += datetime.timedelta(weeks=1)

    @classmethod
    def now(cls) -> Week:
        today = datetime.date.today()
        cal = today.isocalendar()
        return cls(cal.year, cal.week)

    def start(self) -> datetime.date:
        return datetime.date.fromisocalendar(self.year, self.week, 1)

    def to_interval(self) -> tuple[datetime.date, datetime.date]:
        start = self.start()
        stop = datetime.date.fromisocalendar(self.year, self.week, 7)
        return start, stop

    def to_interval_padded(self, days: int) -> tuple[datetime.date, datetime.date]:
        return pad_interval(self.to_interval(), days)

    def __repr__(self) -> str:
        return f"Week({self.year}-{self.week:02})"


@dataclass(order=True, frozen=True)
class Month:
    year: int
    month: int

    @classmethod
    def from_str(cls, month_str: str) -> Month:
        dt = datetime.date.fromisoformat(month_str + "-01")
        return cls(dt.year, dt.month)

    @classmethod
    def to_str(cls) -> Literal["month"]:
        return "month"

    @classmethod
    def range(cls, start: Month, stop: Month) -> Generator[Month, None, None]:
        start_months = start.year * 12 + start.month - 1
        stop_months = stop.year * 12 + stop.month - 1
        for month in range(start_months, stop_months + 1):
            y, m = divmod(month, 12)
            yield cls(y, m + 1)

    @classmethod
    def now(cls) -> Month:
        today = datetime.date.today()
        return cls(today.year, today.month)

    def start(self) -> datetime.date:
        return datetime.date(self.year, self.month, 1)

    def to_interval(self) -> tuple[datetime.date, datetime.date]:
        start = self.start()
        _, ndays = calendar.monthrange(self.year, self.month)
        stop = datetime.date(self.year, self.month, ndays)
        return start, stop

    def to_interval_padded(self, days: int) -> tuple[datetime.date, datetime.date]:
        return pad_interval(self.to_interval(), days)

    def __repr__(self) -> str:
        return f"Month({self.year}-{self.month:02})"


@dataclass(order=True, frozen=True)
class Year:
    year: int

    @classmethod
    def from_str(cls, year_str: str) -> Year:
        dt = datetime.date.fromisoformat(year_str + "-01-01")
        return cls(dt.year)

    @classmethod
    def to_str(cls) -> Literal["year"]:
        return "year"

    @classmethod
    def range(cls, start: Year, stop: Year) -> Generator[Year, None, None]:
        for year in range(start.year, stop.year):
            yield cls(year)

    @classmethod
    def now(cls) -> Year:
        return cls(datetime.date.today().year)

    def start(self) -> datetime.date:
        return datetime.date(self.year, 1, 1)

    def to_interval(self) -> tuple[datetime.date, datetime.date]:
        start = self.start()
        stop = datetime.date(self.year, 12, 31)
        return start, stop

    def to_interval_padded(self, days: int) -> tuple[datetime.date, datetime.date]:
        return pad_interval(self.to_interval(), days)

    def __repr__(self) -> str:
        return f"Year({self.year})"


class All:
    @classmethod
    def to_str(cls) -> Literal["all"]:
        return "all"

    def __repr__(self) -> str:
        return f"All"


PeriodType = PeriodProtocol | All


def period_cls_from_str(x: str) -> type[PeriodType]:
    match x:
        case "day":
            return Day
        case "week":
            return Week
        case "month":
            return Month
        case "year":
            return Year
        case "all":
            return All
        case _:
            raise ValueError


def period_str_from_cls(
    cls: type[PeriodType],
) -> PeriodName:
    if cls is Day:
        return "day"
    elif cls is Week:
        return "week"
    elif cls is Month:
        return "month"
    elif cls is Year:
        return "year"
    elif cls is All:
        return "all"
    else:
        raise ValueError


def pad_interval(
    interval_in: tuple[datetime.date, datetime.date], days: int
) -> tuple[datetime.date, datetime.date]:
    start_in, stop_in = interval_in
    padding = datetime.timedelta(days=days)
    start_out = start_in - padding
    stop_out = stop_in + padding
    return start_out, stop_out
