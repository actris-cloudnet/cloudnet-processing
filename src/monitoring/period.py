import datetime
from dataclasses import dataclass


@dataclass(order=True, frozen=True)
class Day:
    date: datetime.date

    @classmethod
    def from_str(cls, date_str: str) -> "Day":
        return cls(datetime.date.fromisoformat(date_str))

    @classmethod
    def range(cls, start: "Day", stop: "Day"):
        current = start.date
        while current <= stop.date:
            yield Day(current)
            current += datetime.timedelta(days=1)

    @classmethod
    def now(cls):
        return cls(datetime.date.today())


@dataclass(order=True, frozen=True)
class Week:
    year: int
    week: int

    @classmethod
    def from_str(cls, week_str: str) -> "Week":
        dt = datetime.datetime.strptime(week_str + "-1", "%G-%V-%u")
        cal = dt.isocalendar()
        return cls(cal.year, cal.week)

    @classmethod
    def range(cls, start: "Week", stop: "Week"):
        current_date = datetime.date.fromisocalendar(start.year, start.week, 1)
        stop_date = datetime.date.fromisocalendar(stop.year, stop.week, 1)
        while current_date <= stop_date:
            iso_year, iso_week, _ = current_date.isocalendar()
            yield cls(iso_year, iso_week)
            current_date += datetime.timedelta(weeks=1)

    @classmethod
    def now(cls):
        today = datetime.date.today()
        cal = today.isocalendar()
        return cls(cal.year, cal.week)


@dataclass(order=True, frozen=True)
class Month:
    year: int
    month: int

    @classmethod
    def from_str(cls, month_str: str) -> "Month":
        dt = datetime.date.fromisoformat(month_str + "-01")
        return cls(dt.year, dt.month)

    @classmethod
    def range(cls, start: "Month", stop: "Month"):
        start_months = start.year * 12 + start.month - 1
        stop_months = stop.year * 12 + stop.month - 1
        for month in range(start_months, stop_months + 1):
            y, m = divmod(month, 12)
            yield cls(y, m + 1)

    @classmethod
    def now(cls):
        today = datetime.date.today()
        return cls(today.year, today.month)


@dataclass(order=True, frozen=True)
class Year:
    year: int

    @classmethod
    def from_str(cls, year_str: str) -> "Year":
        dt = datetime.date.fromisoformat(year_str + "-01-01")
        return cls(dt.year)

    @classmethod
    def range(cls, start: "Year", stop: "Year"):
        for year in range(start.year, stop.year):
            yield cls(year)

    @classmethod
    def now(cls):
        return cls(datetime.date.today().year)


class All:
    pass


PeriodWithRangeType = Day | Week | Month | Year
PeriodType = PeriodWithRangeType | All
