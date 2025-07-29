from abc import ABC, abstractmethod
import datetime
import calendar


class Period(ABC):
    @staticmethod
    def types() -> list[str]:
        return ["all", "year", "month", "week", "day"]

    @staticmethod
    def all() -> "AllPeriod":
        return AllPeriod()

    @staticmethod
    def year(start: datetime.date) -> "YearPeriod":
        return YearPeriod(start)

    @staticmethod
    def month(start: datetime.date) -> "MonthPeriod":
        return MonthPeriod(start)

    @staticmethod
    def week(start: datetime.date) -> "WeekPeriod":
        return WeekPeriod(start)

    @staticmethod
    def day(start: datetime.date) -> "DayPeriod":
        return DayPeriod(start)

    @staticmethod
    def from_strings(period: str, start: str | None = None) -> "Period":
        if period == "all":
            return Period.all()
        if start is None:
            raise ValueError("start must be provided for period type other than 'all'")

        date = datetime.datetime.strptime(start, "%Y-%m-%d").date()
        match period:
            case "year":
                return Period.year(date)
            case "month":
                return Period.month(date)
            case "week":
                return Period.week(date)
            case "day":
                return Period.day(date)
            case _:
                raise ValueError(f"Unsupported period: {period}")

    @abstractmethod
    def as_range(self) -> tuple[datetime.date, datetime.date]: ...


class AllPeriod(Period):
    def __str__(self) -> str:
        return "all"

    def as_range(self) -> tuple[datetime.date, datetime.date]:
        start = datetime.date(1900, 1, 1)
        end = datetime.date.today()
        return (start, end)


class YearPeriod(Period):
    def __init__(self, start: datetime.date):
        self.start = datetime.date(start.year, 1, 1)
        self.end = datetime.date(start.year, 12, 31)

    def __str__(self) -> str:
        return "year"

    def as_range(self) -> tuple[datetime.date, datetime.date]:
        return (self.start, self.end)


class MonthPeriod(Period):
    def __init__(self, start: datetime.date):
        self.start = datetime.date(start.year, start.month, 1)
        last_day = calendar.monthrange(start.year, start.month)[1]
        self.end = datetime.date(start.year, start.month, last_day)

    def __str__(self) -> str:
        return "month"

    def as_range(self) -> tuple[datetime.date, datetime.date]:
        return (self.start, self.end)


class WeekPeriod(Period):
    def __init__(self, start: datetime.date):
        weekday = start.weekday()
        self.start = start - datetime.timedelta(days=weekday)
        self.end = self.start + datetime.timedelta(days=6)

    def __str__(self) -> str:
        return "week"

    def as_range(self) -> tuple[datetime.date, datetime.date]:
        return (self.start, self.end)


class DayPeriod(Period):
    def __init__(self, start: datetime.date):
        self.start = start
        self.end = start

    def __str__(self) -> str:
        return "day"

    def as_range(self) -> tuple[datetime.date, datetime.date]:
        return (self.start, self.end)
