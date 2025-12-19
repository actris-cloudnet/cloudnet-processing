from monitoring.period import All
from monitoring.period import Year
from monitoring.period import Month
from monitoring.period import Week
from monitoring.period import Day
from monitoring.period import PeriodType


def monitor(period: PeriodType, instrument: str, product: str):
    print(instrument, product, end = " ")
    match period:
        case Day(date):
            print(date)
        case Week(year, week):
            print(year, week)
        case Month(year, month):
            print(year, month)
        case Year(year):
            print(year)
        case All():
            print(period)
