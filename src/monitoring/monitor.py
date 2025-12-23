from monitoring.period import All
from monitoring.period import Year
from monitoring.period import Month
from monitoring.period import Week
from monitoring.period import Day
from monitoring.period import PeriodType

from monitoring.instrument.halo_doppler_lidar import (
    monitor as monitor_halo,
)


def monitor(period: PeriodType, instrument: str, product: str):
    match instrument:
        case "halo-doppler-lidar":
            monitor_halo(period, product)
            pass
