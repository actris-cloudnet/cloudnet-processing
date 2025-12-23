from monitoring.period import PeriodType


def monitor(period: PeriodType, product: str):
    match product:
        case "halo-doppler-lidar_housekeeping":
            pass
        case "halo-doppler-lidar_background":
            pass
        case "halo-doppler-lidar_signal":
            pass


def monitor_housekeeping(period: PeriodType):
    pass


def monitor_background(period: PeriodType):
    pass


def monitor_signal(period: PeriodType):
    pass
