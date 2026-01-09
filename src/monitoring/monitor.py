import logging

from monitoring.instrument.halo_doppler_lidar import (
    monitor as monitor_halo,
)
from monitoring.monitor_options import MonitorOptions


def monitor(opts: MonitorOptions) -> None:
    if opts.product.id.startswith("halo-doppler-lidar"):
        monitor_halo(opts)
        logging.info(f"Monitored {opts.period!r} {opts.product} {opts.site}")
    else:
        raise ValueError(f"Unexpected product: '{opts.product}'")
