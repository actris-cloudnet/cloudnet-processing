from monitoring.instrument import Instrument
from monitoring.halo_doppler_lidar import process as process_halo

from typing import Protocol

from monitoring.period import Period
from processing.processor import Processor


class InstrumentMonitoringProcessor(Protocol):
    def __call__(self,processor: Processor, instrument: Instrument, site_id: str, period: Period): ...


def processor_from_instrument(instrument: Instrument) -> InstrumentMonitoringProcessor:
    match instrument.id:
        case "halo-doppler-lidar":
            return process_halo
        case _:
            raise ValueError(f"Monitoring not implemented for {instrument.id}")
