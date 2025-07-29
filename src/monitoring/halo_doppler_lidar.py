from pathlib import Path
import tempfile
from monitoring.instrument import Instrument
from monitoring.period import Period
from processing.processor import Processor


def process(processor: Processor, instrument: Instrument, site_id: str, period: Period):
    process_system_parameters(processor, instrument, site_id, period)


def process_system_parameters(
    processor: Processor, instrument: Instrument, site_id: str, period: Period
):
    with tempfile.TemporaryDirectory() as tempdir:
        paths, uuids = processor.download_instrument(
            site_id=site_id,
            instrument_id=instrument.id,
            directory=Path(tempdir),
            date=period.as_range(),
            instrument_pid=instrument.pid,
            include_pattern=r"system_parameters_.*\.txt",
        )
        breakpoint()
        pass
