import logging

import housekeeping

from processing import utils
from processing.processor import InstrumentParams, Processor
from processing.utils import utctoday


def process_housekeeping(processor: Processor, params: InstrumentParams) -> None:
    if params.date < utctoday() - processor.md_api.config.housekeeping_retention:
        logging.info("Skipping housekeeping for old data")
        return
    logging.info("Processing housekeeping data")
    raw_api = utils.RawApi(processor.md_api.config, processor.md_api.session)
    records = _get_housekeeping_records(processor, params)
    try:
        with housekeeping.Database() as db:
            for record in records:
                housekeeping.process_record(record, raw_api=raw_api, db=db)
    except housekeeping.HousekeepingException:
        logging.exception("Housekeeping failed")


def _get_housekeeping_records(
    processor: Processor, params: InstrumentParams
) -> list[dict]:
    if params.instrument.type == "halo-doppler-lidar":
        first_day_of_month = params.date.replace(day=1)
        payload = processor._get_payload(
            site=params.site.id,
            date=(first_day_of_month, params.date),
            instrument_pid=params.instrument.pid,
            filename_prefix="system_parameters",
        )
        records = processor.md_api.get("api/raw-files", payload)
        return _select_halo_doppler_lidar_hkd_records(records)

    payload = processor._get_payload(
        site=params.site.id, date=params.date, instrument_pid=params.instrument.pid
    )
    return processor.md_api.get("api/raw-files", payload)


def _select_halo_doppler_lidar_hkd_records(records: list[dict]) -> list[dict]:
    return [
        max(
            records,
            key=lambda x: (
                x.get("measurementDate"),
                x.get("createdAt"),
                x.get("updatedAt"),
                x.get("size"),
            ),
        )
    ]
