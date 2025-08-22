import logging

import housekeeping
from cloudnet_api_client.containers import RawMetadata

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
) -> list[RawMetadata]:
    if params.instrument.instrument_id == "halo-doppler-lidar":
        first_day_of_month = params.date.replace(day=1)
        records = processor.client.raw_files(
            site_id=params.site.id,
            date_from=first_day_of_month,
            date_to=params.date,
            instrument_pid=params.instrument.pid,
            filename_prefix="system_parameters",
        )
        return _select_halo_doppler_lidar_hkd_records(records)
    return processor.client.raw_files(
        site_id=params.site.id, date=params.date, instrument_pid=params.instrument.pid
    )


def _select_halo_doppler_lidar_hkd_records(
    records: list[RawMetadata]
) -> list[RawMetadata]:
    if not records:
        return []
    return [
        max(
            records,
            key=lambda x: (
                x.measurement_date,
                x.created_at,
                x.updated_at,
                x.size,
            ),
        )
    ]
