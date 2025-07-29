from monitoring.period import DayPeriod, MonthPeriod, Period, WeekPeriod, YearPeriod
from processing import utils
from monitoring.instrument import Instrument
from monitoring.processor import processor_from_instrument
from processing.dvas import Dvas
from processing.metadata_api import MetadataApi
from processing.pid_utils import PidUtils
from processing.processor import Processor
from processing.storage_api import StorageApi


def monitor(pid: str, period: Period):
    config = utils.read_main_conf()
    session = utils.make_session()
    md_api = MetadataApi(config, session)
    storage_api = StorageApi(config, session)
    pid_utils = PidUtils(config, session)
    dvas = Dvas(config, md_api)
    processor = Processor(md_api, storage_api, pid_utils, dvas)

    instrument = _instrument_from_pid(pid)
    process = processor_from_instrument(instrument)
    sites = _sites_for_instrument_and_period(instrument, period)
    for site in sites:
        process(processor,instrument, site, period)


def _instrument_from_pid(pid: str):
    instruments = [
        i
        for i in utils.get_from_data_portal_api("/api/instrument-pids")
        if i["pid"] == pid
    ]
    if not instruments:
        raise ValueError(f"Invalid pid: {pid}")
    data = instruments[0]
    return Instrument.from_dict(data)


def _sites_for_instrument_and_period(instrument: Instrument, period: Period):
    payload = {"instrumentPid": instrument.pid}
    if isinstance(period, (YearPeriod, MonthPeriod, WeekPeriod, DayPeriod)):
        payload["dateFrom"] = str(period.start)
        payload["dateTo"] = str(period.end)
    records = utils.get_from_data_portal_api("/api/raw-files", payload)
    sites = {r["siteId"] for r in records}
    return list(sites)
