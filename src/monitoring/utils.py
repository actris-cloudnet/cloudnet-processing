import datetime

from monitoring.period import AllPeriod, Period, PeriodWithRange
from processing.config import Config
from processing.metadata_api import MetadataApi
from processing.storage_api import StorageApi
from processing.utils import make_session


def get_apis() -> tuple[MetadataApi, StorageApi]:
    config = Config()
    session = make_session()
    md_api = MetadataApi(config, session)
    storage_api = StorageApi(config, session)
    return md_api, storage_api


def range_from_period(period: Period) -> tuple[datetime.date, datetime.date]:
    match period:
        case AllPeriod():
            start = datetime.date(1900, 1, 1)
            end = datetime.date.today() + datetime.timedelta(days=1)
        case PeriodWithRange():
            start = period.start_date
            end = period.end_date
    return start, end
