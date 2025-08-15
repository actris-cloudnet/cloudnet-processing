from pathlib import Path
from uuid import uuid4

from processing.metadata_api import MetadataApi
from processing.storage_api import StorageApi

from monitoring.instrument import Instrument
from monitoring.period import (
    AllPeriod,
    DayPeriod,
    MonthPeriod,
    Period,
    WeekPeriod,
    YearPeriod,
)


class Dimensions:
    def __init__(
        self,
        width: int,
        height: int,
        margin_top: int,
        margin_right: int,
        margin_bottom: int,
        margin_left: int,
    ):
        self.width = width
        self.height = height
        self.margin_top = margin_top
        self.margin_right = margin_right
        self.margin_bottom = margin_bottom
        self.margin_left = margin_left

    def as_payload_dict(self) -> dict[str, int]:
        return {
            "width": self.width,
            "height": self.height,
            "marginTop": self.margin_top,
            "marginRight": self.margin_right,
            "marginBottom": self.margin_bottom,
            "marginLeft": self.margin_left,
        }


class MonitoringVisualization:
    def __init__(
        self,
        img_path: Path,
        variable_id: str,
        dimensions: Dimensions,
    ):
        self.img_path = img_path
        self.variable_id = variable_id
        self.dimensions = dimensions


class MonitoringFile:
    def __init__(
        self,
        instrument: Instrument,
        site_id: str,
        period: Period,
        product_id: str,
    ):
        self.instrument = instrument
        self.site_id = site_id
        self.period = period
        self.product_id = product_id
        self.monitoring_file_uuid = str(uuid4())

    def put_file(self, md_api: MetadataApi):
        start_date = self.period.start_date()
        start_date = str(start_date) if start_date else None

        payload = {
            "uuid": self.monitoring_file_uuid,
            "startDate": start_date,
            "periodType": str(self.period),
            "site": self.site_id,
            "monitoringProduct": self.product_id,
            "instrumentInfo": self.instrument.uuid,
        }

        resp = md_api.session.put(f"{md_api._url}/api/monitoring-files", json=payload)
        if not resp.ok:
            raise ValueError("Failed to create a monitoring file")

    def put_visualization(
        self, storage_api: StorageApi, md_api: MetadataApi, vis: MonitoringVisualization
    ):
        period_str = _period_for_plotname(self.period)
        s3key = f"monitoring/{self.instrument.id}-{self.instrument.uuid}-{self.site_id}-{self.product_id}-{vis.variable_id}-{period_str}.png"
        storage_api.upload_image(full_path=vis.img_path, s3key=s3key)
        payload = {
            "s3key": s3key,
            "sourceFile": self.monitoring_file_uuid,
            "monitoringProductVariable": vis.variable_id,
            **vis.dimensions.as_payload_dict(),
        }
        resp = md_api.session.put(
            f"{md_api._url}/api/monitoring-visualizations", json=payload
        )
        print(resp)


def _period_for_plotname(period: Period) -> str:
    match period:
        case AllPeriod():
            return "All"
        case YearPeriod(start=start, end=end):
            return start.strftime("Year%Y")
        case MonthPeriod(start=start, end=end):
            return start.strftime("Month%Y%B")
        case WeekPeriod(start=start, end=end):
            return f"Week{start.isoformat()}-{end.isoformat()}"
        case DayPeriod(start=start, end=end):
            return f"Day{start.isoformat()}-{end.isoformat()}"
        case _:
            raise ValueError("Unsupported period")
