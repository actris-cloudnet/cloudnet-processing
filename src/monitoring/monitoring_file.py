from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from tempfile import NamedTemporaryFile

from cloudnet_api_client.containers import Instrument, Site

from monitoring.period import AllPeriod, Period, PeriodWithRange
from monitoring.product import MonitoringProduct, MonitoringVariable
from monitoring.utils import get_apis


@dataclass
class Dimensions:
    width: int
    height: int
    margin_top: int | None = None
    margin_right: int | None = None
    margin_bottom: int | None = None
    margin_left: int | None = None

    def as_payload_dict(self) -> dict[str, int | None]:
        return {
            "width": self.width,
            "height": self.height,
            "marginTop": self.margin_top,
            "marginRight": self.margin_right,
            "marginBottom": self.margin_bottom,
            "marginLeft": self.margin_left,
        }


@dataclass
class MonitoringVisualization:
    fig: bytes
    variable: MonitoringVariable
    dimensions: Dimensions


@dataclass
class MonitoringFile:
    instrument: Instrument
    site: Site
    period: Period
    product: MonitoringProduct
    visualisations: list[MonitoringVisualization]

    def upload(self) -> None:
        if not self.visualisations:
            raise ValueError(
                f"No visualisations for product {self.product.id}, {self.site.id}, {self.instrument.name}, {self.period}"
            )
        md_api, storage_api = get_apis()
        payload = {
            "periodType": self.period.period,
            "siteId": self.site.id,
            "productId": self.product.id,
            "instrumentUuid": str(self.instrument.uuid),
        }
        if isinstance(self.period, PeriodWithRange):
            payload["startDate"] = str(self.period.start_date)
        res = md_api.post("monitoring-files", payload=payload)
        if not res.ok:
            raise RuntimeError(f"Could not post file: {payload}")
        try:
            data = res.json()
        except (JSONDecodeError, ValueError) as err:
            raise RuntimeError(
                f"Failed to decode JSON from POST monitoring-files with payload {payload}"
            ) from err

        file_uuid = data["uuid"]
        for vis in self.visualisations:
            s3key = generate_s3_key(
                self.site, self.instrument, self.product, vis.variable, self.period
            )
            with NamedTemporaryFile() as tempfile:
                tempfile.write(vis.fig)
                storage_api.upload_image(full_path=Path(tempfile.name), s3key=s3key)
            payload = {
                "s3key": s3key,
                "sourceFileUuid": file_uuid,
                "variableId": vis.variable.id,
            }
            res = md_api.post("monitoring-visualizations", payload=payload)
            if not res.ok:
                raise RuntimeError(f"Could not post visualisation: {payload}")


def generate_s3_key(
    site: Site,
    instrument: Instrument,
    product: MonitoringProduct,
    variable: MonitoringVariable,
    period: Period,
) -> str:
    instrument_uuid_short = str(instrument.uuid)[:8]
    period_str = _period_for_s3key(period)
    return f"monitoring/{period_str}_{site.id}_{product.id}_{variable.id}_{instrument_uuid_short}.png"


def _period_for_s3key(p: Period) -> str:
    if isinstance(p, AllPeriod):
        return "All"
    period_str = p.period.capitalize()
    match p.period:
        case "year":
            date_str = p.start_date.strftime("%Y")
        case "month":
            date_str = p.start_date.strftime("%Y-%m")
        case "week":
            date_str = f"{p.start_date.isocalendar().week:02d}"
        case "day":
            date_str = p.start_date.isoformat()

    return f"{period_str}{date_str}"
