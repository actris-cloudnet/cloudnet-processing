from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from tempfile import NamedTemporaryFile

from cloudnetpy.plotting.plotting import Dimensions

from monitoring.period import All, PeriodProtocol, PeriodType
from monitoring.product import MonitoringProduct, MonitoringVariable
from monitoring.utils import get_md_api, get_storage_api


@dataclass
class MonitoringVisualization:
    fig: bytes
    variable: MonitoringVariable
    dimensions: Dimensions


def _dimensions_as_payload(dim: Dimensions) -> dict[str, int]:
    return {
        "width": dim.width,
        "height": dim.height,
        "marginTop": dim.margin_top,
        "marginRight": dim.margin_right,
        "marginBottom": dim.margin_bottom,
        "marginLeft": dim.margin_left,
    }


@dataclass
class MonitoringFile:
    instrument_uuid: str
    site: str
    period: PeriodType
    product: MonitoringProduct
    visualisations: list[MonitoringVisualization]

    def upload(self) -> None:
        if not self.visualisations:
            raise ValueError(
                f"No visualisations for product {self.product.id}, {self.site}, {self.instrument_uuid}, {self.period}"
            )
        md_api = get_md_api()
        storage_api = get_storage_api()
        payload = {
            "periodType": self.period.to_str(),
            "siteId": self.site,
            "productId": self.product.id,
            "instrumentUuid": self.instrument_uuid,
        }
        if isinstance(self.period, PeriodProtocol):
            payload["startDate"] = str(self.period.start())
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
                self.site, self.instrument_uuid, self.product, vis.variable, self.period
            )
            with NamedTemporaryFile() as tempfile:
                tempfile.write(vis.fig)
                storage_api.upload_image(full_path=Path(tempfile.name), s3key=s3key)
            payload_vis: dict[str, str | int] = {
                "s3key": s3key,
                "sourceFileUuid": file_uuid,
                "variableId": vis.variable.id,
                **_dimensions_as_payload(vis.dimensions),
            }
            res = md_api.post("monitoring-visualizations", payload=payload_vis)
            if not res.ok:
                raise RuntimeError(f"Could not post visualisation: {payload_vis}")


def generate_s3_key(
    site: str,
    instrument_uuid: str,
    product: MonitoringProduct,
    variable: MonitoringVariable,
    period: PeriodType,
) -> str:
    instrument_uuid_short = instrument_uuid[:8]
    period_str = _period_for_s3key(period)
    return f"monitoring/{period_str}_{site}_{product.id}_{variable.id}_{instrument_uuid_short}.png"


def _period_for_s3key(p: PeriodType) -> str:
    if isinstance(p, All):
        return "all"
    period_str = p.to_str()
    match period_str:
        case "year":
            date_str = p.start().strftime("%Y")
        case "month":
            date_str = p.start().strftime("%Y-%m")
        case "week":
            date_str = f"{p.start().isocalendar().week:02d}"
        case "day":
            date_str = p.start().isoformat()

    return f"{period_str}{date_str}"
