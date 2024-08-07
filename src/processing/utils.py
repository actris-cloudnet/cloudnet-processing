import datetime
import logging
from typing import Literal

from data_processing.config import Config
from data_processing.utils import make_session

ErrorSource = Literal["model", "pid", "data", "wrapper", "img", "worker"]


def send_slack_alert(
    config: Config,
    exception: Exception,
    source: ErrorSource,
    log: str | None = None,
    date: str | None = None,
    site: str | None = None,
    product: str | None = None,
    model: str | None = None,
) -> None:
    """Sends notification to Slack."""
    if not config.slack_api_token or not config.slack_channel_id:
        logging.warning("Slack is not configured: no notification will be sent!")
        return

    match source:
        case "model":
            label = ":earth_africa: Model processing"
        case "pid":
            label = ":id: PID generation"
        case "data":
            label = ":desktop_computer: Data processing"
        case "wrapper":
            label = ":fire: Main wrapper"
        case "img":
            label = ":frame_with_picture: Image creation"
        case "worker":
            label = ":construction_worker: Worker"
        case unknown_source:
            label = f":interrobang: Unknown error source ({unknown_source})"

    padding = " " * 7
    msg = f"*{label}*\n\n"

    for name, var in zip(
        ("Site", "Date", "Product", "Model"), (site, date, product, model)
    ):
        if var is not None:
            msg += f"*{name}:* {var}{padding}"

    timestamp = datetime.datetime.now(datetime.timezone.utc)
    msg += f"*Time:* {timestamp:%Y-%m-%d %H:%M:%S}\n\n"
    msg += f"*Error:* {exception}"

    payload = {
        "content": log or "(empty log)",
        "channels": config.slack_channel_id,
        "title": "Full log",
        "initial_comment": msg,
    }

    session = make_session()
    r = session.post(
        "https://slack.com/api/files.upload",
        data=payload,
        headers={"Authorization": f"Bearer {config.slack_api_token}"},
    )
    r.raise_for_status()
    body = r.json()
    if not body["ok"]:
        logging.fatal(f"Failed to send Slack notification: {body.text}")


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def utctoday() -> datetime.date:
    return utcnow().date()
