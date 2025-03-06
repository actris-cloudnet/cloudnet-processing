#!/usr/bin/env python3

import datetime
import logging
import signal
import traceback
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event

from processing import utils
from processing.config import Config
from processing.dvas import Dvas
from processing.instrument import process_instrument
from processing.jobs import freeze, hkd, update_plots, update_qc, upload_to_dvas
from processing.metadata_api import MetadataApi
from processing.model import process_model
from processing.pid_utils import PidUtils
from processing.processor import (
    InstrumentParams,
    ModelParams,
    Processor,
    ProcessParams,
    Product,
    ProductParams,
    Site,
)
from processing.product import process_me, process_product
from processing.storage_api import StorageApi
from processing.utils import send_slack_alert, utcnow, utctoday


class MemoryLogger:
    """Logger that outputs to stderr but also keeps content in memory."""

    def __init__(self):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        self._memory = StringIO()
        memory_handler = logging.StreamHandler(self._memory)
        memory_handler.setFormatter(formatter)
        logger.addHandler(memory_handler)

        stderr_handler = logging.StreamHandler()
        stderr_handler.setFormatter(formatter)
        logger.addHandler(stderr_handler)

    def clear_memory(self):
        """Clear log memory."""
        self._memory.truncate(0)
        self._memory.seek(0)

    @property
    def content(self) -> str:
        """Log content in memory."""
        return self._memory.getvalue()


class Worker:
    def __init__(self, config: Config):
        self.config = config
        self.dataportal_url = config.dataportal_url
        self.session = utils.make_session()
        self.md_api = MetadataApi(self.config, self.session)
        self.storage_api = StorageApi(self.config, self.session)
        self.pid_utils = PidUtils(self.config, self.session)
        self.dvas = Dvas(self.config, self.md_api)
        self.processor = Processor(
            self.md_api, self.storage_api, self.pid_utils, self.dvas
        )
        self.logger = MemoryLogger()
        self.n_processed_tasks = 0

    def process_task(self) -> bool:
        """Get task from queue and process it. Returns True if a task was
        processed, or False if there's was no task to process."""
        res = self.session.post(f"{self.dataportal_url}/queue/receive")
        if res.status_code == 204:
            return False
        res.raise_for_status()
        task = res.json()
        self.logger.clear_memory()
        logging.info(f"Processing task: {task}")
        try:
            site = self.processor.get_site(task["siteId"])
            date = datetime.date.fromisoformat(task["measurementDate"])
            product = self.processor.get_product(task["productId"])
            params: ProcessParams
            with TemporaryDirectory() as directory:
                if product.id == "model":
                    params = ModelParams(
                        site=site,
                        date=date,
                        product=product,
                        model=self.processor.get_model(task["modelId"]),
                    )
                    if task["type"] == "plot":
                        update_plots(self.processor, params, Path(directory))
                    elif task["type"] == "qc":
                        update_qc(self.processor, params, Path(directory))
                    elif task["type"] == "freeze":
                        freeze(self.processor, params, Path(directory))
                    elif task["type"] == "hkd":
                        raise utils.SkipTaskError(
                            "Housekeeping not supported for model products"
                        )
                    elif task["type"] == "dvas":
                        raise utils.SkipTaskError(
                            "DVAS not supported for model products"
                        )
                    elif task["type"] == "process":
                        process_model(self.processor, params, Path(directory))
                        if task["options"]["derivedProducts"]:
                            self.publish_followup_tasks(site, product, params)
                    else:
                        raise ValueError(f"Unknown task type: {task['type']}")
                elif product.id in ("l3-cf", "l3-lwc", "l3-iwc"):
                    params = ModelParams(
                        site=site,
                        date=date,
                        product=product,
                        model=self.processor.get_model("ecmwf"),  # hard coded for now
                    )
                    if task["type"] == "plot":
                        update_plots(self.processor, params, Path(directory))
                    elif task["type"] == "qc":
                        update_qc(self.processor, params, Path(directory))
                    elif task["type"] == "freeze":
                        freeze(self.processor, params, Path(directory))
                    elif task["type"] == "hkd":
                        raise utils.SkipTaskError(
                            "Housekeeping not supported for L3 products"
                        )
                    elif task["type"] == "dvas":
                        raise utils.SkipTaskError("DVAS not supported for L3 products")
                    elif task["type"] == "process":
                        process_me(self.processor, params, Path(directory))
                        if task["options"]["derivedProducts"]:
                            self.publish_followup_tasks(site, product, params)
                    else:
                        raise ValueError(f"Unknown task type: {task['type']}")
                elif product.source_instrument_ids:
                    params = InstrumentParams(
                        site=site,
                        date=date,
                        product=product,
                        instrument=self.processor.get_instrument(
                            task["instrumentInfoUuid"]
                        ),
                    )
                    if task["type"] == "plot":
                        update_plots(self.processor, params, Path(directory))
                    elif task["type"] == "qc":
                        update_qc(self.processor, params, Path(directory))
                    elif task["type"] == "freeze":
                        freeze(self.processor, params, Path(directory))
                    elif task["type"] == "hkd":
                        hkd(self.processor, params)
                    elif task["type"] == "dvas":
                        raise utils.SkipTaskError(
                            "DVAS not supported for instrument products"
                        )
                    elif task["type"] == "process":
                        process_instrument(self.processor, params, Path(directory))
                        if task["options"]["derivedProducts"]:
                            self.publish_followup_tasks(site, product, params)
                    else:
                        raise ValueError(f"Unknown task type: {task['type']}")
                else:
                    params = ProductParams(
                        site=site,
                        date=date,
                        product=product,
                        instrument=self.processor.get_instrument(
                            task["instrumentInfoUuid"]
                        )
                        if task["instrumentInfoUuid"]
                        else None,
                    )
                    if task["type"] == "plot":
                        update_plots(self.processor, params, Path(directory))
                    elif task["type"] == "qc":
                        update_qc(self.processor, params, Path(directory))
                    elif task["type"] == "freeze":
                        freeze(self.processor, params, Path(directory))
                    elif task["type"] == "dvas":
                        upload_to_dvas(self.processor, params)
                    elif task["type"] == "hkd":
                        raise utils.SkipTaskError(
                            "Housekeeping not supported for products"
                        )
                    elif task["type"] == "process":
                        process_product(self.processor, params, Path(directory))
                        if task["options"]["derivedProducts"]:
                            self.publish_followup_tasks(site, product, params)
                    else:
                        raise ValueError(f"Unknown task type: {task['type']}")
            action = "complete"
        except utils.SkipTaskError as err:
            logging.warning("Skipped task: %s", err)
            action = "complete"
        except Exception as err:
            logging.exception("Failed to process task")
            try:
                send_slack_alert(
                    self.config,
                    err,
                    source="data",
                    log=self.logger.content,
                    site=task.get("siteId"),
                    date=task.get("measurementDate"),
                    model=task.get("modelId"),
                    product=task.get("productId"),
                )
            except Exception:
                logging.exception("Failed to send Slack alert")
            action = "fail"
        res = self.session.put(f"{self.dataportal_url}/queue/{action}/{task['id']}")
        res.raise_for_status()
        logging.info("Task processed")
        self.n_processed_tasks += 1
        return True

    def publish_followup_tasks(
        self, site: Site, product: Product, params: ProcessParams
    ):
        if "hidden" in site.types or "model" in site.types:
            logging.info("Site is model / hidden, will not publish followup tasks")
            return
        for product_id in product.derived_product_ids:
            self.publish_followup_task(product_id, params)

    def publish_followup_task(self, product_id: str, params: ProcessParams):
        product = self.processor.get_product(product_id)
        if product.experimental and product.id not in (
            "cpr-simulation",
            "epsilon-lidar",
        ):
            logging.info(
                f"Will not publish task for experimental product: {product.id}"
            )
            return

        if "instrument" in product.type:
            assert isinstance(params, InstrumentParams | ProductParams)
            assert params.instrument is not None
            instrument = params.instrument
        else:
            instrument = None

        payload = {
            "site": params.site.id,
            "date": params.date.isoformat(),
            "product": product.id,
        }
        if instrument:
            payload["instrumentPid"] = instrument.pid
        metadata = self.processor.md_api.get("api/files", payload)
        is_freezed = len(metadata) == 1 and not metadata[0]["volatile"]

        if is_freezed:
            delay = datetime.timedelta(hours=1)
        elif len(product.source_product_ids) > 1:
            delay = datetime.timedelta(minutes=15)
        else:
            delay = datetime.timedelta(seconds=0)
        scheduled_at = utcnow() + delay
        diff_days = abs((utctoday() - params.date) / datetime.timedelta(days=1))
        priority = min(diff_days, 10)

        task = {
            "type": "process",
            "siteId": params.site.id,
            "productId": product.id,
            "measurementDate": params.date.isoformat(),
            "scheduledAt": scheduled_at.isoformat(),
            "priority": priority,
        }
        if instrument:
            task["instrumentInfoUuid"] = instrument.uuid
        logging.info(f"Publish task: {task}")
        res = self.session.post(
            f"{self.dataportal_url}/api/queue/publish",
            json=task,
            auth=self.config.data_submission_auth,
        )
        res.raise_for_status()


def main():
    config = utils.read_main_conf()
    worker = Worker(config)
    exit = Event()

    def signal_handler(sig, frame):
        logging.info("Received termination signal")
        exit.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        logging.info("Waiting for a task...")
        while not exit.is_set() and worker.n_processed_tasks < 100:
            if not worker.process_task():
                exit.wait(10)
        logging.info("Terminate after processing the maximum number of tasks")
    except Exception as err:
        logging.exception("Fatal error in worker")
        send_slack_alert(config, err, source="worker", log=traceback.format_exc())


if __name__ == "__main__":
    main()
