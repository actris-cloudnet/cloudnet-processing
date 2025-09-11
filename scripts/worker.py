#!/usr/bin/env python3

import argparse
import datetime
import logging
import signal
import traceback
from argparse import Namespace
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from types import FrameType

import torch
from cloudnet_api_client import APIClient
from cloudnet_api_client.containers import (
    ExtendedInstrument,
    ExtendedProduct,
    Instrument,
    Site,
)

from processing import utils
from processing.config import Config
from processing.dvas import Dvas
from processing.instrument import process_instrument
from processing.jobs import freeze, update_plots, update_qc, upload_to_dvas
from processing.metadata_api import MetadataApi
from processing.model import process_model
from processing.pid_utils import PidUtils
from processing.processor import (
    InstrumentParams,
    ModelParams,
    Processor,
    ProcessParams,
    ProductParams,
)
from processing.product import process_product
from processing.storage_api import StorageApi
from processing.utils import send_slack_alert, utcnow, utctoday


class MemoryLogger:
    """Logger that outputs to stderr but also keeps content in memory."""

    def __init__(self) -> None:
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

    def clear_memory(self) -> None:
        """Clear log memory."""
        self._memory.truncate(0)
        self._memory.seek(0)

    @property
    def content(self) -> str:
        """Log content in memory."""
        return self._memory.getvalue()


class Worker:
    def __init__(self, config: Config, queue: str | None) -> None:
        self.config = config
        self.dataportal_url = config.dataportal_url
        self.session = utils.make_session()
        self.client = APIClient(f"{config.dataportal_url}/api/", self.session)
        md_api = MetadataApi(config, self.session)
        storage_api = StorageApi(config, self.session)
        pid_utils = PidUtils(config, self.session)
        dvas = Dvas(config, md_api, self.client)
        self.processor = Processor(md_api, storage_api, pid_utils, dvas, self.client)
        self.logger = MemoryLogger()
        self.n_processed_tasks = 0
        self.queue = queue

    def process_task(self) -> bool:
        """Get task from queue and process it. Returns True if a task was
        processed, or False if there's was no task to process."""
        if self.queue:
            task = self.get_task(self.queue) or self.get_task()
        else:
            task = self.get_task()
        if task is None:
            return False
        self.logger.clear_memory()
        logging.info(f"Processing task: {task}")
        try:
            date = datetime.date.fromisoformat(task["measurementDate"])
            site = self.processor.get_site(task["siteId"], date)
            product = self.client.product(task["productId"])
            params: ProcessParams
            with TemporaryDirectory() as temp_dir:
                directory = Path(temp_dir)
                if product.id == "model":
                    params = ModelParams(
                        site=site,
                        date=date,
                        product=product,
                        model=self.client.model(task["modelId"]),
                    )
                    if task["type"] == "plot":
                        update_plots(self.processor, params, directory)
                    elif task["type"] == "qc":
                        update_qc(self.processor, params, directory)
                    elif task["type"] == "freeze":
                        freeze(self.processor, params, directory)
                    elif task["type"] in ("hkd", "dvas"):
                        raise utils.SkipTaskError(
                            f"{task['type'].upper()} not supported for model products"
                        )
                    elif task["type"] == "process":
                        process_model(self.processor, params, directory)
                        if task["options"]["derivedProducts"]:
                            self.publish_followup_tasks(site, product, params)
                    else:
                        raise ValueError(f"Unknown task type: {task['type']}")
                elif product.id in ("l3-cf", "l3-lwc", "l3-iwc"):
                    params = ModelParams(
                        site=site,
                        date=date,
                        product=product,
                        model=self.client.model("ecmwf"),  # hard coded for now
                    )
                    if task["type"] == "plot":
                        update_plots(self.processor, params, directory)
                    elif task["type"] == "qc":
                        update_qc(self.processor, params, directory)
                    elif task["type"] == "freeze":
                        freeze(self.processor, params, directory)
                    elif task["type"] in ("hkd", "dvas"):
                        raise utils.SkipTaskError(
                            f"{task['type'].upper()} not supported for L3 products"
                        )
                    elif task["type"] == "process":
                        process_product(self.processor, params, directory)
                        if task["options"]["derivedProducts"]:
                            self.publish_followup_tasks(site, product, params)
                    else:
                        raise ValueError(f"Unknown task type: {task['type']}")
                elif product.source_instrument_ids:
                    params = InstrumentParams(
                        site=site,
                        date=date,
                        product=product,
                        instrument=self.client.instrument(task["instrumentInfoUuid"]),
                    )
                    if task["type"] == "plot":
                        update_plots(self.processor, params, directory)
                    elif task["type"] == "qc":
                        update_qc(self.processor, params, directory)
                    elif task["type"] == "freeze":
                        freeze(self.processor, params, directory)
                    elif task["type"] == "hkd":
                        self.processor.process_housekeeping(params)
                    elif task["type"] == "dvas":
                        raise utils.SkipTaskError(
                            "DVAS not supported for instrument products"
                        )
                    elif task["type"] == "process":
                        process_instrument(self.processor, params, directory)
                        if task["options"]["derivedProducts"]:
                            self.publish_followup_tasks(site, product, params)
                    else:
                        raise ValueError(f"Unknown task type: {task['type']}")
                else:
                    params = ProductParams(
                        site=site,
                        date=date,
                        product=product,
                        instrument=self.client.instrument(task["instrumentInfoUuid"])
                        if task["instrumentInfoUuid"]
                        else None,
                    )
                    if task["type"] == "plot":
                        update_plots(self.processor, params, directory)
                    elif task["type"] == "qc":
                        update_qc(self.processor, params, directory)
                    elif task["type"] == "freeze":
                        freeze(self.processor, params, directory)
                    elif task["type"] == "dvas":
                        upload_to_dvas(self.processor, params)
                    elif task["type"] == "hkd":
                        raise utils.SkipTaskError(
                            "Housekeeping not supported for products"
                        )
                    elif task["type"] == "process":
                        process_product(self.processor, params, directory)
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

    def get_task(self, queue: str | None = None) -> dict | None:
        params = {"queue": queue} if queue is not None else None
        res = self.session.post(f"{self.dataportal_url}/queue/receive", params=params)
        if res.status_code == 204:
            return None
        res.raise_for_status()
        return res.json()

    def publish_followup_tasks(
        self, site: Site, product: ExtendedProduct, params: ProcessParams
    ) -> None:
        if "hidden" in site.type or "model" in site.type:
            logging.info("Site is model / hidden, will not publish followup tasks")
            return
        for product_id in product.derived_product_ids:
            derived_product = self.client.product(product_id)
            if _should_skip_derived_product(derived_product):
                continue
            elif "instrument" not in derived_product.type:
                self.publish_followup_task(derived_product, params, instrument=None)
            elif product.id == "lidar" and derived_product.id == "mwr-l1c":
                # there can be multiple MWRs, process all
                for instrument in self._fetch_mwrpy_instruments(
                    params, derived_product
                ):
                    self.publish_followup_task(
                        derived_product,
                        params,
                        instrument,
                        delay=datetime.timedelta(minutes=5),
                    )
            else:
                assert isinstance(params, (InstrumentParams, ProductParams))
                self.publish_followup_task(derived_product, params, params.instrument)

    def publish_followup_task(
        self,
        derived_product: ExtendedProduct,
        params: ProcessParams,
        instrument: ExtendedInstrument | None,
        delay: datetime.timedelta | None = None,
    ) -> None:
        metadata = self.client.files(
            site_id=params.site.id,
            date=params.date,
            product_id=derived_product.id,
            instrument_pid=instrument.pid if instrument else None,
        )
        is_freezed = len(metadata) == 1 and not metadata[0].volatile
        if is_freezed:
            delay = datetime.timedelta(hours=1)
        elif delay is not None:
            delay = delay
        elif len(derived_product.source_product_ids) > 1:
            delay = datetime.timedelta(minutes=15)
        else:
            delay = datetime.timedelta(seconds=0)
        scheduled_at = utcnow() + delay
        diff_days = abs((utctoday() - params.date) / datetime.timedelta(days=1))
        priority = min(diff_days, 10)
        task = {
            "type": "process",
            "siteId": params.site.id,
            "productId": derived_product.id,
            "measurementDate": params.date.isoformat(),
            "scheduledAt": scheduled_at.isoformat(),
            "priority": priority,
        }
        if instrument:
            task["instrumentInfoUuid"] = str(instrument.uuid)
        logging.info(f"Publish task: {task}")
        res = self.session.post(
            f"{self.dataportal_url}/api/queue/publish",
            json=task,
            auth=self.config.data_submission_auth,
        )
        res.raise_for_status()

    def _fetch_mwrpy_instruments(
        self, params: ProcessParams, derived_product: ExtendedProduct
    ) -> set[Instrument]:
        metadata = self.client.raw_files(
            site_id=params.site.id,
            date=params.date,
            instrument_id=derived_product.source_instrument_ids,
        )
        return {m.instrument for m in metadata}


def _should_skip_derived_product(product: ExtendedProduct) -> bool:
    if product.experimental and product.id not in (
        "cpr-simulation",
        "epsilon-lidar",
    ):
        logging.info(f"Will not publish task for experimental product: {product.id}")
        return True
    return False


def _parse_args() -> Namespace:
    parser = argparse.ArgumentParser(
        description="Cloudnet processing worker.",
        epilog="Enjoy the program! :)",
    )
    parser.add_argument(
        "--queue",
        help="Process tasks from this queue, or from default queue if the specified queue is empty",
    )
    args = parser.parse_args()
    return args


def main() -> None:
    config = Config()
    args = _parse_args()
    worker = Worker(config, args.queue)
    exit = Event()

    # Limit number of threads for VOODOO. In production, the numbers are set too
    # high by default.
    torch.set_num_threads(2)
    torch.set_num_interop_threads(2)

    def signal_handler(sig: int, frame: FrameType | None) -> None:
        logging.info("Received termination signal")
        exit.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        logging.info("Waiting for a task...")
        while not exit.is_set() and worker.n_processed_tasks < 1000:
            if not worker.process_task():
                exit.wait(10)
        logging.info("Terminate after processing the maximum number of tasks")
    except Exception as err:
        logging.exception("Fatal error in worker")
        send_slack_alert(config, err, source="worker", log=traceback.format_exc())


if __name__ == "__main__":
    main()
