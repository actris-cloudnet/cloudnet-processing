#!/usr/bin/env python3

import datetime
import logging
import signal
import traceback
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event

from data_processing import utils
from data_processing.config import Config
from data_processing.metadata_api import MetadataApi
from data_processing.pid_utils import PidUtils
from data_processing.storage_api import StorageApi
from processing.instrument import process_instrument
from processing.model import process_model
from processing.processor import InstrumentParams, ModelParams, Processor
from processing.utils import send_slack_alert


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
        self.processor = Processor(self.md_api, self.storage_api, self.pid_utils)
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
            product_id = task["productId"]
            with TemporaryDirectory() as directory:
                if product_id == "model":
                    model_params = ModelParams(
                        site=site,
                        date=date,
                        product_id=product_id,
                        model_id=task["modelId"],
                    )
                    process_model(self.processor, model_params, Path(directory))
                else:
                    instru_params = InstrumentParams(
                        site=site,
                        date=date,
                        product_id=product_id,
                        instrument=self.processor.get_instrument(
                            task["instrumentInfoUuid"]
                        ),
                    )
                    process_instrument(self.processor, instru_params, Path(directory))
            action = "complete"
        except Exception as err:
            logging.exception("Failed to process task")
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
            action = "fail"
        res = self.session.put(f"{self.dataportal_url}/queue/{action}/{task['id']}")
        res.raise_for_status()
        logging.info("Task proccessed")
        self.n_processed_tasks += 1
        return True


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
