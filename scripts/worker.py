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
from data_processing.storage_api import StorageApi
from processing.model import process_model
from processing.processor import ModelParams, Processor
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
        self.processor = Processor(self.md_api, self.storage_api)
        self.logger = MemoryLogger()

    def process_task(self) -> bool:
        """Get task from queue and process it. Returns True if a task was
        processed, or False if there's was no task to process."""
        res = self.session.post(f"{self.dataportal_url}/queue/model/receive")
        if res.status_code == 204:
            return False
        res.raise_for_status()
        task = res.json()
        self.logger.clear_memory()
        logging.info(f"Processing task: {task}")
        try:
            params = ModelParams(
                site=task["siteId"],
                date=datetime.date.fromisoformat(task["measurementDate"]),
                model=task["modelId"],
            )
            with TemporaryDirectory() as directory:
                process_model(self.processor, params, Path(directory))
            action = "complete"
        except Exception as err:
            logging.exception("Failed to process task")
            send_slack_alert(
                self.config,
                err,
                source="model",
                log=self.logger.content,
                site=task.get("siteId"),
                date=task.get("measurementDate"),
                model=task.get("modelId"),
                product=task.get("productId"),
            )
            action = "fail"
        res = self.session.put(
            f"{self.dataportal_url}/queue/model/{action}/{task['id']}"
        )
        res.raise_for_status()
        return True


def main():
    config = utils.read_main_conf()
    worker = Worker(config)
    exit = Event()

    def signal_handler(sig, frame):
        exit.set()

    signal.signal(signal.SIGINT, signal_handler)

    try:
        while not exit.is_set():
            if not worker.process_task():
                exit.wait(10)
    except Exception as err:
        logging.exception("Fatal error in worker")
        send_slack_alert(config, err, source="worker", log=traceback.format_exc())


if __name__ == "__main__":
    main()