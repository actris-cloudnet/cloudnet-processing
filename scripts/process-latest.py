#!/usr/bin/env python3
import argparse
import datetime
import subprocess

from data_processing import utils
from data_processing.metadata_api import MetadataApi


def main(args_in: argparse.Namespace):
    class_map = {
        "raw": RawMetadata,
        "products": ProductsMetadata,
        "qc": QcMetadata,
    }
    metadata = class_map[args_in.command](args_in)
    metadata.process()


class MetaData:
    def __init__(self, args_in: argparse.Namespace):
        self.args_in = args_in
        self.md_api = MetadataApi(utils.read_main_conf())
        self.time_limit = self._get_time_limit()

    def process(self) -> None:
        pass

    def _get_time_limit(self) -> str:
        """Get time limit for metadata queries."""
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        time_limit = (now - datetime.timedelta(hours=self.args_in.hours)).isoformat()
        return time_limit

    def _get_sites(self) -> list[str]:
        """Find candidate sites for processing."""
        payload = {type: ["campaign", "cloudnet"]}
        site_metadata = self.md_api.get("api/sites", payload)
        return [s["id"] for s in site_metadata]

    @staticmethod
    def _extract_unique_sites(metadata: list[dict]) -> list[str]:
        return list({m["site"]["id"] for m in metadata})

    @staticmethod
    def _extract_unique_site_dates(metadata: list[dict]) -> list[tuple[str, str]]:
        site_dates = {(m["site"]["id"], m["measurementDate"]) for m in metadata}
        return list(site_dates)

    @staticmethod
    def _call_subprocess(site: str, args: list[str]):
        prefix = ["python3", "scripts/wrapper.py", "python3", "scripts/cloudnet.py"]
        subprocess.check_call(prefix + ["-s", site] + args)


class QcMetadata(MetaData):
    def process(self) -> None:
        metadata = self._get_metadata()
        for site, date in self._extract_unique_site_dates(metadata):
            products = [
                m["product"]["id"]
                for m in metadata
                if m["site"]["id"] == site and m["measurementDate"] == date
            ]
            args = ["-p", ",".join(products), "-d", date, "qc", "-f"]
            self._call_subprocess(site, args)

    def _get_metadata(self) -> list[dict]:
        metadata = self.md_api.get("api/files", {"updatedAtFrom": self.time_limit})
        return metadata


class RawMetadata(MetaData):
    def process(self) -> None:
        metadata = self._get_metadata()
        for site in self._extract_unique_sites(metadata):
            args = ["process", "-u", str(self.args_in.hours)]
            self._call_subprocess(site, args)

    def _get_metadata(self) -> list[dict]:
        payload = {
            "updatedAtFrom": self.time_limit,
            "site": self._get_sites(),
            "status": "uploaded",
        }
        metadata = self.md_api.get("api/raw-files", payload)
        metadata = [m for m in metadata if not m["filename"].lower().endswith(".lv0")]
        return metadata


class ProductsMetadata(MetaData):
    def process(self) -> None:
        products = "categorize,classification,iwc,lwc,drizzle,ier,der,mwr-l1c,mwr-single,mwr-multi"
        metadata = self._get_metadata()
        for site, date in self._extract_unique_site_dates(metadata):
            args = ["-p", products, "-d", date, "process"]
            args += ["-r"] if self.args_in.reprocess else []
            self._call_subprocess(site, args)

    def _get_metadata(self) -> list[dict]:
        l1b_metadata = self._get_l1b_metadata()
        model_metadata = self._get_model_metadata()
        return l1b_metadata + model_metadata

    def _get_l1b_metadata(self) -> list[dict]:
        categorize_input = ["radar", "lidar", "mwr"]
        payload = {"updatedAtFrom": self.time_limit, "product": categorize_input}
        metadata = self.md_api.get("api/files", payload)
        return metadata

    def _get_model_metadata(self) -> list[dict]:
        payload = {"updatedAtFrom": self.time_limit, "site": self._get_sites()}
        metadata = self.md_api.get("api/model-files", payload)
        cloudnet_metadata = [m for m in metadata if m["site"]["type"] == "cloudnet"]
        # Most of the campaign sites have been inactive for ages
        # so let's focus on the active ones
        active_campaign_metadata = [
            m
            for m in metadata
            if m["site"]["type"] == "campaign" and m["site"]["status"] == "active"
        ]
        return active_campaign_metadata + cloudnet_metadata


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["raw", "products", "qc"])
    parser.add_argument("--hours", type=float, default=2)
    parser.add_argument(
        "-r",
        "--reprocess",
        action="store_true",
        default=False,
    )
    main(parser.parse_args())
