#!/usr/bin/env python3
import argparse
import datetime
import subprocess

from data_processing import utils
from data_processing.metadata_api import MetadataApi

L1B_PRODUCTS = ",".join(utils.get_product_types(level="1b"))
L1C_AND_L2_PRODUCTS = ",".join(
    [
        product
        for product in utils.get_product_types(level="1c")
        + utils.get_product_types(level="2")
        if "voodoo" not in product
    ]
)


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
        return (now - datetime.timedelta(hours=self.args_in.hours)).isoformat()

    def _get_sites(self) -> list[str]:
        """Find candidate sites for processing."""
        if self.args_in.sites in ("campaign", "cloudnet"):
            payload = {"type": self.args_in.sites}
        else:
            payload = {"type": ["campaign", "cloudnet"]}
        site_metadata = self.md_api.get("api/sites", payload)
        return [s["id"] for s in site_metadata]

    def _get_model_metadata(self) -> list[dict]:
        metadata = self.md_api.get("api/model-files", self._get_payload())
        cloudnet_metadata = [m for m in metadata if "cloudnet" in m["site"]["type"]]
        # Most of the campaign sites have been inactive for ages
        # so let's focus on the active ones
        active_campaign_metadata = [
            m
            for m in metadata
            if m["site"]["type"] == "campaign" and m["site"]["status"] == "active"
        ]
        return active_campaign_metadata + cloudnet_metadata

    @staticmethod
    def _extract_unique_sites(metadata: list[dict]) -> set[str]:
        return {m["site"]["id"] for m in metadata}

    @staticmethod
    def _extract_unique_site_dates(metadata: list[dict]) -> set[tuple[str, str]]:
        return {(m["site"]["id"], m["measurementDate"]) for m in metadata}

    @staticmethod
    def _call_subprocess(site: str, args: list[str]):
        prefix = ["python3", "scripts/wrapper.py", "python3", "scripts/cloudnet.py"]
        subprocess.check_call(prefix + ["-s", site] + args)

    def _get_payload(self) -> dict:
        return {"updatedAtFrom": self.time_limit, "site": self._get_sites()}


class QcMetadata(MetaData):
    def process(self) -> None:
        metadata = self._get_metadata()
        for site, date in self._extract_unique_site_dates(metadata):
            products = {
                m["product"]["id"]
                for m in metadata
                if m["site"]["id"] == site and m["measurementDate"] == date
            }
            args = ["-p", ",".join(products), "-d", date, "qc"]
            if self.args_in.force:
                args.append("-f")
            self._call_subprocess(site, args)

    def _get_metadata(self) -> list[dict]:
        metadata = self.md_api.get("api/files", self._get_payload())
        model_metadata = self._get_model_metadata()
        return metadata + model_metadata


class RawMetadata(MetaData):
    def process(self) -> None:
        metadata = self._get_metadata()
        for site in self._extract_unique_sites(metadata):
            args = ["-p", L1B_PRODUCTS, "process", "-u", str(self.args_in.hours)]
            if self.args_in.housekeeping:
                args.append("-H")
            self._call_subprocess(site, args)

    def _get_metadata(self) -> list[dict]:
        payload = {**self._get_payload(), "status": "uploaded"}
        metadata = self.md_api.get("api/raw-files", payload)
        return metadata


class ProductsMetadata(MetaData):
    def process(self) -> None:
        metadata = self._get_metadata()
        for site, date in self._extract_unique_site_dates(metadata):
            args = ["-p", L1C_AND_L2_PRODUCTS, "-d", date, "process"]
            if self.args_in.reprocess:
                args.append("-r")
            self._call_subprocess(site, args)

    def _get_metadata(self) -> list[dict]:
        l1b_metadata = self._get_l1b_metadata()
        model_metadata = self._get_model_metadata()
        return l1b_metadata + model_metadata

    def _get_l1b_metadata(self) -> list[dict]:
        payload = {
            **self._get_payload(),
            "product": ["radar", "lidar", "mwr", "disdrometer", "doppler-lidar"],
        }
        metadata = self.md_api.get("api/files", payload)
        return metadata


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
    parser.add_argument(
        "-H",
        "--housekeeping",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        default=False,
        help="Force QC checks",
    )
    parser.add_argument(
        "-s",
        "--sites",
        choices=["all", "campaign", "cloudnet"],
        default="all",
        help="Force QC checks",
    )
    main(parser.parse_args())
