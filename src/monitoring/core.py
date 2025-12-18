import argparse

from cloudnet_api_client import APIClient
from processing.config import Config

from monitoring.monitor import monitor
from monitoring.options import (
    build_opts,
)


def main():
    config = Config()
    client = APIClient(f"{config.dataportal_url}/api/")
    parser = argparse.ArgumentParser()
    parser.add_argument("period_type", choices=("all", "year", "month", "week", "day"))
    parser.add_argument("--site", type=list_str)
    parser.add_argument("--instrument", type=list_str)
    parser.add_argument("--product", type=list_str)
    parser.add_argument("--period", type=list_str)
    parser.add_argument("--start")
    parser.add_argument("--stop")
    args = parser.parse_args()
    opts = build_opts(args)
    monitor(opts, config, client)


def list_str(x: str) -> list[str]:
    return x.split(",")
