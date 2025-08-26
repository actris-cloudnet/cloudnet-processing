#!/usr/bin/env python3
import argparse
import datetime
import json
import sys
from uuid import UUID

from cloudnet_api_client import APIClient
from processing.config import Config
from processing.dvas import DvasMetadata
from processing.metadata_api import MetadataApi

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "file_uuid", type=UUID, help="Output DVAS metadata for this file UUID"
    )
    args = parser.parse_args()
    config = Config()
    md_api = MetadataApi(config)
    client = APIClient(config.dataportal_url + "/api")
    file = client.file(args.file_uuid)
    dvas_metadata = DvasMetadata(file, md_api, client)
    dvas_timestamp = datetime.datetime.now(datetime.timezone.utc)
    dvas_json = dvas_metadata.create_dvas_json(dvas_timestamp)
    json.dump(dvas_json, sys.stdout, indent=2)
