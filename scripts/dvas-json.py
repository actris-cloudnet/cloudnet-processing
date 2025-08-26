#!/usr/bin/env python3
import datetime
import json
import sys

from cloudnet_api_client import APIClient
from processing.config import Config
from processing.dvas import DvasMetadata
from processing.metadata_api import MetadataApi

file_uuid = sys.argv[1]
config = Config()
md_api = MetadataApi(config)
client = APIClient(config.dataportal_url + "/api")
file = client.file(file_uuid)
dvas_metadata = DvasMetadata(file, md_api, client)
dvas_json = dvas_metadata.create_dvas_json()
json.dump(dvas_json, sys.stdout, indent=2)
