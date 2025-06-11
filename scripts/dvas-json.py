#!/usr/bin/env python3
import datetime
import json
import sys

from processing.config import Config
from processing.dvas import DvasMetadata
from processing.metadata_api import MetadataApi

file_uuid = sys.argv[1]
config = Config()
md_api = MetadataApi(config)
file = md_api.get(f"api/files/{file_uuid}")
dvas_metadata = DvasMetadata(file, md_api)
dvas_timestamp = datetime.datetime.now(datetime.timezone.utc)
dvas_json = dvas_metadata.create_dvas_json(dvas_timestamp)
json.dump(dvas_json, sys.stdout, indent=2)
