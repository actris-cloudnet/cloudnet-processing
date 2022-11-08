#!/usr/bin/env python3
import os
import subprocess
import sys
from tempfile import NamedTemporaryFile

import test_utils.utils as utils
from data_processing.subcmds import freeze

sys.path.append("scripts/")
cloudnet = __import__("cloudnet")

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))

session, adapter, mock_addr = utils.init_test_session()
temp_file = NamedTemporaryFile()


def register_storage_urls():
    def save_product(request):
        with open(temp_file.name, mode="wb") as file:
            file.write(request.body.read())
        return True

    prod_path = f"{mock_addr}cloudnet-product-volatile/"
    filename = "20201022_bucharest_categorize.nc"
    url = f"{prod_path}{filename}"

    adapter.register_uri("DELETE", url)
    adapter.register_uri("GET", url, body=open(f"tests/data/products/{filename}", "rb"))
    adapter.register_uri(
        "PUT",
        f'{url.replace("-volatile","")}',
        additional_matcher=save_product,
        json={"size": 667, "version": "abc"},
    )


def main():

    utils.start_server(5000, "tests/data/server/metadata/freeze", f"{SCRIPT_PATH}/md.log")
    utils.start_server(5001, "tests/data/server/pid", f"{SCRIPT_PATH}/pid.log")
    register_storage_urls()

    args = cloudnet._parse_args(["-s=all", "freeze"])
    freeze.main(args, storage_session=session)

    try:
        subprocess.check_call(
            ["pytest", "-v", f"{SCRIPT_PATH}/tests.py", "--full_path", temp_file.name]
        )
    except subprocess.CalledProcessError:
        raise


if __name__ == "__main__":
    main()
