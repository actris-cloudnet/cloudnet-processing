#!/usr/bin/env python3
import subprocess

from cloudnet_processing import utils

interpreter = "python3"
wrapper = "scripts/wrapper.py"
script = "scripts/cloudnet.py"
subcommand = "plot"

sites = utils.get_all_but_hidden_sites()
products_list = utils.get_product_types_excluding_level3()
products = ",".join([str(product) for product in products_list])

for site in sites:
    subprocess.check_call(
        [
            interpreter,
            wrapper,
            interpreter,
            script,
            "-s",
            site,
            "-p",
            products,
            "--start",
            "2000-01-01",
            subcommand,
            "-m",
        ]
    )
