#!/usr/bin/env python3
import subprocess

from data_processing import utils

sites = utils.get_all_but_hidden_sites()

wrapper = "scripts/wrapper.py"
script = "scripts/cloudnet.py"
subcommand = "process"
interpreter = "python3"

for site in sites:
    subprocess.check_call(
        [
            interpreter,
            wrapper,
            interpreter,
            script,
            "-s",
            site,
            subcommand,
            "-u=1",
            "-H",
        ]
    )
