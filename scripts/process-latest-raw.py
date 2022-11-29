#!/usr/bin/env python3
import subprocess
from sys import argv

from data_processing import utils

default_sites = utils.get_cloudnet_sites()
additional_sites = argv[1:]
sites = default_sites + additional_sites

wrapper = "scripts/wrapper.py"
script = "scripts/cloudnet.py"
subcommand = "process"
interpreter = "python3"

for site in sites:
    subprocess.check_call(
        [interpreter, wrapper, interpreter, script, "-s", site, subcommand, "-u=1"]
    )
