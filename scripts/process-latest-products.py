#!/usr/bin/env python3
import subprocess
from datetime import datetime, timedelta, timezone

from data_processing import utils

sites = utils.get_all_but_hidden_sites()
sites = [site for site in sites if not site.startswith("arm-")]

interpreter = "python3"
script = "scripts/cloudnet.py"
subcommand = "process"
wrapper = "scripts/wrapper.py"
products = "categorize,classification,iwc,lwc,drizzle,ier,der"

for site in sites:
    date = datetime.now(timezone.utc) - timedelta(3)
    subprocess.check_call(
        [
            interpreter,
            wrapper,
            interpreter,
            script,
            "-s",
            site,
            "--p",
            products,
            "--start",
            date.strftime("%Y-%m-%d"),
            subcommand,
        ]
    )
