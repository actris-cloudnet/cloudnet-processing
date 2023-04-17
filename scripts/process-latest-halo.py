#!/usr/bin/env python3
import subprocess

from data_processing import utils

sites = utils.get_all_but_hidden_sites()


for site in sites:
    subprocess.check_call(
        [
            "python3",
            "scripts/cloudnet.py",
            "-s",
            site,
            "-p",
            "doppler-lidar",
            "process",
            "-u=1",
        ]
    )
