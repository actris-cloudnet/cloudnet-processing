#!/usr/bin/env python3
import subprocess
from data_processing import utils

interpreter = 'python3'
wrapper = 'scripts/wrapper.py'
script = 'scripts/cloudnet.py'
subcommand = 'plot'

sites = utils.get_all_but_hidden_sites()

for site in sites:
    subprocess.check_call([interpreter,
                           wrapper,
                           interpreter,
                           script,
                           '-s', site,
                           '--start', '2000-01-01',
                           subcommand,
                           '-m'])
