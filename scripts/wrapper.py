#!/usr/bin/env python3
import subprocess
import sys
from data_processing import utils


def main(args):

    try:
        subprocess.check_call(args)
    except subprocess.CalledProcessError as err:
        utils.send_slack_alert(err, 'wrapper', critical=True)


if __name__ == "__main__":
    main(sys.argv[1:])
