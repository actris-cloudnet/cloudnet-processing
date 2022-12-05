#!/usr/bin/env python3
import subprocess
import sys

from data_processing import utils


def main(args):
    try:
        subprocess.run(
            args,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            errors="replace",
        )
    except subprocess.CalledProcessError as err:
        utils.send_slack_alert(err, "wrapper", critical=True, log=err.output)


if __name__ == "__main__":
    main(sys.argv[1:])
