#!/usr/bin/env python3
import sys
from subprocess import PIPE, STDOUT, Popen

from data_processing import utils


def main(args):
    log = ""
    with Popen(args, stdout=PIPE, stderr=STDOUT, encoding="utf-8", errors="replace") as proc:
        while proc.poll() is None:
            buf = proc.stdout.readline()
            sys.stdout.write(buf)
            log += buf
        buf = proc.stdout.read()
        sys.stdout.write(buf)
        log += buf
        if proc.returncode != 0:
            utils.send_slack_alert(
                Exception(f"Command returned non-zero exit status {proc.returncode}"),
                "wrapper",
                critical=True,
                log=log,
            )


if __name__ == "__main__":
    main(sys.argv[1:])
