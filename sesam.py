import requests
import argparse
import logging
import shutil
import logging.handlers
from datetime import datetime, date, timedelta
from pprint import pformat, pprint
from tempfile import NamedTemporaryFile
import time
import sys
import os
import io
from threading import Thread
import math
import glob
from copy import copy
import sesamclient

sesam_version = "1.0"

logger = logging.getLogger('gdpr-ms')


class SesamParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n\n' % message)
        self.print_help()
        print("Exiting 2!")
        sys.exit(2)


if __name__ == '__main__':
    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Log to stdout
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    logger.propagate = False

    parser = SesamParser(prog="sesam", description="""
Commands:
  clean     Clean the build folder
  wipe      Deletes all the pipes, systems, user datasets and environment variables in the node
  upload    Replace node config with local config
  download  Replace local config with node config
  status    Compare node config with local config (requires external diff command)
  run       Run configuration until it stabilizes
  update    Store current output as expected output
  verify    Compare output against expected output
  test      Upload, run and verify output
""", formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-version', dest='version', required=False, action='store_true', help="print version number")

    parser.add_argument('-v', dest='verbose', required=False, action='store_true', help="be verbose")

    parser.add_argument('-vv', dest='extra_verbose', required=False, action='store_true', help="be extra verbose")

    parser.add_argument('-skip-tls-verification', dest='skip_tls_verification', required=False, action='store_true',
                        help="skip verifying the TLS certificate")

    parser.add_argument('-dump', dest='dump', required=False, help="dump zip content to disk", action='store_true')

    parser.add_argument('-print-scheduler-log', dest='print_scheduler_log', required=False,
                        help="print scheduler log during run", action='store_true')

    parser.add_argument('-custom-scheduler', dest='custom_scheduler', required=False,
                        help="by default a scheduler system will be added, enable this flag if you have configured a "
                             "custom scheduler as part of the config", action='store_true')

    parser.add_argument('-node', dest='node', metavar="<string>", required=False, help="service url")
    parser.add_argument('-jwt', dest='jwt', metavar="<string>", required=False, help="authorization token")

    parser.add_argument('-single', dest='single', required=False, metavar="<string>", help="update or verify just a single pipe")

    parser.add_argument('-profile', dest='profile', metavar="<string>", required=False, help="env profile to use <profile>-env.json")

    parser.add_argument('-scheduler-id', dest='scheduler_id', metavar="<string>", required=False, help="system id for the scheduler system")

    parser.add_argument('-runs', dest='runs', type=int, metavar="<int>", required=False, default=1,
                        help="number of test cycles to check for stability")

    parser.add_argument('-scheduler-poll-frequency', metavar="<int>", dest='scheduler-poll-frequency', type=int, required=False,
                        default=5000, help="milliseconds between each poll while waiting for the scheduler")

    parser.add_argument('command', metavar="command", help="a valid command from the list above")

    try:
        args = parser.parse_args()
    except SystemExit as e:
        sys.exit(e.code)
    except BaseException as e:
        sys.exit(1)

    if args.version:
        print("sesam version %s", sesam_version)
        sys.exit(0)
