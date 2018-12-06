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
import os.path
import io
from threading import Thread
import math
import glob
from copy import copy
import sesamclient
import configparser
import itertools

sesam_version = "1.0"

logger = logging.getLogger('sesam')


class SesamParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n\n' % message)
        self.print_help()
        print("Exiting 2!")
        sys.exit(2)


class SesamNode:
    """ Sesam node functions wrapped in a class to facilitate unit tests """

    def __init__(self, node_url, jwt_token, logger, verify_ssl=True):
        self.logger = logger

        if jwt_token[0] == '"' and jwt_token[-1] == '"':
            self.jwt_token = jwt_token[1:-1]

        self.jwt_token = self.jwt_token.replace("bearer ", "")

        self.node_url = node_url.replace('"', "")
        if not self.node_url.startswith("http"):
            self.node_url = "https://%s/api" % node_url

        self.logger.info("Connecting to Seasam using url '%s' and JWT token '%s'", node_url, jwt_token)

        self.api_connection = sesamclient.Connection(sesamapi_base_url=self.node_url, jwt_auth_token=self.jwt_token,
                                                     timeout=60 * 10, verify_ssl=verify_ssl)


class SesamCmdClient:
    """ Commands wrapped in a class to make it easier to write unit tests """

    def __init__(self, args, logger):
        self.args = args
        self.logger = logger
        self.sesam_node = None

    def read_config_file(self, filename):
        parser = configparser.ConfigParser(strict=False)

        with open(filename) as fp:
            parser.read_file(itertools.chain(['[sesam]'], fp), source=filename)
            config = {}
            for key, value in parser.items("sesam"):
                config[key.lower()] = value

            return config

    def _coalesce(self, items):
        for item in items:
            if item is not None:
                return item

    def get_node_and_jwt_token(self):
        try:
            curr_dir = os.getcwd()
            if curr_dir is None:
                raise AssertionError("Failed to open current directory. Check your permissions.")

            # Find config on disk, if any
            try:
                file_config = {}
                if os.path.isfile(".syncconfig"):
                    # Found a local .syncconfig file, read it
                    file_config = self.read_config_file(".syncconfig")
                else:
                    # Look in the parent folder
                    if os.path.isfile("../.syncconfig"):
                        file_config = self.read_config_file("../.syncconfig")
                        if file_config:
                            curr_dir = os.path.abspath("../")
                            self.logger.info("Found .syncconfig in parent path. Using %s as base directory", path)
                            os.chdir(curr_dir)

                self.logger.info("Using %s as base directory", curr_dir)

                node_url = self._coalesce([args.node, os.environ.get("NODE"), file_config.get("node")])
                jwt_token = self._coalesce([args.jwt, os.environ.get("JWT"), file_config.get("jwt")])

                return node_url, jwt_token
            except BaseException as e:
                logger.exception("Failed to read '.syncconfig' from either the current directory or the "
                                 "parent directory. Check that you are in the correct directory, that you have the"
                                 "required permissions to read the files and that the files have the correct format.")

        except BaseException as e:
            logger.exception("Failed to find node url and/or jwt token")

        return None, None

    def clean(self):
        pass

    def upload(self):
        pass

    def download(self):
        pass

    def status(self):
        pass

    def verify(self):
        pass

    def test(self):
        pass

    def run(self):
        pass

    def wipe(self):
        pass


if __name__ == '__main__':
    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Log to stdout
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)
    logger.setLevel(logging.INFO)

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

    command = args.command.lower()

    sesam_cmd_client = SesamCmdClient(args, logger)

    try:
        node_url, jwt_token = sesam_cmd_client.get_node_and_jwt_token()
    except:
        logger.exception("failed to find a valid node url and jwt token")
        print("jwt and node must be specifed either as parameter, os env or in config file")
        sys.exit(1)

    try:
        sesam_cmd_client.sesam_node = SesamNode(node_url, jwt_token, logger,
                                                verify_ssl=args.skip_tls_verification is False)
    except:
        print("failed to connect to the sesam node using the url and jwt token we were given:\n%s\n%s" %
              (node_url, jwt_token))
        print("please verify the url and token is correct, and that there isn't any network issues "
              "(i.e. firewall, internet connection etc)")
        sys.exit(1)

    if command == "clean":
        sesam_cmd_client.clean()
    elif command == "upload":
        sesam_cmd_client.upload()
    elif command == "download":
        sesam_cmd_client.download()
    elif command == "status":
        sesam_cmd_client.status()
    elif command == "verify":
        sesam_cmd_client.verify()
    elif command == "test":
        sesam_cmd_client.test()
    elif command == "run":
        sesam_cmd_client.run()
    elif command == "wipe":
        sesam_cmd_client.wipe()
    else:
        print("unknown command: %s", command)
        sys.exit(1)
