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
import json
import os
import zipfile
import uuid

sesam_version = "1.0"

logger = logging.getLogger('sesam')
LOGLEVEL_TRACE = 2


class SesamParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n\n' % message)
        self.print_help()
        print("Exiting 2!")
        sys.exit(2)


class TestSpec:
    """ Test specification """

    def __init__(self, filename):
        self._spec = {}
        self._spec["filename"] = filename
        self._spec["name"] = filename[:-len(".test.json")]
        self._spec["file"] = self.name + ".json"
        self._spec["endpoint"] = "json"
        if self.name.find("/") > -1:
            self._spec["pipe"] = self.name.split("/")[:-1]

        with open(filename, "r") as fp:
            spec_dict = json.load(fp)
            if isinstance(spec_dict, dict):
                self._spec.update(spec_dict)
            else:
                logger.error("Test spec '%s' not in correct json format" % filename)
                raise AssertionError("Test spec not a json object")

    @property
    def spec(self):
        return self.spec

    @property
    def filename(self):
        return self.spec.get("filename")

    @property
    def name(self):
        return self.spec.get("name")

    @property
    def endpoint(self):
        return self.spec.get("endpoint")

    @property
    def pipe(self):
        return self.spec.get("pipe")

    @property
    def blacklist(self):
        return self.spec.get("blacklist")

    @property
    def id(self):
        return self.spec.get("_id")

    @property
    def ignore(self):
        return self.spec.get("ignore", False) is True

    @property
    def parameters(self):
        return self.spec.get("parameters")


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

        self.logger.debug("Connecting to Seasam using url '%s' and JWT token '%s'", node_url, jwt_token)

        self.api_connection = sesamclient.Connection(sesamapi_base_url=self.node_url, jwt_auth_token=self.jwt_token,
                                                     timeout=60 * 10, verify_ssl=verify_ssl)

    def put_config(self, config, force=False):
        self.logger.log(LOGLEVEL_TRACE, "PUT config to %s" % self.node_url)
        self.api_connection.upload_config(config, force=force)

    def put_env(self, env_vars):
        self.logger.log(LOGLEVEL_TRACE, "PUT env vars to %s" % self.node_url)
        self.api_connection.put_env_vars(env_vars)

    def get_system(self, system_id):
        self.logger.log(LOGLEVEL_TRACE, "Get system '%s' from %s" % (system_id, self.node_url))
        return self.api_connection.get_system(system_id)

    def add_system(self, config):
        self.logger.log(LOGLEVEL_TRACE, "Add system '%s' to %s" % (config, self.node_url))
        return self.api_connection.add_systems([config])

    def add_systems(self, config):
        self.logger.log(LOGLEVEL_TRACE, "Add systems '%s' to %s" % (config, self.node_url))
        return self.api_connection.add_systems(config)

    def remove_system(self, system_id):
        self.logger.log(LOGLEVEL_TRACE, "Remove system '%s' from %s" % (system_id, self.node_url))
        system = self.api_connection.get_system(system_id)
        if system is not None:
            system.delete()
        else:
            raise AssertionError("Could not remove system '%s' as it doesn't exist" % system_id)

    def get_config(self, filename=None):
        data = self.api_connection.get_config_as_zip()

        if filename:
            with open(filename, "wb") as fp:
                fp.write(data)

            return data
        else:
            # Return as zip object
            return zipfile.ZipFile(io.BytesIO(data))

    def remove_all_datasets(self):
        self.logger.log(LOGLEVEL_TRACE, "Remove alle datasets from %s" % self.node_url)
        for dataset in self.api_connection.get_datasets():
            dataset_id = dataset.id
            if not dataset.id.startswith("system:"):
                try:
                    dataset.delete()
                    self.logger.debug("Dataset '%s' deleted" % dataset_id)
                except BaseException as e:
                    self.logger.error("Failed to delete dataset '%s'" % dataset.id)
                    raise e

    def get_pipe_type(self, pipe):
        source_config = pipe.config["effective"].get("source",{})
        sink_config = pipe.config["effective"].get("sink",{})
        source_type = source_config.get("type", "")
        sink_type = sink_config.get("type", "")

        if source_type == "embedded":
            return "input"

        if isinstance(sink_type, str) and sink_type.endswith("_endpoint"):
            return "endpoint"

        if (source_config.get("dataset") or source_config.get("datasets")) and\
                sink_type.get("dataset"):
            return "internal"

        if not sink_type.get("dataset"):
            return "output"

        return "internal"

    def get_output_pipes(self):
        return [p for p in self.api_connection.get_pipes() if self.get_pipe_type() == "output"]

    def get_input_pipes(self):
        return [p for p in self.api_connection.get_pipes() if self.get_pipe_type() == "input"]

    def get_endpoint_pipes(self):
        return [p for p in self.api_connection.get_pipes() if self.get_pipe_type() == "endpoint"]

    def get_internal_pipes(self):
        return [p for p in self.api_connection.get_pipes() if self.get_pipe_type() == "internal"]

    def get_system_status(self, system_id):

        system_url = self.api_connection.get_system_url(system_id)

        resp = self.api_connection.session.get(system_url + "/status")
        resp.raise_for_status()

        status = resp.json()
        if isinstance(status, dict):
            return status

        return None

    def wait_for_microservice(self, microservice_id, timeout=300):
        """ Polls the microservice status API until it is running (or we time out) """

        system = self.get_system(microservice_id)
        if system is None:
            raise AssertionError("Microservice system '%s' doesn't exist" % microservice_id)

        sleep_time = 0.5
        while timeout > 0:
            system_status = self.get_system_status(microservice_id)
            if system_status is not None and system_status.get("running", False) is True:
                return True

            time.sleep(sleep_time)

            timeout -= sleep_time

        return False

    def microservice_get_proxy_request(self, microservice_id, path, params=None, result_as_json=True):

        system = self.get_system(microservice_id)
        if system is None:
            raise AssertionError("Microservice system '%s' doesn't exist" % microservice_id)

        system_url = self.api_connection.get_system_url(microservice_id)
        resp = self.api_connection.session.get(system_url + "/proxy/" + path, params=params)
        resp.raise_for_status()

        if result_as_json:
            return resp.json()

        return resp.text

    def microservice_post_proxy_request(self, microservice_id, path, params=None, data=None, result_as_json=True):
        return self.microservice_post_put_proxy_request(microservice_id, "POST", path, params=params, data=data,
                                                        result_as_json=result_as_json)

    def microservice_put_proxy_request(self, microservice_id, path, params=None, data=None, result_as_json=True):
        return self.microservice_post_put_proxy_request(microservice_id, "PUT", path, params=params, data=data,
                                                        result_as_json=result_as_json)

    def microservice_post_put_proxy_request(self, microservice_id, method, path, params=None, data=None,
                                            result_as_json=True):

        system = self.get_system(microservice_id)
        if system is None:
            raise AssertionError("Microservice system '%s' doesn't exist" % microservice_id)

        system_url = self.api_connection.get_system_url(microservice_id)
        if method.lower() == "post":
            resp = self.api_connection.session.post(system_url + "/proxy/" + path, params=params, data=data)
        elif method.lower() == "put":
            resp = self.api_connection.session.put(system_url + "/proxy/" + path, params=params, data=data)
        else:
            raise AssertionError("Unknown method '%s'" % method)

        resp.raise_for_status()

        if result_as_json:
            return resp.json()

        return resp.text


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

    def zip_dir(self, zipfile, dir):
        for root, dirs, files in os.walk(dir):
            for file in files:
                if file.endswith(".conf.json"):
                    zipfile.write(os.path.join(root, file))

    def get_zip_config(self, remove_zip=True):
        if os.path.isfile("sesam-config.zip"):
            os.remove("sesam-config.zip")

        zip_file = zipfile.ZipFile('sesam-config.zip', 'w', zipfile.ZIP_DEFLATED)

        self.zip_dir(zip_file, "pipes")
        self.zip_dir(zip_file, "systems")

        if os.path.isfile("node-metadata.conf.json"):
            zip_file.write("node-metadata.conf.json")

        zip_file.close()

        with open("sesam-config.zip", "rb") as fp:
            zip_data = fp.read()

        if remove_zip:
            os.remove("sesam-config.zip")

        return zip_data

    def get_node_and_jwt_token(self):
        try:
            curr_dir = os.getcwd()
            if curr_dir is None:
                self.logger.error("Failed to open current directory. Check your permissions.")
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

                self.node_url = self._coalesce([args.node, os.environ.get("NODE"), file_config.get("node")])
                self.jwt_token = self._coalesce([args.jwt, os.environ.get("JWT"), file_config.get("jwt")])

                return self.node_url, self.jwt_token
            except BaseException as e:
                self.logger.error("Failed to read '.syncconfig' from either the current directory or the "
                                  "parent directory. Check that you are in the correct directory, that you have the"
                                  "required permissions to read the files and that the files have the correct format.")
                raise e

        except BaseException as e:
            logger.error("Failed to find node url and/or jwt token")
            raise e

    def clean(self):
        try:
            if os.path.isdir("./build"):
                shutil.rmtree("./build")
        except BaseException as e:
            self.logger.error("Failed to remove 'build' directory. Check permissions.")
            raise e

    def upload(self):
        # Find env vars to upload
        profile_file = "%s-env.json" % self.args.profile
        try:
            with open(profile_file, "r") as fp:
                json_data = json.load(fp)
        except BaseException as e:
            self.logger.error("Failed to parse profile: '%s'" % profile_file)
            raise e

        try:
            self.sesam_node.put_env(json_data)
        except BaseException as e:
            self.logger.error("Failed to replace environment variables in Sesam")
            raise e

        # Zip the relevant directories and upload to Sesam
        try:
            zip_config = self.get_zip_config(remove_zip=args.dump is False)
        except BaseException as e:
            logger.error("Failed to create zip archive of config")
            raise e

        try:
            self.sesam_node.put_config(zip_config, force=True)
        except BaseException as e:
            self.logger.error("Failed to upload config to sesam")
            raise e

        self.logger.info("Config uploaded successfully")

    def dump(self):
        try:
            zip_config = self.get_zip_config(remove_zip=False)
        except BaseException as e:
            logger.error("Failed to create zip archive of config")
            raise e

    def download(self):
        if not self.args.custom_scheduler:
            # Remove the scheduler, if it exists
            system = self.sesam_node.get_system(args.scheduler_id)
            if system is not None:
                try:
                    self.sesam_node.remove_system(args.scheduler_id)
                except BaseException as e:
                    self.logger.error("Failed to remove the scheduler system '%s'" % self.args.scheduler_id)
                    raise e

        if self.args.dump:
            if os.path.isfile("sesam-config.zip"):
                    os.remove("sesam-config.zip")

            zip_data = self.sesam_node.get_config(filename="sesam-config.zip")
            self.logger.info("Dumped downloaded config to 'sesam-config.zip'")
            zip_config = zipfile.ZipFile(io.BytesIO(zip_data))
        else:
            zip_config = self.sesam_node.get_config()

        try:
            zip_config.extractall()
        except BaseException as e:
            self.logger.error("Failed to unzip config file from Sesam to current directory")
            raise e

        zip_config.close()

        self.logger.info("Replaced local config successfully")

    def status(self):
        logger.error("The 'status' command is not yet implemented")

    def filter_entity(self, entity):
        return entity

    def load_test_specs(self, existing_output_pipes, update=False):
        test_specs = {}

        # Load test specifications
        for filename in glob.glob("expected/*.test.json"):
            logger.debug("Processing spec file '%s'" % filename)

            with open(filename, "r") as fp:
                test_spec = json.load(fp)

                pipe_id = test_spec.get("pipe", filename[:-(len(".test.json"))])
                logger.debug("Pipe id for spec '%s' is '%s" % pipe_id)

                # If spec says 'ignore' then the corresponding output file should not exist
                if test_spec.get("ignore") is True:
                    output_filename = test_spec.get("file")
                    if not output_filename:
                        output_filename = pipe_id + ".json"

                    if os.path.isfile("expected/%s" % output_filename):
                        if update:
                            logger.debug("Removing existing output file '%s'" % output_filename)
                            os.remove(output_filename)
                        else:
                            logger.warning(
                                "pipe '%s' is ignored, but output file '%s' still exists" % (pipe_id, filename))
                elif pipe_id not in existing_output_pipes:
                    logger.error("Test spec references non-exisiting output "
                                 "pipe '%s' - remove '%s'" % (pipe_id, filename))
                    continue

                if pipe_id not in test_specs:
                    test_specs[pipe_id] = []

                test_specs[pipe_id].append(test_spec)

        if update:
            for pipe in existing_output_pipes.items():
                logger.debug("Updating pipe '%s" % pipe.id)

                if pipe.id not in test_specs:
                    logger.warning("Found no spec for pipe %s - creating empty spec file")
                    with open("%s.test.json" % pipe.id, "w") as fp:
                        json.dump(fp, {})
                        test_specs[pipe.id] = [{}]

                # Download endpoint data, sort each entity on key and store the json output
                entities = [self.filter_entity(e) for e in self.sesam_node.get_pipe_entities(pipe.id)]

                filename = pipe.id + ".json"
                with open("expected/%s" % filename, "w") as fp:
                    json.dump(fp, entities, indent=2, sort_keys=True)

        return test_specs

    def verify(self):
        output_pipes = {}
        for p in self.sesam_node.get_output_pipes():
            output_pipes[p.id] = p

        test_specs = self.load_test_specs(output_pipes)

        if not test_specs:
            raise AssertionError("Found no tests (*.test.json) to run")

        for pipe in output_pipes:
            logger.debug("Verifying pipe '%s" % pipe.id)







    def update(self):
        logger.error("The 'update' command is not yet implemented")

    def test(self):
        self.upload()

        for i in range(self.args.runs):
            self.run()
            self.verify()

    def start_scheduler(self, timeout=300):
        if self.sesam_node.get_system(self.args.scheduler_id) is not None:
            self.sesam_node.remove_system(self.args.scheduler_id)

        if not self.args.custom_scheduler:
            self.sesam_node.add_system({
                "_id": "%s" % self.args.scheduler_id,
                "type": "system:microservice",
                "docker": {
                    "environment": {
                        "JWT": "%s" % self.jwt_token,
                        "URL": "%s" % self.node_url,
                        "DUMMY": "%s" % str(uuid.uuid4())
                    },
                    "image":  "sesamcommunity/scheduler:latest",
                    "port": 5000
                }
            })

        # Wait for Microservice system to start up
        if self.sesam_node.wait_for_microservice(self.args.scheduler_id, timeout=timeout) is False:
            raise RuntimeError("Timed out waiting for scheduler to load")

        # Start the microservice
        params = {"reset_pipes": "true", "delete_datasets": "true", "compact_execution_datasets": "true"}
        try:
            self.sesam_node.microservice_post_request(self.args.scheduler_id, "start", params=params,
                                                      result_as_json=False)
        except BaseException as e:
            logger.error("Failed to start the scheduler microservice")
            raise e

        sleep_interval = args.scheduler_poll_frequency/1000
        try:
            while sleep_interval > 0:
                status_json = self.get_scheduler_status()
                if status_json.get("state", "") != "init":
                    break

                time.sleep(sleep_interval)
                timeout -= sleep_interval
        except BaseException as e:
            logger.error("Scheduler failed to initialise after %s seconds" % timeout)
            raise e

    def get_scheduler_status(self):

        try:
            return self.sesam_node.microservice_get_proxy_request(self.args.scheduler_id, "")
        except BaseException as e:
            logger.error("Failed to get scheduler status")
            raise e

    def run(self):
        self.start_scheduler()

        try:
            while True:
                status = self.get_scheduler_status()

                if status == "success":
                    logger.debug("Scheduler finished successfully")
                    break
                elif status == "failed":
                    logger.error("Scheduler finished with failure")
                    return

                time.sleep(args.scheduler_poll_frequency/1000)

        except BaseException as e:
            logger.error("Failed to run scheduler")
            raise e
        finally:
            self.sesam_node.remove_system(args.scheduler_id)

        self.logger.info("Successfully ran all pipes to completion")

    def wipe(self):
        try:
            self.sesam_node.put_config([], force=True)
            self.logger.info("Removed pipes and systems")
        except BaseException as e:
            logger.error("Failed to wipe config")
            raise e

        try:
            self.sesam_node.put_env({})
            self.logger.info("Removed environment variables")
        except BaseException as e:
            logger.error("Failed to wipe environment variables")
            raise e

        try:
            self.sesam_node.remove_all_datasets()
            self.logger.info("Removed datasets")
        except BaseException as e:
            logger.error("Failed to delete datasets")
            raise e

        logger.info("Successfully wiped node")


if __name__ == '__main__':
    parser = SesamParser(prog="sesam", description="""
Commands:
  clean     Clean the build folder
  wipe      Deletes all the pipes, systems, user datasets and environment variables in the node
  upload    Replace node config with local config
  download  Replace local config with node config
  dump      Create a zip archive of the config and store it as 'sesam-config.zip'
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

    parser.add_argument('-profile', dest='profile', metavar="<string>", default="test", required=False, help="env profile to use <profile>-env.json")

    parser.add_argument('-scheduler-id', dest='scheduler_id', default="scheduler", metavar="<string>", required=False, help="system id for the scheduler system")

    parser.add_argument('-runs', dest='runs', type=int, metavar="<int>", required=False, default=1,
                        help="number of test cycles to check for stability")

    parser.add_argument('-logformat', dest='logformat', type=str, metavar="<string>", required=False, default="short",
                        help="output format (normal or log)")

    parser.add_argument('-scheduler-poll-frequency', metavar="<int>", dest='scheduler_poll_frequency', type=int, required=False,
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

    if args.logformat == "log":
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    else:
        format_string = '%(message)s'

    # Log to stdout
    logging.addLevelName(LOGLEVEL_TRACE, "TRACE")
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    elif args.extra_verbose:
        from http.client import HTTPConnection
        HTTPConnection.debuglevel = 1
        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True
        logger.setLevel(LOGLEVEL_TRACE)
    else:
        logger.setLevel(logging.INFO)

    logger.propagate = False

    command = args.command.lower()

    sesam_cmd_client = SesamCmdClient(args, logger)

    try:
        node_url, jwt_token = sesam_cmd_client.get_node_and_jwt_token()
    except BaseException as e:
        if args.verbose or args.extra_verbose:
            logger.exception(e)
        logger.error("jwt and node must be specifed either as parameter, os env or in config file")
        sys.exit(1)

    try:
        sesam_cmd_client.sesam_node = SesamNode(node_url, jwt_token, logger,
                                                verify_ssl=args.skip_tls_verification is False)
    except BaseException as e:
        if args.verbose or args.extra_verbose:
            logger.exception(e)
        logger.error("failed to connect to the sesam node using the url and jwt token we were given:\n%s\n%s" %
                     (node_url, jwt_token))
        logger.error("please verify the url and token is correct, and that there isn't any network issues "
                     "(i.e. firewall, internet connection etc)")
        sys.exit(1)

    try:
        if command == "clean":
            sesam_cmd_client.clean()
        elif command == "upload":
            sesam_cmd_client.upload()
        elif command == "download":
            sesam_cmd_client.download()
        elif command == "status":
            sesam_cmd_client.status()
        elif command == "update":
            sesam_cmd_client.update()
        elif command == "verify":
            sesam_cmd_client.verify()
        elif command == "test":
            sesam_cmd_client.test()
        elif command == "run":
            sesam_cmd_client.run()
        elif command == "wipe":
            sesam_cmd_client.wipe()
        elif command == "dump":
            sesam_cmd_client.dump()
        else:
            logger.error("unknown command: %s", command)
            raise AssertionError("unknown command: %s" % command)
    except BaseException as e:
        if args.verbose or args.extra_verbose:
            logger.exception(e)

        sys.exit(1)
