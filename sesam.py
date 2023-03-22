import requests
from requests.exceptions import HTTPError
import argparse
import logging
import threading
import shutil
import logging.handlers
import time
import sys
import os
import os.path
import io
import copy
import glob
from lxml import etree
import sesamclient
import configparser
import itertools
import json
import os
import zipfile
import uuid
from difflib import unified_diff
from fnmatch import fnmatch
from decimal import Decimal
import pprint
from jsonformat import format_object, FormatStyle
import simplejson as json
from connector_cli.connectorpy import *
from connector_cli.oauth2login import *
from connector_cli.tripletexlogin import *

sesam_version = "2.5.19"

logger = logging.getLogger('sesam')
LOGLEVEL_TRACE = 2
BASE_DIR = os.getcwd()
GIT_ROOT = None


def normalize_path(filename):
    # Normalize windows paths to linux
    return filename.replace("\\", "/")


class SesamParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n\n' % message)
        self.print_help()
        sys.exit(2)


class TestSpec:
    """ Test specification """

    def __init__(self, filename):
        self._spec = {}
        self._spec_file = filename

        self._spec["name"] = filename[:-len(".test.json")]
        self._spec["file"] = self.name + ".json"
        self._spec["endpoint"] = "json"

        if self.name.find(os.sep) > -1:
            self._spec["pipe"] = self.name.split(os.sep)[-1]
        else:
            self._spec["pipe"] = self.name

        with open(filename, "r", encoding="utf-8-sig") as fp:
            spec_dict = json.load(fp)
            if isinstance(spec_dict, dict):
                self._spec.update(spec_dict)
            else:
                logger.error("Test spec '%s' not in correct json format" % filename)
                raise AssertionError("Test spec not a json object")

    @property
    def spec(self):
        return self._spec

    @property
    def spec_file(self):
        filename = self._spec_file
        if not filename.startswith("expected" + os.sep):
            filename = os.path.join("expected", filename)
        return filename

    @property
    def file(self):
        filename = self._spec.get("file")
        if not filename.startswith("expected" + os.sep):
            filename = os.path.join("expected", filename)
        return filename

    @property
    def name(self):
        return self._spec.get("name")

    @property
    def ignore_deletes(self):
        return self._spec.get("ignore_deletes", True) is True

    @property
    def endpoint(self):
        return self._spec.get("endpoint")

    @property
    def pipe(self):
        return self._spec.get("pipe")

    @property
    def stage(self):
        return self._spec.get("stage")

    @property
    def blacklist(self):
        return self._spec.get("blacklist")

    @property
    def id(self):
        return self._spec.get("_id")

    @property
    def ignore(self):
        return self._spec.get("ignore", False) is True

    @property
    def parameters(self):
        return self.spec.get("parameters")

    @property
    def expected_data(self):
        filename = self.file
        if not filename.startswith("expected" + os.sep):
            filename = os.path.join("expected", filename)

        with open(filename, "rb") as fp:
            return fp.read()

    @property
    def expected_entities(self):
        filename = self.file
        if not filename.startswith("expected" + os.sep):
            filename = os.path.join("expected", filename)

        with open(filename, "r", encoding="utf-8-sig") as fp:
            return json.load(fp)

    def update_expected_data(self, data):
        filename = self.file
        if not filename.startswith("expected" + os.sep):
            filename = os.path.join("expected", filename)

        if os.path.isfile(filename) is False:
            logger.debug("Creating new expected data file '%s'" % filename)

        with open(filename, "wb") as fp:
            fp.write(data)

    def is_path_blacklisted(self, path):
        blacklist = self.blacklist
        if blacklist and isinstance(blacklist, list):
            prop_path = ".".join(path).replace("\.", ".")

            for pattern in blacklist:
                if fnmatch(prop_path, pattern.replace("[].", ".*.")):
                    return True

        return False


class SesamNode:
    """ Sesam node functions wrapped in a class to facilitate unit tests """

    def __init__(self, node_url, jwt_token, logger, verify_ssl=True):
        self.logger = logger

        self.node_url = node_url
        self.jwt_token = jwt_token

        safe_jwt = "{}*********{}".format(jwt_token[:10], jwt_token[-10:])
        self.logger.debug("Connecting to Sesam using url '%s' and JWT '%s'", node_url, safe_jwt)

        if verify_ssl is False:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.api_connection = sesamclient.Connection(sesamapi_base_url=self.node_url, jwt_auth_token=self.jwt_token,
                                                     timeout=60 * 10, verify_ssl=verify_ssl)

    def wait_for_all_pipes_to_deploy(self, timeout=30 * 60):
        starttime = time.time()
        while True:
            deploying = []
            for pipe in [p for p in self.api_connection.get_pipes() if self.is_user_pipe(p)]:
                if pipe.runtime["state"] == "Deploying":
                    deploying.append(pipe)

            if deploying:
                # As an optimization we wait for one pipe to be deployed before we call get_pipes() again.
                pipe = deploying[0]
                while True:
                    try:
                        pipe.wait_for_pipe_to_be_deployed(timeout=0)
                        logger.debug(f"Pipe '{pipe.id}' was deployed...")
                        # We only wait for one pipe, since getting all the pipes with get_pipes() is much faster
                        # that waiting for each individual pipe. Once one pipe has been deployed, usually all the other
                        # pipes will also be deployed.
                        break
                    except BaseException as e:
                        logger.debug(f"Pipe '{pipe.id}' is still deploying...")
                        elapsedtime = time.time() - starttime
                        if elapsedtime > timeout:
                            raise RuntimeError("Waiting for pipes to deploy timed out after %s seconds!" % timeout)

                    time.sleep(5)

            elapsedtime = time.time() - starttime
            if deploying and elapsedtime > timeout:
                raise RuntimeError("Waiting for pipes to deploy timed out after %s seconds!" % timeout)

            if not deploying:
                self.logger.debug("All pipes were deployed in %s seconds" % elapsedtime)
                break

            logger.info(f"Waiting for {len(deploying)} pipes to finish deploying...")
            time.sleep(5)

    def is_user_pipe(self, pipe):
        if self.get_pipe_origin(pipe) in ["system", "search", "replica", "aggregator-storage-node"]:
            return False

        if pipe.id.find(":singlenode") > -1:
            # IS-14078: temporary fix for the API intermittently returning pipes from worker-nodes (with pre or
            # postfixes). This typically happens after a wipe or config update that deletes multiple pipes.
            return False

        return True

    def get_pipe_origin(self, pipe):
        return pipe.config.get("original", {}).get("metadata", {}).get("origin", "user")

    def wait_for_all_pipes_to_disappear(self, timeout=30 * 60):
        starttime = time.time()
        while True:
            elapsedtime = time.time() - starttime
            pipes = [p for p in self.api_connection.get_pipes() if self.is_user_pipe(p)]
            if not pipes:
                self.logger.debug("All pipes were removed in %s seconds" % elapsedtime)
                return

            if elapsedtime > timeout:
                raise RuntimeError("Waiting for pipes to br removed timed out after %s seconds!" % timeout)

            logger.info(f"Waiting for {len(pipes)} pipes to be removed...")
            time.sleep(5)

    def restart(self, timeout):
        old_stats = self.api_connection.get_status()
        restart = self.api_connection.restart_node()
        if restart != {"message": "OK"}:
            self.logger.debug("Restart node API call failed! It returned '%s', "
                              "expected '{\"message\": \"OK\"}'" % restart)
            raise RuntimeError("Failed to restart node!")

        # Wait until status works and gives a new start-time
        starttime = time.monotonic()
        while True:
            try:
                new_stats = self.api_connection.get_status()
                if old_stats["node_start_time"] != new_stats["node_start_time"]:
                    break
                msg = "No new node_start_time"
            except BaseException as e:
                msg = str(e)

            elapsed_time = time.monotonic() - starttime
            if elapsed_time > timeout:
                raise RuntimeError("Failed to start node - wait for node restart timed "
                                   "out after %s seconds. The last errror was: %s" % (timeout, msg))
            time.sleep(3)

    def reset(self, timeout):
        old_stats = self.api_connection.get_status()
        restart = self.api_connection.reset_node()
        if restart != {"message": "OK"}:
            self.logger.debug("Reset node API call failed! It returned '%s', "
                              "expected '{\"message\": \"OK\"}'" % restart)
            raise RuntimeError("Failed to reset node!")

        # Wait until status works and gives a new start-time
        starttime = time.monotonic()
        while True:
            try:
                new_stats = self.api_connection.get_status()
                if old_stats["node_start_time"] != new_stats["node_start_time"]:
                    break
                msg = "No new node_start_time"
            except BaseException as e:
                msg = str(e)

            elapsed_time = time.monotonic() - starttime
            if elapsed_time > timeout:
                raise RuntimeError("Failed to start node - wait for node restart timed "
                                   "out after %s seconds. The last errror was: %s" % (timeout, msg))
            time.sleep(3)

    def put_config(self, config, force=False):
        self.logger.log(LOGLEVEL_TRACE, "PUT config to %s" % self.node_url)
        self.api_connection.upload_config(config, force=force)

    def put_env(self, env_vars):
        self.logger.log(LOGLEVEL_TRACE, "PUT env vars to %s" % self.node_url)
        self.api_connection.put_env_vars(env_vars)

    def get_env(self):
        self.logger.log(LOGLEVEL_TRACE, "GET env vars from %s" % self.node_url)
        return self.api_connection.get_env_vars()

    def get_system(self, system_id):
        self.logger.log(LOGLEVEL_TRACE, "Get system '%s' from %s" % (system_id, self.node_url))
        try:
            return self.api_connection.get_system(system_id)
        except:
            return None

    def get_pipe(self, pipe_id):
        self.logger.log(LOGLEVEL_TRACE, "Get pipe '%s' from %s" % (pipe_id, self.node_url))
        try:
            return self.api_connection.get_pipe(pipe_id)
        except:
            return None

    def add_system(self, config, verify=False, timeout=300):
        self.logger.log(LOGLEVEL_TRACE, "Add system '%s' to %s" % (config, self.node_url))

        self.api_connection.add_systems([config])

        if not verify:
            return True

        # If verify is set, we wait until the runtime config matches the given config
        self.logger.debug("Verifying posted system '%s'.." % config["_id"])
        sleep_time = 5.0
        while timeout > 0:
            try:
                system = self.get_system(config["_id"])
                # Check if config now matches the config we posted
                if system.config["original"] == config:
                    self.logger.debug("Posted system '%s' verified OK!" % config["_id"])
                    return True
            except BaseException as e:
                pass

            time.sleep(sleep_time)

            timeout -= sleep_time

        self.logger.debug("Failed to verify posted system '%s'!" % config["_id"])
        return False

    def add_systems(self, config):
        self.logger.log(LOGLEVEL_TRACE, "Add systems '%s' to %s" % (config, self.node_url))
        return self.api_connection.add_systems(config)

    def remove_system(self, system_id):
        self.logger.log(LOGLEVEL_TRACE, "Remove system '%s' from %s" % (system_id, self.node_url))
        try:
            system = self.api_connection.get_system(system_id)
            if system is not None:
                system.delete()
        except:
            logger.warning("Could not remove system '%s' - perhaps it doesn't exist" % system_id)

    def get_config(self, binary=False):
        data = self.api_connection.get_config_as_zip()
        if not binary:
            return zipfile.ZipFile(io.BytesIO(data))

        return data

    def get_pipe_type(self, pipe):
        source_config = pipe.config["effective"].get("source", {})
        sink_config = pipe.config["effective"].get("sink", {})
        source_type = source_config.get("type", "")
        sink_type = sink_config.get("type", "")

        if source_type == "embedded":
            return "input"

        if isinstance(sink_type, str) and sink_type.endswith("_endpoint"):
            return "endpoint"

        if (source_config.get("dataset") or source_config.get("datasets")) and \
                sink_config.get("dataset"):
            return "internal"

        if not sink_config.get("dataset"):
            return "output"

        return "internal"

    def get_output_pipes(self):
        return [p for p in self.api_connection.get_pipes() if self.is_user_pipe(p) and
                self.get_pipe_type(p) == "output"]

    def get_input_pipes(self):
        return [p for p in self.api_connection.get_pipes() if self.is_user_pipe(p) and
                self.get_pipe_type(p) == "input"]

    def get_endpoint_pipes(self):
        return [p for p in self.api_connection.get_pipes() if self.is_user_pipe(p) and
                self.get_pipe_type(p) == "endpoint"]

    def get_internal_pipes(self):
        return [p for p in self.api_connection.get_pipes() if self.is_user_pipe(p) and
                self.get_pipe_type(p) == "internal"]

    def run_internal_scheduler(self, zero_runs=None, max_run_time=None, max_runs=None, delete_input_datasets=True,
                               check_input_pipes=False, output_run_statistics=False, scheduler_mode=None,
                               request_mode=None):
        internal_scheduler_url = "%s/pipes/run-all-pipes" % self.node_url

        params = {}

        if zero_runs is not None:
            params["extra_zero_runs"] = zero_runs

        if max_run_time is not None:
            params["max_run_time"] = max_run_time

        if max_runs is not None:
            params["max_runs"] = max_runs

        if not delete_input_datasets:
            # Default is True
            params["delete_input_datasets"] = False

        if check_input_pipes is True:
            params["check_input_pipes"] = True

        if output_run_statistics is True:
            params["output_run_statistics"] = True

        if scheduler_mode is not None:
            params["scheduler_mode"] = scheduler_mode

        if request_mode is not None:
            params["request_mode"] = request_mode

        resp = self.api_connection.session.post(internal_scheduler_url, params=params)
        resp.raise_for_status()

        return resp.json()

    def stop_internal_scheduler(self, terminate_timeout=30):
        internal_scheduler_url = "%s/pipes/stop-run-all-pipes" % self.node_url

        params = {"terminate_timeout": terminate_timeout}

        resp = self.api_connection.session.post(internal_scheduler_url, params=params)
        resp.raise_for_status()

        return resp.json()

    def get_internal_scheduler_log(self, since=None, token=None):
        scheduler_log_url = "%s/pipes/get-run-all-pipes-log" % self.node_url

        params = {}
        if since:
            params["since"] = since

        if token:
            params["token"] = token

        resp = self.api_connection.session.get(scheduler_log_url, params=params)
        resp.raise_for_status()

        return resp.json()

    def get_internal_scheduler_status(self, token):
        scheduler_log_url = "%s/pipes/get-pipe-runner-status" % self.node_url

        params = {"token": token}

        resp = self.api_connection.session.get(scheduler_log_url, params=params)
        resp.raise_for_status()

        return resp.json()

    def get_pipe_entities(self, pipe, stage=None):
        if stage is None:
            pipe_url = "%s/pipes/%s/entities" % (self.node_url, pipe.id)
        else:
            pipe_url = "%s/pipes/%s/entities?stage=%s" % (self.node_url, pipe.id, stage)

        resp = self.api_connection.session.get(pipe_url)
        resp.raise_for_status()

        return resp.json()

    def get_published_data(self, pipe, type="entities", params=None, binary=False):

        pipe_url = "%s/publishers/%s/%s" % (self.node_url, pipe.id, type)

        # Enable the pump, if it is disabled, or else we can't get the data
        pump = pipe.get_pump()
        if pump.is_disabled:
            pump.enable()

        resp = self.api_connection.session.get(pipe_url, params=params)
        resp.raise_for_status()

        if binary:
            return resp.content

        return resp.text

    def get_system_status(self, system_id):
        system_url = self.api_connection.get_system_url(system_id)

        resp = self.api_connection.session.get(system_url + "/status")
        resp.raise_for_status()

        status = resp.json()
        if isinstance(status, dict):
            return status

        return None

    def get_system_log(self, system_id, params=None):

        system_url = self.api_connection.get_system_url(system_id)

        resp = self.api_connection.session.get("%s/logs" % system_url, params=params)
        resp.raise_for_status()

        return resp.text

    def wait_for_microservice(self, microservice_id, timeout=300):
        """ Polls the microservice status API until it is running (or we time out) """

        system = self.get_system(microservice_id)
        if system is None:
            raise AssertionError("Microservice system '%s' doesn't exist" % microservice_id)

        sleep_time = 5.0
        while timeout > 0:
            try:
                system_status = self.get_system_status(microservice_id)
            except BaseException as e:
                self.logger.debug("Failed to get system status for microservice '%s'", microservice_id)
                system_status = None

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

    def pipe_receiver_post_request(self, pipe_id, **kwargs):
        pipe = self.get_pipe(pipe_id)
        if pipe is None:
            raise AssertionError("Pipe '%s' doesn't exist" % pipe_id)

        pipe_url = self.api_connection.get_pipe_receiver_endpoint_url(pipe_id)

        timeout = 60
        starttime = time.monotonic()
        while True:
            resp = self.api_connection.session.post(pipe_url, **kwargs)

            # Sometimes a subscription may take little while to deploy all the API routes, let's give it a chance
            # to catch up before we give up
            if resp.status_code == 503:
                if time.monotonic() - starttime > timeout:
                    logger.error(f"Failed to post request to HTTP receiver for pipe '{pipe_id}' after "
                                 f"retrying for {timeout} seconds...")
                    resp.raise_for_status()
                time.sleep(1)
            else:
                resp.raise_for_status()
                break

        return resp.json()

    def enable_pipe(self, pipe_id):
        self.get_pipe(pipe_id).get_pump().enable()

    def disable_pipe(self, pipe_id):
        self.get_pipe(pipe_id).get_pump().disable()

    def delete_dataset(self, pipe_id):
        dataset_url = "%s/datasets/%s" % (self.node_url, pipe_id)
        resp = self.api_connection.session.delete(dataset_url)
        resp.raise_for_status()

        return resp.json()


class SesamCmdClient:
    """ Commands wrapped in a class to make it easier to write unit tests """

    def __init__(self, args, logger):
        self.args = args
        self.logger = logger
        self.sesam_node = None
        self.formatstyle = FormatStyle()
        self.whitelisted_files = None
        self.whitelisted_pipes = None
        self.whitelisted_systems = None

        if args.whitelist_file is not None:
            try:
                self.whitelisted_files = []
                self.whitelisted_pipes = []
                self.whitelisted_systems = []
                with open(args.whitelist_file, "r") as infile:
                    for line in infile.read().split("\n"):
                        self.whitelisted_files.append(line.strip())
                        if line.startswith("pipes/"):
                            pipe = line.replace("pipes/", "").replace(".conf.json", "")
                            self.whitelisted_pipes.append(pipe)
                        elif line.startswith("systems/"):
                            system = line.replace("systems/", "").replace(".conf.json", "")
                            self.whitelisted_systems.append(system)
            except BaseException as e:
                logger.error(f"Failed to read whitelistfile '{args.whitelist_file}'")
                raise e

    def parse_config_file(self, filename):
        config = {}
        # try to parse as json, if fails parse as ini
        with open(filename) as fp:
            try:
                config = json.load(fp)
            except ValueError as e:
                pass
        if not config:
            with open(filename) as fp:
                parser = configparser.ConfigParser(strict=False)
                # [sesam] section is prepended to support .syncconfig file
                #  in which section is omitted
                parser.read_file(itertools.chain(['[sesam]'], fp), source=filename)
                config = {}
                for section in parser.sections():
                    for key, value in parser.items(section):
                        config[key.lower()] = value

        return config

    def read_config_file(self, filename, is_required=False):
        try:
            curr_dir = os.getcwd()
            if curr_dir is None:
                self.logger.error("Failed to open current directory. Check your permissions.")
                raise AssertionError("Failed to open current directory. Check your permissions.")

            # Find config on disk, if any
            file_config = {}
            parents_dirs: list[str] = os.path.abspath(curr_dir).split(os.sep)[1:]
            parent_path = curr_dir
            if os.path.isfile(filename):
                file_config = self.parse_config_file(filename)
            else:
                # iterate over all parent directories and look for .syncconfig file
                for _ in parents_dirs:
                    parent_path = os.path.dirname(parent_path)
                    file_path = os.path.join(parent_path, filename)
                    if os.path.isfile(file_path):
                        file_config = self.parse_config_file(file_path)
                        if file_config:
                            curr_dir = parent_path
                            break

            if file_config:
                self.logger.debug("Found config file '%s' in '%s'" % (filename, curr_dir))
            else:
                if is_required:
                    raise BaseException()
                else:
                    self.logger.debug("Cannot locate config file '%s' in current or parent folder. "
                                      "Proceeding without it." % (filename))
            return file_config
        except BaseException as e:
            self.logger.error("Failed to read '%s' from either the current directory or the "
                              "parent directory. Check that you are in the correct directory, that you have the"
                              "required permissions to read the files and that the files have the correct format." % filename)
            raise e

    def _coalesce(self, items):
        for item in items:
            if item is not None:
                return item

    def zip_dir(self, zipfile, dir):
        for root, dirs, files in os.walk(dir):
            for file in files:
                if file.endswith(".conf.json"):
                    if self.whitelisted_files is not None:
                        filepath = os.path.join(root, file)
                        if normalize_path(filepath) not in self.whitelisted_files:
                            continue

                    zipfile.write(os.path.join(root, file))

    def get_zip_config(self, remove_zip=True):
        """ Create a ZIP file from the local content on disk and return a bytes object
            If "remove_zip" is False, we dump it to disk as "sesam-config.zip" as well.
        """
        if os.path.isfile("sesam-config.zip"):
            os.remove("sesam-config.zip")

        zip_file = zipfile.ZipFile('sesam-config.zip', 'w', zipfile.ZIP_DEFLATED)

        self.zip_dir(zip_file, "pipes")
        self.zip_dir(zip_file, "systems")

        if os.path.isfile("node-metadata.conf.json"):
            if not self.whitelisted_files or "node-metadata.conf.json" in self.whitelisted_files:
                zip_file.write("node-metadata.conf.json")

        zip_file.close()

        with open("sesam-config.zip", "rb") as fp:
            zip_data = fp.read()

        if remove_zip:
            os.remove("sesam-config.zip")

        return zip_data

    def get_zipfile_data_by_filename(self, zip_data, filename):
        zin = zipfile.ZipFile(io.BytesIO(zip_data))

        for item in zin.infolist():
            if item.filename == filename:
                return zin.read(item.filename)

        zin.close()
        return None

    def replace_file_in_zipfile(self, zip_data, filename, replacement):
        zin = zipfile.ZipFile(io.BytesIO(zip_data))
        buffer = io.BytesIO()
        zout = zipfile.ZipFile(buffer, mode="w")

        for item in zin.infolist():
            if item.filename == filename:
                zout.writestr(item, replacement)
            else:
                zout.writestr(item, zin.read(item.filename))

        zout.close()
        zin.close()

        buffer.seek(0)
        return buffer.read()

    def remove_task_manager_settings(self, zip_data):
        node_metadata = {}
        if os.path.isfile("node-metadata.conf.json"):
            with open("node-metadata.conf.json", "r") as infile:
                node_metadata = json.load(infile)

        remote_data = self.get_zipfile_data_by_filename(zip_data, "node-metadata.conf.json")
        if remote_data:
            remote_metadata = json.loads(str(remote_data, encoding="utf-8"))

            if "task_manager" in remote_metadata and "disable_user_pipes" in remote_metadata["task_manager"] and \
                    remote_metadata["task_manager"]["disable_user_pipes"] is True:

                if "disable_user_pipes" in node_metadata.get("task_manager", {}):
                    # Restore the original, if present
                    remote_metadata["task_manager"]["disable_user_pipes"] = \
                        node_metadata["task_manager"]["disable_user_pipes"]
                else:
                    # Not present originally, so just remove it from remote
                    remote_metadata["task_manager"].pop("disable_user_pipes")
                    # Remove the entire task_manager section if its empty
                    if len(remote_metadata["task_manager"]) == 0:
                        remote_metadata.pop("task_manager")

            if "global_defaults" in remote_metadata:
                if "enable_cpp_extensions" in remote_metadata["global_defaults"] and \
                        remote_metadata["global_defaults"]["enable_cpp_extensions"] is False:

                    if "enable_cpp_extensions" in node_metadata.get("global_defaults", {}):
                        # Restore the original, if present
                        remote_metadata["global_defaults"]["enable_cpp_extensions"] = \
                            node_metadata["global_defaults"]["enable_cpp_extensions"]
                    else:
                        # Not present originally, so just remove it from remote
                        remote_metadata["global_defaults"].pop("enable_cpp_extensions")
                        # Remove the entire global_defaults section if its empty
                        if len(remote_metadata["global_defaults"]) == 0:
                            remote_metadata.pop("global_defaults")

                if "eager_load_microservices" in remote_metadata["global_defaults"] and \
                        remote_metadata["global_defaults"]["eager_load_microservices"] is False:

                    if "eager_load_microservices" in node_metadata.get("global_defaults", {}):
                        # Restore the original, if present
                        remote_metadata["global_defaults"]["eager_load_microservices"] = \
                            node_metadata["global_defaults"]["eager_load_microservices"]
                    else:
                        # Not present originally, so just remove it from remote
                        remote_metadata["global_defaults"].pop("eager_load_microservices")
                        # Remove the entire global_defaults section if its empty
                        if len(remote_metadata["global_defaults"]) == 0:
                            remote_metadata.pop("global_defaults")

            # Replace the file and return the new zipfile
            return self.replace_file_in_zipfile(zip_data, "node-metadata.conf.json",
                                                json.dumps(remote_metadata, indent=2,
                                                           ensure_ascii=False).encode("utf-8"))

        return zip_data

    def get_formatstyle_from_configfile(self):
        configfilename = self.args.sesamconfig_file or ".sesamconfig.json"
        is_required = self.args.sesamconfig_file is not None
        configuration = self.read_config_file(configfilename, is_required)
        return FormatStyle(**configuration.get("formatstyle", {}))

    def get_node_and_jwt_token(self):
        configfilename = self.args.sync_config_file
        try:
            file_config = self.read_config_file(configfilename, is_required=False)

            self.node_url = self._coalesce([args.node, os.environ.get("NODE"), file_config.get("node")])
            self.jwt_token = self._coalesce([args.jwt, os.environ.get("JWT"), file_config.get("jwt")])

            if self.jwt_token and self.jwt_token.startswith('"') and self.jwt_token[-1] == '"':
                self.jwt_token = self.jwt_token[1:-1]

            if self.jwt_token.startswith("bearer "):
                self.jwt_token = self.jwt_token.replace("bearer ", "")

            if self.jwt_token.startswith("Bearer "):
                self.jwt_token = self.jwt_token.replace("Bearer ", "")

            self.node_url = self.node_url.replace('"', "")

            if not self.node_url.startswith("http"):
                self.node_url = "https://%s" % self.node_url

            if not self.node_url[-4:] == "/api":
                self.node_url = "%s/api" % self.node_url

            return self.node_url, self.jwt_token

        except BaseException as e:
            logger.error("Failed to find node url and/or jwt token")
            raise e

    def format_zip_config(self, zip_data, binary=False):
        zip_config = zipfile.ZipFile(io.BytesIO(zip_data))
        buffer = io.BytesIO()
        zout = zipfile.ZipFile(buffer, mode="w")

        for item in zip_config.infolist():
            formatted_item = format_object(json.load(zip_config.open(item.filename)), self.formatstyle)
            zout.writestr(item, formatted_item)

        zout.close()

        buffer.seek(0)
        return buffer.read()


    def set_authconfig_credentials(self, *args):
        try:
            auth_credentials = self.read_config_file(".authconfig")
            for arg in args:
                token=auth_credentials[arg]
                if token.startswith('"') and token.endswith('"'):
                    setattr(self.args, arg, token[1:-1])
                else:
                    setattr(self.args, arg, token)
            logger.info("Found authentication credentials in .authconfig file.")
        except KeyError:
            logger.warning("Could not find %s in .authconfig file. Checking the arguments." % arg)


    def authenticate(self):
        os.chdir(self.args.connector_dir)
        self.args.service_url=self.node_url
        self.args.service_jwt=self.jwt_token
        if os.path.isfile("manifest.json"): # If manifest.json is in working directory
            self.args.connector_manifest = "manifest.json"
        elif os.path.exists(os.path.join(args.connector_dir, "manifest.json")):# If manifest.json is in connector directory
            self.args.connector_manifest = os.path.join(args.connector_dir, "manifest.json")
        else:# If manifest.json is not found
            logger.error("Could not find manifest.json in connector directory")
            sys.exit(1)

        with open(args.connector_manifest, "r") as f:
            connector_manifest = json.load(f)

        if "auth_variant" in connector_manifest and connector_manifest["auth_variant"].lower() == "tripletex":
            if os.path.exists(".authconfig"):
                self.set_authconfig_credentials("consumer_token", "employee_token")
            else:
                self.args.consumer_token = args.consumer_token
                self.args.employee_token = args.employee_token
            if self.args.consumer_token is None or self.args.employee_token is None:
                logger.error("Missing consumer_token and/or employee_token. Please provide them in .authconfig or as arguments.")
                sys.exit(1)
            self.args.base_url = args.base_url
            login_via_tripletex(self.sesam_node,self.args)
        elif "auth" in connector_manifest and connector_manifest["auth"].lower() == "oauth2":
            self.args.login_url = connector_manifest["oauth2"]["login_url"]
            self.args.token_url = connector_manifest["oauth2"]["token_url"]
            self.args.scopes = connector_manifest["oauth2"]["scopes"]
            if os.path.exists(".authconfig"):
                self.set_authconfig_credentials("client_id", "client_secret")
            else:
                self.args.client_id = args.client_id
                self.args.client_secret = args.client_secret
            if self.args.client_id is None or self.args.client_secret is None:
                logger.error("Missing client_id and/or client_secret. Please provide them in .authconfig or as arguments.")
                sys.exit(1)
            login_via_oauth(self.sesam_node,self.args)
        else:
            pass

    def upload(self):
        # Find env vars to upload
        profile_file = "%s-env.json" % self.args.profile
        try:
            with open(profile_file, "r", encoding="utf-8-sig") as fp:
                json_data = json.load(fp)
        except FileNotFoundError as e:
            self.logger.error("Cannot locate profile file '%s'" % profile_file)
            raise e
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

            # Modify the node-metadata.conf.json to stop the pipe scheduler
            if os.path.isfile("node-metadata.conf.json"):
                with open("node-metadata.conf.json", "r") as infile:
                    node_metadata = json.load(infile)
            else:
                node_metadata = {}

            if self.args.enable_user_pipes:
                if "task_manager" not in node_metadata:
                    node_metadata["task_manager"] = {}

                node_metadata["task_manager"]["disable_user_pipes"] = False
            else:
                # Default disabled
                if "task_manager" not in node_metadata:
                    node_metadata["task_manager"] = {}

                node_metadata["task_manager"]["disable_user_pipes"] = True

            if self.args.disable_cpp_extensions:
                # Default on
                if "global_defaults" not in node_metadata:
                    node_metadata["global_defaults"] = {}

                node_metadata["global_defaults"]["enable_cpp_extensions"] = False

            if self.args.enable_eager_ms:
                if "global_defaults" not in node_metadata:
                    node_metadata["global_defaults"] = {}

                node_metadata["global_defaults"]["eager_load_microservices"] = True
            else:
                # Default off
                if "global_defaults" not in node_metadata:
                    node_metadata["global_defaults"] = {}

                node_metadata["global_defaults"]["eager_load_microservices"] = False

            zip_config = self.replace_file_in_zipfile(zip_config, "node-metadata.conf.json",
                                                      json.dumps(node_metadata).encode("utf-8"))
        except BaseException as e:
            logger.error("Failed to create zip archive of config")
            raise e

        try:
            self.sesam_node.put_config(zip_config, force=True)
        except BaseException as e:
            self.logger.error("Failed to upload config to sesam")
            raise e

        self.sesam_node.wait_for_all_pipes_to_deploy()

        self.logger.info("Config uploaded successfully")

        if os.path.isdir("testdata"):
            for root, dirs, files in os.walk("testdata"):
                for filename in files:
                    pipe_id = filename.replace(".json", "")
                    if self.whitelisted_pipes and pipe_id not in self.whitelisted_pipes:
                        continue

                    try:
                        with open(os.path.join(root, filename), "r", encoding="utf-8") as f:
                            entities_json = json.load(f)

                        if entities_json is not None:
                            # deleting dataset before pushing data, since http_endpoint receiver will not delete
                            # existing test data.
                            try:
                                self.sesam_node.delete_dataset(pipe_id)
                            except HTTPError as http_e:
                                self.logger.log(LOGLEVEL_TRACE,
                                                f"Failed to delete dataset {pipe_id}. It probably doesn't exist, "
                                                f"which is fine. Error: {http_e}")
                            self.sesam_node.enable_pipe(pipe_id)
                            self.sesam_node.pipe_receiver_post_request(pipe_id, json=entities_json)
                            self.sesam_node.disable_pipe(pipe_id)

                    except BaseException as e:
                        self.logger.error(
                            f"Failed to post payload to pipe {pipe_id}. {e}. Response from server was: {e.response.text}")
                        raise e

            self.logger.info("Test data uploaded successfully")
        else:
            self.logger.info("No test data found to upload")

    def dump(self):
        try:
            zip_config = self.get_zip_config(remove_zip=False)
        except BaseException as e:
            logger.error("Failed to create zip archive of config")
            raise e

    def download(self):
        if self.args.is_connector:
            if not os.path.isdir(os.path.join(self.args.connector_dir, self.args.expanded_dir)):
                logger.warning("Expanded directory '%s' does not exist. creating the directory." % self.args.expanded_dir)
                os.makedirs(os.path.join(self.args.connector_dir, self.args.expanded_dir))
            os.chdir(os.path.join(self.args.connector_dir, self.args.expanded_dir))

        # Find env vars to download
        profile_file = "%s-env.json" % self.args.profile
        try:
            with open(profile_file, "w", encoding="utf-8-sig") as fp:
                fp.write(format_object(self.sesam_node.get_env(), self.formatstyle))
        except BaseException as e:
            self.logger.error("Failed to save profile file  '%s'" % profile_file)
            raise e

        if self.args.dump:
            if os.path.isfile("sesam-config.zip"):
                os.remove("sesam-config.zip")

            zip_data = self.sesam_node.get_config(binary=True)
            zip_data = self.remove_task_manager_settings(zip_data)

            # normalize formatting
            formatted_zip_data = self.format_zip_config(zip_data, binary=True)
            with open("sesam-config.zip", "wb") as fp:
                fp.write(formatted_zip_data)

            self.logger.info("Dumped downloaded config to 'sesam-config.zip'")
        else:
            zip_data = self.sesam_node.get_config(binary=True)
            zip_data = self.remove_task_manager_settings(zip_data)

        try:
            # Remove all previous pipes and systems
            for filename in glob.glob("pipes%s*.conf.json" % os.sep):
                # Don't delete non-whitelisted config files
                # Normalize path
                if self.whitelisted_files and normalize_path(filename) not in self.whitelisted_files:
                    continue

                self.logger.debug("Deleting pipe config file '%s'" % filename)
                os.remove(filename)

            for filename in glob.glob("systems%s*.conf.json" % os.sep):
                # Don't delete non-whitelisted config files
                if self.whitelisted_files and normalize_path(filename) not in self.whitelisted_files:
                    continue

                self.logger.debug("Deleting system config file '%s'" % filename)
                os.remove(filename)

            # normalize formatting
            zip_data = self.format_zip_config(zip_data)
            zip_config = zipfile.ZipFile(io.BytesIO(zip_data))
            zip_config.extractall()
        except BaseException as e:
            self.logger.error("Failed to unzip config file from Sesam to current directory")
            raise e

        zip_config.close()
        self.logger.info("Replaced local config successfully")

        curr_dir = os.getcwd()
        if self.args.is_connector:
            if curr_dir.endswith(self.args.expanded_dir):
                os.chdir(os.pardir)
                collapse_connector(".", self.args.system_placeholder, self.args.expanded_dir)


    def status(self):
        def log_and_get_diff_flag(file_content1, file_content2, file_name1, file_name2, log_diff=True):
            diff_found = False
            if file_content1 != file_content2:
                self.logger.info("File '%s' differs from Sesam!" % file_name1)

                diff = self.get_diff_string(file_content1, file_content2, file_name1, file_name2)
                if log_diff:
                    self.logger.info("Diff:\n%s" % diff)

                diff_found = True
            return diff_found

        logger.error("Comparing local and node config...")

        local_config = zipfile.ZipFile(io.BytesIO(self.get_zip_config()))
        if self.args.dump:
            zip_data = self.sesam_node.get_config(binary=True)
            zip_data = self.remove_task_manager_settings(zip_data)

            with open("sesam-config.zip", "wb") as fp:
                fp.write(zip_data)

            self.logger.info("Dumped downloaded config to 'sesam-config.zip'")
        else:
            remote_config = self.sesam_node.get_config(binary=True)
            zip_data = self.remove_task_manager_settings(remote_config)

        remote_config = zipfile.ZipFile(io.BytesIO(zip_data))

        remote_files = sorted(remote_config.namelist())
        local_files = sorted(local_config.namelist())

        diff_found = False
        # compare profile_file content with the variables
        profile_file = "%s-env.json" % self.args.profile
        try:
            with open(profile_file, "r", encoding="utf-8-sig") as local_env_file:
                local_file_data = format_object(json.load(local_env_file), self.formatstyle)
            remote_file_data = format_object(self.sesam_node.get_env(), self.formatstyle)

            diff_found = log_and_get_diff_flag(local_file_data, remote_file_data, profile_file, profile_file,
                                               self.args.diff) or diff_found
        except FileNotFoundError as ex:
            logger.error("Cannot locate profile file '%s'" % profile_file)

        for remote_file in remote_files:
            if remote_file not in local_files:
                self.logger.info("Sesam file '%s' was not found locally" % remote_file)
                diff_found = True

        for local_file in local_files:
            if local_file not in remote_files:
                self.logger.info("Local file '%s' was not found in Sesam" % local_file)
                diff_found = True
            else:
                local_file_data = str(local_config.read(local_file), encoding="utf-8")
                remote_file_data = format_object(json.load(remote_config.open(local_file)), self.formatstyle)

                diff_found = log_and_get_diff_flag(local_file_data, remote_file_data, local_file, local_file,
                                                   self.args.diff) or diff_found

        if diff_found:
            logger.info("Sesam config is NOT in sync with local config!")
        else:
            logger.info("Sesam config is up-to-date with local config!")

    def filter_entity(self, entity, test_spec):
        """ Remove most underscore keys and filter potential blacklisted keys """

        def filter_item(parent_path, item):
            result = copy.deepcopy(item)
            if isinstance(item, dict):
                for key, value in item.items():
                    path = parent_path + [key]
                    if key.startswith("_"):
                        if key == "_id" or (key == "_deleted" and value is True):
                            continue
                        result.pop(key)
                    elif test_spec.is_path_blacklisted(path):
                        result.pop(key)
                    else:
                        result[key] = filter_item(path, value)
                return result
            elif isinstance(item, list):
                result = []
                for list_item in item:
                    result.append(filter_item(parent_path, list_item))
                return result

            return item

        return filter_item([], entity)

    def load_test_specs(self, existing_output_pipes, update=False):
        test_specs = {}
        failed = False

        # Load test specifications
        for filename in glob.glob("expected%s*.test.json" % os.sep):
            self.logger.debug("Processing spec file '%s'" % filename)

            test_spec = TestSpec(filename)

            pipe_id = test_spec.pipe
            self.logger.log(LOGLEVEL_TRACE, "Pipe id for spec '%s' is '%s" % (filename, pipe_id))

            if self.whitelisted_pipes and pipe_id not in self.whitelisted_pipes:
                logger.warning(f"Skipping test spec for non-whitelisted pipe '{pipe_id} - add it to the whitelist if "
                               f"this is not correct!'")
                continue

            if pipe_id not in existing_output_pipes:
                if update is False:
                    logger.error("Test spec '%s' references a non-exisiting output "
                                 "pipe '%s' - please remove '%s'" % (test_spec.spec_file, pipe_id, test_spec.spec_file))
                    failed = True
                else:
                    if test_spec.ignore is False:
                        # Remove the test spec file
                        if os.path.isfile("%s" % test_spec.spec_file):
                            logger.warning("Test spec '%s' references a non-exisiting output "
                                           "pipe '%s' - removing '%s'.." % (test_spec.spec_file, pipe_id,
                                                                            test_spec.spec_file))
                            os.remove(test_spec.spec_file)
                            continue
                    else:
                        logger.warning("Test spec '%s' references a non-exisiting output "
                                       "pipe '%s' but is marked as 'ignore' - consider "
                                       "removing '%s'.." % (test_spec.spec_file, pipe_id, test_spec.spec_file))

            if test_spec.ignore is False and not os.path.isfile("%s" % test_spec.file):
                logger.warning("Test spec '%s' references non-exisiting 'expected' output "
                               "file '%s'" % (test_spec.spec_file, test_spec.file))
                if update is True:
                    logger.info("Creating empty 'expected' output file '%s'..." % test_spec.file)
                    with open(test_spec.file, "w") as fp:
                        fp.write("[]\n")
                else:
                    failed = True

            # If spec says 'ignore' then the corresponding output file should not exist
            if failed is False and test_spec.ignore is True:
                output_filename = test_spec.file

                if os.path.isfile(output_filename):
                    if update:
                        self.logger.debug("Removing existing output file '%s'" % output_filename)
                        os.remove(output_filename)
                    else:
                        self.logger.warning(
                            "pipe '%s' is ignored, but output file '%s' still exists" % (pipe_id, filename))

            if pipe_id not in test_specs:
                test_specs[pipe_id] = []

            test_specs[pipe_id].append(test_spec)

        if failed:
            logger.error("Test specs verify failed, correct errors and retry")
            raise RuntimeError("Test specs verify failed, correct errors and retry")

        if update:
            for pipe in existing_output_pipes.values():
                if self.whitelisted_pipes and pipe.id not in self.whitelisted_pipes:
                    logger.warning(f"Not updating non-whitelisted pipe '{pipe.id} - add it to the whitelist if "
                                   f"this is not correct!'")
                    continue

                self.logger.debug("Updating pipe '%s" % pipe.id)

                if pipe.id not in test_specs:
                    self.logger.warning("Found no spec for pipe %s - creating empty spec file" % pipe.id)

                    filename = os.path.join("expected", "%s.test.json" % pipe.id)
                    with open(filename, "w") as fp:
                        fp.write("{\n}")
                    test_specs[pipe.id] = [TestSpec(filename)]

        return test_specs

    def get_diff_string(self, a, b, a_filename, b_filename):
        a_lines = io.StringIO(a).readlines()
        b_lines = io.StringIO(b).readlines()

        return "".join(unified_diff(a_lines, b_lines, fromfile=a_filename, tofile=b_filename))

    def bytes_to_xml_string(self, xml_data):

        xml_declaration, standalone = self.find_xml_header_settings(xml_data)
        xml_doc_root = etree.fromstring(xml_data)

        try:
            result = str(etree.tostring(xml_doc_root, encoding="utf-8",
                                        xml_declaration=xml_declaration,
                                        standalone=standalone,
                                        pretty_print=True), encoding="utf-8")
        except UnicodeEncodeError as e:
            result = str(etree.tostring(xml_doc_root, encoding="latin-1",
                                        xml_declaration=xml_declaration,
                                        standalone=standalone,
                                        pretty_print=True), encoding="latin-1")

        return result

    def _fix_decimal_to_ints(self, value):
        if isinstance(value, dict):
            for key, dict_value in value.items():
                value[key] = self._fix_decimal_to_ints(dict_value)
        elif isinstance(value, list):
            for ix, list_item in enumerate(value):
                value[ix] = self._fix_decimal_to_ints(list_item)
        else:
            if isinstance(value, (Decimal, float)):
                v = str(value)

                if v and v.endswith(".0"):
                    return self._fix_decimal_to_ints(int(value))
            elif not args.no_large_int_bugs and isinstance(value, int):
                v = str(value)
                if v and len(v) > len("9007199254740991"):
                    # Simulate go client bug :P
                    return int(Decimal(str(float(value))))

        return value

    def verify(self):
        self.logger.info("Verifying that expected output matches current output...")
        output_pipes = {}
        failed = False

        for p in self.sesam_node.get_output_pipes() + self.sesam_node.get_endpoint_pipes():
            if p.runtime.get("is-valid-config", False) is False:
                self.logger.error("The pipe '%s' has invalid config, cannot verify pipe!" % p.id)
                self.logger.error("The error(s) reported was: %s" % p.runtime.get("config-errors", "unknown"))
            else:
                output_pipes[p.id] = p

        test_specs = self.load_test_specs(output_pipes)

        if not test_specs:
            # IS-8560: no test files should result in a warning, not an error
            self.logger.warning("Found no tests (*.test.json) to run")
            return

        failed_tests = []
        missing_tests = []
        for pipe in output_pipes.values():
            if self.whitelisted_pipes and pipe.id not in self.whitelisted_pipes:
                self.logger.warning(f"Skipping verify for pipe '{pipe.id}' - add it to the whitelist if this is not "
                                    f"correct!")
                continue

            self.logger.debug("Verifying pipe '%s'.." % pipe.id)

            if pipe.id in test_specs:
                # Verify all tests specs for this pipe
                for test_spec in test_specs[pipe.id]:
                    if test_spec.ignore is True:
                        self.logger.debug("Skipping test spec '%s' because it was marked as 'ignore'" % test_spec.name)
                        continue

                    if test_spec.endpoint == "json" or test_spec.endpoint == "excel":
                        # Get current entities from pipe in json form
                        expected_entities = test_spec.expected_entities

                        expected_output = sorted(expected_entities,
                                                 key=lambda _e: (_e['_id'],
                                                                 json.dumps(_e, ensure_ascii=False,
                                                                            sort_keys=True)))

                        if test_spec.ignore_deletes:
                            # Gather any expected deletes
                            expected_deletes = [_e["_id"] for _e in expected_entities if
                                                _e.get("_deleted", False) is True]

                            # Filter away any unexpected deleted from the current output
                            current_entities = []
                            for en in [self.filter_entity(_e, test_spec)
                                       for _e in self.sesam_node.get_pipe_entities(pipe,
                                                                                   stage=test_spec.stage)]:
                                if en.get("_deleted", False) is True and en["_id"] not in expected_deletes:
                                    continue
                                current_entities.append(en)
                        else:
                            current_entities = [self.filter_entity(_e, test_spec)
                                                for _e in self.sesam_node.get_pipe_entities(pipe,
                                                                                            stage=test_spec.stage)]

                        current_output = sorted(current_entities, key=lambda _e: _e['_id'])

                        fixed_current_output = self._fix_decimal_to_ints(copy.deepcopy(current_output))

                        fixed_current_output = sorted(fixed_current_output,
                                                      key=lambda _e: (_e['_id'],
                                                                      json.dumps(_e, ensure_ascii=False,
                                                                                 sort_keys=True)))

                        if len(fixed_current_output) != len(expected_output):
                            file_path = os.path.join(os.path.relpath(BASE_DIR, GIT_ROOT), test_spec.file)
                            msg = "Pipe verify failed! Length mismatch for test spec '%s': " \
                                  "expected %d got %d" % (test_spec.spec_file,
                                                          len(expected_output), len(fixed_current_output))
                            self.logger.error(msg, {"file_path": file_path})

                            self.logger.info("Expected output:\n%s", pprint.pformat(expected_output))

                            if self.args.extra_extra_verbose:
                                self.logger.info("Got raw output:\n%s", pprint.pformat(current_output))

                            self.logger.info("Got output:\n%s", pprint.pformat(fixed_current_output))

                            diff = self.get_diff_string(json.dumps(expected_output, indent=2,
                                                                   ensure_ascii=False, sort_keys=True),
                                                        json.dumps(fixed_current_output, indent=2, ensure_ascii=False,
                                                                   sort_keys=True),
                                                        test_spec.file, "current-output.json")
                            self.logger.info("Diff:\n%s" % diff)
                            failed_tests.append(test_spec)
                            failed = True
                        else:
                            expected_json = json.dumps(expected_output, ensure_ascii=False, indent=2, sort_keys=True)
                            current_json = json.dumps(fixed_current_output, ensure_ascii=False, indent=2,
                                                      sort_keys=True)

                            if expected_json != current_json:
                                file_path = os.path.join(os.path.relpath(BASE_DIR, GIT_ROOT), test_spec.file)
                                self.logger.error("Pipe verify failed! "
                                                  "Content mismatch for test spec '%s'" % test_spec.file,
                                                  {"file_path": file_path})

                                self.logger.info("Expected output:\n%s" % pprint.pformat(expected_output))

                                if self.args.extra_extra_verbose:
                                    self.logger.info("Expected output JSON:\n%s" % expected_json)
                                    self.logger.info("Got raw output:\n%s" % pprint.pformat(current_output))
                                    self.logger.info("Got output JSON:\n%s" % current_json)

                                self.logger.info("Got output:\n%s" % pprint.pformat(fixed_current_output))

                                diff = self.get_diff_string(expected_json, current_json,
                                                            test_spec.file, "current-output.json")

                                self.logger.info("Diff:\n%s" % diff)
                                failed_tests.append(test_spec)
                                failed = True

                    elif test_spec.endpoint == "xml":
                        # Special case: download and format xml document as a string
                        self.logger.debug("Comparing XML output..")
                        expected_output = test_spec.expected_data
                        current_output = self.sesam_node.get_published_data(pipe, "xml",
                                                                            params=test_spec.parameters,
                                                                            binary=True)

                        try:
                            # Compare prettified versions of expected and current output so we have the
                            # same serialisation to look at (XML documents may be semanticaly identical even if
                            # their serialisations differ).
                            expected_output = self.bytes_to_xml_string(expected_output)
                            current_output = self.bytes_to_xml_string(current_output)

                            if expected_output != current_output:
                                failed_tests.append(test_spec)
                                failed = True

                                self.logger.info("Pipe verify failed! Content mismatch:\n%s" %
                                                 self.get_diff_string(expected_output, current_output, test_spec.file,
                                                                      "current_data.xml"))

                        except BaseException as e:
                            # Unable to parse the expected input and/or the current output, we'll have to just
                            # compare them byte-by-byte

                            self.logger.debug("Failed to parse expected output and/or current output as XML")
                            self.logger.debug("Falling back to byte-level comparison. Note that this might generate "
                                              "false differences for XML data.")

                            if expected_output != current_output:
                                failed_tests.append(test_spec)
                                failed = True
                                self.logger.error("Pipe verify failed! Content mismatch!")
                    else:
                        # Download contents as-is as a byte buffer
                        expected_output = test_spec.expected_data
                        current_output = self.sesam_node.get_published_data(pipe, test_spec.endpoint,
                                                                            params=test_spec.parameters, binary=True)

                        if expected_output != current_output:
                            failed_tests.append(test_spec)
                            failed = True

                            # Try to show diff - first try utf-8 encoding
                            try:
                                expected_output = str(expected_output, encoding="utf-8")
                                current_output = str(current_output, encoding="utf-8")
                            except UnicodeDecodeError as e:
                                try:
                                    expected_output = str(expected_output, encoding="latin-1")
                                    current_output = str(current_output, encoding="latin-1")
                                except UnicodeDecodeError as e2:
                                    self.logger.error("Pipe verify failed! Content mismatch!")
                                    self.logger.warning("Unable to read expected and/or output data as "
                                                        "unicode text so I can't show diff")
                                    continue

                            self.logger.error("Pipe verify failed! Content mismatch:\n%s" %
                                              self.get_diff_string(expected_output, current_output, test_spec.file,
                                                                   "current_data.txt"))
            else:
                self.logger.error("No tests references pipe '%s'" % pipe.id)
                missing_tests.append(pipe.id)
                failed = True

        if failed:
            if len(failed_tests) > 0:
                self.logger.error("Failed %s of %s tests!" % (len(failed_tests), len(list(test_specs.keys()))))
                self.logger.error("Failed pipe id (spec file):")
                for failed_test_spec in failed_tests:
                    self.logger.error("%s (%s)" % (failed_test_spec.pipe, failed_test_spec.spec_file))

            if len(missing_tests) > 0:
                self.logger.error("Missing %s tests!" % len(missing_tests))
                self.logger.error("Missing test for pipe:")
                for pipe_id in missing_tests:
                    self.logger.error(pipe_id)

            raise RuntimeError("Verify failed")
        else:
            self.logger.info("All tests passed! Ran %s tests." % len(list(test_specs.keys())))

    def find_xml_header_settings(self, xml_data):
        xml_declaration = False
        standalone = None

        if xml_data.startswith(b"<?xml "):
            xml_declaration = True

            end_decl = xml_data.find(b"?>")
            if end_decl > -1:
                xmldecl = xml_data[0:end_decl]
                parts = xmldecl.split(b"standalone=")

                if len(parts) > 1:
                    arg = parts[1]
                    if arg.startswith(b'"'):
                        endix = arg[1:].find(b'"')
                        standalone = arg[1:endix]
                    elif arg.startswith(b"'"):
                        endix = arg[1:].find(b"'")
                        standalone = arg[1:endix]

        if standalone is not None:
            standalone = str(standalone, encoding="utf-8")

        return xml_declaration, standalone

    def test_entities_to_pipe(self, pipe):
        # Get input entities from a live node and add these to the local pipe configuration as test entities
        self.logger.info(f"Adding test entities to pipe '{pipe['_id']}'")
        node_pipe = self.sesam_node.get_pipe(pipe['_id'])
        if node_pipe is not None:
            dataset_id = node_pipe.config['effective'].get("sink", {}).get("dataset", node_pipe.id)
        else:
            self.logger.warning(f"Configuration for '{pipe['_id']}' was not found on the node. Continuing without "
                                f"adding any test entities.")
            return pipe, 0

        try:
            dataset = self.sesam_node.api_connection.get_dataset(dataset_id)
            entities = list(dataset.get_entities(history=False, deleted=False, limit=10, do_transit_decoding=False))
        except BaseException as e:
            self.logger.warning(f"Unable to get entities from '{pipe['_id']}' due to an exception:\n{e}")
            entities = []

        pipe["source"]["alternatives"]["test"]["entities"] = entities
        if len(entities) == 0:
            self.logger.info(f"No input entities were found for '{pipe['_id']}', so test entities will not be updated.")

        return pipe, len(entities)

    def add_test_alternative(self, pipe):
        logger.debug(f"Adding test alternative to pipe {pipe['_id']}")
        pipe["source"]["alternatives"]["test"] = {
            "type": "embedded",
            "entities": []
        }

        return pipe

    def add_conditional_source(self, pipe):
        logger.debug(f"Adding conditional source to pipe {pipe['_id']}")
        prod_source = pipe["source"]

        pipe["source"] = {
            "type": "conditional",
            "alternatives": {
                "prod": prod_source,
                "test": {
                    "type": "embedded",
                    "entities": []
                }
            },
            "condition": "$ENV(node-env)"}

        return pipe

    def init(self):
        self.logger.info("Adding conditional sources to input pipes...")

        files = glob.glob("pipes%s*.conf.json" % os.sep)

        # Conditional sources should not be added to dataset-type sources or embedded sources
        excluded_types = ["dataset", "merge", "merge_datasets", "union_datasets", "diff_datasets", "embedded"]
        added_sources = 0
        added_entities = 0
        modified_sources = 0

        for cfg_path in files:
            new_cfg = None
            with open(cfg_path) as f:
                p = json.load(f)
                source_type = p["source"]["type"]

                # Check if pipe already has a conditional source, then add test alternative if needed
                # If add-test-entities is True, input entities from prod are added as test entities
                if source_type == 'conditional':
                    if 'test' not in p["source"]["alternatives"]:
                        new_cfg = self.add_test_alternative(p)
                        added_sources += 1

                    if self.args.add_test_entities:
                        current_entities = p["source"]["alternatives"]["test"]["entities"]

                        # If there are no entities in the test alternative, or if existing test entities should be
                        # overwritten, then add test entities from a Sesam node (such as prod)
                        if len(current_entities) == 0 or self.args.force_add:
                            new_cfg, num_added = self.test_entities_to_pipe(p)
                            added_entities += num_added
                            if num_added > 0:
                                modified_sources += 1
                        else:
                            self.logger.info(f"Pipe {p['_id']} already has test entities. Re-run with '-force-add' "
                                             f"if you want to overwrite these entities.")

                elif source_type not in excluded_types:
                    new_cfg = self.add_conditional_source(p)
                    if self.args.add_test_entities:
                        new_cfg, num_added = self.test_entities_to_pipe(p)
                        added_entities += num_added
                        if num_added > 0:
                            modified_sources += 1

                    added_sources += 1

                if new_cfg is not None:
                    with open(cfg_path, 'w', encoding="utf-8") as pipe_file:
                        pipe_file.write(format_object(new_cfg, self.formatstyle))

        if added_sources > 0:
            self.logger.info("Successfully added test sources to %i pipes." % added_sources)
        else:
            self.logger.info("All input pipes already have conditional sources with test alternatives. "
                             "No test sources were added.")
        if modified_sources > 0:
            self.logger.info("Successfully added a total of %i test entities to %i pipes."
                             % (added_entities, modified_sources))
        elif modified_sources + added_entities == 0:
            self.logger.info("No pipe configurations were modified.")

        if not self.args.is_connector and self.args.connector_dir!=".":
            with open(Path(self.args.connector_dir, "manifest.json"), "w") as f:
                json.dump({"datatypes": {}, "additional_parameters": {}}, f, indent=2, sort_keys=True)

    def update(self):
        self.logger.info("Updating expected output from current output...")
        output_pipes = {}

        for p in self.sesam_node.get_output_pipes() + self.sesam_node.get_endpoint_pipes():
            output_pipes[p.id] = p

        test_specs = self.load_test_specs(output_pipes, update=True)

        if not test_specs:
            raise AssertionError("Found no tests (*.test.json) to update")

        i = 0
        for pipe in output_pipes.values():
            if pipe.id in test_specs:
                if self.whitelisted_pipes and pipe.id not in self.whitelisted_pipes:
                    self.logger.warning(
                        f"Skipping updating expected output for pipe '{pipe.id}' - add it to the whitelist if "
                        f"this is not correct!")
                    continue

                self.logger.debug("Updating pipe '%s'.." % pipe.id)

                # Process all tests specs for this pipe
                for test_spec in test_specs[pipe.id]:
                    if test_spec.ignore is True:
                        self.logger.debug("Skipping test spec '%s' because it was marked as 'ignore'" % test_spec.name)
                        continue

                    self.logger.debug("Updating spec '%s' for pipe '%s'.." % (test_spec.name, pipe.id))
                    if test_spec.endpoint == "json" or test_spec.endpoint == "excel":
                        # Get current entities from pipe in json form
                        current_output = self._fix_decimal_to_ints([self.filter_entity(e, test_spec)
                                                                    for e in self.sesam_node.get_pipe_entities(
                                pipe, stage=test_spec.stage)])

                        if test_spec.ignore_deletes:
                            # Filter away any deletes from the current output
                            current_output = [en for en in current_output if en.get("_deleted", False) is False]

                        current_output = sorted(current_output,
                                                key=lambda e: (e['_id'],
                                                               json.dumps(e,
                                                                          indent="  ",
                                                                          ensure_ascii=self.args.unicode_encoding,
                                                                          sort_keys=True)))

                        current_output = (json.dumps(current_output, indent="  ",
                                                     sort_keys=True,
                                                     ensure_ascii=self.args.unicode_encoding) +
                                          "\n").encode("utf-8")

                        if self.args.disable_json_html_escape is False:
                            current_output = current_output.replace(b"<", b"\\u003c")
                            current_output = current_output.replace(b">", b"\\u003e")
                            current_output = current_output.replace(b"&", b"\\u0026")

                    elif test_spec.endpoint == "xml":
                        # Special case: download and format xml document as a string
                        xml_data = self.sesam_node.get_published_data(pipe, "xml", params=test_spec.parameters,
                                                                      binary=True)
                        xml_doc_root = etree.fromstring(xml_data)

                        xml_declaration, standalone = self.find_xml_header_settings(xml_data)

                        current_output = etree.tostring(xml_doc_root, encoding="utf-8",
                                                        xml_declaration=xml_declaration,
                                                        standalone=standalone,
                                                        pretty_print=True)
                    else:
                        # Download contents as-is as a string
                        current_output = self.sesam_node.get_published_data(pipe, test_spec.endpoint,
                                                                            params=test_spec.parameters, binary=True)

                    test_spec.update_expected_data(current_output)
                    i += 1

        self.logger.info("%s tests updated!" % i)

    def stop(self, throw_error=True):
        try:
            self.logger.info("Trying to stop a previously running scheduler..")

            self.sesam_node.stop_internal_scheduler()

            self.logger.info("Any previously running scheduler has been stopped")
        except BaseException as e:
            self.logger.error("Failed to stop running schedulers!")
            if throw_error:
                raise e

    def test(self):
        last_additional_info = None
        try:
            self.logger.info("Running test: upload, run and verify..")
            self.upload()

            for i in range(self.args.runs):
                last_additional_info = self.run()

            self.verify()
            self.logger.info("Test was successful!")
            if last_additional_info is not None:
                self.logger.info(last_additional_info)
        except BaseException as e:
            self.logger.error("Test failed!")
            raise e

    def run_internal_scheduler(self):
        start_time = time.monotonic()

        zero_runs = self.args.scheduler_zero_runs
        max_runs = self.args.scheduler_max_runs
        max_run_time = self.args.scheduler_max_run_time
        delete_input_datasets = not os.path.isdir("testdata")
        check_input_pipes = self.args.scheduler_check_input_pipes
        output_run_statistics = self.args.output_run_statistics
        scheduler_mode = self.args.scheduler_mode
        requests_mode = self.args.scheduler_request_mode

        if scheduler_mode is not None and scheduler_mode not in ["active", "poll"]:
            raise RuntimeError("'scheduler_mode' can only be set to 'active' or 'poll'")

        if requests_mode is not None and requests_mode not in ["sync", "async"]:
            raise RuntimeError("'request_mode' can only be set to 'sync' or 'async'")

        class SchedulerRunner(threading.Thread):
            def __init__(self, sesam_node):
                super().__init__()
                self.sesam_node = sesam_node
                self.status = None
                self.token = None
                self.additional_info = None
                self.result = {}

            def run(self):
                try:
                    self.result = self.sesam_node.run_internal_scheduler(max_run_time=max_run_time,
                                                                         max_runs=max_runs,
                                                                         zero_runs=zero_runs,
                                                                         delete_input_datasets=delete_input_datasets,
                                                                         check_input_pipes=check_input_pipes,
                                                                         output_run_statistics=output_run_statistics,
                                                                         scheduler_mode=scheduler_mode,
                                                                         request_mode=requests_mode
                                                                         )

                    if requests_mode == "sync":
                        if self.result["status"] == "success":
                            self.status = "finished"
                        else:
                            self.status = "failed"
                    else:
                        # In async mode we loop until status changes (or status request fails)
                        if "token" not in self.result:
                            raise AssertionError("Response from scheduler with 'async' request_mode didn't "
                                                 "contain a token!")

                        self.token = self.result["token"]
                        while True:
                            status = self.sesam_node.get_internal_scheduler_status(self.token)

                            if status["status"] == "success":
                                self.status = "finished"
                                break
                            elif status["status"] == "failed":
                                self.status = "failed"
                                break
                            elif status["status"] == "not-running":
                                self.status = "failed"
                                self.result = "Scheduler is not running"
                                break

                            time.sleep(10)

                except BaseException as e:
                    self.status = "failed"
                    self.result = e

        scheduler_runner = SchedulerRunner(self.sesam_node)
        scheduler_runner.start()

        time.sleep(1)

        since = None

        def print_internal_scheduler_log(since_val, token=None):
            log_lines = self.sesam_node.get_internal_scheduler_log(since=since_val, token=token)
            for log_line in log_lines:
                if isinstance(log_line, dict):
                    s = "%s - %s - %s" % (log_line["timestamp"], log_line["loglevel"], log_line["logdata"])
                    logger.info(s)
                else:
                    logger.debug(f"Log line was not a dict! Was {type(log_line)} ('{log_line}')")
                    return None

            if len(log_lines) > 0:
                return log_lines[-1]["timestamp"]

            return since_val

        while True:
            if self.args.print_scheduler_log is True:
                since = print_internal_scheduler_log(since, token=scheduler_runner.token)

            if scheduler_runner.status is not None:
                break

            time.sleep(1)

        if scheduler_runner.status == "failed":
            self.logger.error("Failed to run pipes to completion")
            if self.args.print_scheduler_log is True:
                print_internal_scheduler_log(since, token=scheduler_runner.token)
            raise RuntimeError(scheduler_runner.result)

        if self.args.print_scheduler_log is True:
            print_internal_scheduler_log(since, token=scheduler_runner.token)

        self.logger.info("Successfully ran all pipes to completion in %s seconds" % int(time.monotonic() - start_time))

        additional_info = scheduler_runner.result.get("additional_info")
        if additional_info is not None:
            self.logger.info(additional_info)
            return additional_info

        return None

    def run(self):
        try:
            self.stop()

            return self.run_internal_scheduler()
        except BaseException as e:
            logger.error("Failed to run the tests")
            raise e

    def wipe(self):
        try:
            self.stop()

            self.logger.info("Wiping node...")

            # We need to include the "disable-user-pipes" setting when wiping the pipes and systems, or they will
            # start running (asserting datasets, compiling dtl etc) while we're doing the wipe,
            # which makes it take a lot longer to run

            if os.path.isfile("node-metadata.conf.json"):
                with open("node-metadata.conf.json", "rt") as infile:
                    node_metadata = json.loads(infile.read())
            else:
                node_metadata = {
                    "_id": "node",
                    "type": "metadata"
                }

            if "task_manager" not in node_metadata:
                node_metadata["task_manager"] = {}

            if "global_defaults" not in node_metadata:
                node_metadata["global_defaults"] = {}

            node_metadata["global_defaults"]["use_signalling_internally"] = False
            node_metadata["global_defaults"]["use_signalling_externally"] = False
            node_metadata["global_defaults"]["enable_cpp_extensions"] = False
            node_metadata["task_manager"]["disable_user_pipes"] = True

            self.sesam_node.put_config([node_metadata], force=True)
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

        self.sesam_node.wait_for_all_pipes_to_disappear()

        self.logger.info("Successfully wiped node!")

    def reset(self):
        try:
            self.stop(throw_error=False)

            self.logger.info("Resetting target node...")

            self.sesam_node.reset(timeout=self.args.restart_timeout)
        except BaseException as e:
            logger.error("Failed to reset target node!")
            raise e

        self.logger.info("Successfully reset target node!")

    def restart(self):
        try:
            self.stop(throw_error=False)

            self.logger.info("Restarting target node...")

            self.sesam_node.restart(timeout=self.args.restart_timeout)
        except BaseException as e:
            logger.error("Failed to restart target node!")
            raise e

        self.logger.info("Successfully restarted target node!")

    def convert(self):

        def get_pipe_id(path):
            basename = os.path.basename(path)
            return basename.replace(".conf.json", "")

        def has_conditional_embedded_source(pipe_config, env):
            source_config = pipe_config.get("source", {})
            source_type = source_config.get("type", "")
            if source_type == "conditional":
                alternatives = source_config.get("alternatives")
                current_profile_alternative = alternatives.get(env, {})
                if current_profile_alternative.get("type", "") == "embedded":
                    return True
            return False

        def convert_pipe_config(pipe_config):
            entities = None
            modified_pipe_config = None
            if has_conditional_embedded_source(pipe_config, self.args.profile):
                alternatives = pipe["source"]["alternatives"]
                entities = alternatives[self.args.profile]["entities"]
                # rewrite the case which corresponds to env profile
                alternatives[self.args.profile] = {
                    "type": "http_endpoint"
                }
                modified_pipe_config = pipe_config

            return modified_pipe_config, entities

        def save_testdata_file(pipe_id, entities):
            os.makedirs("testdata", exist_ok=True)
            with open(f"testdata{os.sep}{pipe_id}.json", "w", encoding="utf-8") as testdata_file:
                testdata_file.write(format_object(entities, self.formatstyle))

        def save_modified_pipe(pipe_json, path):
            with open(path, 'w', encoding="utf-8") as pipe_file:
                pipe_file.write(format_object(pipe_json, self.formatstyle))

        self.logger.info("Starting converting conditional embedded sources")

        if self.args.dump:
            self.logger.info("Dumping config for backup")
            self.dump()

        for filepath in glob.glob("pipes%s*.conf.json" % os.sep):
            pipe_id_from_basename = get_pipe_id(filepath)

            with open(filepath, 'r', encoding="utf-8") as pipe_file:
                pipe = json.load(pipe_file)
                pipe_to_rewrite, entities = convert_pipe_config(pipe)

            if pipe_to_rewrite is not None:
                save_modified_pipe(pipe_to_rewrite, filepath)

            if entities is not None:
                save_testdata_file(pipe["_id"], entities)

        self.logger.info("Successfully converted pipes and created testdata folder")


class AzureFormatter(logging.Formatter):
    """Azure syntax log formatter to enrich build feedback"""
    error_format = '##vso[task.logissue type=error;]%(message)s'
    warning_format = '##vso[task.logissue type=warning;]%(message)s'
    debug_format = '##[debug]%(message)s'
    default_format = '%(message)s'

    def format(self, record):
        if record.levelno == logging.ERROR:
            error_format = self.error_format
            if hasattr(record.args, "get"):
                record.file_path = record.args.get("file_path")
                record.line_number = record.args.get("line_number")
                record.column_number = record.args.get("column_number")
                error_format = "##vso[task.logissue type=error;"
                if record.file_path:
                    error_format += "sourcepath=%(file_path)s;"
                if record.line_number:
                    error_format += "linenumber=%(line_number)s;"
                if record.column_number:
                    error_format += "columnnumber=%(column_number)s;"
                error_format += "]%(message)s"
            return logging.Formatter(error_format).format(record)
        elif record.levelno == logging.WARNING:
            return logging.Formatter(self.warning_format).format(record)
        elif record.levelno == logging.DEBUG:
            return logging.Formatter(self.debug_format).format(record)
        return logging.Formatter(self.default_format).format(record)


if __name__ == '__main__':
    parser = SesamParser(prog="sesam", description="""
Commands:
  wipe      Deletes all the pipes, systems, user datasets and environment variables in the node
  restart   Restarts the target node (typically used to release used resources if the environment is strained)
  reset     Deletes the entire node database and restarts the node (this is a more thorough version than "wipe" - requires the target node to be a designated developer node, contact support@sesam.io for help)
  init      Add conditional sources with testing and production alternatives to all input pipes in the local config.
  upload    Replace node config with local config. Also tries to upload testdata if 'testdata' folder present.
  download  Replace local config with node config
  dump      Create a zip archive of the config and store it as 'sesam-config.zip'
  status    Compare node config with local config (requires external diff command)
  run       Run configuration until it stabilizes
  update    Store current output as expected output
  convert   Convert embedded sources in input pipes to http_endpoints and extract data into files
  verify    Compare output against expected output
  test      Upload, run and verify output
  stop      Stop any running schedulers (for example if the client was permaturely terminated or disconnected)
""", formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-version', dest='version', required=False, action='store_true', help="print version number")

    parser.add_argument('-v', dest='verbose', required=False, action='store_true', help="be verbose")

    parser.add_argument('-vv', dest='extra_verbose', required=False, action='store_true', help="be extra verbose")

    parser.add_argument('-vvv', dest='extra_extra_verbose', required=False, action='store_true',
                        help="be extra extra verbose")

    parser.add_argument('-skip-tls-verification', dest='skip_tls_verification', required=False, action='store_true',
                        help="skip verifying the TLS certificate")

    parser.add_argument('-sync-config-file', dest='sync_config_file', metavar="<string>",
                        default=".syncconfig", type=str, help="sync config file to use, the default is "
                                                              "'.syncconfig' in the current directory")

    parser.add_argument('-whitelist-file', dest='whitelist_file', metavar="<string>",
                        type=str, help="whitelist file to use, the default is none")

    parser.add_argument('-dont-remove-scheduler', dest='dont_remove_scheduler', required=False, action='store_true',
                        help="don't remove scheduler after failure (DEPRECATED)")

    parser.add_argument('-dump', dest='dump', required=False, help="dump zip content to disk", action='store_true')

    parser.add_argument('-print-scheduler-log', dest='print_scheduler_log', required=False,
                        help="print scheduler log during run", action='store_true')

    parser.add_argument('-output-run-statistics', dest='output_run_statistics', required=False,
                        help="output detailed pipe run statistics after scheduler run", action='store_true')

    parser.add_argument('-use-internal-scheduler', dest='use_internal_scheduler', required=False,
                        help="use the built-in scheduler in sesam instead of a microservice (DEPRECATED)",
                        action='store_true')

    parser.add_argument('-custom-scheduler', dest='custom_scheduler', required=False,
                        help="by default a scheduler system will be added, enable this flag if you have configured a "
                             "custom scheduler as part of the config (DEPRECATED)", action='store_true')

    parser.add_argument('-scheduler-image-tag', dest='scheduler_image_tag', required=False,
                        help="the scheduler image tag to use (DEPRECATED)", type=str,
                        metavar="<string>")

    parser.add_argument('-scheduler-mode', dest='scheduler_mode', required=False,
                        help="the scheduler mode to use ('active' or 'poll') - the default is 'active'", type=str,
                        metavar="<string>")

    parser.add_argument('-node', dest='node', metavar="<string>", required=False, help="service url")
    parser.add_argument('-scheduler-node', dest='scheduler_node', metavar="<string>", required=False,
                        help="service url for scheduler")
    parser.add_argument('-jwt', dest='jwt', metavar="<string>", required=False, help="authorization token")

    parser.add_argument('-single', dest='single', required=False, metavar="<string>",
                        help="update or verify just a single pipe")

    parser.add_argument('-no-large-int-bugs', dest='no_large_int_bugs', required=False, action='store_true',
                        help="don't reproduce old large int bugs")

    parser.add_argument('-disable-user-pipes', dest='disable_user_pipes', required=False, action='store_true',
                        help="turn off user pipe scheduling in the target node (DEPRECATED)")

    parser.add_argument('-enable-eager-ms', dest='enable_eager_ms', required=False, action='store_true',
                        help="run all microservices even if they are not in use (note: multinode only)")

    parser.add_argument('-enable-user-pipes', dest='enable_user_pipes', required=False, action='store_true',
                        help="turn on user pipe scheduling in the target node")

    parser.add_argument('-compact-execution-datasets', dest='compact_execution_datasets', required=False,
                        action='store_true',
                        help="compact all execution datasets when running scheduler")

    parser.add_argument('-disable-cpp-extensions', dest='disable_cpp_extensions', required=False, action='store_true',
                        help="turns off cpp extensions which saves dtl compile time at the expense of possibly slower dtl exeution time")

    parser.add_argument('-unicode-encoding', dest='unicode_encoding', required=False, action='store_true',
                        help="store the 'expected output' json files using unicode encoding ('\\uXXXX') - "
                             "the default is UTF-8")

    parser.add_argument('-disable-json-html-escape', dest='disable_json_html_escape',
                        required=False, action='store_true',
                        help="turn off escaping of '<', '>' and '&' characters in 'expected output' json files")

    parser.add_argument('-profile', dest='profile', metavar="<string>", default="test", required=False,
                        help="env profile to use <profile>-env.json")

    parser.add_argument('-scheduler-id', dest='scheduler_id', metavar="<string>", required=False,
                        help="system id for the scheduler system (DEPRECATED)")

    parser.add_argument('-scheduler-request-mode', dest='scheduler_request_mode', required=False, type=str,
                        metavar="<string>", default="sync",
                        help="run the scheduler in 'sync' or 'async' mode, long running tests should run "
                             "in 'async' mode")

    parser.add_argument('-scheduler-zero-runs', dest='scheduler_zero_runs', default=2, metavar="<int>", type=int,
                        required=False,
                        help="the number of runs that has to yield zero changes for the scheduler to finish")

    parser.add_argument('-scheduler-max-runs', dest='scheduler_max_runs', default=100, metavar="<int>", type=int,
                        required=False,
                        help=" maximum number of runs that scheduler can do to before exiting (internal scheduler only)")

    parser.add_argument('-scheduler-max-run-time', dest='scheduler_max_run_time', default=15 * 60, metavar="<int>",
                        type=int, required=False,
                        help="the maximum time the internal scheduler is allowed to use to finish "
                             "(in seconds, internal scheduler only)")

    parser.add_argument('-scheduler-check-input-pipes', dest='scheduler_check_input_pipes', required=False,
                        action="store_true",
                        help="controls whether failing input pipes should make the scheduler run fail")

    parser.add_argument('-restart-timeout', dest='restart_timeout', default=15 * 60, metavar="<int>", type=int,
                        required=False,
                        help="the maximum time to wait for the node to restart and become available again "
                             "(in seconds). The default is 15 minutes. A value of 0 will skip the back-up-again verification.")

    parser.add_argument('-runs', dest='runs', type=int, metavar="<int>", required=False, default=1,
                        help="number of test cycles to check for stability")

    parser.add_argument('-logformat', dest='logformat', type=str, metavar="<string>", required=False, default="short",
                        help="output format (normal, log or azure)")

    parser.add_argument('-scheduler-poll-frequency', metavar="<int>", dest='scheduler_poll_frequency', type=int,
                        required=False,
                        default=5000, help="milliseconds between each poll while waiting for the scheduler")

    parser.add_argument('-sesamconfig-file', dest='sesamconfig_file', metavar="<string>", type=str,
                        help="sesamconfig file to use, the default is '.sesamconfig.json' in the current directory")

    parser.add_argument('-diff', dest='diff', required=False, action='store_true',
                        help="use with the status command to show the diff of the files")

    parser.add_argument('-add-test-entities', dest='add_test_entities', required=False, action='store_true',
                        help="use with the init command to add test entities to input pipes")

    parser.add_argument('-force-add', dest='force_add', required=False, action='store_true',
                        help="use with the '-add-test-entities' option to overwrite test entities that exist locally")

    parser.add_argument('command', metavar="command", nargs='?', help="a valid command from the list above")

    parser.add_argument('-force', dest='force', required=False, action='store_true',
                        help="force the command to run (only for 'upload' and 'download' commands) for non-dev "
                             "subscriptions")

    parser.add_argument("--system-placeholder", metavar="<string>",
                        default="xxxxxx", type=str, help="Name of the system _id placeholder (available only when working on connectors)")

    parser.add_argument("-d", dest="connector_dir", metavar="<string>",
                        default=".", type=str, help="Connector folder to work with (available only when working on connectors)")

    parser.add_argument("-e", dest="expanded_dir", metavar="<string>",
                        default=".expanded", type=str, help="Directory to expand the config into (available only when working on connectors)")

    parser.add_argument("--client_id", metavar="<string>",
                        type=str, help="OAuth2 client id (available only when working on connectors)")

    parser.add_argument("--client_secret", metavar="<string>",
                        type=str, help="OAuth2 client secret (available only when working on connectors)")

    parser.add_argument("--service_url", metavar="<string>",
                        type=str, help="url to service api (include /api) (available only when working on connectors)")

    parser.add_argument("--service_jwt", metavar="<string>",
                        type=str, help="jwt token to the service api (available only when working on connectors)")

    parser.add_argument("--consumer_token", metavar="<string>",
                        type=str, help="consumer token (available only when working on connectors)")

    parser.add_argument("--employee_token", metavar="<string>",
                        type=str, help="employee token (available only when working on connectors)")

    parser.add_argument("--base_url", metavar="<string>",
                        type=str, default="https://api.tripletex.io", help="override to use prod env (available only when working on connectors)")

    parser.add_argument("--days", metavar="<string>",
                        type=int, default=10, help="number of days until the token should expire (available only when working on connectors)")

    try:
        args = parser.parse_args()
        args.is_connector = os.path.isfile(os.path.join(args.connector_dir, "manifest.json"))
    except SystemExit as e:
        sys.exit(e.code)
    except BaseException as e:
        sys.exit(1)

    if args.version:
        print("sesam version %s" % sesam_version)
        sys.exit(0)

    if args.logformat == "log":
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(format_string)
    elif args.logformat == "azure":
        formatter = AzureFormatter()

        # Need to find git root to use when reporting file path in log messages
        cur_dir = os.getcwd()

        while True:
            file_list = os.listdir(cur_dir)
            parent_dir = os.path.dirname(cur_dir)
            if ".git" in file_list and os.path.isdir(os.path.join(cur_dir, ".git", "objects")):
                GIT_ROOT = cur_dir
                break
            else:
                if cur_dir == parent_dir:
                    logger.debug("git root not found")
                    break
                else:
                    cur_dir = parent_dir
    else:
        format_string = '%(message)s'
        formatter = logging.Formatter(format_string)

    # Log to stdout
    logging.addLevelName(LOGLEVEL_TRACE, "TRACE")
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    elif args.extra_verbose:
        logger.setLevel(LOGLEVEL_TRACE)
    elif args.extra_extra_verbose:
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

    logger.info("Using %s as base directory", BASE_DIR)

    command = args.command and args.command.lower() or ""

    if command not in ["authenticate","upload", "download", "status", "init", "update", "verify", "test", "run", "wipe",
                       "restart", "reset", "dump", "stop", "convert"]:
        if command:
            logger.error("Unknown command: '%s'", command)
        else:
            logger.error("No command given")

        parser.print_usage()
        sys.exit(1)

    try:
        sesam_cmd_client = SesamCmdClient(args, logger)
    except BaseException as e:
        if args.verbose or args.extra_verbose:
            logger.exception(e)

        sys.exit(1)

    try:
        node_url, jwt_token = sesam_cmd_client.get_node_and_jwt_token()
    except BaseException as e:
        if args.verbose is True or args.extra_verbose is True or args.extra_extra_verbose is True:
            logger.exception(e)
        logger.error("jwt and node must be specified either as parameter, os env or in syncconfig file")
        sys.exit(1)

    try:
        sesam_cmd_client.formatstyle = sesam_cmd_client.get_formatstyle_from_configfile()
    except BaseException as e:
        if args.verbose is True or args.extra_verbose is True or args.extra_extra_verbose is True:
            logger.exception(e)
        logger.error("config file is mandatory when -sesamconfig-file argument is specified")
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

    start_time = time.monotonic()
    allowed_commands_for_non_dev_subscriptions = ["upload", "download"]
    try:
        if sesam_cmd_client.sesam_node.api_connection.get_api_info().get("status").get("developer_mode") or \
                (command in allowed_commands_for_non_dev_subscriptions and args.force):
            if command == "authenticate":
                sesam_cmd_client.authenticate()
            elif command == "upload":
                if not args.is_connector:
                    sesam_cmd_client.upload()
                else:
                    expand_connector(args.connector_dir, args.system_placeholder, args.expanded_dir,args.profile)
                    os.chdir(os.path.join(args.connector_dir, args.expanded_dir))
                    sesam_cmd_client.upload()
                    os.chdir(os.pardir) if args.connector_dir == "." else os.chdir(os.path.join(os.pardir, os.pardir))
                    sesam_cmd_client.authenticate()
            elif command == "download":
                sesam_cmd_client.download()
            elif command == "status":
                sesam_cmd_client.status()
            elif command == "init":
                sesam_cmd_client.init()
            elif command == "update":
                sesam_cmd_client.update()
            elif command == "verify":
                sesam_cmd_client.verify()
            elif command == "test":
                sesam_cmd_client.test()
            elif command == "stop":
                sesam_cmd_client.stop()
            elif command == "run":
                if args.enable_user_pipes is True:
                    logger.warning("Note that the -enable-user-pipes flag has no effect on the actual sesam instance "
                                   "outside the 'upload' or 'test' commands")

                if args.disable_cpp_extensions is True:
                    logger.warning(
                        "Note that the -disable-cpp-extensions flag has no effect on the actual node configuration "
                        "outside the 'upload' or 'test' commands")

                if args.enable_eager_ms is True:
                    logger.warning("Note that the -enable-eager-ms flag has no effect on the actual node configuration "
                                   "outside the 'upload' or 'test' commands")

                sesam_cmd_client.run()
            elif command == "wipe":
                sesam_cmd_client.wipe()
            elif command == "restart":
                sesam_cmd_client.restart()
            elif command == "reset":
                sesam_cmd_client.reset()
            elif command == "convert":
                sesam_cmd_client.convert()
            elif command == "dump":
                sesam_cmd_client.dump()
            else:
                logger.error("Unknown command: %s" % command)
                sys.exit(1)
        else:
            logger.error(
                f"The targeted Sesam subscription is not a developer environment, please contact support@sesam.io if this is unexpected. "
                f"{'To override this check use -force flag.' if command in allowed_commands_for_non_dev_subscriptions else ''}")
            sys.exit(1)
    except BaseException as e:
        logger.error("Sesam client failed!")
        if args.extra_verbose is True or args.extra_extra_verbose is True:
            logger.exception("Underlying exception was: %s" % str(e))

        sys.exit(1)
    finally:
        run_time = time.monotonic() - start_time
        logger.info("Total run time was %d seconds" % run_time)
