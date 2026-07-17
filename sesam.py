import itertools
import json
import logging
import logging.handlers
import os
import random
import re
import sys
import time
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from base64 import urlsafe_b64decode
from configparser import ConfigParser
from copy import deepcopy
from decimal import Decimal
from difflib import unified_diff
from glob import glob
from io import BytesIO, StringIO
from pprint import pformat
from threading import Lock
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
import sesamclient
from lxml import etree
from requests import post
from requests.exceptions import RequestException

from jsonformat import format_json
from sesam_cli.cli import (
    ALLOWED_NON_DEV_SUBSCRIPTION_COMMANDS,
    VALID_COMMANDS,
    execute_command,
)
from sesam_cli.commands.convert import execute_convert
from sesam_cli.commands.config_sync import execute_download, execute_status
from sesam_cli.commands.format_cmd import execute_format
from sesam_cli.commands.init_connector import (
    execute_add_datatype,
    execute_connector_init,
    execute_init,
    get_datatype_template as command_get_datatype_template,
)
from sesam_cli.commands.authenticate import execute_authenticate
from sesam_cli.commands.scheduler import execute_run_internal_scheduler
from sesam_cli.commands.upload import execute_upload
from sesam_cli.commands.update import execute_update
from sesam_cli.commands.verify import execute_verify
from sesam_cli.commands.validate import execute_validate
from sesam_cli.test_spec_loader import load_test_specs as load_test_specs_impl
from sesam_cli.test_specs import normalize_path
from sesam_cli.zip_cleanup import remove_task_manager_settings as remove_task_manager_settings_impl

sesam_version = "2.11.13"

logger = logging.getLogger("sesam")
LOGLEVEL_TRACE = 2
BASE_DIR = os.getcwd()
GIT_ROOT = None


class UploadException(Exception):
    def __init__(self, errors):
        self.errors = errors

    def __str__(self):
        return self.get_validation_report()

    def get_validation_report(self):
        output = "******************Validation Errors*****************\n"
        for validation_error in self.errors:
            validation_id = validation_error.get("posted-config", {}).get("_id")
            full_config_errors = validation_error.get("config-errors")
            critical_config_errors = [
                error for error in full_config_errors if error["level"] == "error"
            ]
            if args.verbose and critical_config_errors:
                output += f"{validation_id}:\n{pformat(critical_config_errors)}\n" + "-" * 50 + "\n"
            elif args.extra_verbose and full_config_errors:
                output += f"{validation_id}:\n{pformat(full_config_errors)}\n" + "-" * 50 + "\n"
            elif args.extra_extra_verbose and validation_error:
                output += f"{validation_id}:\n{pformat(validation_error)}\n" + "-" * 50 + "\n"

        output += "**************End of Validation Errors**************"
        return output


class TestDataUploadException(Exception):
    def __init__(self, failures):
        self.failures = failures

    def __str__(self):
        output = [f"Testdata upload failed for {len(self.failures)} file(s):"]
        for failure in self.failures:
            output.append(f"- {failure['path']} ({failure['pipe_id']}): {failure['error']}")
        return "\n".join(output)


class SesamParser(ArgumentParser):
    def error(self, message):
        sys.stderr.write("error: %s\n\n" % message)
        self.print_help()
        sys.exit(2)


class SesamNode:
    """Sesam node functions wrapped in a class to facilitate unit tests"""

    def __init__(self, node_url, jwt_token, logger, verify_ssl=True):
        self.logger = logger

        self.node_url = node_url
        self.jwt_token = jwt_token
        self._last_registered_action_ts = None
        self.subscription_id = None

        # Pull data chunk from the jwt token
        _, payload, _ = self.jwt_token.split(".")
        # Add padding to base64 and decode it
        jwt_data = json.loads(urlsafe_b64decode(payload + "=="))

        # Extract the sub ID from the data
        if jwt_data:
            principals = jwt_data.get("principals", {})
            subscriptions = list(principals.keys())
            if len(subscriptions) > 0:
                self.subscription_id = subscriptions[0]

        safe_jwt = "{}*********{}".format(jwt_token[:10], jwt_token[-10:])
        self.logger.debug("Connecting to Sesam using url '%s' and JWT '%s'", node_url, safe_jwt)

        if verify_ssl is False:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Register a user-interaction even if the subsequent connection fails
        # (the subscription might be hibernated or not responding for whatever
        # reason)
        self.register_user_interaction()

        self.api_connection = sesamclient.Connection(
            sesamapi_base_url=self.node_url,
            jwt_auth_token=self.jwt_token,
            timeout=60 * 10,
            verify_ssl=verify_ssl,
        )

    def _get_deploying(self):
        deploying = []
        for pipe in [p for p in self.api_connection.get_pipes() if self.is_user_pipe(p)]:
            if pipe.runtime["state"] == "Deploying":
                deploying.append(pipe)

        return deploying

    def wait_for_all_pipes_to_deploy(self, timeout=30 * 60):
        starttime = time.time()
        deploying = self._get_deploying()

        logger.info(f"Deploying {len(deploying)} pipes...")

        while deploying:
            # As an optimization we wait for one pipe to be deployed before
            # we call get_pipes() again.
            pipe = deploying[0]
            elapsedtime = time.time() - starttime

            try:
                pipe.wait_for_pipe_to_be_deployed(timeout=0)
                logger.debug(f"Pipe '{pipe.id}' was deployed...")
                deploying = self._get_deploying()

            except BaseException:
                logger.debug(f"Pipe '{pipe.id}' is still deploying...")
                if elapsedtime > timeout:
                    raise RuntimeError(
                        "Waiting for pipes to deploy timed " f"out after {timeout} seconds!"
                    )

            time.sleep(5)

        if not deploying:
            elapsedtime = time.time() - starttime
            self.logger.debug("All pipes were deployed in %s seconds" % elapsedtime)

    def register_user_interaction(self):
        # IS-15613: attempt to register a user interaction with the portal.
        # We don't want to spam the analytics api so if it's
        # less than 60s since the last time we registered
        # an interaction just skip it
        now_ts = time.monotonic()

        if self.subscription_id is None or (
            self._last_registered_action_ts is not None
            and (now_ts - self._last_registered_action_ts) < 60
        ):
            return

        self._last_registered_action_ts = now_ts
        try:
            r = post(
                "https://portal.sesam.io/api/analytics",
                data=json.dumps({"subscription_id": self.subscription_id, "action": "api_call"}),
                headers={
                    "Authorization": "bearer %s" % self.jwt_token,
                    "Content-Type": "application/json",
                },
            )
            r.raise_for_status()
        except RequestException as e:
            logger.debug("Failed to register interaction with Sesam Portal: %s" % str(e))

    def is_user_pipe(self, pipe):
        if self.get_pipe_origin(pipe) in [
            "system",
            "search",
            "replica",
            "aggregator-storage-node",
        ]:
            return False

        if pipe.id.find(":singlenode") > -1:
            # IS-14078: temporary fix for the API intermittently returning pipes from
            # worker-nodes (with pre or
            # postfixes). This typically happens after a wipe or config update that
            # deletes multiple pipes.
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
                raise RuntimeError(
                    "Waiting for pipes to br removed timed out after %s seconds!" % timeout
                )

            logger.info(f"Waiting for {len(pipes)} pipes to be removed...")
            time.sleep(5)

    def restart(self, timeout):
        old_stats = self.api_connection.get_status()
        restart = self.api_connection.restart_node()
        if restart != {"message": "OK"}:
            self.logger.debug(
                "Restart node API call failed! It returned '%s', "
                'expected \'{"message": "OK"}\'' % restart
            )
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
                raise RuntimeError(
                    "Failed to start node - wait for node restart timed "
                    "out after %s seconds. The last errror was: %s" % (timeout, msg)
                )
            time.sleep(3)

    def reset(self, timeout):
        old_stats = self.api_connection.get_status()
        restart = self.api_connection.reset_node()
        if restart != {"message": "OK"}:
            self.logger.debug(
                "Reset node API call failed! It returned '%s', "
                'expected \'{"message": "OK"}\'' % restart
            )
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
                raise RuntimeError(
                    "Failed to start node - wait for node restart timed "
                    "out after %s seconds. The last errror was: %s" % (timeout, msg)
                )
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
        except BaseException:
            return None

    def get_pipe(self, pipe_id):
        self.logger.log(LOGLEVEL_TRACE, "Get pipe '%s' from %s" % (pipe_id, self.node_url))
        try:
            return self.api_connection.get_pipe(pipe_id)
        except BaseException:
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
            except BaseException:
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
        except BaseException as e:
            logger.warning(
                f"Could not remove system '{system_id}' - perhaps it doesn't exist" f"\nError: {e}"
            )

    def get_config(self, binary=False):
        data = self.api_connection.get_config_as_zip()
        if not binary:
            return ZipFile(BytesIO(data))

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

        if (source_config.get("dataset") or source_config.get("datasets")) and sink_config.get(
            "dataset"
        ):
            return "internal"

        if not sink_config.get("dataset"):
            return "output"

        return "internal"

    def get_output_pipes(self):
        return [
            p
            for p in self.api_connection.get_pipes()
            if self.is_user_pipe(p) and self.get_pipe_type(p) == "output"
        ]

    def get_input_pipes(self):
        return [
            p
            for p in self.api_connection.get_pipes()
            if self.is_user_pipe(p) and self.get_pipe_type(p) == "input"
        ]

    def get_endpoint_pipes(self):
        return [
            p
            for p in self.api_connection.get_pipes()
            if self.is_user_pipe(p) and self.get_pipe_type(p) == "endpoint"
        ]

    def get_internal_pipes(self):
        return [
            p
            for p in self.api_connection.get_pipes()
            if self.is_user_pipe(p) and self.get_pipe_type(p) == "internal"
        ]

    def run_internal_scheduler(
        self,
        zero_runs=None,
        max_run_time=None,
        max_runs=None,
        delete_input_datasets=True,
        reset_pipes_and_delete_sink_datasets=None,
        check_input_pipes=False,
        output_run_statistics=False,
        scheduler_mode=None,
        request_mode=None,
    ):
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

        if reset_pipes_and_delete_sink_datasets is False:
            params["reset_pipes_and_delete_sink_datasets"] = reset_pipes_and_delete_sink_datasets

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
        """Polls the microservice status API until it is running (or we time out)"""

        system = self.get_system(microservice_id)
        if system is None:
            raise AssertionError("Microservice system '%s' doesn't exist" % microservice_id)

        sleep_time = 5.0
        while timeout > 0:
            try:
                system_status = self.get_system_status(microservice_id)
            except BaseException as e:
                self.logger.debug(
                    f"Failed to get system status for microservice '{microservice_id}'"
                    f"\nError: {e}"
                )
                system_status = None

            if system_status is not None and system_status.get("running", False) is True:
                return True

            time.sleep(sleep_time)

            timeout -= sleep_time

        return False

    def microservice_get_proxy_request(
        self, microservice_id, path, params=None, result_as_json=True
    ):
        system = self.get_system(microservice_id)
        if system is None:
            raise AssertionError("Microservice system '%s' doesn't exist" % microservice_id)

        system_url = self.api_connection.get_system_url(microservice_id)
        resp = self.api_connection.session.get(system_url + "/proxy/" + path, params=params)
        resp.raise_for_status()

        if result_as_json:
            return resp.json()

        return resp.text

    def microservice_post_proxy_request(
        self, microservice_id, path, params=None, data=None, result_as_json=True
    ):
        return self.microservice_post_put_proxy_request(
            microservice_id,
            "POST",
            path,
            params=params,
            data=data,
            result_as_json=result_as_json,
        )

    def microservice_put_proxy_request(
        self, microservice_id, path, params=None, data=None, result_as_json=True
    ):
        return self.microservice_post_put_proxy_request(
            microservice_id,
            "PUT",
            path,
            params=params,
            data=data,
            result_as_json=result_as_json,
        )

    def microservice_post_put_proxy_request(
        self, microservice_id, method, path, params=None, data=None, result_as_json=True
    ):
        system = self.get_system(microservice_id)
        if system is None:
            raise AssertionError("Microservice system '%s' doesn't exist" % microservice_id)

        system_url = self.api_connection.get_system_url(microservice_id)
        if method.lower() == "post":
            resp = self.api_connection.session.post(
                system_url + "/proxy/" + path, params=params, data=data
            )
        elif method.lower() == "put":
            resp = self.api_connection.session.put(
                system_url + "/proxy/" + path, params=params, data=data
            )
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

            # Sometimes a subscription may take little while to deploy all the API
            # routes, let's give it a chance
            # to catch up before we give up
            if resp.status_code == 503:
                if time.monotonic() - starttime > timeout:
                    logger.error(
                        f"Failed to post request to HTTP receiver for pipe "
                        f"'{pipe_id}' after retrying for {timeout} seconds..."
                    )
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
    """Commands wrapped in a class to make it easier to write unit tests"""

    DEFAULT_TESTDATA_UPLOAD_WORKERS = 8
    TESTDATA_UPLOAD_PROGRESS_LOG_INTERVAL = 25
    TESTDATA_UPLOAD_MAX_RETRIES = 3
    TESTDATA_UPLOAD_RETRY_BASE_DELAY_SECONDS = 1.0
    TESTDATA_UPLOAD_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
    DEFAULT_TESTDATA_UPLOAD_RATE = 0.0

    def __init__(self, args, logger):
        self.args = args
        self.logger = logger
        self.sesam_node = None
        self.whitelisted_files = None
        self.whitelisted_pipes = None
        self.whitelisted_systems = None
        self.node_url = None
        self.jwt_token = None
        self._testdata_upload_rate_lock = Lock()
        self._testdata_next_upload_slot = 0.0

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
        with open(filename, "r") as fp:
            try:
                config = json.load(fp)
            except ValueError:
                pass
        if not config:
            with open(filename) as fp:
                parser = ConfigParser(strict=False)
                # [sesam] section is prepended to support .syncconfig file
                #  in which section is omitted
                parser.read_file(itertools.chain(["[sesam]"], fp), source=filename)
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
                    self.logger.debug(
                        "Cannot locate config file '%s' in current or parent folder. "
                        "Proceeding without it." % (filename)
                    )
            return file_config
        except BaseException as e:
            self.logger.error(
                f"Failed to read '{filename}' from either the current directory or the "
                "parent directory. Check that you are in the correct directory, that "
                "you have therequired permissions to read the files and that the files "
                "have the correct format."
            )
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

                    if self.args.is_connector:
                        zipfile.write(os.path.join(root, file))
                    else:
                        with open(os.path.join(root, file), "rb") as f:
                            contents = f.read()
                        modified_contents = self.replace_jinja_variables(contents.decode())
                        zipfile.writestr(os.path.join(root, file), modified_contents)

    def replace_jinja_variables(self, contents):
        modified_contents = contents
        if self.args.jinja_vars:
            for var in self.args.jinja_vars:
                pattern = rf"{{{{@ {var} @}}}}"
                new_pattern = rf"{self.args.jinja_vars[var]}"
                modified_contents = re.sub(pattern, new_pattern, modified_contents)
            modified_contents = modified_contents.encode("utf-8")
        return modified_contents

    def replace_template_variables(self, dir):
        for filename in os.listdir(dir):
            if filename.endswith(".json"):
                with open(os.path.join(dir, filename), "r+") as file:
                    contents = file.read()
                    modified_contents = contents
                    for var in self.args.jinja_vars:
                        pattern = rf"{self.args.jinja_vars[var]}"
                        new_pattern = rf"{{{{@ {var} @}}}}"
                        modified_contents = re.sub(pattern, new_pattern, modified_contents)

                    file.seek(0)
                    file.write(modified_contents)
                    file.truncate()
                    file.close()

    def get_zip_config(self, remove_zip=True):
        """Create a ZIP file from the local content on disk and return a bytes object
        If "remove_zip" is False, we dump it to disk as "sesam-config.zip" as well.
        """
        if os.path.isfile("sesam-config.zip"):
            os.remove("sesam-config.zip")

        zip_file = ZipFile("sesam-config.zip", "w", ZIP_DEFLATED)

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
        zin = ZipFile(BytesIO(zip_data))

        for item in zin.infolist():
            if item.filename == filename:
                return zin.read(item.filename)

        zin.close()
        return None

    def replace_file_in_zipfile(self, zip_data, filename, replacement):
        zin = ZipFile(BytesIO(zip_data))
        buffer = BytesIO()
        zout = ZipFile(buffer, mode="w")

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
        return remove_task_manager_settings_impl(self, zip_data)

    def get_node_and_jwt_token(self, args):
        try:
            node_url, jwt_token = sesamclient.utils.get_node_and_jwt_token(
                node_url=args.node, jwt_token=args.jwt, config_filename=args.sync_config_file
            )

            self.node_url = node_url
            self.jwt_token = jwt_token
            return node_url, jwt_token

        except BaseException as e:
            logger.error("Failed to find node url and/or jwt token")
            raise e

    def format_zip_config(self, zip_data, binary=False):
        zip_config = ZipFile(BytesIO(zip_data))
        buffer = BytesIO()
        zout = ZipFile(buffer, mode="w")

        for item in zip_config.infolist():
            formatted_item = format_json(json.load(zip_config.open(item.filename)))
            zout.writestr(item, formatted_item)

        zout.close()

        buffer.seek(0)
        return buffer.read()

    def set_authconfig_credentials(self, *args):
        try:
            auth_credentials = self.read_config_file(".authconfig")
            for arg in args:
                token = auth_credentials[arg]
                if token.startswith('"') and token.endswith('"'):
                    setattr(self.args, arg, token[1:-1])
                else:
                    setattr(self.args, arg, token)
            logger.info("Found authentication credentials in .authconfig file.")
        except KeyError:
            logger.warning("Could not find %s in .authconfig file. Checking the arguments." % arg)

    def authenticate(self):
        execute_authenticate(self, parser=parser, cli_args=args)

    def check_template_sink(self):
        """
        Checks the pipe templates for `sink.dataset`, if it has a value we exit with a
        warning. (See task IS-14872)

        Returns:
            Boolean: True/False
        """
        connector_dir = os.getcwd()
        with open(f"{connector_dir}/manifest.json", "r") as mf:
            manifest = json.load(mf)
            template_files = [
                manifest.get("datatypes").get(tf).get("template")
                for tf in manifest.get("datatypes")
            ]

        files_with_warnings = list()
        for template_file in template_files:
            with open(f"{connector_dir}/{template_file}", "r") as tf:
                template = json.load(tf)

            if type(template) is not list:
                continue

            for obj in template:
                if obj.get("type") == "pipe" and obj.get("sink", {}).get("dataset"):
                    files_with_warnings.append(
                        f"[!] Sink.dataset should not have a value in {template_file}. "
                        "Please remove the value to continue."
                    )

        if len(files_with_warnings) > 0:
            for warning in files_with_warnings:
                logger.error(warning)
            return False

        return True

    def validate(self):
        execute_validate(self)

    def upload(self):
        execute_upload(self, UploadException, TestDataUploadException)

    def get_testdata_jobs(self):
        for root, _, files in os.walk("testdata"):
            for filename in files:
                if not filename.lower().endswith(".json"):
                    continue
                pipe_id = os.path.splitext(filename)[0]
                if self.whitelisted_pipes and pipe_id not in self.whitelisted_pipes:
                    continue

                yield root, filename, pipe_id

    def upload_testdata(self, root, filename, pipe_id):
        file_path = os.path.join(root, filename)
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                entities_json = json.load(
                    f, parse_float=Decimal if self.args.do_float_as_decimal else float
                )

            if entities_json is None:
                return

            self.logger.info(f"Uploading entities for {pipe_id}")

            max_attempts = self.TESTDATA_UPLOAD_MAX_RETRIES + 1
            for attempt in range(1, max_attempts + 1):
                try:
                    self.wait_for_testdata_upload_slot()
                    self.sesam_node.pipe_receiver_post_request(pipe_id, json=entities_json)
                    return
                except BaseException as e:
                    if attempt == max_attempts or not self.is_retryable_testdata_upload_error(e):
                        raise e

                    retry_delay = self.get_testdata_upload_retry_delay(attempt)
                    self.logger.warning(
                        f"Transient failure while uploading entities for {pipe_id} "
                        f"(attempt {attempt}/{max_attempts}, retrying in {retry_delay:.1f}s): {e}"
                    )
                    time.sleep(retry_delay)
        except BaseException as e:
            response = getattr(e, "response", None)
            response_text = response.text if response and hasattr(response, "text") else "n/a"
            self.logger.error(
                f"Failed to post payload to pipe {pipe_id}. "
                f"{e}. Response from server was: {response_text}"
            )
            raise e
        return

    def get_testdata_upload_retry_delay(self, attempt):
        exponential_delay = self.TESTDATA_UPLOAD_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1))
        jitter = random.uniform(0, self.TESTDATA_UPLOAD_RETRY_BASE_DELAY_SECONDS)
        return exponential_delay + jitter

    def wait_for_testdata_upload_slot(self):
        if self.args.upload_rate <= 0:
            return

        min_interval = 1.0 / self.args.upload_rate
        while True:
            with self._testdata_upload_rate_lock:
                now = time.monotonic()
                if now >= self._testdata_next_upload_slot:
                    self._testdata_next_upload_slot = now + min_interval
                    return

                delay = self._testdata_next_upload_slot - now

            time.sleep(delay)

    def is_retryable_testdata_upload_error(self, error):
        response = getattr(error, "response", None)
        if response is not None:
            status_code = response.status_code
            if status_code in self.TESTDATA_UPLOAD_RETRYABLE_STATUS_CODES:
                return True
            if 400 <= status_code < 500:
                return False
            return status_code >= 500

        return isinstance(error, RequestException)

    def log_testdata_upload_progress(self, uploaded, failed, total):
        processed = uploaded + failed
        if (
            processed % self.TESTDATA_UPLOAD_PROGRESS_LOG_INTERVAL != 0
            and processed != total
        ):
            return

        self.logger.info(
            f"Test data upload progress: processed={processed}/{total}, "
            f"uploaded={uploaded}, failed={failed}, remaining={total - processed}"
        )

    def log_testdata_upload_summary(self, uploaded, failed, total, started_at):
        elapsed_time = time.monotonic() - started_at
        self.logger.info(
            f"Test data upload summary: total={total}, uploaded={uploaded}, "
            f"failed={failed}, elapsed_time={elapsed_time:.1f}s"
        )

    def dump(self):
        try:
            self.get_zip_config(remove_zip=False)
        except BaseException as e:
            logger.error("Failed to create zip archive of config")
            raise e

    def download(self):
        execute_download(self)

    def status(self):
        execute_status(self)

    def filter_entity(self, entity, test_spec):
        """Remove most underscore keys and filter potential blacklisted keys"""

        def filter_item(parent_path, item):
            result = deepcopy(item)
            if isinstance(item, dict):
                for key, value in item.items():
                    path = parent_path + [key]
                    if test_spec.is_path_blacklisted(path):
                        result.pop(key)
                    elif key.startswith("_"):
                        if key == "_id" or (key == "_deleted" and value is True):
                            continue
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
        return load_test_specs_impl(self, existing_output_pipes, update=update)

    def get_diff_string(self, a, b, a_filename, b_filename):
        a_lines = StringIO(a).readlines()
        b_lines = StringIO(b).readlines()

        return "".join(unified_diff(a_lines, b_lines, fromfile=a_filename, tofile=b_filename))

    def bytes_to_xml_string(self, xml_data):
        xml_declaration, standalone = self.find_xml_header_settings(xml_data)
        xml_doc_root = etree.fromstring(xml_data)

        try:
            result = str(
                etree.tostring(
                    xml_doc_root,
                    encoding="utf-8",
                    xml_declaration=xml_declaration,
                    standalone=standalone,
                    pretty_print=True,
                ),
                encoding="utf-8",
            )
        except UnicodeEncodeError:
            result = str(
                etree.tostring(
                    xml_doc_root,
                    encoding="latin-1",
                    xml_declaration=xml_declaration,
                    standalone=standalone,
                    pretty_print=True,
                ),
                encoding="latin-1",
            )

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
        execute_verify(self, base_dir=BASE_DIR, git_root=GIT_ROOT)

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
        # Get input entities from a live node and add these to the local pipe
        # configuration as test entities
        self.logger.info(f"Adding test entities to pipe '{pipe['_id']}'")
        node_pipe = self.sesam_node.get_pipe(pipe["_id"])
        if node_pipe is not None:
            dataset_id = node_pipe.config["effective"].get("sink", {}).get("dataset", node_pipe.id)
        else:
            self.logger.warning(
                f"Configuration for '{pipe['_id']}' was not found on the node. "
                f"Continuing without adding any test entities."
            )
            return pipe, 0

        try:
            dataset = self.sesam_node.api_connection.get_dataset(dataset_id)
            entities = list(
                dataset.get_entities(
                    history=False, deleted=False, limit=10, do_transit_decoding=False
                )
            )
        except BaseException as e:
            self.logger.warning(
                f"Unable to get entities from '{pipe['_id']}' due to an exception:\n{e}"
            )
            entities = []

        pipe["source"]["alternatives"]["test"]["entities"] = entities
        if len(entities) == 0:
            self.logger.info(
                f"No input entities were found for '{pipe['_id']}', "
                "so test entities will not be updated."
            )

        return pipe, len(entities)

    def add_test_alternative(self, pipe):
        logger.debug(f"Adding test alternative to pipe {pipe['_id']}")
        pipe["source"]["alternatives"]["test"] = {"type": "embedded", "entities": []}

        return pipe

    def add_conditional_source(self, pipe):
        logger.debug(f"Adding conditional source to pipe {pipe['_id']}")
        prod_source = pipe["source"]

        pipe["source"] = {
            "type": "conditional",
            "alternatives": {
                "prod": prod_source,
                "test": {"type": "embedded", "entities": []},
            },
            "condition": "$ENV(node-env)",
        }

        return pipe

    def init(self):
        execute_init(self)

    def connector_init(self):
        execute_connector_init(self)

    def get_datatype_template(self, datatype):
        return command_get_datatype_template(self.args, datatype)

    def add_datatype(self):
        execute_add_datatype(self)

    def update(self):
        execute_update(self)

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
            self.sesam_node.wait_for_all_pipes_to_deploy()

            for i in range(self.args.runs):
                last_additional_info = self.run()

            self.verify()
            self.logger.info("Test was successful!")
            if last_additional_info is not None:
                self.logger.info(last_additional_info)
        except BaseException as e:
            self.logger.error("Test failed!")
            raise e

    def run_pytest_tests(self, is_standalone_run=False):
        test_dir = self.args.pytest_tests_folder
        test_files = glob(os.path.join(test_dir, "test_*.py"))
        if not test_files:
            self.logger.warning(
                f"No test_*.py files were found in '{test_dir}', so no tests have been run."
            )
            return

        test_args_str = self.args.pytest_args
        pytest_args = [test_dir] + test_args_str.split()

        self.logger.info(
            f"Found {len(test_files)} test files in folder '{test_dir}', running "
            f"pytest with these options: {pytest_args}"
        )

        if is_standalone_run:
            self.logger.warning(
                "Note that some tests require that the most recent versions of the pipes have "
                "finished running in order to provide accurate results. Make sure that "
                "you've done an 'upload' and 'run' recently before running these tests."
            )

        result = pytest.main(pytest_args)

        if result.value == 0:
            self.logger.info("Ran unit tests successfully.")
        else:
            raise RuntimeError("One or more Python tests failed, see above output.")

    def run_internal_scheduler(self):
        return execute_run_internal_scheduler(self)

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

            # We need to include the "disable-user-pipes" setting when wiping
            # the pipes and systems, or they will
            # start running (asserting datasets, compiling dtl etc) while
            # we're doing the wipe, which makes it take a lot longer to run

            if os.path.isfile("node-metadata.conf.json"):
                with open("node-metadata.conf.json", "rt") as infile:
                    node_metadata = json.loads(infile.read())
            else:
                node_metadata = {"_id": "node", "type": "metadata"}

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
        execute_convert(args=self.args, logger=self.logger, dump_callback=self.dump)

    def format(self, option):
        execute_format(self, option)


class AzureFormatter(logging.Formatter):
    """Azure syntax log formatter to enrich build feedback"""

    error_format = "##vso[task.logissue type=error;]%(message)s"
    warning_format = "##vso[task.logissue type=warning;]%(message)s"
    debug_format = "##[debug]%(message)s"
    default_format = "%(message)s"

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


if __name__ == "__main__":
    parser = SesamParser(
        prog="sesam",
        # Need to rework this I think
        description="""
Commands:
  authenticate    Authenticates against the external service of the connector and updates secrets and environment variables (available only when working on a connector)
  wipe            Deletes all the pipes, systems, user datasets and environment variables in the node
  restart         Restarts the target node (typically used to release used resources if the environment is strained)
  reset           Deletes the entire node database and restarts the node (this is a more thorough version than "wipe" - requires the target node to be a designated developer node, contact support@sesam.io for help)
  init            Add conditional sources with testing and production alternatives to all input pipes in the local config.
  validate        Validate local config for proper formatting and internal consistency
  upload          Replace node config with local config. Also tries to upload testdata if 'testdata' folder present and updates secrets and environment variables when working on a connector (might ask for authentication).
  download        Replace local config with node config
  dump            Create a zip archive of the config and store it as 'sesam-config.zip'
  status          Compare node config with local config (requires external diff command)
  run             Run configuration until it stabilizes
  update          Store current output as expected output
  convert         Convert embedded sources in input pipes to http_endpoints and extract data into files
  verify          Compare output against expected output
  test            Upload, run and verify output
  stop            Stop any running schedulers (for example if the client was prematurely terminated or disconnected)
  update-schemas  Generate schemas for all datatypes (only works in connector development context)
  connector_init  Initialize a connector in the working directory with a sample manifest, template and system
  expand          Expand a connector without running other operations (upload or validate).
  run-pytest      Runs Python tests in the specified folder using the pytest framework. The folder must be placed on the same level as the pipes and systems.
  format          Formats pipes, systems, testdata, and expected files in the same way that the portal does, just offline now instead.
""",  # noqa: E501
        formatter_class=RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-version",
        dest="version",
        required=False,
        action="store_true",
        help="print version number",
    )

    parser.add_argument(
        "-v", dest="verbose", required=False, action="store_true", help="be verbose"
    )

    parser.add_argument(
        "-vv",
        dest="extra_verbose",
        required=False,
        action="store_true",
        help="be extra verbose",
    )

    parser.add_argument(
        "-vvv",
        dest="extra_extra_verbose",
        required=False,
        action="store_true",
        help="be extra extra verbose",
    )

    parser.add_argument(
        "-skip-tls-verification",
        dest="skip_tls_verification",
        required=False,
        action="store_true",
        help="skip verifying the TLS certificate",
    )

    parser.add_argument(
        "-sync-config-file",
        dest="sync_config_file",
        metavar="<string>",
        default=".syncconfig",
        type=str,
        help="sync config file to use, the default is " "'.syncconfig' in the current directory",
    )

    parser.add_argument(
        "-whitelist-file",
        dest="whitelist_file",
        metavar="<string>",
        type=str,
        help="whitelist file to use, the default is none",
    )

    parser.add_argument(
        "-dont-remove-scheduler",
        dest="dont_remove_scheduler",
        required=False,
        action="store_true",
        help="don't remove scheduler after failure (DEPRECATED)",
    )

    parser.add_argument(
        "-dump",
        dest="dump",
        required=False,
        help="dump zip content to disk",
        action="store_true",
    )

    parser.add_argument(
        "-print-scheduler-log",
        dest="print_scheduler_log",
        required=False,
        help="print scheduler log during run",
        action="store_true",
    )

    parser.add_argument(
        "-output-run-statistics",
        dest="output_run_statistics",
        required=False,
        help="output detailed pipe run statistics after scheduler run",
        action="store_true",
    )

    parser.add_argument(
        "-use-internal-scheduler",
        dest="use_internal_scheduler",
        required=False,
        help="use the built-in scheduler in sesam instead " "of a microservice (DEPRECATED)",
        action="store_true",
    )

    parser.add_argument(
        "-custom-scheduler",
        dest="custom_scheduler",
        required=False,
        help="by default a scheduler system will be added, enable this flag "
        "if you have configured a custom scheduler as part of the config (DEPRECATED)",
        action="store_true",
    )

    parser.add_argument(
        "-scheduler-image-tag",
        dest="scheduler_image_tag",
        required=False,
        help="the scheduler image tag to use (DEPRECATED)",
        type=str,
        metavar="<string>",
    )

    parser.add_argument(
        "-scheduler-mode",
        dest="scheduler_mode",
        required=False,
        help="the scheduler mode to use ('active' or 'poll') - the default is 'active'",
        type=str,
        metavar="<string>",
    )

    parser.add_argument(
        "-node", dest="node", metavar="<string>", required=False, help="service url"
    )
    parser.add_argument(
        "-scheduler-node",
        dest="scheduler_node",
        metavar="<string>",
        required=False,
        help="service url for scheduler",
    )
    parser.add_argument(
        "-jwt",
        dest="jwt",
        metavar="<string>",
        required=False,
        help="authorization token",
    )

    parser.add_argument(
        "-single",
        dest="single",
        required=False,
        metavar="<string>",
        help="update or verify just a single pipe",
    )

    parser.add_argument(
        "-no-large-int-bugs",
        dest="no_large_int_bugs",
        required=False,
        action="store_true",
        help="don't reproduce old large int bugs",
    )

    parser.add_argument(
        "-disable-user-pipes",
        dest="disable_user_pipes",
        required=False,
        action="store_true",
        help="turn off user pipe scheduling in the target node (DEPRECATED)",
    )

    parser.add_argument(
        "-enable-eager-ms",
        dest="enable_eager_ms",
        required=False,
        action="store_true",
        help="run all microservices even if they are not in use (note: multinode only)",
    )

    parser.add_argument(
        "-enable-user-pipes",
        dest="enable_user_pipes",
        required=False,
        action="store_true",
        help="turn on user pipe scheduling in the target node",
    )

    parser.add_argument(
        "-compact-execution-datasets",
        dest="compact_execution_datasets",
        required=False,
        action="store_true",
        help="compact all execution datasets when running scheduler",
    )

    parser.add_argument(
        "-disable-cpp-extensions",
        dest="disable_cpp_extensions",
        required=False,
        action="store_true",
        help="turns off cpp extensions which saves dtl compile time "
        "at the expense of possibly slower dtl exeution time",
    )

    parser.add_argument(
        "-unicode-encoding",
        dest="unicode_encoding",
        required=False,
        action="store_true",
        help="store the 'expected output' json files using unicode "
        "encoding ('\\uXXXX') - the default is UTF-8",
    )

    parser.add_argument(
        "-disable-json-html-escape",
        dest="disable_json_html_escape",
        required=False,
        action="store_true",
        help="turn off escaping of '<', '>' and '&' characters "
        "in 'expected output' json files"
        " including 'sesam format expected'",
    )

    parser.add_argument(
        "-upload-delete-sink-datasets",
        dest="upload_delete_sink_datasets",
        required=False,
        default=False,
        action="store_true",
        help="If specified with the 'upload' command, the 'upload' command will delete all "
        "existing sink datasets before uploading the new config. In some cases, this can be "
        "quicker than doing a 'sesam wipe' or 'sesam reset' command when running ci-tests. "
        "The downside is that there is a larger risk of data and/or config from previous "
        "tests influencing the new test-run.",
    )

    parser.add_argument(
        "-profile",
        dest="profile",
        metavar="<string>",
        default="test",
        required=False,
        help="env profile to use <profile>-env.json",
    )

    parser.add_argument(
        "-scheduler-id",
        dest="scheduler_id",
        metavar="<string>",
        required=False,
        help="system id for the scheduler system (DEPRECATED)",
    )

    parser.add_argument(
        "-scheduler-request-mode",
        dest="scheduler_request_mode",
        required=False,
        type=str,
        metavar="<string>",
        default="sync",
        help="run the scheduler in 'sync' or 'async' mode, long running "
        "tests should run in 'async' mode",
    )

    parser.add_argument(
        "-scheduler-zero-runs",
        dest="scheduler_zero_runs",
        default=2,
        metavar="<int>",
        type=int,
        required=False,
        help="the number of runs that has to yield zero " "changes for the scheduler to finish",
    )

    parser.add_argument(
        "-scheduler-max-runs",
        dest="scheduler_max_runs",
        default=100,
        metavar="<int>",
        type=int,
        required=False,
        help="maximum number of runs that scheduler can do "
        "to before exiting (internal scheduler only)",
    )

    parser.add_argument(
        "-scheduler-max-run-time",
        dest="scheduler_max_run_time",
        default=15 * 60,
        metavar="<int>",
        type=int,
        required=False,
        help="the maximum time the internal scheduler is allowed to use to finish "
        "(in seconds, internal scheduler only)",
    )

    parser.add_argument(
        "-scheduler-check-input-pipes",
        dest="scheduler_check_input_pipes",
        required=False,
        action="store_true",
        help="controls whether failing input pipes should make the scheduler run fail",
    )

    parser.add_argument(
        "-scheduler-dont-reset-pipes-or-delete-sink-datasets",
        dest="scheduler_dont_reset_pipes_or_delete_sink_datasets",
        required=False,
        default=False,
        action="store_true",
        help="controls whether the scheduler should reset any pipes or delete their sink-datasets",
    )

    parser.add_argument(
        "-restart-timeout",
        dest="restart_timeout",
        default=15 * 60,
        metavar="<int>",
        type=int,
        required=False,
        help="the maximum time to wait for the node to restart and become "
        "available again (in seconds). The default is 15 minutes. "
        "A value of 0 will skip the back-up-again verification.",
    )

    parser.add_argument(
        "-runs",
        dest="runs",
        type=int,
        metavar="<int>",
        required=False,
        default=1,
        help="number of test cycles to check for stability",
    )

    parser.add_argument(
        "-logformat",
        dest="logformat",
        type=str,
        metavar="<string>",
        required=False,
        default="short",
        help="output format (normal, log or azure)",
    )

    parser.add_argument(
        "-scheduler-poll-frequency",
        metavar="<int>",
        dest="scheduler_poll_frequency",
        type=int,
        required=False,
        default=5000,
        help="milliseconds between each poll while waiting for the scheduler",
    )

    parser.add_argument(
        "-sesamconfig-file",
        dest="sesamconfig_file",
        metavar="<string>",
        type=str,
        help="sesamconfig file to use, the default is "
        "'.sesamconfig.json' in the current directory",
    )

    parser.add_argument(
        "-diff",
        dest="diff",
        required=False,
        action="store_true",
        help="use with the status command to show the diff of the files",
    )

    parser.add_argument(
        "-add-test-entities",
        dest="add_test_entities",
        required=False,
        action="store_true",
        help="use with the init command to add test entities to input pipes",
    )

    parser.add_argument(
        "-force-add",
        dest="force_add",
        required=False,
        action="store_true",
        help="use with the '-add-test-entities' option to "
        "overwrite test entities that exist locally",
    )

    parser.add_argument(
        "command",
        metavar="command",
        nargs="*",
        help="a valid command from the list above",
    )

    parser.add_argument(
        "-force",
        dest="force",
        required=False,
        action="store_true",
        help="force the command to run (only for 'upload' and 'download' commands) "
        "for non-dev subscriptions",
    )

    parser.add_argument(
        "-run-pytest",
        dest="pytest_tests_folder",
        metavar="<string>",
        type=str,
        help="specifies a folder containing Python tests that sesam-py should run. These tests "
        "will run after the command (e.g. upload, run) has finished. Uses the pytest "
        "framework. The folder should be placed on the same level as 'pipes', 'systems' etc.",
    )

    parser.add_argument(
        "-pytest-args",
        dest="pytest_args",
        metavar="<string>",
        default="-rP -v",
        type=str,
        help="specify the options that sesam-py should use when running pytest. "
        "The arguments must be provided inside double quotes with each argument separated by a"
        ' space, e.g. -pytest-args="-vv -x"',
    )

    parser.add_argument(
        "-skip-auth",
        dest="skip_auth",
        required=False,
        action="store_true",
        help="skips the authentication step after upload command.",
    )

    parser.add_argument(
        "--system-placeholder",
        metavar="<string>",
        default="xxxxxx",
        type=str,
        help="Name of the system _id placeholder " "(available only when working on connectors)",
    )

    parser.add_argument(
        "-d",
        dest="connector_dir",
        metavar="<string>",
        default=".",
        type=str,
        help="Connector folder to work with " "(available only when working on connectors)",
    )

    parser.add_argument(
        "-e",
        dest="expanded_dir",
        metavar="<string>",
        default=".expanded",
        type=str,
        help="Directory to expand the config into " "(available only when working on connectors)",
    )

    parser.add_argument(
        "--client_id",
        metavar="<string>",
        type=str,
        help="OAuth2 client id (available only when working on connectors)",
    )

    parser.add_argument(
        "--client_secret",
        metavar="<string>",
        type=str,
        help="OAuth2 client secret (available only when working on connectors)",
    )

    parser.add_argument(
        "--account_id",
        metavar="<string>",
        type=str,
        help="OAuth2 account_id variable override " "(available only when working on connectors)",
    )

    parser.add_argument(
        "--ignore-refresh-token",
        dest="ignore_refresh_token",
        required=False,
        action="store_true",
        help="use with sesam upload/authenticate to ignore "
        "refresh tokens for systems that don't have them",
    )

    parser.add_argument(
        "--api_key",
        metavar="<string>",
        type=str,
        help="api_key secret (available only when working on connectors)",
    )

    parser.add_argument(
        "--service_url",
        metavar="<string>",
        type=str,
        help="url to service api (include /api) " "(available only when working on connectors)",
    )

    parser.add_argument(
        "--service_jwt",
        metavar="<string>",
        type=str,
        help="jwt token to the service api (available only when working on connectors)",
    )

    parser.add_argument(
        "--consumer_token",
        metavar="<string>",
        type=str,
        help="consumer token (available only when working on connectors)",
    )

    parser.add_argument(
        "--employee_token",
        metavar="<string>",
        type=str,
        help="employee token (available only when working on connectors)",
    )

    parser.add_argument(
        "--base_url",
        metavar="<string>",
        type=str,
        default="https://api.tripletex.io",
        help="override to use prod env (available only when working on connectors)",
    )

    parser.add_argument(
        "--days",
        metavar="<string>",
        type=int,
        default=10,
        help="number of days until the token should "
        "expire(available only when working on connectors)",
    )

    parser.add_argument(
        "--use-client-secret",
        dest="use_client_secret",
        required=False,
        action="store_true",
        help="use with sesam upload/authenticate to send add "
        "the client_secret parameter to the /authorize URL",
    )

    parser.add_argument(
        "--do-float-as-decimal",
        dest="do_float_as_decimal",
        required=False,
        action="store_true",
        help="use with sesam upload/test to maintain full precision "
        "of decimals instead of converting them to floats",
    )

    parser.add_argument(
        "--auth",
        metavar="<string>",
        type=str,
        default="oauth2",
        help="auth scheme (oauth2, api_key, jwt)",
    )

    parser.add_argument(
        "--datatype", metavar="<string>", type=str, help="datatype to add", nargs="?"
    )

    parser.add_argument(
        "--share",
        dest="share",
        required=False,
        action="store_true",
        help="set this flag to enable sharing",
    )

    parser.add_argument(
        "-single-thread-upload",
        dest="single_thread_upload",
        required=False,
        action="store_true",
        help="Makes the testdata section of the upload command use a single thread",
    )

    parser.add_argument(
        "--upload-workers",
        dest="upload_workers",
        required=False,
        type=int,
        metavar="<int>",
        default=SesamCmdClient.DEFAULT_TESTDATA_UPLOAD_WORKERS,
        help="maximum number of concurrent workers for testdata upload (default: 8)",
    )

    parser.add_argument(
        "--upload-rate",
        dest="upload_rate",
        required=False,
        type=float,
        metavar="<float>",
        default=SesamCmdClient.DEFAULT_TESTDATA_UPLOAD_RATE,
        help="max testdata upload request rate (requests/sec). 0 means unlimited",
    )

    try:
        args = parser.parse_args()
        args.is_connector = os.path.isfile(os.path.join(args.connector_dir, "manifest.json"))
    except SystemExit as e:
        sys.exit(e.code)
    except BaseException:
        sys.exit(1)

    if args.version:
        print("sesam version %s" % sesam_version)
        sys.exit(0)

    if args.upload_workers < 1:
        parser.error("--upload-workers must be an integer >= 1")
    if args.upload_rate < 0:
        parser.error("--upload-rate must be a number >= 0")

    if args.logformat == "log":
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
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
        format_string = "%(message)s"
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

    command_args = []
    if args.command:
        command = args.command[0] and args.command[0].lower()
        if len(args.command) > 1:
            command_args = args.command[1:]

    else:
        command = ""

    if (
        args.single_thread_upload
        and args.upload_workers != SesamCmdClient.DEFAULT_TESTDATA_UPLOAD_WORKERS
        and command in ["upload", "test"]
    ):
        logger.warning("--upload-workers is ignored when -single-thread-upload is set")

    if command not in VALID_COMMANDS:
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

    offline = command in ["validate", "format"]
    if not offline:
        try:
            node_url, jwt_token = sesam_cmd_client.get_node_and_jwt_token(args)
        except BaseException as e:
            if (
                args.verbose is True
                or args.extra_verbose is True
                or args.extra_extra_verbose is True
            ):
                logger.exception(e)
            logger.error(
                "jwt and node must be specified either as parameter, "
                "os env or in syncconfig file"
            )
            sys.exit(1)

        if not args.is_connector:
            args.jinja_vars = None
            try:
                if os.path.isfile(".jinja_vars"):
                    args.jinja_vars = sesam_cmd_client.parse_config_file(".jinja_vars")
                    if args.jinja_vars == {}:
                        logger.warning(
                            "No variables found in .jinja_vars file. " "Proceeding without it."
                        )
                    else:
                        logger.info("Found variables in .jinja_vars file: %s", args.jinja_vars)
            except BaseException:
                if (
                    args.verbose is True
                    or args.extra_verbose is True
                    or args.extra_extra_verbose is True
                ):
                    logger.error("Failed to parse .jinja_vars file. Proceeding without it.")

        try:
            sesam_cmd_client.sesam_node = SesamNode(
                node_url, jwt_token, logger, verify_ssl=args.skip_tls_verification is False
            )
        except BaseException as e:
            if (
                args.verbose is True
                or args.extra_verbose is True
                or args.extra_extra_verbose is True
            ):
                logger.exception(e)
            logger.error(
                "failed to connect to the sesam node using the url and jwt token "
                f"we were given:\n{node_url}\n{jwt_token}"
            )
            logger.error(
                "please verify the url and token is correct, and that there isn't "
                "any network issues (i.e. firewall, internet connection etc)"
            )
            sys.exit(1)

    start_time = time.monotonic()
    try:
        if (
            offline
            or sesam_cmd_client.sesam_node.api_connection.get_api_info()
            .get("status")
            .get("developer_mode")
            or (command in ALLOWED_NON_DEV_SUBSCRIPTION_COMMANDS and args.force)
        ):
            execute_command(command, command_args, args, sesam_cmd_client, logger)
        else:
            if command in ALLOWED_NON_DEV_SUBSCRIPTION_COMMANDS:
                error_text = "To override this check use -force flag."
            else:
                error_text = ""

            logger.error(
                "The targeted Sesam subscription is not a developer environment, "
                f"please contact support@sesam.io if this is unexpected. {error_text}"
            )
            sys.exit(1)
    except UploadException as e:
        logger.error(e)
        sys.exit(1)
    except TestDataUploadException as e:
        logger.error(e)
        sys.exit(1)
    except BaseException as e:
        logger.error("Sesam client failed!")
        if args.extra_verbose is True or args.extra_extra_verbose is True:
            logger.exception("Underlying exception was: %s" % str(e))

        sys.exit(1)
    finally:
        run_time = time.monotonic() - start_time
        logger.info("Total run time was %d seconds" % run_time)
