import json
import os
from glob import glob
from io import BytesIO
from zipfile import ZipFile

from connector_cli import connectorpy
from jsonformat import format_json
from sesam_cli.test_specs import normalize_path


def execute_download(client):
    if client.args.is_connector:
        if not os.path.isdir(os.path.join(client.args.connector_dir, client.args.expanded_dir)):
            client.logger.warning(
                "Expanded directory '%s' does not exist. creating the directory."
                % client.args.expanded_dir
            )
            os.makedirs(os.path.join(client.args.connector_dir, client.args.expanded_dir))
        os.chdir(os.path.join(client.args.connector_dir, client.args.expanded_dir))

    # Find env vars to download
    profile_file = "%s-env.json" % client.args.profile
    try:
        with open(profile_file, "w", encoding="utf-8-sig") as fp:
            fp.write(format_json(client.sesam_node.get_env()))
    except BaseException as e:
        client.logger.error("Failed to save profile file  '%s'" % profile_file)
        raise e

    if client.args.dump:
        if os.path.isfile("sesam-config.zip"):
            os.remove("sesam-config.zip")

        zip_data = client.sesam_node.get_config(binary=True)
        zip_data = client.remove_task_manager_settings(zip_data)

        # normalize formatting
        formatted_zip_data = client.format_zip_config(zip_data, binary=True)
        with open("sesam-config.zip", "wb") as fp:
            fp.write(formatted_zip_data)

        client.logger.info("Dumped downloaded config to 'sesam-config.zip'")
    else:
        zip_data = client.sesam_node.get_config(binary=True)
        zip_data = client.remove_task_manager_settings(zip_data)

    try:
        # Remove all previous pipes and systems
        for filename in glob("pipes%s*.conf.json" % os.sep):
            # Don't delete non-whitelisted config files
            if (
                client.whitelisted_files
                and normalize_path(filename) not in client.whitelisted_files
            ):
                continue

            client.logger.debug("Deleting pipe config file '%s'" % filename)
            os.remove(filename)

        for filename in glob("systems%s*.conf.json" % os.sep):
            # Don't delete non-whitelisted config files
            if (
                client.whitelisted_files
                and normalize_path(filename) not in client.whitelisted_files
            ):
                continue

            client.logger.debug("Deleting system config file '%s'" % filename)
            os.remove(filename)

        # normalize formatting
        zip_data = client.format_zip_config(zip_data)
        zip_config = ZipFile(BytesIO(zip_data))
        zip_config.extractall()
        if not client.args.is_connector:
            if client.args.jinja_vars:
                if os.path.exists("pipes") and os.path.exists("systems"):
                    client.replace_template_variables("pipes")
                    client.replace_template_variables("systems")
                else:
                    client.logger.warning("No pipes or systems found in downloaded config")
            else:
                client.logger.info(
                    "No jinja variables found. Not replacing any variables in " "config files"
                )
    except BaseException as e:
        client.logger.error("Failed to unzip config file from Sesam to current directory")
        raise e

    zip_config.close()
    client.logger.info("Replaced local config successfully")

    curr_dir = os.getcwd()
    if client.args.is_connector and curr_dir.endswith(client.args.expanded_dir):
        os.chdir(os.pardir)
        connectorpy.collapse_connector(
            ".", client.args.system_placeholder, client.args.expanded_dir
        )


def execute_status(client):
    def log_and_get_diff_flag(file_content1, file_content2, file_name1, file_name2, log_diff=True):
        diff_found = False
        if file_content1 != file_content2:
            client.logger.info("File '%s' differs from Sesam!" % file_name1)

            diff = client.get_diff_string(file_content1, file_content2, file_name1, file_name2)
            if log_diff:
                client.logger.info("Diff:\n%s" % diff)

            diff_found = True
        return diff_found

    client.logger.error("Comparing local and node config...")

    local_config = ZipFile(BytesIO(client.get_zip_config()))
    if client.args.dump:
        zip_data = client.sesam_node.get_config(binary=True)
        zip_data = client.remove_task_manager_settings(zip_data)

        with open("sesam-config.zip", "wb") as fp:
            fp.write(zip_data)

        client.logger.info("Dumped downloaded config to 'sesam-config.zip'")
    else:
        remote_config = client.sesam_node.get_config(binary=True)
        zip_data = client.remove_task_manager_settings(remote_config)

    remote_config = ZipFile(BytesIO(zip_data))

    remote_files = sorted(remote_config.namelist())
    local_files = sorted(local_config.namelist())

    diff_found = False
    # compare profile_file content with the variables
    profile_file = "%s-env.json" % client.args.profile
    try:
        with open(profile_file, "r", encoding="utf-8-sig") as local_env_file:
            local_file_data = format_json(json.load(local_env_file))
        remote_file_data = format_json(client.sesam_node.get_env())

        diff_found = (
            log_and_get_diff_flag(
                local_file_data, remote_file_data, profile_file, profile_file, client.args.diff
            )
            or diff_found
        )
    except FileNotFoundError:
        client.logger.error("Cannot locate profile file '%s'" % profile_file)

    for remote_file in remote_files:
        if remote_file not in local_files:
            client.logger.info("Sesam file '%s' was not found locally" % remote_file)
            diff_found = True

    for local_file in local_files:
        if local_file not in remote_files:
            client.logger.info("Local file '%s' was not found in Sesam" % local_file)
            diff_found = True
        else:
            local_file_data = str(local_config.read(local_file), encoding="utf-8")
            remote_file_data = format_json(json.load(remote_config.open(local_file)))

            diff_found = (
                log_and_get_diff_flag(
                    local_file_data, remote_file_data, local_file, local_file, client.args.diff
                )
                or diff_found
            )

    if diff_found:
        client.logger.info("Sesam config is NOT in sync with local config!")
    else:
        client.logger.info("Sesam config is up-to-date with local config!")
