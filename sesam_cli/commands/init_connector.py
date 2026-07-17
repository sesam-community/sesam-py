import json
import os
import shutil
import sys
from glob import glob
from pathlib import Path

from jsonformat import format_json
from sesam_cli.connectors.datatype_templates import get_datatype_template


def execute_init(client):
    client.logger.info("Adding conditional sources to input pipes...")

    files = glob("pipes%s*.conf.json" % os.sep)

    # Conditional sources should not be added to dataset-type sources or embedded sources
    excluded_types = [
        "dataset",
        "merge",
        "merge_datasets",
        "union_datasets",
        "diff_datasets",
        "embedded",
    ]
    added_sources = 0
    added_entities = 0
    modified_sources = 0

    for cfg_path in files:
        new_cfg = None
        with open(cfg_path) as f:
            pipe = json.load(f)
            source_type = pipe["source"]["type"]

            # Check if pipe already has a conditional source and test alternative.
            if source_type == "conditional":
                if "test" not in pipe["source"]["alternatives"]:
                    new_cfg = client.add_test_alternative(pipe)
                    added_sources += 1

                if client.args.add_test_entities:
                    current_entities = pipe["source"]["alternatives"]["test"]["entities"]
                    if len(current_entities) == 0 or client.args.force_add:
                        new_cfg, num_added = client.test_entities_to_pipe(pipe)
                        added_entities += num_added
                        if num_added > 0:
                            modified_sources += 1
                    else:
                        client.logger.info(
                            f"Pipe {pipe['_id']} already has test entities. "
                            "Re-run with '-force-add' "
                            f"if you want to overwrite these entities."
                        )

            elif source_type not in excluded_types:
                new_cfg = client.add_conditional_source(pipe)
                if client.args.add_test_entities:
                    new_cfg, num_added = client.test_entities_to_pipe(pipe)
                    added_entities += num_added
                    if num_added > 0:
                        modified_sources += 1

                added_sources += 1

            if new_cfg is not None:
                with open(cfg_path, "w", encoding="utf-8") as pipe_file:
                    pipe_file.write(format_json(new_cfg))

    if added_sources > 0:
        client.logger.info("Successfully added test sources to %i pipes." % added_sources)
    else:
        client.logger.info(
            "All input pipes already have conditional sources "
            "with test alternatives. No test sources were added."
        )
    if modified_sources > 0:
        client.logger.info(
            "Successfully added a total of %i test entities to %i pipes."
            % (added_entities, modified_sources)
        )
    elif modified_sources + added_entities == 0:
        client.logger.info("No pipe configurations were modified.")

    if not client.args.is_connector and client.args.connector_dir != ".":
        with open(Path(client.args.connector_dir, "manifest.json"), "w") as f:
            json.dump({"datatypes": {}, "additional_parameters": {}}, f, indent=2, sort_keys=True)


def execute_connector_init(client):
    if client.args.connector_dir == ".":
        if not os.getcwd().split("/")[-1].endswith("-connector"):
            client.logger.error(
                "The current directory does not appear to be a valid "
                "directory. Please run this command from the root of the "
                "connector directory or make sure it follows the naming convention "
                "(<name>-connector)."
            )
            sys.exit(1)
        connector_name = os.getcwd().split("/")[-1].split("-connector")[0]
        root_dir = os.path.dirname(os.getcwd())
    else:
        if not client.args.connector_dir.endswith("-connector"):
            client.logger.error(
                "The connector directory does not appear to be a valid "
                "directory. Please make sure it follows the naming convention "
                "(<name>-connector)."
            )
            sys.exit(1)
        connector_name = client.args.connector_dir.split("-connector")[0]
        root_dir = os.getcwd()
    if not os.path.exists(Path(client.args.connector_dir, "manifest.json")):
        client.logger.info("manifest.json not found, initializing it...")

        templates_dir = os.path.join(client.args.connector_dir, "templates")
        if not os.path.exists(templates_dir):
            client.logger.info("templates directory not found, initializing it...")
            os.makedirs(templates_dir)

        manifest_obj = {
            "auth": client.args.auth,
            "datatypes": {},
            "additional_parameters": {},
            "system-template": "templates/system.json",
        }
        system_obj = {
            "_id": "{{@ system @}}",
            "operations": {},
            "type": "system:rest",
            "url_pattern": "",
            "verify_ssl": True,
        }
        if client.args.auth == "oauth2":
            manifest_obj["oauth2"] = {
                "login_url": "",
                "token_url": "",
                "scopes": [],
            }
            system_obj["oauth2"] = {
                "access_token": "$SECRET(oauth_access_token)",
                "client_id": "$SECRET(oauth_client_id)",
                "client_secret": "$SECRET(oauth_client_secret)",
                "refresh_token": "$SECRET(oauth_refresh_token)",
                "token_url": "{{@ token_url @}}",
            }

        if client.args.auth == "api_key":
            manifest_obj["auth"] = "api_key"
            system_obj["password"] = "$SECRET(api_key)"

        if client.args.auth == "jwt":
            manifest_obj["auth"] = "api_key"
            manifest_obj["auth_variant"] = "jwt"
            manifest_obj["jwt"] = {
                "jwt_header_key": "",
                "login_url": "",
                "refresh_url": "",
            }
            system_obj["jwt_access_token"] = "$SECRET(jwt_access_token)"

        readme_obj = (
            f"# A sesam connector for {connector_name}\n\n## Description\n\n"
            f"## Configuration\n\n## Datatypes\n\n## Notes\n\n## Environment "
            f"variables\n\n## Authentication"
        )

        shutil.copyfile(Path(root_dir, "LICENSE"), Path(client.args.connector_dir, "LICENSE"))
        with open(Path(client.args.connector_dir, "manifest.json"), "w") as f:
            json.dump(manifest_obj, f, indent=2, sort_keys=True)

        with open(Path(client.args.connector_dir, "templates", "system.json"), "w") as f:
            json.dump(system_obj, f, indent=2, sort_keys=True)

        with open(Path(client.args.connector_dir, "README.md"), "w") as f:
            f.write(readme_obj)
    else:
        client.logger.info("manifest.json found, skipping initialization...")


def execute_add_datatype(client):
    if len(client.args.command) <= 1:
        client.logger.error("Please provide at least one datatype.")
        sys.exit(1)

    command_args = client.args.command[1:]
    for datatype in command_args:
        datatype_template_obj, operations_obj = get_datatype_template(client.args, datatype)

        with open(f"{client.args.connector_dir}/manifest.json", "r") as f:
            manifest_obj = json.load(f)
            manifest_obj["datatypes"][datatype] = {"template": f"templates/{datatype}.json"}

        with open(f"{client.args.connector_dir}/manifest.json", "w") as f:
            json.dump(manifest_obj, f, indent=2, sort_keys=True)

        with open(f"{client.args.connector_dir}/templates/{datatype}.json", "w") as f:
            json.dump(datatype_template_obj, f, indent=2, sort_keys=True)

        with open(f"{client.args.connector_dir}/templates/system.json", "r") as f:
            system_obj = json.load(f)
            system_obj["operations"].update(operations_obj)

        with open(f"{client.args.connector_dir}/templates/system.json", "w") as f:
            json.dump(system_obj, f, indent=2, sort_keys=True)
