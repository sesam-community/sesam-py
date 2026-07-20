import json
import os
import sys
from urllib.parse import urlparse

from connector_cli import api_key_login, oauth2login, tripletexlogin


def execute_authenticate(client, parser, cli_args):
    os.chdir(client.args.connector_dir)
    client.args.service_url = client.node_url
    client.args.service_jwt = client.jwt_token
    if os.path.isfile("manifest.json"):  # If manifest.json is in working directory
        client.args.connector_manifest = "manifest.json"
    elif os.path.exists(
        os.path.join(cli_args.connector_dir, "manifest.json")
    ):  # If manifest.json is in connector directory
        client.args.connector_manifest = os.path.join(cli_args.connector_dir, "manifest.json")
    else:  # If manifest.json is not found
        client.logger.error("Could not find manifest.json in connector directory")
        sys.exit(1)

    with open(cli_args.connector_manifest, "r") as f:
        connector_manifest = json.load(f)

    if (
        "auth_variant" in connector_manifest
        and connector_manifest["auth_variant"].lower() == "tripletex"
    ):
        if os.path.exists(".authconfig"):
            client.set_authconfig_credentials("consumer_token", "employee_token")
        else:
            client.args.consumer_token = cli_args.consumer_token
            client.args.employee_token = cli_args.employee_token
        if client.args.consumer_token is None or client.args.employee_token is None:
            client.logger.error(
                "Missing consumer_token and/or employee_token. Please provide them "
                "in .authconfig or as arguments."
            )
            sys.exit(1)
        client.args.base_url = cli_args.base_url
        tripletexlogin.login_via_tripletex(client.sesam_node, client.args)
    elif "auth" in connector_manifest and connector_manifest["auth"].lower() == "oauth2":
        client.args.login_url = connector_manifest["oauth2"]["login_url"]
        client.args.token_url = connector_manifest["oauth2"]["token_url"]
        client.args.scopes = connector_manifest["oauth2"]["scopes"]
        client.args.optional_scopes = connector_manifest["oauth2"].get("optional_scopes", [])
        client.args.base_url = (
            cli_args.base_url
            if cli_args.base_url != parser.get_default("base_url")
            else (
                f"{urlparse(client.args.token_url).scheme}://"
                f"{urlparse(client.args.token_url).netloc}"
            )
        )
        if os.path.exists(".authconfig"):
            client.set_authconfig_credentials("client_id", "client_secret")
            client.set_authconfig_credentials("account_id")
        else:
            client.args.client_id = cli_args.client_id
            client.args.client_secret = cli_args.client_secret
            client.args.account_id = cli_args.account_id

        if client.args.client_id is None or client.args.client_secret is None:
            client.logger.error(
                "Missing client_id and/or client_secret. Please provide them in "
                ".authconfig or as arguments."
            )
            sys.exit(1)
        if connector_manifest.get("auth_variant", "").lower() == "superoffice-ticket":
            oauth2login.login_via_oauth(client.sesam_node, client.args, require_so_ticket=True)
        else:
            oauth2login.login_via_oauth(client.sesam_node, client.args)

    elif "auth" in connector_manifest and connector_manifest["auth"].lower() == "api_key":
        if os.path.exists(".authconfig"):
            client.set_authconfig_credentials("api_key")
        else:
            client.args.api_key = cli_args.api_key
        api_key_login.login_via_api_key(client.sesam_node, client.args)
