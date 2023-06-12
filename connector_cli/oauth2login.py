import hashlib
import json
import os
import os.path
import signal
import subprocess
import threading
from base64 import urlsafe_b64decode
from urllib.parse import urlencode

import requests
from flask import Flask, g, redirect, request

from connector_cli.connectorpy import expand_connector_config

redirect_uri = "http://localhost:5010/login_callback"

event = threading.Event()
app = Flask(__name__)


def wait_on_server_shutdown():
    event.wait()
    cmd = "lsof -i tcp:5010"
    result = subprocess.run(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    lines = result.stdout.decode().split("\n")
    header = lines[0]
    pid_index = header.split().index("PID")
    pids = [line.split()[pid_index] for line in lines[1:] if line]

    for pid in pids:
        os.kill(int(pid), signal.SIGKILL)


@app.teardown_appcontext
def teardown_appcontext(exception=None):
    if hasattr(g, "shutdown_server"):
        event.set()


@app.route("/")
def index():
    return redirect(login_url)


def get_account_id_from_jwt(jwt_token):
    """
    Decode the JWT and retrieve the account_id

    Params:
        jwt_token - str: JSON Web Token
    returns:
        account_id - str: Account ID/name (sometimes used in requests)
    """

    _, payload, _ = jwt_token.split(".")
    account_info = json.loads(urlsafe_b64decode(f"{payload}"))

    account_id = account_info.get(
        manifest.get("oauth2", {}).get("tenant_id_expression")[1]
    )

    return account_id


@app.route("/login_callback")
def login_callback():
    is_failed = False
    # get secrets
    secrets = {}
    account_id = ""
    try:
        the_data = {
            "code": request.args.get("code"),
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        }

        resp = requests.post(token_url, data=the_data)
        data = resp.json()
        secrets = {
            "oauth_access_token": data["access_token"],
            "oauth_refresh_token": data["refresh_token"],
            "oauth_client_id": client_id,
            "oauth_client_secret": client_secret,
        }

        identity_url = manifest.get("oauth2", {}).get("identity_url")
        tenant_id = manifest.get("oauth2", {}).get("tenant_id_expression")

        if type(tenant_id) is list:
            account_id = get_account_id_from_jwt(data.get("id_token"))
        elif tenant_id in data:
            account_id = data.get(tenant_id)
        elif identity_url:
            account_id = (
                requests.get(f"{identity_url}{data.get('access_token')}")
                .json()
                .get(tenant_id)
            )

        if manifest.get("requires_service_api_access"):
            secrets["service_jwt"] = service_jwt
        if manifest.get("use_webhook_secret"):
            to_hash = service_url + "/" + system_id
            secrets["webhook_secret"] = hashlib.sha256(
                to_hash.encode("utf-8-sig")
            ).hexdigest()[:12]
    except Exception as e:
        is_failed = True
        sesam_node.logger.error("Failed to get secrets: %s" % e)
    # put secrets
    try:
        system = sesam_node.api_connection.get_system(system_id)
        system.put_secrets(secrets)
    except Exception as e:
        is_failed = True
        sesam_node.logger.error("Failed to put secrets: %s" % e)
    # get env
    env = {}
    try:
        env = sesam_node.get_env()
        if manifest.get("requires_service_api_access"):
            env["service_url"] = service_url
        if os.path.isfile(profile_file):
            with open(profile_file, "r", encoding="utf-8-sig") as f:
                for key, value in json.load(f).items():
                    env[key] = value
        env["token_url"] = token_url
        env["base_url"] = base_url
        env["account_id"] = account_id
    except Exception as e:
        is_failed = True
        sesam_node.logger.error("Failed to get env: %s" % e)
    # put env
    try:
        sesam_node.put_env(env)
    except Exception as e:
        is_failed = True
        sesam_node.logger.error("Failed to put env: %s" % e)
    g.shutdown_server = True
    if not is_failed:
        sesam_node.logger.info(
            "All secrets and environment variables have been updated successfully, "
            "now go and do your development!"
        )
        return (
            "All secrets and environment variables have been updated successfully, "
            "now go and do your development!"
        )
    else:
        sesam_node.logger.error(
            "Failed to update all secrets and environment variables. "
            "See the log for details."
        )
        return (
            "Failed to update all secrets and environment variables. "
            "See the log for details."
        )


def start_server(args):
    global system_id, client_id, client_secret, base_url, login_url
    global token_url, event, profile_file, manifest, service_url, service_jwt
    profile_file = "%s-env.json" % args.profile
    system_id = args.system_placeholder
    client_id = args.client_id
    client_secret = args.client_secret
    service_url = args.service_url
    service_jwt = args.service_jwt
    base_url = args.base_url
    login_url = args.login_url
    token_url = args.token_url
    scopes = args.scopes
    use_client_secret = args.use_client_secret
    _, manifest = expand_connector_config(system_id)
    if (
        system_id
        and client_id
        and client_secret
        and service_url
        and login_url
        and token_url
        and scopes
    ):
        params = {
            "client_id": client_id,
            "scope": " ".join(scopes),
            "redirect_uri": redirect_uri,
            "response_type": "code",
        }

        if use_client_secret:
            params["client_secret"] = client_secret
            
        if not login_url.endswith("?"):
            login_url += "?"
        sesam_node.logger.info(
            "\nThis tool will add oauth2 system secrets and add token_url to the environment variables:"  # noqa: E501
            "\n  Service API: %s"
            "\n  System id: %s"
            "\n"
            "\nTo continue open the following link in your browser:"
            "\n  Link: %s"
            "\n\n" % (service_url, system_id, login_url + urlencode(params))
        )
        app.run(port=5010)


def login_via_oauth(node, args):
    global sesam_node
    sesam_node = node
    start_server_thread = threading.Thread(target=start_server, args=(args,))
    wait_on_server_shutdown_thread = threading.Thread(target=wait_on_server_shutdown)

    start_server_thread.start()
    wait_on_server_shutdown_thread.start()

    start_server_thread.join()
    wait_on_server_shutdown_thread.join()
