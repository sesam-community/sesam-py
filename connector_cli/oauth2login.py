import hashlib
import threading
import os
import signal
import subprocess
import json
import os.path
from flask import Flask, request, redirect, g
from urllib.parse import urlencode
import requests
from connector_cli.connectorpy import expand_connector_config

redirect_uri = "http://localhost:5010/login_callback"

event = threading.Event()
app = Flask(__name__)


def wait_on_server_shutdown():
    event.wait()
    cmd = "lsof -i tcp:5010"
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    lines = result.stdout.decode().split("\n")
    header = lines[0]
    pid_index = header.split().index("PID")
    pids = [line.split()[pid_index] for line in lines[1:] if line]

    for pid in pids:
        os.kill(int(pid), signal.SIGKILL)


@app.teardown_appcontext
def teardown_appcontext(exception=None):
    if hasattr(g, 'shutdown_server'):
        event.set()


@app.route("/")
def index():
    return redirect(login_url)


@app.route("/login_callback")
def login_callback():
    is_failed = False
    # get secrets
    secrets = {}
    try:
        the_data = {
            "code": request.args.get('code'),
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
        if manifest.get("requires_service_api_access"):
            secrets["service_api_access"] = service_jwt
        if manifest.get("use_webhook_secret"):
            to_hash = service_url + "/" + system_id
            secrets["webhook_secret"] = hashlib.sha256(to_hash.encode('utf-8-sig')).hexdigest()[:12]
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
        if os.path.isfile(os.path.join(connector_dir, profile_file)):
            with open(os.path.join(connector_dir, profile_file), "r", encoding="utf-8-sig") as f:
                for key, value in json.load(f).items():
                    env[key] = value
        env["token_url"] = token_url
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
            "All secrets and environment variables have been updated successfully, now go and do your development!")
        return "All secrets and environment variables have been updated successfully, now go and do your development!"
    else:
        sesam_node.logger.error("Failed to update all secrets and environment variables. see the log for details.")
        return "Failed to update all secrets and environment variables. see the log for details."


def start_server(args):
    global system_id, client_id, client_secret, login_url, token_url, event, profile_file, connector_dir,manifest,service_url,service_jwt
    connector_dir = args.connector_dir
    profile_file = "%s-env.json" % args.profile
    system_id = args.system_placeholder
    client_id = args.client_id
    client_secret = args.client_secret
    service_url = args.service_url
    service_jwt = args.service_jwt
    login_url = args.login_url
    token_url = args.token_url
    scopes = args.scopes
    _, manifest = expand_connector_config(connector_dir, system_id)
    if system_id and client_id and client_secret and service_url and login_url and token_url and scopes:
        params = {
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": " ".join(scopes),
            "redirect_uri": redirect_uri,
            "response_type": "code",
        }
        if not login_url.endswith("?"):
            login_url += "?"
        sesam_node.logger.info(
            "\nThis tool will add oauth2 system secrets and add token_url to the environment variables:"
            "\n  Service API: %s"
            "\n  System id: %s"
            "\n"
            "\nTo continue open the following link in your browser:"
            "\n  Link: %s"
            "\n\n" % (service_url, system_id, login_url + urlencode(params)))
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
