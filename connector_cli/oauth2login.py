import threading
import os
import signal
import subprocess
import json
import os.path
from flask import Flask, request, redirect, g
from urllib.parse import urlencode
import requests

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

    with open(".oauth.secrets", "w") as f:
        f.write("oauth_access_token=%s" % data["access_token"])
        f.write("\n")
        f.write("oauth_refresh_token=%s" % data["refresh_token"])


    # post secrets
    for secret, value in secrets.items():
        response = requests.post(service_url + "/systems/%s/secrets" % system_placeholder,
                                 headers={"Authorization": "Bearer %s" % service_jwt}, json={secret: value})
        if response.status_code==200:
            print("Updated secret: %s" % secret)
        else:
            print("Failed to update secret: %s" % secret)
            print(response.text)

    # update env

    env = requests.get(service_url + "/env", headers={"Authorization": "Bearer %s" % service_jwt}).json()
    if os.path.isfile(os.path.join(connector_dir,profile_file)):
        with open(os.path.join(connector_dir,profile_file), "r",encoding="utf-8-sig") as f:
            for key, value in json.load(f).items():
                env[key] = value
    env["token_url"]=token_url
    env["token_url"] = token_url
    response = requests.put(service_url + "/env", headers={"Authorization": "Bearer %s" % service_jwt}, json=env)
    print(response.status_code)
    if response.status_code==200:
        print("Updated environment variables")
        print("Secrets and env has been updated, now go and do your development!")
    else:
        print("Failed to update environment variables")
        print(response.text)
    g.shutdown_server = True
    return "Secrets and env has been updated, now go and do your development!"


def start_server(args):
    global system_placeholder, client_id, client_secret, service_url, service_jwt, login_url, token_url, scopes, event, profile_file, connector_dir
    connector_dir=args.connector_dir
    profile_file = "%s-env.json" % args.profile
    system_placeholder = args.system_placeholder
    client_id = args.client_id
    client_secret = args.client_secret
    service_url = args.service_url
    service_jwt = args.service_jwt
    with open(args.connector_manifest, "r") as f:
        connector_manifest = json.load(f)
    login_url = connector_manifest["oauth2"]["login_url"]
    token_url = connector_manifest["oauth2"]["token_url"]
    scopes = connector_manifest["oauth2"]["scopes"]

    if system_placeholder and client_id and client_secret and service_url and service_jwt and login_url and token_url and scopes:
        params = {
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": " ".join(scopes),
            "redirect_uri": redirect_uri,
            "response_type": "code",
        }
        if not login_url.endswith("?"):
            login_url += "?"
        print()
        print("This tool will add oauth2 system secrets and add token_url to the environment variables:")
        print("  Service API: %s" % service_url)
        print("  System id: %s" % system_placeholder)
        print()
        print("To continue open the following link in your browser:")
        print("  Link: %s" % login_url + urlencode(params))
        print("")
        print("")
        app.run(port=5010)


def login_via_oauth(args):
    start_server_thread = threading.Thread(target=start_server, args=(args,))
    wait_on_server_shutdown_thread = threading.Thread(target=wait_on_server_shutdown)

    start_server_thread.start()
    wait_on_server_shutdown_thread.start()

    start_server_thread.join()
    wait_on_server_shutdown_thread.join()