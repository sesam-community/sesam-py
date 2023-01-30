import argparse
import json
import sys

from flask import Flask, request
from urllib.parse import urlencode
import requests

app = Flask(__name__)


# NOTE! This needs to be added to all application registrations if this tool should work
redirect_uri = "http://localhost:5010/login_callback"


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
    # post secrets
    for secret, value in secrets.items():
        requests.post(service_url + "/systems/%s/secrets" % system_placeholder, headers={"Authorization": "Bearer %s" % service_jwt}, json={secret: value})
        print("Updated secret: %s" % secret)

    # update env
    env = requests.get(service_url + "/env", headers={"Authorization": "Bearer %s" % service_jwt}).json()
    env["token_url"] = token_url
    requests.put(service_url + "/env", headers={"Authorization": "Bearer %s" % service_jwt}, json=env)
    print("Updated environment variables")
    return "Secrets and env has been updated, now go and do your development!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--system-placeholder", metavar="<string>",
                        default="xxxxxx", type=str, help="Name of the system _id placeholder")
    parser.add_argument("--client_id", metavar="<string>",
                        type=str, help="oauth client id")
    parser.add_argument("--client_secret", metavar="<string>",
                        type=str, help="oauth client secret")
    parser.add_argument("--service_url", metavar="<string>",
                        type=str, help="url to service api (include /api)")
    parser.add_argument("--service_jwt", metavar="<string>",
                        type=str, help="jwt token to the service api")
    parser.add_argument("--connector_manifest", metavar="<string>",
                        default="manifest.json", type=argparse.FileType('r'), help="which connector manifest to use, needs to include oauth2.login_url, oauth2.token_url and oauth2.scopes")

    args = parser.parse_args()

    system_placeholder = args.system_placeholder
    client_id = args.client_id
    client_secret = args.client_secret
    service_url = args.service_url
    service_jwt = args.service_jwt
    connector_manifest = json.load(args.connector_manifest)
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
        print("This tool will add oauth system secrets and add token_url to the environment variables:")
        print("  Service API: %s" % service_url)
        print("  System id: %s" % system_placeholder)
        print()
        print("To continue open the following link in your browser:")
        print("  Link: %s" % login_url + urlencode(params))
        print("")
        print("")
        app.run(port=5010)
    else:
        parser.print_usage()
        sys.exit(1)


