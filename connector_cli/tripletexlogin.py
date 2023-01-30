import argparse
import json
import sys
from datetime import timedelta, date
import base64

import requests

# bespoke login flow for Tripletex

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--system-placeholder", metavar="<string>",
                            default="xxxxxx", type=str, help="Name of the system _id placeholder")
    parser.add_argument("--consumer_token", metavar="<string>",
                        type=str, help="consumer token")
    parser.add_argument("--employee_token", metavar="<string>",
                        type=str, help="employee token")
    parser.add_argument("--base_url", metavar="<string>",
                        type=str, default="https://api.tripletex.io", help="override to use prod env")
    parser.add_argument("--days", metavar="<string>",
                        type=int, default=10, help="number of days until the token should expire")
    parser.add_argument("--service_url", metavar="<string>",
                        type=str, help="url to service api (include /api)")
    parser.add_argument("--service_jwt", metavar="<string>",
                        type=str, help="jwt token to the service api")
    parser.add_argument("--connector_manifest", metavar="<string>",
                        default="manifest.json", type=argparse.FileType('r'), help="which connector manifest to use, needs to include oauth2.login_url, oauth2.token_url and oauth2.scopes")

    args = parser.parse_args()

    system_placeholder = args.system_placeholder
    consumer_token = args.consumer_token
    employee_token = args.employee_token
    service_url = args.service_url
    service_jwt = args.service_jwt
    connector_manifest = json.load(args.connector_manifest)
    base_url = args.base_url

    expiration = (date.today() + timedelta(days=args.days)).strftime("%Y-%m-%d")
    if system_placeholder and consumer_token and employee_token and service_url and service_jwt and base_url:
        params = {
            "consumerToken": consumer_token,
            "employeeToken": employee_token,
            "expirationDate": expiration,
        }
        token_url = base_url + "/v2/token/session/:create"
        resp = requests.put(token_url, params=params)
        data = resp.json()

        secrets = {
            "sessionToken": data["value"]["token"],
        }
        # post secrets
        for secret, value in secrets.items():
            requests.post(service_url + "/systems/%s/secrets" % system_placeholder, headers={"Authorization": "Bearer %s" % service_jwt}, json={secret: value})
            print("Updated secret: %s" % secret)

        # update env
        env = requests.get(service_url + "/env", headers={"Authorization": "Bearer %s" % service_jwt}).json()
        env["base_url"] = base_url
        requests.put(service_url + "/env", headers={"Authorization": "Bearer %s" % service_jwt}, json=env)
        print("Updated environment variables")
        print("Secrets and env has been updated, now go and do your development!")
    else:
        parser.print_usage()
        sys.exit(1)


