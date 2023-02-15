import json
import os
from datetime import timedelta, date
import requests


# bespoke login flow for Tripletex

def login_via_tripletex(args):
    system_placeholder = args.system_placeholder
    consumer_token = args.consumer_token
    employee_token = args.employee_token
    service_url = args.service_url
    service_jwt = args.service_jwt
    with open(args.connector_manifest, "r") as f:
        connector_manifest = json.load(f)
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
            requests.post(service_url + "/systems/%s/secrets" % system_placeholder,
                          headers={"Authorization": "Bearer %s" % service_jwt}, json={secret: value})
            print("Updated secret: %s" % secret)

        # update env
        env = requests.get(service_url + "/env", headers={"Authorization": "Bearer %s" % service_jwt}).json()
        env["base_url"] = base_url
        if os.path.isfile(".additionalprops"):
            with open(".params", "r") as f:
                for line in f.readlines():
                    key, value = line.split("=")
                    env[key] = value.strip()

        requests.put(service_url + "/env", headers={"Authorization": "Bearer %s" % service_jwt}, json=env)
        print("Updated environment variables")
        print("Secrets and env has been updated, now go and do your development!")
    else:
        print("Missing arguments, please provide all required arguments")
