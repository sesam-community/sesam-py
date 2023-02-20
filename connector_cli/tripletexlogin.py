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
            response = requests.post(service_url + "/systems/%s/secrets" % system_placeholder,
                                     headers={"Authorization": "Bearer %s" % service_jwt}, json={secret: value})
            if response.status_code == 200:
                print("Updated secret: %s" % secret)
            else:
                print("Failed to update secret: %s" % secret)
                print(response.text)

        # update env
        profile_file = "%s-env.json" % args.profile
        env = requests.get(service_url + "/env", headers={"Authorization": "Bearer %s" % service_jwt}).json()
        if os.path.isfile(os.path.join(args.connector_dir, profile_file)):
            with open(os.path.join(args.connector_dir, profile_file), "r", encoding="utf-8-sig") as f:
                for key, value in json.load(f).items():
                    env[key] = value
        env["base_url"] = base_url
        env["token_url"] = token_url

        response = requests.put(service_url + "/env", headers={"Authorization": "Bearer %s" % service_jwt}, json=env)
        if response.status_code == 200:
            print("Updated environment variables")
            print("Secrets and env has been updated, now go and do your development!")
        else:
            print("Failed to update environment variables")
            print(response.text)
    else:
        print("Missing arguments, please provide all required arguments")
