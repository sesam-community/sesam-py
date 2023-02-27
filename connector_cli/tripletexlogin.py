import json
import os
from datetime import timedelta, date
import requests


# bespoke login flow for Tripletex

def login_via_tripletex(sesam_node, args):
    system_id = args.system_placeholder
    consumer_token = args.consumer_token
    employee_token = args.employee_token
    base_url = args.base_url

    expiration = (date.today() + timedelta(days=args.days)).strftime("%Y-%m-%d")
    if system_id and consumer_token and employee_token and base_url:
        # get secrets
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
        # put secrets
        system = sesam_node.get_system(system_id)
        system.put_secrets(secrets)
        # get env
        profile_file = "%s-env.json" % args.profile
        env = sesam_node.get_env()
        if os.path.isfile(os.path.join(args.connector_dir, profile_file)):
            with open(os.path.join(args.connector_dir, profile_file), "r", encoding="utf-8-sig") as f:
                for key, value in json.load(f).items():
                    env[key] = value
        env["base_url"] = base_url
        env["token_url"] = token_url
        # put env
        sesam_node.put_env(dict(env.items()))
        print("All secrets and environment variables have been updated successfully, now go and do your development!")
    else:
        print("Missing arguments, please provide all required arguments")
