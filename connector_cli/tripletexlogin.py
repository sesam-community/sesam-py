import hashlib
import json
import os
from datetime import timedelta, date
import requests
from connector_cli.connectorpy import expand_connector_config


# bespoke login flow for Tripletex

def login_via_tripletex(sesam_node, args):
    system_id = args.system_placeholder
    consumer_token = args.consumer_token
    employee_token = args.employee_token
    base_url = args.base_url
    profile = args.profile
    _, manifest = expand_connector_config(system_id)

    expiration = (date.today() + timedelta(days=args.days)).strftime("%Y-%m-%d")
    if system_id and consumer_token and employee_token and base_url:
        is_failed = False
        # get secrets
        secrets = {}
        token_url = ""
        try:
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

            if manifest.get("requires_service_api_access"):
                secrets["service_jwt"] = args.service_jwt
            if manifest.get("use_webhook_secret"):
                to_hash = args.service_url+"/"+system_id
                secrets["webhook_secret"] = hashlib.sha256(to_hash.encode('utf-8-sig')).hexdigest()[:12]
        except Exception as e:
            is_failed = True
            sesam_node.logger.error("Failed to get secrets: %s" % e)
        # put secrets
        try:
            system = sesam_node.get_system(system_id)
            system.put_secrets(secrets)
        except Exception as e:
            is_failed = True
            sesam_node.logger.error("Failed to put secrets: %s" % e)
        # get env
        env = {}
        try:
            profile_file = "%s-env.json" % profile
            env = sesam_node.get_env()
            if manifest.get("requires_service_api_access"):
                env["service_url"] = args.service_url
            if os.path.isfile(profile_file):
                with open(profile_file, "r", encoding="utf-8-sig") as f:
                    for key, value in json.load(f).items():
                        env[key] = value
            env["base_url"] = base_url
            env["token_url"] = token_url
        except Exception as e:
            is_failed = True
            sesam_node.logger.error("Failed to get env: %s" % e)
        # put env
        try:
            sesam_node.put_env(dict(env.items()))
        except Exception as e:
            is_failed = True
            sesam_node.logger.error("Failed to put env: %s" % e)

        if not is_failed:
            sesam_node.logger.info("All secrets and environment variables have been updated successfully, now go and do your development!")
        else:
            sesam_node.logger.error("Failed to update all secrets and environment variables. see the log for details.")
    else:
        sesam_node.logger.error("Missing arguments, please provide all required arguments")
