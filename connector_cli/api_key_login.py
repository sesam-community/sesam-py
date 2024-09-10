# generic login flow for connectors that uses an api_key (and JWT)
# for authorization (for instance Freshteam or webCRM)
import requests
from connector_cli.connectorpy import expand_connector_config


def login_via_api_key(sesam_node, args):
    system_id = args.system_placeholder
    api_key = args.api_key
    base_url = args.base_url
    systems = sesam_node.api_connection.get_systems()
    _, manifest = expand_connector_config(system_id)
    if base_url and system_id and api_key:
        is_failed = False
        try:
            secrets = {
                "api_key": api_key,
            }
            if manifest.get("auth_variant") == "jwt":
                login_url = manifest.get("jwt").get("login_url")
                jwt_header_key = manifest.get("jwt").get("jwt_header_key")
                header = {
                    jwt_header_key: api_key,
                }
                response = requests.post(login_url, headers=header)
                data = response.json()
                secrets["jwt_access_token"]=data["AccessToken"]
                secrets["jwt_refresh_token"] = data["RefreshToken"]

            for system in systems:
                system.put_secrets(secrets)
        except Exception as e:
            is_failed = True
            sesam_node.logger.error("Failed to put secrets: %s" % e)

        if not is_failed:
            sesam_node.logger.info(
                "All secrets and environment variables have been updated successfully, "
                "now go and do your development!"
            )
        else:
            sesam_node.logger.error(
                "Failed to update all secrets and environment variables. see the log "
                "for details."
            )
    else:
        sesam_node.logger.error(
            "Missing arguments, please provide all required arguments"
        )
