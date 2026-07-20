"""Generic login flow for connectors using an API key."""

from connector_cli.auth_io import put_secrets_for_all_systems, request_json
from connector_cli.connectorpy import expand_connector_config


def login_via_api_key(sesam_node, args):
    system_id = args.system_placeholder
    api_key = args.api_key
    base_url = args.base_url
    _, manifest = expand_connector_config(system_id)
    if not (base_url and system_id and api_key):
        sesam_node.logger.error("Missing arguments, please provide all required arguments")
        return

    secrets = {"api_key": api_key}
    try:
        if manifest.get("auth_variant") == "jwt":
            jwt_config = manifest.get("jwt", {})
            login_url = jwt_config.get("login_url")
            jwt_header_key = jwt_config.get("jwt_header_key")

            if not login_url or not jwt_header_key:
                raise RuntimeError("JWT auth variant requires jwt.login_url and jwt.jwt_header_key")

            data = request_json("POST", login_url, headers={jwt_header_key: api_key})
            secrets["jwt_access_token"] = data["AccessToken"]
            secrets["jwt_refresh_token"] = data["RefreshToken"]

        put_secrets_for_all_systems(sesam_node, secrets)
    except (KeyError, RuntimeError, TypeError, ValueError, AttributeError) as exc:
        sesam_node.logger.error("Failed to put secrets: %s", exc)
        sesam_node.logger.error(
            "Failed to update all secrets and environment variables. see the log for details."
        )
        return

    sesam_node.logger.info(
        "All secrets and environment variables have been updated successfully, "
        "now go and do your development!"
    )
