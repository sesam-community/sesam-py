import hashlib

from connector_cli.auth_io import put_secrets_for_system, update_env
from connector_cli.connectorpy import expand_connector_config

# bespoke login flow for Tripletex


def login_via_tripletex(sesam_node, args):
    system_id = args.system_placeholder
    consumer_token = args.consumer_token
    employee_token = args.employee_token
    base_url = args.base_url
    profile = args.profile
    _, manifest = expand_connector_config(system_id)

    if not (system_id and consumer_token and employee_token and base_url):
        sesam_node.logger.error("Missing arguments, please provide all required arguments")
        return

    token_url = base_url + "/v2/token/session/:create"
    secrets = {"consumer_token": consumer_token, "employee_token": employee_token}
    if manifest.get("requires_service_api_access"):
        secrets["service_jwt"] = args.service_jwt
    if manifest.get("use_webhook_secret"):
        to_hash = args.service_url + "/" + system_id
        secrets["webhook_secret"] = hashlib.sha256(to_hash.encode("utf-8-sig")).hexdigest()[:12]

    env_updates = {"base_url": base_url, "token_url": token_url}
    if manifest.get("requires_service_api_access"):
        env_updates["service_url"] = args.service_url

    try:
        put_secrets_for_system(sesam_node, system_id, secrets)
        update_env(sesam_node, profile, env_updates)
    except (RuntimeError, TypeError, ValueError, KeyError, AttributeError) as exc:
        sesam_node.logger.error("Failed to update Tripletex auth config: %s", exc)
        sesam_node.logger.error(
            "Failed to update all secrets and environment variables. see the log for details."
        )
        return

    sesam_node.logger.info(
        "All secrets and environment variables have been updated successfully, "
        "now go and do your development!"
    )
