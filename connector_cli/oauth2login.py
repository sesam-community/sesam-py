import hashlib
import json
import threading
from base64 import urlsafe_b64decode
from dataclasses import dataclass
from urllib.parse import urlencode

from flask import Flask, redirect, request

from connector_cli.auth_io import put_secrets_for_all_systems, request_json, update_env
from connector_cli.connectorpy import expand_connector_config
from connector_cli.superofficelogin import get_so_ticket

REDIRECT_URI = "http://localhost:5010/login_callback"

SUCCESS_MESSAGE = (
    "All secrets and environment variables have been updated successfully, "
    "now go and do your development!"
)
FAILED_MESSAGE = "Failed to update all secrets and environment variables. See the log for details."


@dataclass
class OAuthLoginState:
    sesam_node: object
    require_so_ticket: bool
    system_id: str
    client_id: str
    client_secret: str
    account_id_override: str
    service_url: str
    service_jwt: str
    base_url: str
    login_url: str
    token_url: str
    scopes: list
    optional_scopes: list
    use_client_secret: bool
    ignore_refresh_token: bool
    profile: str
    manifest: dict
    shutdown_event: threading.Event


def _decode_jwt_payload(jwt_token):
    if not jwt_token:
        raise RuntimeError("Missing id_token in oauth response")
    parts = jwt_token.split(".")
    if len(parts) != 3:
        raise RuntimeError("Invalid JWT format for id_token")
    payload = parts[1]
    padding = "=" * (-len(payload) % 4)
    decoded_payload = urlsafe_b64decode(f"{payload}{padding}")
    return json.loads(decoded_payload)


def _get_account_id_from_token(state, token_data):
    account_id = state.account_id_override or ""
    if account_id:
        return account_id

    oauth2_manifest = state.manifest.get("oauth2", {})
    tenant_id = oauth2_manifest.get("tenant_id_expression")
    identity_url = oauth2_manifest.get("identity_url")

    if isinstance(tenant_id, list) and len(tenant_id) > 1:
        account_data = _decode_jwt_payload(token_data.get("id_token"))
        return account_data.get(tenant_id[1], "")

    if isinstance(tenant_id, str) and tenant_id in token_data:
        return token_data.get(tenant_id, "")

    if identity_url and isinstance(tenant_id, str):
        identity_data = request_json("GET", f"{identity_url}{token_data.get('access_token')}")
        return identity_data.get(tenant_id, "")

    return ""


def _build_secrets(state, token_data):
    secrets = {
        "oauth_access_token": token_data["access_token"],
        "oauth_client_id": state.client_id,
        "oauth_client_secret": state.client_secret,
    }
    if not state.ignore_refresh_token:
        secrets["oauth_refresh_token"] = token_data["refresh_token"]
    if state.manifest.get("requires_service_api_access"):
        secrets["service_jwt"] = state.service_jwt
    if state.manifest.get("use_webhook_secret"):
        to_hash = state.service_url + "/" + state.system_id
        secrets["webhook_secret"] = hashlib.sha256(to_hash.encode("utf-8-sig")).hexdigest()[:12]
    return secrets


def _build_login_url(state):
    params = {
        "client_id": state.client_id,
        "scope": " ".join(state.scopes),
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
    }
    if state.optional_scopes:
        params["optional_scopes"] = " ".join(state.optional_scopes)
    if state.use_client_secret:
        params["client_secret"] = state.client_secret
    separator = "?" if "?" not in state.login_url else "&"
    return f"{state.login_url}{separator}{urlencode(params)}"


def process_login_callback(state, auth_code):
    if not auth_code:
        state.sesam_node.logger.error("Failed to get secrets: missing authorization code")
        return False, FAILED_MESSAGE

    token_data = request_json(
        "POST",
        state.token_url,
        data={
            "code": auth_code,
            "client_id": state.client_id,
            "client_secret": state.client_secret,
            "grant_type": "authorization_code",
            "redirect_uri": REDIRECT_URI,
        },
    )
    secrets = _build_secrets(state, token_data)
    account_id = _get_account_id_from_token(state, token_data)
    base_url = state.base_url

    if state.require_so_ticket:
        so_ticket, account_id, so_base_url = get_so_ticket(token_data, secrets)
        if not so_ticket or "so_ticket" not in so_ticket:
            raise RuntimeError("Failed to get so_ticket.")
        secrets["so_ticket"] = so_ticket["so_ticket"]
        if so_base_url:
            base_url = so_base_url

    env_updates = {"token_url": state.token_url, "base_url": base_url, "account_id": account_id}
    if state.manifest.get("requires_service_api_access"):
        env_updates["service_url"] = state.service_url

    put_secrets_for_all_systems(state.sesam_node, secrets)
    update_env(state.sesam_node, state.profile, env_updates)
    return True, SUCCESS_MESSAGE


def _build_state(sesam_node, args, require_so_ticket):
    _, manifest = expand_connector_config(args.system_placeholder)
    return OAuthLoginState(
        sesam_node=sesam_node,
        require_so_ticket=require_so_ticket,
        system_id=args.system_placeholder,
        client_id=args.client_id,
        client_secret=args.client_secret,
        account_id_override=args.account_id,
        service_url=args.service_url,
        service_jwt=args.service_jwt,
        base_url=args.base_url,
        login_url=args.login_url,
        token_url=args.token_url,
        scopes=args.scopes,
        optional_scopes=args.optional_scopes,
        use_client_secret=args.use_client_secret,
        ignore_refresh_token=args.ignore_refresh_token,
        profile=args.profile,
        manifest=manifest,
        shutdown_event=threading.Event(),
    )


def login_via_oauth(node, args, **kwargs):
    state = _build_state(node, args, kwargs.get("require_so_ticket", False))
    if not (
        state.system_id
        and state.client_id
        and state.client_secret
        and state.service_url
        and state.login_url
        and state.token_url
        and state.scopes
    ):
        state.sesam_node.logger.error("Missing arguments, please provide all required arguments")
        return

    login_url = _build_login_url(state)
    state.sesam_node.logger.info(
        "\nThis tool will add oauth2 system secrets and add token_url to the environment variables:"
        "\n  Service API: %s"
        "\n  System id: %s"
        "\n"
        "\nTo continue open the following link in your browser:"
        "\n  Link: %s"
        "\n\n",
        state.service_url,
        state.system_id,
        login_url,
    )

    app = Flask(__name__)

    @app.route("/")
    def index():
        return redirect(login_url)

    @app.route("/login_callback")
    def login_callback():
        try:
            is_success, message = process_login_callback(state, request.args.get("code"))
        except (RuntimeError, KeyError, TypeError, ValueError, AttributeError) as exc:
            is_success = False
            message = FAILED_MESSAGE
            state.sesam_node.logger.error("Failed to run oauth login callback: %s", exc)
        finally:
            state.shutdown_event.set()
            shutdown_server = request.environ.get("werkzeug.server.shutdown")
            if shutdown_server:
                shutdown_server()

        if is_success:
            state.sesam_node.logger.info(message)
        else:
            state.sesam_node.logger.error(message)
        return message

    def run_server():
        app.run(port=5010, use_reloader=False)

    server_thread = threading.Thread(target=run_server)
    server_thread.start()
    state.shutdown_event.wait()
    server_thread.join(timeout=5)
