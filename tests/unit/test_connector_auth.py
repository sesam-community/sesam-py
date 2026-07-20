from types import SimpleNamespace

import pytest

from connector_cli import api_key_login, oauth2login, tripletexlogin


class _DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, message, *args):
        self.messages.append(("info", message % args if args else message))

    def error(self, message, *args):
        self.messages.append(("error", message % args if args else message))


class _DummySystem:
    def __init__(self):
        self.secrets = []

    def put_secrets(self, secrets):
        self.secrets.append(dict(secrets))


class _DummyAPIConnection:
    def __init__(self, systems):
        self._systems = systems

    def get_systems(self):
        return self._systems


class _DummyNode:
    def __init__(self):
        self.logger = _DummyLogger()
        self._system = _DummySystem()
        self.api_connection = _DummyAPIConnection([self._system])
        self._env = {"existing": "value"}
        self.saved_env = None

    def get_system(self, _system_id):
        return self._system

    def get_env(self):
        return dict(self._env)

    def put_env(self, env):
        self.saved_env = dict(env)


def _oauth_state(node):
    return oauth2login.OAuthLoginState(
        sesam_node=node,
        require_so_ticket=False,
        system_id="system-1",
        client_id="cid",
        client_secret="csecret",
        account_id_override="",
        service_url="https://service",
        service_jwt="jwt-token",
        base_url="https://api",
        login_url="https://login",
        token_url="https://token",
        scopes=["scope.a"],
        optional_scopes=[],
        use_client_secret=False,
        ignore_refresh_token=False,
        profile="test",
        manifest={"oauth2": {"tenant_id_expression": "tenant"}},
        shutdown_event=None,
    )


def test_api_key_login_jwt_happy_path(monkeypatch):
    node = _DummyNode()
    args = SimpleNamespace(system_placeholder="sys", api_key="key", base_url="https://base")

    monkeypatch.setattr(api_key_login, "expand_connector_config", lambda _sys: ([], {
        "auth_variant": "jwt",
        "jwt": {"login_url": "https://auth/login", "jwt_header_key": "X-Key"},
    }))
    monkeypatch.setattr(
        api_key_login,
        "request_json",
        lambda *_, **__: {"AccessToken": "access", "RefreshToken": "refresh"},
    )

    api_key_login.login_via_api_key(node, args)

    assert node._system.secrets[-1]["api_key"] == "key"
    assert node._system.secrets[-1]["jwt_access_token"] == "access"
    assert node._system.secrets[-1]["jwt_refresh_token"] == "refresh"


def test_tripletex_login_updates_secrets_and_env(tmp_path, monkeypatch):
    node = _DummyNode()
    args = SimpleNamespace(
        system_placeholder="tripletex-system",
        consumer_token="consumer",
        employee_token="employee",
        base_url="https://tripletex",
        profile="test",
        service_jwt="service-jwt",
        service_url="https://service",
    )

    monkeypatch.chdir(tmp_path)
    (tmp_path / "test-env.json").write_text('{"from_profile": "1"}', encoding="utf-8")
    monkeypatch.setattr(tripletexlogin, "expand_connector_config", lambda _sys: ([], {}))

    tripletexlogin.login_via_tripletex(node, args)

    assert node._system.secrets[-1]["consumer_token"] == "consumer"
    assert node.saved_env["from_profile"] == "1"
    assert node.saved_env["base_url"] == "https://tripletex"
    assert node.saved_env["token_url"].endswith("/v2/token/session/:create")


def test_oauth_process_callback_happy_path(monkeypatch):
    node = _DummyNode()
    state = _oauth_state(node)
    captured = {}

    monkeypatch.setattr(
        oauth2login,
        "request_json",
        lambda *_args, **_kwargs: {
            "access_token": "a-token",
            "refresh_token": "r-token",
            "tenant": "account-1",
        },
    )
    monkeypatch.setattr(
        oauth2login,
        "put_secrets_for_all_systems",
        lambda _node, secrets: captured.setdefault("secrets", secrets),
    )
    monkeypatch.setattr(
        oauth2login,
        "update_env",
        lambda _node, profile, updates: captured.setdefault("env", (profile, updates)),
    )

    success, message = oauth2login.process_login_callback(state, "auth-code")

    assert success is True
    assert message == oauth2login.SUCCESS_MESSAGE
    assert captured["secrets"]["oauth_access_token"] == "a-token"
    assert captured["env"][0] == "test"
    assert captured["env"][1]["account_id"] == "account-1"


def test_oauth_process_callback_missing_code():
    node = _DummyNode()
    state = _oauth_state(node)

    success, message = oauth2login.process_login_callback(state, None)

    assert success is False
    assert message == oauth2login.FAILED_MESSAGE
    assert ("error", "Failed to get secrets: missing authorization code") in node.logger.messages


def test_oauth_process_callback_http_error(monkeypatch):
    node = _DummyNode()
    state = _oauth_state(node)

    def _raise_runtime_error(*_args, **_kwargs):
        raise RuntimeError("upstream auth failed")

    monkeypatch.setattr(oauth2login, "request_json", _raise_runtime_error)

    with pytest.raises(RuntimeError):
        oauth2login.process_login_callback(state, "auth-code")


def test_oauth_process_callback_superoffice_ticket_happy_path(monkeypatch):
    node = _DummyNode()
    state = _oauth_state(node)
    state.require_so_ticket = True
    captured = {}

    monkeypatch.setattr(
        oauth2login,
        "request_json",
        lambda *_args, **_kwargs: {
            "access_token": "a-token",
            "refresh_token": "r-token",
            "tenant": "account-1",
        },
    )
    monkeypatch.setattr(
        oauth2login,
        "get_so_ticket",
        lambda *_args, **_kwargs: ({"so_ticket": "so-1"}, "so-account", "https://so.base"),
    )
    monkeypatch.setattr(
        oauth2login,
        "put_secrets_for_all_systems",
        lambda _node, secrets: captured.setdefault("secrets", secrets),
    )
    monkeypatch.setattr(
        oauth2login,
        "update_env",
        lambda _node, profile, updates: captured.setdefault("env", (profile, updates)),
    )

    success, message = oauth2login.process_login_callback(state, "auth-code")

    assert success is True
    assert message == oauth2login.SUCCESS_MESSAGE
    assert captured["secrets"]["so_ticket"] == "so-1"
    assert captured["env"][1]["account_id"] == "so-account"
    assert captured["env"][1]["base_url"] == "https://so.base"


def test_oauth_process_callback_superoffice_ticket_missing_ticket(monkeypatch):
    node = _DummyNode()
    state = _oauth_state(node)
    state.require_so_ticket = True

    monkeypatch.setattr(
        oauth2login,
        "request_json",
        lambda *_args, **_kwargs: {
            "access_token": "a-token",
            "refresh_token": "r-token",
            "tenant": "account-1",
        },
    )
    monkeypatch.setattr(
        oauth2login,
        "get_so_ticket",
        lambda *_args, **_kwargs: (None, None, None),
    )

    with pytest.raises(RuntimeError):
        oauth2login.process_login_callback(state, "auth-code")
