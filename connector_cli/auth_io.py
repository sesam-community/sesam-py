import json
import os

import requests

HTTP_TIMEOUT_SECONDS = 30


def request_json(method, url, **kwargs):
    timeout = kwargs.pop("timeout", HTTP_TIMEOUT_SECONDS)
    try:
        response = requests.request(method=method, url=url, timeout=timeout, **kwargs)
    except requests.RequestException as exc:
        raise RuntimeError(f"Request to {url} failed: {exc}") from exc

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise RuntimeError(
            f"Request to {url} failed with status {response.status_code}: {response.text}"
        ) from exc

    try:
        return response.json()
    except ValueError as exc:
        raise RuntimeError(f"Request to {url} did not return valid JSON") from exc


def put_secrets_for_all_systems(sesam_node, secrets):
    for system in sesam_node.api_connection.get_systems():
        system.put_secrets(secrets)


def put_secrets_for_system(sesam_node, system_id, secrets):
    sesam_node.get_system(system_id).put_secrets(secrets)


def merge_profile_env(base_env, profile):
    env = dict(base_env)
    profile_file = f"{profile}-env.json"
    if not os.path.isfile(profile_file):
        return env

    try:
        with open(profile_file, "r", encoding="utf-8-sig") as profile_handle:
            profile_env = json.load(profile_handle)
    except OSError as exc:
        raise RuntimeError(f"Failed to read profile env file {profile_file}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Profile env file {profile_file} contains invalid JSON: {exc}"
        ) from exc

    if not isinstance(profile_env, dict):
        raise RuntimeError(f"Profile env file {profile_file} must contain an object")

    env.update(profile_env)
    return env


def update_env(sesam_node, profile, updates):
    env = merge_profile_env(sesam_node.get_env(), profile)
    env.update(updates)
    sesam_node.put_env(env)
