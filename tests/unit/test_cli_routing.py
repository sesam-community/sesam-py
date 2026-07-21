from types import SimpleNamespace

import pytest

from sesam_cli import cli


class _DummyLogger:
    def __init__(self):
        self.messages = []

    def warning(self, message, *args):
        self.messages.append(("warning", message % args if args else message))

    def error(self, message, *args):
        self.messages.append(("error", message % args if args else message))


class _DummyClient:
    def __init__(self):
        self.calls = []
        self.args = SimpleNamespace(pytest_tests_folder=None)
        self.sesam_node = object()

    def upload(self):
        self.calls.append("upload")

    def validate(self):
        self.calls.append("validate")

    def authenticate(self):
        self.calls.append("authenticate")

    def run_pytest_tests(self, is_standalone_run):
        self.calls.append(("run_pytest_tests", is_standalone_run, self.args.pytest_tests_folder))

    def format(self, target):
        self.calls.append(("format", target))


def _base_args(**overrides):
    values = {
        "is_connector": False,
        "connector_dir": ".",
        "expanded_dir": ".expanded",
        "system_placeholder": "system",
        "profile": "test",
        "skip_auth": False,
        "enable_user_pipes": False,
        "disable_cpp_extensions": False,
        "enable_eager_ms": False,
        "pytest_tests_folder": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_execute_command_run_pytest_requires_folder():
    logger = _DummyLogger()
    client = _DummyClient()
    args = _base_args()

    with pytest.raises(SystemExit):
        cli.execute_command("run-pytest", [], args, client, logger)


def test_execute_command_sets_pytest_folder_and_runs():
    logger = _DummyLogger()
    client = _DummyClient()
    args = _base_args()

    cli.execute_command("run-pytest", ["tests/unit"], args, client, logger)

    assert client.calls == [("run_pytest_tests", True, "tests/unit")]


def test_execute_command_triggers_followup_pytest_when_requested():
    logger = _DummyLogger()
    client = _DummyClient()
    args = _base_args(pytest_tests_folder="tests/fair_weather_test/tests")
    client.args = args

    cli.execute_command("upload", [], args, client, logger)

    assert client.calls == ["upload", ("run_pytest_tests", True, "tests/fair_weather_test/tests")]


def test_execute_command_format_defaults_to_all():
    logger = _DummyLogger()
    client = _DummyClient()
    args = _base_args()

    cli.execute_command("format", [], args, client, logger)

    assert client.calls == [("format", "all")]
