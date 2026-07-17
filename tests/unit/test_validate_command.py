from types import SimpleNamespace

import pytest

from sesam_cli.commands.validate import execute_validate


class _DummyClient:
    def __init__(self):
        self.args = SimpleNamespace(command="validate", connector_dir=".")
        self.logger = _DummyLogger()

    def check_template_sink(self):
        return True


class _DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def error(self, *args, **kwargs):
        return None


def test_validate_exits_when_expanded_folder_is_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    client = _DummyClient()
    with pytest.raises(SystemExit):
        execute_validate(client)
