import json
from types import SimpleNamespace

import pytest

from sesam_cli.test_spec_loader import load_test_specs


class _DummyLogger:
    def __init__(self):
        self.messages = []

    def debug(self, message, *args):
        self.messages.append(("debug", message % args if args else message))

    def log(self, _level, message, *args):
        self.messages.append(("log", message % args if args else message))

    def warning(self, message, *args):
        self.messages.append(("warning", message % args if args else message))

    def info(self, message, *args):
        self.messages.append(("info", message % args if args else message))

    def error(self, message, *args):
        self.messages.append(("error", message % args if args else message))


class _Pipe:
    def __init__(self, pipe_id):
        self.id = pipe_id


def _dummy_client():
    return SimpleNamespace(
        logger=_DummyLogger(),
        loglevel_trace=2,
        whitelisted_pipes=None,
    )


def test_load_test_specs_fails_for_unknown_pipe(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    expected_dir = tmp_path / "expected"
    expected_dir.mkdir()
    (expected_dir / "known.test.json").write_text(json.dumps({"pipe": "unknown"}), encoding="utf-8")
    (expected_dir / "known.json").write_text("[]", encoding="utf-8")

    client = _dummy_client()

    with pytest.raises(RuntimeError):
        load_test_specs(client, existing_output_pipes={"known": _Pipe("known")}, update=False)


def test_load_test_specs_update_creates_missing_expected_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    expected_dir = tmp_path / "expected"
    expected_dir.mkdir()
    (expected_dir / "pipe-a.test.json").write_text("{}", encoding="utf-8")

    client = _dummy_client()
    specs = load_test_specs(client, existing_output_pipes={"pipe-a": _Pipe("pipe-a")}, update=True)

    assert "pipe-a" in specs
    assert (expected_dir / "pipe-a.json").exists()
    assert (expected_dir / "pipe-a.json").read_text(encoding="utf-8") == "[]\n"
