import json

from types import SimpleNamespace

from sesam_cli.runtime.performance import CommandProfiler, profile_phase


class _DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, message, *args):
        self.messages.append(message % args if args else message)


def test_command_profiler_records_phases():
    profiler = CommandProfiler("upload")
    with profiler.phase("phase-a"):
        pass
    with profiler.phase("phase-b"):
        pass

    summary = profiler.summary()
    phase_names = [phase["phase"] for phase in summary["phases"]]

    assert summary["command"] == "upload"
    assert "phase-a" in phase_names
    assert "phase-b" in phase_names
    assert summary["total_elapsed_seconds"] >= 0


def test_command_profiler_writes_json_output_file(tmp_path):
    logger = _DummyLogger()
    output_file = tmp_path / "profile.json"

    profiler = CommandProfiler("verify")
    with profiler.phase("execute_command"):
        pass

    payload = profiler.emit(logger, to_log=True, output_file=str(output_file))

    assert payload["command"] == "verify"
    assert output_file.exists()
    data = json.loads(output_file.read_text(encoding="utf-8"))
    assert data["command"] == "verify"
    assert any(message.startswith("Command profile: ") for message in logger.messages)


def test_profile_phase_is_noop_without_profiler():
    client = SimpleNamespace()
    with profile_phase(client, "phase-without-profiler"):
        value = 1
    assert value == 1
