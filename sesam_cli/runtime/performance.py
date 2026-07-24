import json
import time
from contextlib import contextmanager


class CommandProfiler:
    def __init__(self, command):
        self.command = command
        self.command_started_at = time.monotonic()
        self.phases = []

    @contextmanager
    def phase(self, name):
        started_at = time.monotonic()
        try:
            yield
        finally:
            elapsed_seconds = time.monotonic() - started_at
            self.phases.append({"phase": name, "elapsed_seconds": round(elapsed_seconds, 6)})

    def summary(self):
        total_seconds = time.monotonic() - self.command_started_at
        return {
            "command": self.command,
            "total_elapsed_seconds": round(total_seconds, 6),
            "phases": self.phases,
        }

    def emit(self, logger, to_log=False, output_file=None):
        payload = self.summary()
        if to_log:
            logger.info("Command profile: %s", json.dumps(payload, sort_keys=True))
        if output_file:
            with open(output_file, "w", encoding="utf-8") as fp:
                json.dump(payload, fp, indent=2, sort_keys=True)
        return payload


@contextmanager
def profile_phase(client, phase_name):
    profiler = getattr(client, "command_profiler", None)
    if profiler is None:
        yield
        return

    with profiler.phase(phase_name):
        yield
