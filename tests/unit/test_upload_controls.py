import threading
from decimal import Decimal
from types import SimpleNamespace

import sesam
from requests.exceptions import RequestException
from sesam import SesamCmdClient


class _DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, message, *args):
        self.messages.append(("info", message % args if args else message))


def _client_for_upload_methods():
    return SimpleNamespace(
        TESTDATA_UPLOAD_RETRY_BASE_DELAY_SECONDS=1.0,
        TESTDATA_UPLOAD_RETRYABLE_STATUS_CODES={429, 500, 502, 503, 504},
        TESTDATA_UPLOAD_PROGRESS_LOG_INTERVAL=25,
        args=SimpleNamespace(upload_rate=0.0),
        _testdata_upload_rate_lock=threading.Lock(),
        _testdata_next_upload_slot=0.0,
        logger=_DummyLogger(),
        whitelisted_pipes=None,
    )


def test_get_testdata_upload_retry_delay_is_exponential_with_jitter(monkeypatch):
    client = _client_for_upload_methods()
    monkeypatch.setattr(sesam.random, "uniform", lambda *_args, **_kwargs: 0.25)

    delay = SesamCmdClient.get_testdata_upload_retry_delay(client, attempt=3)

    assert delay == 4.25


def test_wait_for_testdata_upload_slot_sets_next_slot(monkeypatch):
    client = _client_for_upload_methods()
    client.args.upload_rate = 2.0
    monkeypatch.setattr(sesam.time, "monotonic", lambda: 10.0)

    SesamCmdClient.wait_for_testdata_upload_slot(client)

    assert client._testdata_next_upload_slot == 10.5


def test_is_retryable_testdata_upload_error_respects_status_codes():
    client = _client_for_upload_methods()

    class _ResponseError(Exception):
        def __init__(self, status_code):
            self.response = SimpleNamespace(status_code=status_code)

    assert SesamCmdClient.is_retryable_testdata_upload_error(client, _ResponseError(429)) is True
    assert SesamCmdClient.is_retryable_testdata_upload_error(client, _ResponseError(500)) is True
    assert SesamCmdClient.is_retryable_testdata_upload_error(client, _ResponseError(404)) is False
    assert (
        SesamCmdClient.is_retryable_testdata_upload_error(client, RequestException("boom")) is True
    )


def test_log_testdata_upload_progress_logs_on_interval_and_completion():
    client = _client_for_upload_methods()

    SesamCmdClient.log_testdata_upload_progress(client, uploaded=10, failed=5, total=30)
    SesamCmdClient.log_testdata_upload_progress(client, uploaded=20, failed=5, total=30)

    assert len(client.logger.messages) == 1
    assert "processed=25/30" in client.logger.messages[0][1]


def test_fix_decimal_to_ints_converts_integral_floats(monkeypatch):
    client = SimpleNamespace()
    monkeypatch.setattr(sesam, "args", SimpleNamespace(no_large_int_bugs=True), raising=False)
    client._fix_decimal_to_ints = lambda value: SesamCmdClient._fix_decimal_to_ints(client, value)

    result = SesamCmdClient._fix_decimal_to_ints(
        client,
        {"x": Decimal("5.0"), "nested": [4.0, Decimal("2.5")]},
    )

    assert result["x"] == 5
    assert result["nested"][0] == 4
    assert result["nested"][1] == Decimal("2.5")


def test_get_diff_string_returns_unified_diff():
    diff = SesamCmdClient.get_diff_string(SimpleNamespace(), "a\n", "b\n", "a.txt", "b.txt")
    assert "--- a.txt" in diff
    assert "+++ b.txt" in diff
    assert "-a" in diff
    assert "+b" in diff


def test_filter_entity_removes_internal_keys_and_blacklisted_paths():
    client = SimpleNamespace()

    class _Spec:
        @staticmethod
        def is_path_blacklisted(path):
            return ".".join(path) == "payload.secret"

    entity = {
        "_id": "1",
        "_internal": "remove",
        "payload": {"visible": "yes", "secret": "remove"},
        "items": [{"_deleted": True, "_ignored": 1, "value": 7}],
    }

    result = SesamCmdClient.filter_entity(client, entity, _Spec())

    assert "_internal" not in result
    assert "secret" not in result["payload"]
    assert result["items"][0]["_deleted"] is True
    assert "_ignored" not in result["items"][0]
