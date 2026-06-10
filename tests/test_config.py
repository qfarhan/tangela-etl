from __future__ import annotations

import pytest

from etl.config import load_settings
from etl.errors import ConfigError

_BASE_ENV: dict[str, str] = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "KAFKA_CONTROL_TOPIC": "ctl",
    "KAFKA_GROUP_ID": "g",
    "ES_HOSTS": "http://localhost:9200,http://other:9200",
    "ES_JOB_INDEX": "jobs",
    "SFTP_HOST": "h",
    "SFTP_USER": "u",
    "SFTP_KEY_PATH": "/tmp/key",
    "SFTP_REMOTE_DIR": "/r",
    "SFTP_KNOWN_HOSTS": "/tmp/kh",
}


def _set_env(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> None:
    for key in list(env.keys()) + list(_BASE_ENV.keys()):
        monkeypatch.delenv(key, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)


def test_load_settings_with_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch, _BASE_ENV)
    s = load_settings()
    assert s.kafka.bootstrap_servers == "localhost:9092"
    assert s.es.hosts == ["http://localhost:9200", "http://other:9200"]
    assert s.pagination.page_size == 1000
    assert s.pagination.pit_keep_alive == "5m"
    assert s.retry.max_attempts == 5
    assert s.retry.backoff_base == 1.0
    assert s.sftp.port == 22


def test_load_settings_missing_required_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    env = dict(_BASE_ENV)
    env.pop("KAFKA_BOOTSTRAP_SERVERS")
    _set_env(monkeypatch, env)
    with pytest.raises(ConfigError, match="KAFKA_BOOTSTRAP_SERVERS"):
        load_settings()


def test_load_settings_retry_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    env = dict(
        _BASE_ENV,
        RETRY_MAX_ATTEMPTS="7",
        RETRY_BACKOFF_BASE="0.5",
        RETRY_BACKOFF_CAP="10",
        RETRY_JITTER="0.1",
    )
    _set_env(monkeypatch, env)
    s = load_settings()
    assert s.retry.max_attempts == 7
    assert s.retry.backoff_base == 0.5
    assert s.retry.backoff_cap == 10.0
    assert s.retry.jitter == 0.1


def test_load_settings_bad_integer(monkeypatch: pytest.MonkeyPatch) -> None:
    env = dict(_BASE_ENV, PAGE_SIZE="not-a-number")
    _set_env(monkeypatch, env)
    with pytest.raises(ConfigError, match="PAGE_SIZE"):
        load_settings()
