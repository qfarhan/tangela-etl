"""Shared pytest fixtures.

Note: every fixture here is a pure-Python fake. No real Kafka/ES/SFTP
clients are imported, so the test suite runs without network or Docker.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from etl.config import (
    EsConfig,
    KafkaConfig,
    PaginationConfig,
    RetryConfig,
    Settings,
    SftpConfig,
)
from etl.models import JobSpec


@pytest.fixture(autouse=True)
def _isolate_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep config tests hermetic.

    ``load_settings()`` calls ``dotenv.load_dotenv()``, which walks up from the
    current working directory and silently loads a developer's local ``.env``
    (e.g. the one created by ``cp .env.local .env`` during smoke testing). That
    repopulates vars a test deliberately removed and breaks isolation. Disable
    it for the whole suite so tests depend only on the env they set explicitly.
    """
    monkeypatch.setattr("etl.config.load_dotenv", lambda *a, **k: False)


@pytest.fixture
def retry_cfg_fast() -> RetryConfig:
    """Retry config with zero delays so unit tests don't sleep."""
    return RetryConfig(max_attempts=5, backoff_base=0.0, backoff_cap=0.0, jitter=0.0)


@pytest.fixture
def sample_job_spec() -> JobSpec:
    return JobSpec(
        job_id="job-1",
        data_index="my-data",
        query={"match_all": {}},
        column_paths={},
        columns=["id", "name", "value"],
        remote_filename="exports/job-1.csv",
    )


@pytest.fixture
def sample_hits() -> list[dict[str, Any]]:
    return [
        {"id": "a", "name": "alpha", "value": 1},
        {"id": "b", "name": "beta", "value": 2},
        {"id": "c", "name": "gamma", "value": 3},
    ]


@pytest.fixture
def tmp_csv_dir(tmp_path: Path) -> Path:
    d = tmp_path / "csv"
    d.mkdir()
    return d


@pytest.fixture
def settings(tmp_path: Path, tmp_csv_dir: Path, retry_cfg_fast: RetryConfig) -> Settings:
    return Settings(
        kafka=KafkaConfig(
            bootstrap_servers="localhost:9092",
            control_topic="ctl",
            group_id="g",
        ),
        es=EsConfig(
            hosts=["http://localhost:9200"],
            username=None,
            password=None,
            api_key=None,
            job_index="jobs",
        ),
        pagination=PaginationConfig(
            page_size=2,
            pit_keep_alive="1m",
        ),
        retry=retry_cfg_fast,
        sftp=SftpConfig(
            host="sftp.example.com",
            port=22,
            user="etl",
            key_path=tmp_path / "id",
            remote_dir="/incoming",
            known_hosts=tmp_path / "kh",
        ),
        csv_output_dir=tmp_csv_dir,
        log_level="DEBUG",
    )


class FakeMessage:
    def __init__(self, value: bytes, *, partition: int = 0, offset: int = 0,
                 err: Any = None) -> None:
        self._value = value
        self._partition = partition
        self._offset = offset
        self._err = err

    def value(self) -> bytes: return self._value
    def partition(self) -> int: return self._partition
    def offset(self) -> int: return self._offset
    def error(self) -> Any: return self._err


@pytest.fixture
def FakeKafkaMessage() -> type[FakeMessage]:
    return FakeMessage
