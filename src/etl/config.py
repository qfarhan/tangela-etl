"""Environment-driven configuration.

All settings are loaded once at process start. Missing required values raise
`ConfigError` immediately — the daemon refuses to start with a bad config.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from etl.errors import ConfigError


def _get(key: str, default: str | None = None, *, required: bool = False) -> str:
    val = os.environ.get(key, default)
    if required and (val is None or val == ""):
        raise ConfigError(f"missing required env var: {key}")
    return val if val is not None else ""


def _get_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ConfigError(f"{key} must be an integer, got: {raw!r}") from e


def _get_float(key: str, default: float) -> float:
    raw = os.environ.get(key)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as e:
        raise ConfigError(f"{key} must be a float, got: {raw!r}") from e


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    control_topic: str
    group_id: str
    security_protocol: str | None = None
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None

    def confluent_config(self) -> dict[str, str]:
        cfg: dict[str, str] = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest",
        }
        if self.security_protocol:
            cfg["security.protocol"] = self.security_protocol
        if self.sasl_mechanism:
            cfg["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            cfg["sasl.username"] = self.sasl_username
        if self.sasl_password:
            cfg["sasl.password"] = self.sasl_password
        return cfg


@dataclass(frozen=True)
class EsConfig:
    hosts: list[str]
    username: str | None
    password: str | None
    api_key: str | None
    job_index: str


@dataclass(frozen=True)
class PaginationConfig:
    page_size: int
    pit_keep_alive: str


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 5
    backoff_base: float = 1.0
    backoff_cap: float = 30.0
    jitter: float = 0.25


@dataclass(frozen=True)
class SftpConfig:
    host: str
    port: int
    user: str
    key_path: Path
    remote_dir: str
    known_hosts: Path


@dataclass(frozen=True)
class Settings:
    kafka: KafkaConfig
    es: EsConfig
    pagination: PaginationConfig
    retry: RetryConfig
    sftp: SftpConfig
    csv_output_dir: Path
    log_level: str = "INFO"
    # Optional diagnostic: when set, each job tees its raw extracted hits to
    # `<raw_dump_dir>/<job_id>.ndjson`. Unset (default) disables the dump.
    raw_dump_dir: Path | None = None


def load_settings(*, dotenv_path: str | None = None) -> Settings:
    """Load and validate settings from environment (and optional .env file)."""
    load_dotenv(dotenv_path=dotenv_path, override=False)

    kafka = KafkaConfig(
        bootstrap_servers=_get("KAFKA_BOOTSTRAP_SERVERS", required=True),
        control_topic=_get("KAFKA_CONTROL_TOPIC", required=True),
        group_id=_get("KAFKA_GROUP_ID", required=True),
        security_protocol=_get("KAFKA_SECURITY_PROTOCOL") or None,
        sasl_mechanism=_get("KAFKA_SASL_MECHANISM") or None,
        sasl_username=_get("KAFKA_SASL_USERNAME") or None,
        sasl_password=_get("KAFKA_SASL_PASSWORD") or None,
    )

    hosts_raw = _get("ES_HOSTS", required=True)
    es = EsConfig(
        hosts=[h.strip() for h in hosts_raw.split(",") if h.strip()],
        username=_get("ES_USERNAME") or None,
        password=_get("ES_PASSWORD") or None,
        api_key=_get("ES_API_KEY") or None,
        job_index=_get("ES_JOB_INDEX", required=True),
    )

    pagination = PaginationConfig(
        page_size=_get_int("PAGE_SIZE", 1000),
        pit_keep_alive=_get("PIT_KEEP_ALIVE", "5m"),
    )

    retry = RetryConfig(
        max_attempts=_get_int("RETRY_MAX_ATTEMPTS", 5),
        backoff_base=_get_float("RETRY_BACKOFF_BASE", 1.0),
        backoff_cap=_get_float("RETRY_BACKOFF_CAP", 30.0),
        jitter=_get_float("RETRY_JITTER", 0.25),
    )

    sftp = SftpConfig(
        host=_get("SFTP_HOST", required=True),
        port=_get_int("SFTP_PORT", 22),
        user=_get("SFTP_USER", required=True),
        key_path=Path(_get("SFTP_KEY_PATH", required=True)),
        # Optional: the job document's `remote_filename` is the authoritative
        # remote path. `remote_dir` is reserved for callers that want to build
        # remote paths from a base dir; it is not required to start the daemon.
        remote_dir=_get("SFTP_REMOTE_DIR"),
        known_hosts=Path(_get("SFTP_KNOWN_HOSTS", required=True)),
    )

    return Settings(
        kafka=kafka,
        es=es,
        pagination=pagination,
        retry=retry,
        sftp=sftp,
        csv_output_dir=Path(_get("CSV_OUTPUT_DIR", "/tmp/etl-csv")),
        log_level=_get("LOG_LEVEL", "INFO").upper(),
        raw_dump_dir=Path(raw_dump) if (raw_dump := _get("ES_RAW_DUMP_DIR")) else None,
    )
