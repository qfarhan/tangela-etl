"""Exception hierarchy for the ETL pipeline.

All recoverable, job-scoped failures inherit from `EtlError` so the consumer
loop can catch them at the boundary and keep running. Anything not derived
from `EtlError` (e.g., import errors, programmer bugs) is allowed to crash
the process.
"""

from __future__ import annotations


class EtlError(Exception):
    """Base class for all ETL-pipeline errors."""


class ConfigError(EtlError):
    """Raised when required configuration is missing or invalid."""


class ControlMessageError(EtlError):
    """Raised when a Kafka control message can't be decoded or is missing fields."""


class JobSpecError(EtlError):
    """Raised when the ES-resident job document is missing or malformed."""


class ElasticsearchQueryError(EtlError):
    """Raised on ES request failures (count, search, scroll, PIT)."""


class TransformError(EtlError):
    """Raised when applying the JOLT spec to an ES hit fails.

    Carries the job_id, hit_id (if any), and the offending JOLT operation
    name so the failure can be diagnosed without re-reading the full doc.
    """

    def __init__(self, message: str, *, job_id: str, hit_id: str | None = None,
                 jolt_op: str | None = None) -> None:
        super().__init__(message)
        self.job_id = job_id
        self.hit_id = hit_id
        self.jolt_op = jolt_op


class CsvWriteError(EtlError):
    """Raised on local CSV / sidecar write failures."""


class RecordCountMismatch(EtlError):
    """Raised when ES `_count` and CSV row count disagree after all retries."""

    def __init__(self, expected: int, actual: int, attempts: list[tuple[int, int]]) -> None:
        super().__init__(
            f"record count mismatch: expected={expected} actual={actual} "
            f"attempts={attempts}"
        )
        self.expected = expected
        self.actual = actual
        self.attempts = attempts


class SftpUploadError(EtlError):
    """Raised when the sftp subprocess exits non-zero or times out."""


class RetryExhausted(EtlError):
    """Wraps the final exception after retry attempts are exhausted.

    Only raised when the retry decorator is invoked with wrap_final=True.
    The default behaviour is to re-raise the original exception unchanged.
    """

    def __init__(self, attempts: int, last_exc: BaseException) -> None:
        super().__init__(f"retry exhausted after {attempts} attempts: {last_exc!r}")
        self.attempts = attempts
        self.last_exc = last_exc
