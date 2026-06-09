"""Two-tier record-count validation.

Tier 1: re-query ES `_count` up to N times with exp backoff. Handles cases
        where a refresh races the initial count read.
Tier 2: one full extract+CSV re-run via a caller-supplied callback.

If both tiers still disagree with the on-disk CSV row count, raise
`RecordCountMismatch` with the full attempt history.
"""

from __future__ import annotations

import logging
import random
from collections.abc import Callable
from typing import Any

from etl.config import RetryConfig
from etl.errors import RecordCountMismatch
from etl.extractor import expected_count
from etl.models import CsvResult

_log = logging.getLogger(__name__)


def validate_counts(expected: int, actual: int) -> None:
    if expected != actual:
        raise RecordCountMismatch(expected=expected, actual=actual,
                                  attempts=[(expected, actual)])


def validate_with_retry(
    *,
    es: Any,
    index: str,
    query: dict[str, Any],
    csv_result: CsvResult,
    retry_cfg: RetryConfig,
    on_full_reextract: Callable[[], CsvResult],
    sleeper: Callable[[float], None] | None = None,
    rng: random.Random | None = None,
    log_extra: dict[str, Any] | None = None,
) -> CsvResult:
    """Validate counts with the two-tier retry strategy.

    Returns the `CsvResult` corresponding to the file that ultimately matched
    (may be the re-extracted one).
    """
    import time as _time
    sleeper = sleeper or _time.sleep
    rng = rng or random.Random()
    attempts_log: list[tuple[int, int]] = []
    current = csv_result

    # Tier 1: re-query _count up to N times.
    for attempt in range(retry_cfg.max_attempts):
        es_count = expected_count(es, index, query)
        attempts_log.append((es_count, current.row_count))
        if es_count == current.row_count:
            return current
        if attempt == retry_cfg.max_attempts - 1:
            break
        delay = min(retry_cfg.backoff_cap, retry_cfg.backoff_base * (2 ** attempt))
        if retry_cfg.jitter > 0:
            delay *= 1.0 + rng.uniform(-retry_cfg.jitter, retry_cfg.jitter)
        _log.warning(
            "count mismatch attempt=%d/%d es_count=%d csv_rows=%d delay=%.3fs",
            attempt + 1, retry_cfg.max_attempts, es_count, current.row_count, delay,
            extra={**(log_extra or {}), "retry_attempt": attempt + 1,
                   "retry_delay_s": delay, "es_count": es_count,
                   "csv_rows": current.row_count},
        )
        sleeper(max(0.0, delay))

    # Tier 2: one full extract+CSV re-run.
    _log.warning(
        "count still mismatched after %d retries; running full re-extract",
        retry_cfg.max_attempts, extra=log_extra,
    )
    current = on_full_reextract()
    final_es_count = expected_count(es, index, query)
    attempts_log.append((final_es_count, current.row_count))
    if final_es_count != current.row_count:
        raise RecordCountMismatch(
            expected=final_es_count,
            actual=current.row_count,
            attempts=attempts_log,
        )
    return current
