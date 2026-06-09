from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from etl.config import RetryConfig
from etl.errors import RecordCountMismatch
from etl.models import CsvResult
from etl.validator import validate_counts, validate_with_retry


def _csv_result(rows: int) -> CsvResult:
    return CsvResult(
        csv_path=Path("/tmp/x.csv"),
        sidecar_path=Path("/tmp/x.csv.sha256"),
        row_count=rows,
        sha256_hex="deadbeef",
    )


def test_validate_counts_equal_ok() -> None:
    validate_counts(5, 5)


def test_validate_counts_unequal_raises() -> None:
    with pytest.raises(RecordCountMismatch):
        validate_counts(5, 4)


def test_validate_with_retry_first_count_matches_no_reextract(
    retry_cfg_fast: RetryConfig,
) -> None:
    es = MagicMock()
    es.count.return_value = {"count": 3}
    reextract = MagicMock(side_effect=AssertionError("should not be called"))

    res = validate_with_retry(
        es=es,
        index="i",
        query={"match_all": {}},
        csv_result=_csv_result(3),
        retry_cfg=retry_cfg_fast,
        on_full_reextract=reextract,
        sleeper=lambda _: None,
    )
    assert res.row_count == 3
    assert es.count.call_count == 1
    reextract.assert_not_called()


def test_validate_with_retry_recovers_after_a_couple_flaps(
    retry_cfg_fast: RetryConfig,
) -> None:
    es = MagicMock()
    es.count.side_effect = [
        {"count": 99},  # mismatch
        {"count": 99},  # mismatch
        {"count": 3},   # match
    ]
    reextract = MagicMock(side_effect=AssertionError("should not be called"))
    res = validate_with_retry(
        es=es, index="i", query={},
        csv_result=_csv_result(3),
        retry_cfg=retry_cfg_fast,
        on_full_reextract=reextract,
        sleeper=lambda _: None,
    )
    assert res.row_count == 3
    assert es.count.call_count == 3
    reextract.assert_not_called()


def test_validate_with_retry_triggers_reextract_and_succeeds(
    retry_cfg_fast: RetryConfig,
) -> None:
    es = MagicMock()
    # First 5 _count attempts: all mismatch.
    # 6th call (post-reextract verification): match.
    es.count.side_effect = [{"count": 99}] * 5 + [{"count": 7}]
    reextract = MagicMock(return_value=_csv_result(7))

    res = validate_with_retry(
        es=es, index="i", query={},
        csv_result=_csv_result(3),
        retry_cfg=retry_cfg_fast,
        on_full_reextract=reextract,
        sleeper=lambda _: None,
    )
    assert res.row_count == 7
    reextract.assert_called_once()
    assert es.count.call_count == 6


def test_validate_with_retry_final_mismatch_raises(retry_cfg_fast: RetryConfig) -> None:
    es = MagicMock()
    es.count.side_effect = [{"count": 99}] * 5 + [{"count": 99}]
    reextract = MagicMock(return_value=_csv_result(3))

    with pytest.raises(RecordCountMismatch) as ei:
        validate_with_retry(
            es=es, index="i", query={},
            csv_result=_csv_result(3),
            retry_cfg=retry_cfg_fast,
            on_full_reextract=reextract,
            sleeper=lambda _: None,
        )
    # Tier 1 (5 retries) + tier 2 (1 attempt) → 6 entries in the history.
    assert len(ei.value.attempts) == 6
    assert ei.value.expected == 99
    assert ei.value.actual == 3
