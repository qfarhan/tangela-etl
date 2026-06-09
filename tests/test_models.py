from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from etl.errors import RecordCountMismatch
from etl.models import JobSpec


def test_jobspec_is_immutable() -> None:
    job = JobSpec("j", "idx", {"match_all": {}}, {}, ["a"], "out.csv")
    with pytest.raises(FrozenInstanceError):
        job.data_index = "other"  # frozen dataclass forbids reassignment


def test_record_count_mismatch_carries_context() -> None:
    err = RecordCountMismatch(expected=5, actual=4, attempts=[(5, 4)])
    assert err.expected == 5
    assert err.actual == 4
    assert "expected=5" in str(err)
