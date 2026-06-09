from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from etl.errors import ElasticsearchQueryError
from etl.extractor import expected_count, iter_hits
from etl.models import JobSpec


def test_expected_count_returns_int() -> None:
    es = MagicMock()
    es.count.return_value = {"count": 42}
    assert expected_count(es, "i", {"match_all": {}}) == 42
    es.count.assert_called_once_with(index="i", body={"query": {"match_all": {}}})


def test_expected_count_wraps_errors() -> None:
    es = MagicMock()
    es.count.side_effect = RuntimeError("boom")
    with pytest.raises(ElasticsearchQueryError):
        expected_count(es, "i", {})


class _Strategy:
    def __init__(self, batches: list[list[dict[str, Any]]]) -> None:
        self.batches = batches
        self.calls: list[dict[str, Any]] = []

    def iter_hits(self, **kwargs: Any):  # type: ignore[no-untyped-def]
        self.calls.append(kwargs)
        for b in self.batches:
            yield from b


def test_iter_hits_delegates_to_strategy() -> None:
    job = JobSpec(
        job_id="j", data_index="d", query={"q": 1}, column_paths={},
        columns=["a"], remote_filename="r.csv",
    )
    strat = _Strategy([[{"a": 1}], [{"a": 2}, {"a": 3}]])
    out = list(iter_hits(MagicMock(), job, strat, page_size=10))
    assert out == [{"a": 1}, {"a": 2}, {"a": 3}]
    assert strat.calls[0]["index"] == "d"
    assert strat.calls[0]["query"] == {"q": 1}
    assert strat.calls[0]["page_size"] == 10
