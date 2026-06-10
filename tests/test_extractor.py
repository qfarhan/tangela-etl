from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from etl.errors import ElasticsearchQueryError
from etl.extractor import expected_count, iter_hits
from etl.models import JobSpec


def _job() -> JobSpec:
    return JobSpec(
        job_id="j", data_index="d", query={"q": 1}, column_paths={},
        columns=["a"], remote_filename="r.csv",
    )


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


def test_iter_hits_streams_source_via_pit() -> None:
    es = MagicMock()
    es.open_point_in_time.return_value = {"id": "pit-1"}
    es.search.side_effect = [
        {"pit_id": "pit-1", "hits": {"hits": [{"_source": {"a": 1}, "sort": [1]},
                                              {"_source": {"a": 2}, "sort": [2]}]}},
        {"pit_id": "pit-1", "hits": {"hits": []}},
    ]
    out = list(iter_hits(es, _job(), page_size=10, keep_alive="2m"))
    assert out == [{"a": 1}, {"a": 2}]
    # job.data_index + keep_alive thread through to the PIT open …
    es.open_point_in_time.assert_called_once_with(index="d", keep_alive="2m")
    # … and job.query lands in the search body.
    assert es.search.call_args_list[0].kwargs["body"]["query"] == {"q": 1}


def test_iter_hits_wraps_errors_in_elasticsearch_query_error() -> None:
    es = MagicMock()
    es.open_point_in_time.side_effect = RuntimeError("down")
    with pytest.raises(ElasticsearchQueryError):
        list(iter_hits(es, _job(), page_size=10))
