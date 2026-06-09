from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from etl.errors import ElasticsearchQueryError
from etl.pagination.search_after import SearchAfterPagination


def _hit(src: dict[str, Any], sort: list[Any]) -> dict[str, Any]:
    return {"_source": src, "sort": sort}


def test_search_after_pagination_pages_via_sort_then_closes_pit() -> None:
    es = MagicMock()
    es.open_point_in_time.return_value = {"id": "pit-1"}
    es.search.side_effect = [
        {"pit_id": "pit-1", "hits": {"hits": [_hit({"id": 1}, [1]),
                                              _hit({"id": 2}, [2])]}},
        {"pit_id": "pit-1", "hits": {"hits": [_hit({"id": 3}, [3])]}},
        {"pit_id": "pit-1", "hits": {"hits": []}},
    ]

    strat = SearchAfterPagination(keep_alive="1m")
    out = list(strat.iter_hits(es=es, index="i", query={"match_all": {}}, page_size=2))
    assert out == [{"id": 1}, {"id": 2}, {"id": 3}]

    # First call has no search_after; second call uses [2]; third uses [3].
    first_body = es.search.call_args_list[0].kwargs["body"]
    second_body = es.search.call_args_list[1].kwargs["body"]
    third_body = es.search.call_args_list[2].kwargs["body"]
    assert "search_after" not in first_body
    assert second_body["search_after"] == [2]
    assert third_body["search_after"] == [3]

    es.close_point_in_time.assert_called_once_with(body={"id": "pit-1"})


def test_search_after_pagination_closes_pit_on_error() -> None:
    es = MagicMock()
    es.open_point_in_time.return_value = {"id": "pit-1"}
    es.search.side_effect = RuntimeError("boom")
    strat = SearchAfterPagination()
    with pytest.raises(ElasticsearchQueryError):
        list(strat.iter_hits(es=es, index="i", query={}, page_size=10))
    es.close_point_in_time.assert_called_once_with(body={"id": "pit-1"})


def test_search_after_pagination_open_pit_error_wrapped() -> None:
    es = MagicMock()
    es.open_point_in_time.side_effect = RuntimeError("denied")
    strat = SearchAfterPagination()
    with pytest.raises(ElasticsearchQueryError):
        list(strat.iter_hits(es=es, index="i", query={}, page_size=10))
    es.close_point_in_time.assert_not_called()
