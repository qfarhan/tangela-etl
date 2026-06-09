from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from etl.errors import ElasticsearchQueryError
from etl.pagination.scroll import ScrollPagination


def _hit(src: dict[str, Any]) -> dict[str, Any]:
    return {"_source": src}


def test_scroll_pagination_yields_all_hits_and_clears_scroll() -> None:
    es = MagicMock()
    es.search.return_value = {
        "_scroll_id": "scroll-1",
        "hits": {"hits": [_hit({"id": 1}), _hit({"id": 2})]},
    }
    es.scroll.side_effect = [
        {"_scroll_id": "scroll-2", "hits": {"hits": [_hit({"id": 3})]}},
        {"_scroll_id": "scroll-3", "hits": {"hits": []}},
    ]

    strat = ScrollPagination(keep_alive="1m")
    out = list(strat.iter_hits(es=es, index="i", query={"match_all": {}}, page_size=2))
    assert out == [{"id": 1}, {"id": 2}, {"id": 3}]
    es.search.assert_called_once()
    assert es.scroll.call_count == 2
    es.clear_scroll.assert_called_once_with(scroll_id="scroll-3")


def test_scroll_pagination_clears_scroll_on_iteration_abort() -> None:
    es = MagicMock()
    es.search.return_value = {
        "_scroll_id": "s",
        "hits": {"hits": [_hit({"id": 1}), _hit({"id": 2})]},
    }
    strat = ScrollPagination()
    gen = strat.iter_hits(es=es, index="i", query={}, page_size=2)
    next(gen)
    gen.close()
    es.clear_scroll.assert_called_once_with(scroll_id="s")


def test_scroll_pagination_wraps_search_errors() -> None:
    es = MagicMock()
    es.search.side_effect = RuntimeError("conn refused")
    strat = ScrollPagination()
    with pytest.raises(ElasticsearchQueryError):
        list(strat.iter_hits(es=es, index="i", query={}, page_size=10))


def test_scroll_pagination_handles_empty_first_page() -> None:
    es = MagicMock()
    es.search.return_value = {"_scroll_id": "s", "hits": {"hits": []}}
    strat = ScrollPagination()
    out = list(strat.iter_hits(es=es, index="i", query={}, page_size=10))
    assert out == []
    es.scroll.assert_not_called()
    es.clear_scroll.assert_called_once_with(scroll_id="s")
