"""Tests for the standalone `es_extract` package (no `etl` imports)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from es_extract import (
    EsExtractError,
    ScrollPagination,
    SearchAfterPagination,
    count,
    dump_to_ndjson,
    iter_hits,
    make_strategy,
    tee_to_ndjson,
)


def _hit(src: dict[str, Any], sort: list[Any] | None = None) -> dict[str, Any]:
    h: dict[str, Any] = {"_source": src, "_id": src.get("id")}
    if sort is not None:
        h["sort"] = sort
    return h


# --- count ---------------------------------------------------------------

def test_count_returns_int() -> None:
    es = MagicMock()
    es.count.return_value = {"count": 7}
    assert count(es, "i", {"match_all": {}}) == 7
    es.count.assert_called_once_with(index="i", body={"query": {"match_all": {}}})


def test_count_wraps_errors_with_default() -> None:
    es = MagicMock()
    es.count.side_effect = RuntimeError("boom")
    with pytest.raises(EsExtractError):
        count(es, "i", {})


def test_count_wraps_errors_with_injected_error_cls() -> None:
    class MyErr(Exception):
        pass

    es = MagicMock()
    es.count.side_effect = RuntimeError("boom")
    with pytest.raises(MyErr):
        count(es, "i", {}, error_cls=MyErr)


# --- scroll --------------------------------------------------------------

def test_scroll_streams_source_and_clears() -> None:
    es = MagicMock()
    es.search.return_value = {"_scroll_id": "s1", "hits": {"hits": [_hit({"id": 1})]}}
    es.scroll.return_value = {"_scroll_id": "s2", "hits": {"hits": []}}
    out = list(ScrollPagination().iter_hits(es=es, index="i", query={}, page_size=10))
    assert out == [{"id": 1}]
    es.clear_scroll.assert_called_once_with(scroll_id="s2")


def test_scroll_source_only_false_yields_full_envelope() -> None:
    es = MagicMock()
    es.search.return_value = {"_scroll_id": "s1", "hits": {"hits": [_hit({"id": 1})]}}
    es.scroll.return_value = {"_scroll_id": "s1", "hits": {"hits": []}}
    out = list(
        ScrollPagination(source_only=False).iter_hits(
            es=es, index="i", query={}, page_size=10
        )
    )
    assert out == [{"_source": {"id": 1}, "_id": 1}]  # envelope retained, incl. _id


def test_scroll_clears_on_early_close() -> None:
    es = MagicMock()
    es.search.return_value = {
        "_scroll_id": "s", "hits": {"hits": [_hit({"id": 1}), _hit({"id": 2})]},
    }
    gen = ScrollPagination().iter_hits(es=es, index="i", query={}, page_size=2)
    next(gen)
    gen.close()
    es.clear_scroll.assert_called_once_with(scroll_id="s")


def test_scroll_wraps_errors_with_injected_cls() -> None:
    class MyErr(Exception):
        pass

    es = MagicMock()
    es.search.side_effect = RuntimeError("down")
    with pytest.raises(MyErr):
        list(
            ScrollPagination(error_cls=MyErr).iter_hits(
                es=es, index="i", query={}, page_size=2
            )
        )


# --- search_after --------------------------------------------------------

def test_search_after_pages_and_closes_pit() -> None:
    es = MagicMock()
    es.open_point_in_time.return_value = {"id": "pit-1"}
    es.search.side_effect = [
        {"pit_id": "pit-1", "hits": {"hits": [_hit({"id": 1}, [1])]}},
        {"pit_id": "pit-1", "hits": {"hits": []}},
    ]
    out = list(
        SearchAfterPagination().iter_hits(es=es, index="i", query={}, page_size=1)
    )
    assert out == [{"id": 1}]
    es.close_point_in_time.assert_called_once_with(body={"id": "pit-1"})


def test_search_after_open_error_does_not_close() -> None:
    es = MagicMock()
    es.open_point_in_time.side_effect = RuntimeError("denied")
    with pytest.raises(EsExtractError):
        list(SearchAfterPagination().iter_hits(es=es, index="i", query={}, page_size=1))
    es.close_point_in_time.assert_not_called()


# --- factory + one-call iter_hits ---------------------------------------

def test_make_strategy_dispatches() -> None:
    assert isinstance(make_strategy("scroll"), ScrollPagination)
    assert isinstance(make_strategy("search_after"), SearchAfterPagination)


def test_make_strategy_unknown_raises_error_cls() -> None:
    class MyErr(Exception):
        pass

    with pytest.raises(MyErr):
        make_strategy("nope", error_cls=MyErr)


def test_iter_hits_convenience_builds_strategy() -> None:
    es = MagicMock()
    es.search.return_value = {"_scroll_id": "s", "hits": {"hits": [_hit({"id": 1})]}}
    es.scroll.return_value = {"_scroll_id": "s", "hits": {"hits": []}}
    out = list(iter_hits(es, "i", {}, strategy="scroll", page_size=5))
    assert out == [{"id": 1}]


# --- diagnostics ---------------------------------------------------------

def test_tee_to_ndjson_passes_through_and_writes(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "dump.ndjson"
    src = [{"a": 1}, {"a": 2}]
    out = list(tee_to_ndjson(iter(src), path))
    assert out == src  # yielded unchanged
    lines = path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == src


def test_dump_to_ndjson_returns_count(tmp_path: Path) -> None:
    path = tmp_path / "dump.ndjson"
    assert dump_to_ndjson(iter([{"a": 1}, {"a": 2}, {"a": 3}]), path) == 3
    assert len(path.read_text(encoding="utf-8").splitlines()) == 3
