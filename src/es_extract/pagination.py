"""Pluggable Elasticsearch pagination — Scroll and PIT + ``search_after``.

Both strategies are generators that own their server-side resource (scroll
context / PIT) and release it in a ``finally`` block, even if the consumer
abandons iteration early. They are duck-typed against the official
``elasticsearch`` client but take any object exposing the handful of methods
they call, so they are trivially testable with a fake.

Three knobs distinguish this from an application-specific extractor:

* ``keep_alive`` — the scroll TTL / PIT keep-alive.
* ``source_only`` — when ``True`` (default) yield each hit's ``_source``; when
  ``False`` yield the full hit envelope (``_id``, ``_score``, ``sort``, …).
* ``error_cls`` — the exception type a request failure is wrapped in, so a host
  application can map failures into its own hierarchy (see ``errors.py``).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Protocol

from es_extract.errors import EsExtractError

_log = logging.getLogger(__name__)


class PaginationStrategy(Protocol):
    def iter_hits(
        self, *, es: Any, index: str, query: dict[str, Any], page_size: int
    ) -> Iterator[dict[str, Any]]: ...


def _emit(hit: dict[str, Any], *, source_only: bool) -> dict[str, Any]:
    """Return the hit's ``_source`` (default) or the whole envelope."""
    if source_only:
        source: Any = hit.get("_source", {})
        return source if isinstance(source, dict) else {}
    return hit


@dataclass
class ScrollPagination:
    """Elasticsearch Scroll API pagination."""

    keep_alive: str = "5m"
    source_only: bool = True
    error_cls: type[Exception] = EsExtractError

    def iter_hits(
        self, *, es: Any, index: str, query: dict[str, Any], page_size: int
    ) -> Iterator[dict[str, Any]]:
        scroll_id: str | None = None
        try:
            try:
                resp = es.search(
                    index=index, scroll=self.keep_alive, size=page_size,
                    body={"query": query},
                )
            except Exception as e:
                raise self.error_cls(f"initial scroll search failed: {e!r}") from e
            scroll_id = resp.get("_scroll_id")
            hits = resp.get("hits", {}).get("hits", [])
            while hits:
                for h in hits:
                    yield _emit(h, source_only=self.source_only)
                if scroll_id is None:
                    break
                try:
                    resp = es.scroll(scroll_id=scroll_id, scroll=self.keep_alive)
                except Exception as e:
                    raise self.error_cls(f"scroll continuation failed: {e!r}") from e
                scroll_id = resp.get("_scroll_id")
                hits = resp.get("hits", {}).get("hits", [])
        finally:
            if scroll_id is not None:
                try:
                    es.clear_scroll(scroll_id=scroll_id)
                except Exception as e:  # best-effort: the scroll TTL reaps it anyway
                    _log.warning("clear_scroll failed: %r", e)


@dataclass
class SearchAfterPagination:
    """Point-in-time + ``search_after`` pagination (modern Scroll replacement)."""

    keep_alive: str = "5m"
    source_only: bool = True
    error_cls: type[Exception] = EsExtractError

    def iter_hits(
        self, *, es: Any, index: str, query: dict[str, Any], page_size: int
    ) -> Iterator[dict[str, Any]]:
        try:
            pit = es.open_point_in_time(index=index, keep_alive=self.keep_alive)
        except Exception as e:
            raise self.error_cls(f"open_point_in_time failed: {e!r}") from e
        pit_id: str | None = pit.get("id")
        try:
            search_after: list[Any] | None = None
            while True:
                body: dict[str, Any] = {
                    "size": page_size,
                    "query": query,
                    "pit": {"id": pit_id, "keep_alive": self.keep_alive},
                    "sort": [{"_shard_doc": "asc"}],
                    "track_total_hits": False,
                }
                if search_after is not None:
                    body["search_after"] = search_after
                try:
                    resp = es.search(body=body)
                except Exception as e:
                    raise self.error_cls(f"search_after page failed: {e!r}") from e
                pit_id = resp.get("pit_id", pit_id)
                hits = resp.get("hits", {}).get("hits", [])
                if not hits:
                    break
                for h in hits:
                    yield _emit(h, source_only=self.source_only)
                last_sort = hits[-1].get("sort")
                if not last_sort:
                    break
                search_after = last_sort
        finally:
            if pit_id is not None:
                try:
                    es.close_point_in_time(body={"id": pit_id})
                except Exception as e:
                    _log.warning("close_point_in_time failed: %r", e)


def make_strategy(
    name: str, *, keep_alive: str = "5m", source_only: bool = True,
    error_cls: type[Exception] = EsExtractError,
) -> PaginationStrategy:
    """Build a strategy by short name (``"scroll"`` | ``"search_after"``)."""
    n = name.lower()
    if n == "scroll":
        return ScrollPagination(
            keep_alive=keep_alive, source_only=source_only, error_cls=error_cls
        )
    if n == "search_after":
        return SearchAfterPagination(
            keep_alive=keep_alive, source_only=source_only, error_cls=error_cls
        )
    raise error_cls(f"unknown pagination strategy: {name!r}")
