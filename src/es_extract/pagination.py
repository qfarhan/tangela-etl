"""Point-in-time + ``search_after`` Elasticsearch pagination.

A single streaming strategy: :class:`SearchAfterPagination` opens a
point-in-time (PIT), pages through every matching hit with ``search_after``,
and **always** closes the PIT in a ``finally`` block — even if the consumer
abandons iteration early. It is a generator, so it stays memory-bounded (one
page at a time) regardless of how large the result set is.

It is duck-typed against the official ``elasticsearch`` client but accepts any
object exposing ``open_point_in_time`` / ``search`` / ``close_point_in_time``,
so it is trivially testable with a fake.

Two knobs distinguish it from an application-specific extractor:

* ``source_only`` — when ``True`` (default) yield each hit's ``_source``; when
  ``False`` yield the full hit envelope (``_id``, ``_score``, ``sort``, …).
* ``error_cls`` — the exception type a request failure is wrapped in, so a host
  application can map failures into its own hierarchy (see ``errors.py``).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from es_extract.errors import EsExtractError

_log = logging.getLogger(__name__)


def _emit(hit: dict[str, Any], *, source_only: bool) -> dict[str, Any]:
    """Return the hit's ``_source`` (default) or the whole envelope."""
    if source_only:
        source: Any = hit.get("_source", {})
        return source if isinstance(source, dict) else {}
    return hit


@dataclass
class SearchAfterPagination:
    """Point-in-time + ``search_after`` pagination (Elastic's recommended
    deep-pagination mechanism).

    Each call to :meth:`iter_hits` owns one PIT for the lifetime of the
    generator and releases it on completion *or* early abandonment.
    """

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
                except Exception as e:  # best-effort: the PIT keep-alive reaps it anyway
                    _log.warning("close_point_in_time failed: %r", e)
