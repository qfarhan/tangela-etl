"""Scroll pagination for the ETL pipeline.

Thin adapter over the standalone :mod:`es_extract` package: the extraction
logic lives there; this wrapper yields ``_source`` dicts (NiFi parity) and
pins request failures to :class:`etl.errors.ElasticsearchQueryError` so they
are caught by the daemon's single ``EtlError`` boundary.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from es_extract.pagination import ScrollPagination as _ScrollPagination
from etl.errors import ElasticsearchQueryError


@dataclass
class ScrollPagination:
    keep_alive: str = "5m"

    def iter_hits(
        self, *, es: Any, index: str, query: dict[str, Any], page_size: int
    ) -> Iterator[dict[str, Any]]:
        return _ScrollPagination(
            keep_alive=self.keep_alive, error_cls=ElasticsearchQueryError
        ).iter_hits(es=es, index=index, query=query, page_size=page_size)
