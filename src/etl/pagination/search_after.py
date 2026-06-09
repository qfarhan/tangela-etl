"""PIT + ``search_after`` pagination for the ETL pipeline.

Thin adapter over the standalone :mod:`es_extract` package (see
``scroll.py`` for the rationale): yields ``_source`` dicts and pins request
failures to :class:`etl.errors.ElasticsearchQueryError`.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from es_extract.pagination import SearchAfterPagination as _SearchAfterPagination
from etl.errors import ElasticsearchQueryError


@dataclass
class SearchAfterPagination:
    keep_alive: str = "5m"

    def iter_hits(
        self, *, es: Any, index: str, query: dict[str, Any], page_size: int
    ) -> Iterator[dict[str, Any]]:
        return _SearchAfterPagination(
            keep_alive=self.keep_alive, error_cls=ElasticsearchQueryError
        ).iter_hits(es=es, index=index, query=query, page_size=page_size)
