"""Standalone, dependency-light Elasticsearch extraction.

Depends only on the standard library and a duck-typed Elasticsearch client
(any object exposing ``count`` / ``search`` / ``open_point_in_time`` /
``close_point_in_time``). It has **no** dependency on the rest of this
repository, so the package can be copied or installed and reused on its own.

Extraction uses point-in-time + ``search_after`` — Elastic's recommended
deep-pagination mechanism — exposed as a memory-bounded streaming generator.

Quick start::

    from elasticsearch import Elasticsearch
    from es_extract import count, iter_hits

    es = Elasticsearch("http://localhost:9200")
    q = {"match_all": {}}
    print(count(es, "my-index", q))
    for src in iter_hits(es, "my-index", q):
        ...  # `src` is each hit's _source dict (pass source_only=False for the envelope)
"""

from __future__ import annotations

from es_extract.diagnostics import dump_to_ndjson, tee_to_ndjson
from es_extract.errors import EsExtractError
from es_extract.extract import count, iter_hits
from es_extract.pagination import SearchAfterPagination

__all__ = [
    "EsExtractError",
    "SearchAfterPagination",
    "count",
    "dump_to_ndjson",
    "iter_hits",
    "tee_to_ndjson",
]

__version__ = "0.2.0"
