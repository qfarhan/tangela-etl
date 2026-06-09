"""Error type for the standalone ES-extraction package.

The functions and strategies here let the *caller* inject which exception type
wraps a failure (`error_cls`), defaulting to `EsExtractError`. That keeps this
package free of any dependency on a host application's error hierarchy: a host
(like this repo's ``etl`` package) can pass its own ``ElasticsearchQueryError``
so failures land in its existing ``except`` boundary, while a standalone user
gets a plain ``EsExtractError``.
"""

from __future__ import annotations


class EsExtractError(Exception):
    """Raised on an Elasticsearch request failure during extraction."""
