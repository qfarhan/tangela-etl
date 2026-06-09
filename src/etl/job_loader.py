"""Resolves a control message's `job_doc_id` to a `JobSpec` from Elasticsearch."""

from __future__ import annotations

from typing import Any

from etl.errors import ElasticsearchQueryError, JobSpecError
from etl.models import JobSpec

_REQUIRED = ("data_index", "query", "columns", "remote_filename")


def load_job(es: Any, *, job_index: str, job_doc_id: str) -> JobSpec:
    try:
        doc = es.get(index=job_index, id=job_doc_id)
    except Exception as e:
        raise ElasticsearchQueryError(
            f"failed to GET job doc {job_doc_id} from {job_index}: {e!r}"
        ) from e

    if not doc.get("found", True):
        raise JobSpecError(f"job doc {job_doc_id} not found in {job_index}")

    source = doc.get("_source") or {}
    missing = [f for f in _REQUIRED if f not in source]
    if missing:
        raise JobSpecError(f"job doc {job_doc_id} missing fields: {missing}")

    query = source["query"]
    if not isinstance(query, dict):
        raise JobSpecError(f"job doc {job_doc_id}: 'query' must be an object")

    columns = source["columns"]
    if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
        raise JobSpecError(f"job doc {job_doc_id}: 'columns' must be list[str]")

    column_paths = source.get("column_paths", {})
    if column_paths is None:
        column_paths = {}
    if not isinstance(column_paths, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in column_paths.items()
    ):
        raise JobSpecError(f"job doc {job_doc_id}: 'column_paths' must be dict[str, str]")

    remote_filename = source["remote_filename"]
    if not isinstance(remote_filename, str) or not remote_filename:
        raise JobSpecError(f"job doc {job_doc_id}: 'remote_filename' must be non-empty string")

    return JobSpec(
        job_id=str(source.get("job_id", job_doc_id)),
        data_index=str(source["data_index"]),
        query=query,
        column_paths=column_paths,
        columns=list(columns),
        remote_filename=remote_filename,
    )
