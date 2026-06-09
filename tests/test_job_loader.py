from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from etl.errors import ElasticsearchQueryError, JobSpecError
from etl.job_loader import load_job


def _doc(source: dict) -> dict:
    return {"_id": "x", "found": True, "_source": source}


def test_load_job_happy_path() -> None:
    es = MagicMock()
    es.get.return_value = _doc({
        "job_id": "job-7",
        "data_index": "sales-2024",
        "query": {"match_all": {}},
        "column_paths": {"x": "user.id", "y": "amount"},
        "columns": ["x", "y"],
        "remote_filename": "exports/job-7.csv",
    })
    spec = load_job(es, job_index="jobs", job_doc_id="job-7")
    assert spec.job_id == "job-7"
    assert spec.data_index == "sales-2024"
    assert spec.columns == ["x", "y"]
    assert spec.column_paths == {"x": "user.id", "y": "amount"}


def test_load_job_missing_doc_raises() -> None:
    es = MagicMock()
    es.get.return_value = {"found": False}
    with pytest.raises(JobSpecError, match="not found"):
        load_job(es, job_index="jobs", job_doc_id="absent")


def test_load_job_missing_fields_raises() -> None:
    es = MagicMock()
    es.get.return_value = _doc({"data_index": "i"})
    with pytest.raises(JobSpecError, match="missing"):
        load_job(es, job_index="jobs", job_doc_id="bad")


def test_load_job_bad_columns_raises() -> None:
    es = MagicMock()
    es.get.return_value = _doc({
        "data_index": "i",
        "query": {},
        "columns": ["a", 1, "b"],
        "remote_filename": "r.csv",
    })
    with pytest.raises(JobSpecError, match="columns"):
        load_job(es, job_index="jobs", job_doc_id="bad")


def test_load_job_es_error_wrapped() -> None:
    es = MagicMock()
    es.get.side_effect = RuntimeError("network")
    with pytest.raises(ElasticsearchQueryError):
        load_job(es, job_index="jobs", job_doc_id="x")


def test_load_job_defaults_empty_column_paths() -> None:
    es = MagicMock()
    es.get.return_value = _doc({
        "data_index": "i",
        "query": {},
        "columns": ["a"],
        "remote_filename": "r.csv",
    })
    spec = load_job(es, job_index="jobs", job_doc_id="x")
    assert spec.column_paths == {}


def test_load_job_bad_column_paths_raises() -> None:
    es = MagicMock()
    es.get.return_value = _doc({
        "data_index": "i",
        "query": {},
        "columns": ["a"],
        "remote_filename": "r.csv",
        "column_paths": {"a": 5},  # value must be str
    })
    with pytest.raises(JobSpecError, match="column_paths"):
        load_job(es, job_index="jobs", job_doc_id="x")
