from __future__ import annotations

import dataclasses
import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from etl.config import Settings
from etl.errors import RecordCountMismatch, SftpUploadError
from etl.models import ControlMessage
from etl.pipeline import run_one


def _ctrl() -> ControlMessage:
    return ControlMessage(job_doc_id="doc-1", correlation_id="corr-1",
                          raw_partition=0, raw_offset=10)


def _es_with_job_and_hits(hits: list[dict[str, Any]]) -> MagicMock:
    es = MagicMock()
    es.get.return_value = {
        "found": True,
        "_source": {
            "job_id": "job-1",
            "data_index": "data",
            "query": {"match_all": {}},
            "column_paths": {},
            "columns": ["id", "name"],
            "remote_filename": "exports/job-1.csv",
        },
    }
    es.count.return_value = {"count": len(hits)}
    es.open_point_in_time.return_value = {"id": "pit-1"}

    # PIT + search_after paging. Every extraction opens a fresh PIT and starts
    # with no `search_after`: return the full page first, then an empty page so
    # iteration terminates. Keyed on the body so repeated extractions (the
    # re-extract path) work too.
    page = {
        "pit_id": "pit-1",
        "hits": {"hits": [{"_source": h, "sort": [i]} for i, h in enumerate(hits)]},
    }
    empty: dict[str, Any] = {"pit_id": "pit-1", "hits": {"hits": []}}

    def _search(**kwargs: Any) -> dict[str, Any]:
        return empty if "search_after" in kwargs["body"] else page

    es.search.side_effect = _search
    return es


def test_pipeline_golden_path(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, tmp_path: Path,
) -> None:
    hits = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
    es = _es_with_job_and_hits(hits)
    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: MagicMock(returncode=0, stderr=b""))

    run_one(ctrl=_ctrl(), es=es, settings=settings)

    staged = settings.csv_output_dir / "job-1.csv"
    sidecar = settings.csv_output_dir / "job-1.csv.sha256"
    assert staged.exists()
    assert sidecar.exists()
    # 2 data rows + header
    assert len(staged.read_text(encoding="utf-8").splitlines()) == 3


def test_pipeline_count_mismatch_after_reextract_raises(
    monkeypatch: pytest.MonkeyPatch, settings: Settings,
) -> None:
    hits = [{"id": "1", "name": "Alice"}]
    es = _es_with_job_and_hits(hits)
    # ES count always disagrees with row count (1).
    es.count.return_value = {"count": 99}

    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: MagicMock(returncode=0, stderr=b""))

    with pytest.raises(RecordCountMismatch):
        run_one(ctrl=_ctrl(), es=es, settings=settings)
    # SFTP should never be invoked.


def test_pipeline_sftp_failure_after_retries(
    monkeypatch: pytest.MonkeyPatch, settings: Settings,
) -> None:
    hits = [{"id": "1", "name": "Alice"}]
    es = _es_with_job_and_hits(hits)
    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: MagicMock(returncode=1, stderr=b"denied"))

    with pytest.raises(SftpUploadError):
        run_one(ctrl=_ctrl(), es=es, settings=settings)


def test_pipeline_transient_sftp_recovers(
    monkeypatch: pytest.MonkeyPatch, settings: Settings,
) -> None:
    hits = [{"id": "1", "name": "Alice"}]
    es = _es_with_job_and_hits(hits)
    calls = {"n": 0}

    def fake_run(argv: list[str], **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] < 3:
            return MagicMock(returncode=1, stderr=b"transient")
        return MagicMock(returncode=0, stderr=b"")

    monkeypatch.setattr(subprocess, "run", fake_run)
    run_one(ctrl=_ctrl(), es=es, settings=settings)
    assert calls["n"] == 3


def test_pipeline_raw_dump_writes_ndjson(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, tmp_path: Path,
) -> None:
    hits = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
    es = _es_with_job_and_hits(hits)
    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: MagicMock(returncode=0, stderr=b""))

    dump_dir = tmp_path / "raw"
    s = dataclasses.replace(settings, raw_dump_dir=dump_dir)
    run_one(ctrl=_ctrl(), es=es, settings=s)

    dump = dump_dir / "job-1.ndjson"
    assert dump.exists()
    lines = dump.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == hits  # raw _source hits, one per line
