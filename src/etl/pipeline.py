"""End-to-end orchestration for one control message.

`run_one` is the single entry point. It does *not* commit offsets — the
caller (`__main__`) holds the commit decision so a failing job leaves the
offset unmoved (the control message is redelivered on the next poll).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from es_extract.diagnostics import tee_to_ndjson
from etl.config import Settings
from etl.csv_writer import write_csv
from etl.extractor import expected_count, iter_hits
from etl.job_loader import load_job
from etl.models import ControlMessage, CsvResult, JobSpec
from etl.sftp_uploader import UploadPlan, upload
from etl.transformer import iter_transformed
from etl.validator import validate_with_retry

_log = logging.getLogger(__name__)


def _staged_paths(csv_dir: Path, job: JobSpec) -> tuple[Path, str, str]:
    """Local staging path for the CSV + remote target paths for csv & sidecar."""
    local_basename = Path(job.remote_filename).name
    local_csv = csv_dir / local_basename
    remote_csv = job.remote_filename
    remote_sidecar = remote_csv + ".sha256"
    return local_csv, remote_csv, remote_sidecar


def _do_extract_to_csv(
    *,
    es: Any,
    job: JobSpec,
    page_size: int,
    keep_alive: str,
    local_csv: Path,
    raw_dump_path: Path | None = None,
) -> CsvResult:
    hits = iter_hits(es, job, page_size=page_size, keep_alive=keep_alive)
    if raw_dump_path is not None:
        # Diagnostic: tee the raw extracted hits to NDJSON as they stream.
        hits = tee_to_ndjson(hits, raw_dump_path)
    rows = iter_transformed(hits, job.column_paths, job.columns, job_id=job.job_id)
    return write_csv(rows, job.columns, local_csv)


def run_one(
    *,
    ctrl: ControlMessage,
    es: Any,
    settings: Settings,
) -> None:
    log_extra = {
        "job_doc_id": ctrl.job_doc_id,
        "correlation_id": ctrl.correlation_id,
        "kafka_partition": ctrl.raw_partition,
        "kafka_offset": ctrl.raw_offset,
    }
    _log.info("loading job", extra=log_extra)
    job = load_job(es, job_index=settings.es.job_index, job_doc_id=ctrl.job_doc_id)
    log_extra["job_id"] = job.job_id
    log_extra["data_index"] = job.data_index

    initial_count = expected_count(es, job.data_index, job.query)
    _log.info("expected_count=%d", initial_count, extra=log_extra)

    local_csv, remote_csv, remote_sidecar = _staged_paths(settings.csv_output_dir, job)

    raw_dump_path = (
        settings.raw_dump_dir / f"{job.job_id}.ndjson"
        if settings.raw_dump_dir is not None
        else None
    )
    if raw_dump_path is not None:
        _log.info("raw hit dump enabled path=%s", raw_dump_path, extra=log_extra)

    csv_result = _do_extract_to_csv(
        es=es,
        job=job,
        page_size=settings.pagination.page_size,
        keep_alive=settings.pagination.pit_keep_alive,
        local_csv=local_csv,
        raw_dump_path=raw_dump_path,
    )
    _log.info("csv written rows=%d sha256=%s",
              csv_result.row_count, csv_result.sha256_hex, extra=log_extra)

    def _reextract() -> CsvResult:
        _log.warning("re-extracting after count mismatch", extra=log_extra)
        # A fresh call opens a new point-in-time; the previous one is spent.
        return _do_extract_to_csv(
            es=es, job=job,
            page_size=settings.pagination.page_size,
            keep_alive=settings.pagination.pit_keep_alive,
            local_csv=local_csv,
            raw_dump_path=raw_dump_path,
        )

    csv_result = validate_with_retry(
        es=es,
        index=job.data_index,
        query=job.query,
        csv_result=csv_result,
        retry_cfg=settings.retry,
        on_full_reextract=_reextract,
        log_extra=log_extra,
    )
    _log.info("counts validated rows=%d", csv_result.row_count, extra=log_extra)

    upload(
        settings.sftp,
        [
            UploadPlan(local=csv_result.csv_path, remote=remote_csv),
            UploadPlan(local=csv_result.sidecar_path, remote=remote_sidecar),
        ],
        retry_cfg=settings.retry,
    )
    _log.info("upload complete remote=%s", remote_csv, extra=log_extra)
