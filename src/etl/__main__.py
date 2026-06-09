"""Daemon entry point — `python -m etl`.

Polls the control topic in a loop, runs one job per message, and commits the
offset only after the job succeeds. Job-scoped errors (`EtlError`) are
logged and the loop continues. Anything else propagates and exits non-zero.
"""

from __future__ import annotations

import contextlib
import logging
import signal
import sys
from types import FrameType
from typing import Any

from etl.config import Settings, load_settings
from etl.control_consumer import ControlConsumer
from etl.errors import ConfigError, EtlError
from etl.logging_setup import configure_logging
from etl.pipeline import run_one

_log = logging.getLogger("etl.main")


def _build_es_client(settings: Settings) -> Any:
    """Construct the official `elasticsearch.Elasticsearch` client."""
    from elasticsearch import Elasticsearch  # local import — heavy dep

    kwargs: dict[str, Any] = {"hosts": settings.es.hosts}
    if settings.es.api_key:
        kwargs["api_key"] = settings.es.api_key
    elif settings.es.username and settings.es.password:
        kwargs["basic_auth"] = (settings.es.username, settings.es.password)
    return Elasticsearch(**kwargs)


def main() -> int:
    try:
        settings = load_settings()
    except ConfigError as e:
        # Logging may not be configured yet; print and exit.
        print(f"config error: {e}", file=sys.stderr)
        return 2

    configure_logging(settings.log_level)
    _log.info("starting etl daemon", extra={"control_topic": settings.kafka.control_topic})

    stopping = {"flag": False}

    def _on_signal(signum: int, _frame: FrameType | None) -> None:
        _log.info("received signal %s; stopping", signum)
        stopping["flag"] = True

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    es = _build_es_client(settings)
    consumer = ControlConsumer(settings.kafka)

    exit_code = 0
    try:
        for ctrl, commit, _raw in consumer.iter_messages(stop=lambda: stopping["flag"]):
            try:
                run_one(ctrl=ctrl, es=es, settings=settings)
            except EtlError as e:
                # The job already exhausted its internal retries (counts, SFTP)
                # before raising. Halt without committing so the offset stays
                # put and this exact message is redelivered on the next start.
                # We must NOT `continue`: advancing to the next message and
                # committing its offset would commit *over* this failed one
                # (Kafka offsets are "up to and including"), silently dropping it.
                _log.error(
                    "job failed; halting without commit so it is redelivered: %r",
                    e,
                    extra={
                        "job_doc_id": ctrl.job_doc_id,
                        "correlation_id": ctrl.correlation_id,
                    },
                )
                exit_code = 1
                break
            commit()
            _log.info("job committed", extra={"job_doc_id": ctrl.job_doc_id})
    finally:
        consumer.close()
        with contextlib.suppress(Exception):  # pragma: no cover - best effort
            es.close()

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
