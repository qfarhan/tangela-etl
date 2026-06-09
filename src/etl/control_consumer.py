"""Thin wrapper around `confluent_kafka.Consumer` for the control topic.

* `enable.auto.commit=False` — offsets are only committed via the explicit
  ack callback returned with each message.
* Decodes message JSON into a `ControlMessage`. Malformed messages raise
  `ControlMessageError` so the orchestrator can decide whether to commit
  past the poison record or skip-and-alert.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterator
from typing import Any

from etl.config import KafkaConfig
from etl.errors import ControlMessageError
from etl.models import ControlMessage

_log = logging.getLogger(__name__)


class ControlConsumer:
    def __init__(self, cfg: KafkaConfig, *, consumer_factory: Callable[[dict[str, str]], Any] | None = None) -> None:
        if consumer_factory is None:
            from confluent_kafka import Consumer  # local import — heavy dep
            consumer_factory = Consumer
        self._cfg = cfg
        self._consumer = consumer_factory(cfg.confluent_config())
        self._consumer.subscribe([cfg.control_topic])

    @staticmethod
    def _decode(raw: bytes, *, partition: int, offset: int) -> ControlMessage:
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ControlMessageError(f"undecodable control message: {e!r}") from e
        if not isinstance(payload, dict):
            raise ControlMessageError(f"control message must be a JSON object, got {type(payload).__name__}")
        job_doc_id = payload.get("job_doc_id") or payload.get("id")
        if not isinstance(job_doc_id, str) or not job_doc_id:
            raise ControlMessageError("control message missing required 'job_doc_id'")
        correlation_id = payload.get("correlation_id")
        if correlation_id is not None and not isinstance(correlation_id, str):
            raise ControlMessageError("'correlation_id' must be string or absent")
        return ControlMessage(
            job_doc_id=job_doc_id,
            correlation_id=correlation_id,
            raw_partition=partition,
            raw_offset=offset,
        )

    def iter_messages(
        self,
        *,
        poll_timeout_s: float = 1.0,
        stop: Callable[[], bool] | None = None,
    ) -> Iterator[tuple[ControlMessage, Callable[[], None], Any]]:
        """Yield `(ControlMessage, commit_fn, raw_kafka_msg)` tuples.

        `commit_fn()` commits the offset for that message synchronously. Call
        it only after the job has been processed successfully — failures
        should leave the offset unmoved so the message is redelivered.
        """
        while True:
            if stop is not None and stop():
                return
            msg = self._consumer.poll(timeout=poll_timeout_s)
            if msg is None:
                continue
            if msg.error():
                _log.warning("kafka poll error: %r", msg.error())
                continue
            # partition()/offset() are typed Optional but are always present on
            # a fetched record; coalesce to satisfy the type checker.
            partition = int(msg.partition() or 0)
            offset = int(msg.offset() or 0)
            value = msg.value()
            if value is None:
                # Null-valued record (e.g. a tombstone): nothing to act on.
                _log.warning("null control message value at p=%s o=%s; skipping",
                             partition, offset)
                self._consumer.commit(message=msg, asynchronous=False)
                continue
            raw = value.encode("utf-8") if isinstance(value, str) else value
            try:
                ctrl = self._decode(raw, partition=partition, offset=offset)
            except ControlMessageError as e:
                _log.error("poison control message at p=%s o=%s: %r", partition, offset, e)
                # Skip past it so the daemon doesn't loop on poison forever.
                self._consumer.commit(message=msg, asynchronous=False)
                continue

            def _commit(_m: Any = msg) -> None:
                self._consumer.commit(message=_m, asynchronous=False)

            yield ctrl, _commit, msg

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception as e:
            _log.warning("consumer close failed: %r", e)
