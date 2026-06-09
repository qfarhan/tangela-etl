from __future__ import annotations

import json
from typing import Any

from etl.config import KafkaConfig
from etl.control_consumer import ControlConsumer


class FakeConsumer:
    def __init__(self, cfg: dict[str, str]) -> None:
        self.cfg = cfg
        self.subscribed: list[str] = []
        self.committed: list[Any] = []
        self.queue: list[Any] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = topics

    def poll(self, timeout: float) -> Any:
        if self.queue:
            return self.queue.pop(0)
        return None

    def commit(self, *, message: Any, asynchronous: bool) -> None:
        self.committed.append(message)

    def close(self) -> None:
        self.closed = True


def _cfg() -> KafkaConfig:
    return KafkaConfig(bootstrap_servers="b", control_topic="ctl", group_id="g")


def _msg(value: bytes, partition: int = 0, offset: int = 0, err: Any = None,
         FakeKafkaMessage: Any = None) -> Any:
    return FakeKafkaMessage(value, partition=partition, offset=offset, err=err)


def test_consumer_subscribes_with_manual_commit_config() -> None:
    fake_holder: list[FakeConsumer] = []

    def factory(cfg: dict[str, str]) -> FakeConsumer:
        c = FakeConsumer(cfg)
        fake_holder.append(c)
        return c

    cc = ControlConsumer(_cfg(), consumer_factory=factory)
    assert fake_holder[0].subscribed == ["ctl"]
    assert fake_holder[0].cfg["enable.auto.commit"] == "false"
    cc.close()
    assert fake_holder[0].closed is True


def test_consumer_decodes_valid_message_and_commit_only_after_ack(
    FakeKafkaMessage: Any,
) -> None:
    consumer_ref: dict[str, FakeConsumer] = {}

    def factory(cfg: dict[str, str]) -> FakeConsumer:
        c = FakeConsumer(cfg)
        consumer_ref["c"] = c
        return c

    cc = ControlConsumer(_cfg(), consumer_factory=factory)
    fake = consumer_ref["c"]
    fake.queue.append(_msg(json.dumps({"job_doc_id": "abc"}).encode("utf-8"),
                           partition=2, offset=99,
                           FakeKafkaMessage=FakeKafkaMessage))

    it = cc.iter_messages(poll_timeout_s=0.0,
                          stop=lambda: not fake.queue and True)
    # Force evaluation: fetch first message then stop the generator.
    ctrl, commit, raw = next(it)
    assert ctrl.job_doc_id == "abc"
    assert ctrl.raw_partition == 2
    assert ctrl.raw_offset == 99
    assert fake.committed == []
    commit()
    assert fake.committed == [raw]
    it.close()


def test_consumer_skips_null_value_message(FakeKafkaMessage: Any) -> None:
    consumer_ref: dict[str, FakeConsumer] = {}

    def factory(cfg: dict[str, str]) -> FakeConsumer:
        c = FakeConsumer(cfg)
        consumer_ref["c"] = c
        return c

    cc = ControlConsumer(_cfg(), consumer_factory=factory)
    fake = consumer_ref["c"]
    fake.queue.append(_msg(None, offset=3, FakeKafkaMessage=FakeKafkaMessage))  # type: ignore[arg-type]
    fake.queue.append(_msg(json.dumps({"job_doc_id": "ok"}).encode("utf-8"),
                           offset=4, FakeKafkaMessage=FakeKafkaMessage))

    it = cc.iter_messages(poll_timeout_s=0.0, stop=lambda: not fake.queue)
    ctrl, _commit, _ = next(it)
    assert ctrl.job_doc_id == "ok"
    # The null-valued record was committed past before the good one.
    assert len(fake.committed) == 1
    it.close()


def test_consumer_skips_poison_message_by_committing(FakeKafkaMessage: Any) -> None:
    consumer_ref: dict[str, FakeConsumer] = {}

    def factory(cfg: dict[str, str]) -> FakeConsumer:
        c = FakeConsumer(cfg)
        consumer_ref["c"] = c
        return c

    cc = ControlConsumer(_cfg(), consumer_factory=factory)
    fake = consumer_ref["c"]
    fake.queue.append(_msg(b"not-json", FakeKafkaMessage=FakeKafkaMessage))
    fake.queue.append(_msg(json.dumps({"job_doc_id": "ok"}).encode("utf-8"),
                           offset=5, FakeKafkaMessage=FakeKafkaMessage))

    it = cc.iter_messages(poll_timeout_s=0.0, stop=lambda: not fake.queue)
    ctrl, commit, _ = next(it)
    assert ctrl.job_doc_id == "ok"
    # The poison message was committed past (1 commit so far); ack the good one.
    assert len(fake.committed) == 1
    commit()
    assert len(fake.committed) == 2
    it.close()
