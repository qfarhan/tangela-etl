from __future__ import annotations

import random

import pytest

from etl.errors import RetryExhausted
from etl.retry import retry, retry_call


class Boom(Exception):
    pass


def test_retry_succeeds_first_try_no_sleep() -> None:
    sleeps: list[float] = []
    result = retry_call(
        lambda: 42,
        on=(Boom,),
        attempts=5,
        sleeper=sleeps.append,
    )
    assert result == 42
    assert sleeps == []


def test_retry_succeeds_on_third_attempt_sleeps_twice() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise Boom(f"attempt {calls['n']}")
        return "ok"

    result = retry_call(
        flaky,
        on=(Boom,),
        attempts=5,
        base=1.0,
        cap=30.0,
        jitter=0.0,
        sleeper=sleeps.append,
    )
    assert result == "ok"
    assert sleeps == [1.0, 2.0]  # 1*2^0, 1*2^1, jitter disabled


def test_retry_exhausts_and_raises_original() -> None:
    sleeps: list[float] = []

    def always_fail() -> None:
        raise Boom("nope")

    with pytest.raises(Boom):
        retry_call(
            always_fail,
            on=(Boom,),
            attempts=3,
            base=1.0,
            cap=30.0,
            jitter=0.0,
            sleeper=sleeps.append,
        )
    # 3 attempts → 2 sleeps between them.
    assert sleeps == [1.0, 2.0]


def test_retry_wrap_final_emits_retry_exhausted() -> None:
    def always_fail() -> None:
        raise Boom("nope")

    with pytest.raises(RetryExhausted) as ei:
        retry_call(
            always_fail,
            on=(Boom,),
            attempts=2,
            base=0.0,
            cap=0.0,
            jitter=0.0,
            sleeper=lambda _: None,
            wrap_final=True,
        )
    assert ei.value.attempts == 2
    assert isinstance(ei.value.last_exc, Boom)


def test_retry_does_not_catch_unrelated_exceptions() -> None:
    sleeps: list[float] = []

    class Other(Exception):
        pass

    def raise_other() -> None:
        raise Other("not in catch list")

    with pytest.raises(Other):
        retry_call(
            raise_other, on=(Boom,), attempts=5, sleeper=sleeps.append,
        )
    assert sleeps == []


def test_retry_decorator_form() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    @retry(on=(Boom,), attempts=3, base=1.0, cap=30.0, jitter=0.0,
           sleeper=sleeps.append)
    def f() -> str:
        calls["n"] += 1
        if calls["n"] < 2:
            raise Boom("once")
        return "done"

    assert f() == "done"
    assert sleeps == [1.0]


def test_retry_attempts_must_be_positive() -> None:
    with pytest.raises(ValueError):
        retry_call(lambda: None, on=(Boom,), attempts=0)


def test_retry_jitter_uses_injected_rng() -> None:
    # Seed rng so result is deterministic; verify jitter scales the delay
    # within the expected bound.
    sleeps: list[float] = []
    rng = random.Random(0)

    def always_fail() -> None:
        raise Boom("x")

    with pytest.raises(Boom):
        retry_call(
            always_fail, on=(Boom,), attempts=2,
            base=1.0, cap=30.0, jitter=0.5,
            sleeper=sleeps.append, rng=rng,
        )
    assert len(sleeps) == 1
    # base * 2^0 = 1.0, jitter ±50% → [0.5, 1.5]
    assert 0.5 <= sleeps[0] <= 1.5
