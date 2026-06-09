"""Generic exponential-backoff-with-jitter retry helper.

`retry_call` is the building block. `@retry(...)` is the decorator form.
The `sleeper` argument is exposed so tests can pass a fake clock instead of
calling `time.sleep` for real.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

from etl.errors import RetryExhausted

T = TypeVar("T")

_log = logging.getLogger(__name__)


def _compute_delay(attempt: int, base: float, cap: float, jitter: float,
                   rng: random.Random) -> float:
    bounded: float = min(cap, base * (2 ** attempt))
    if jitter > 0:
        bounded *= 1.0 + rng.uniform(-jitter, jitter)
    return max(0.0, bounded)


def retry_call(
    fn: Callable[..., T],
    *args: Any,
    on: tuple[type[BaseException], ...],
    attempts: int = 5,
    base: float = 1.0,
    cap: float = 30.0,
    jitter: float = 0.25,
    sleeper: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    wrap_final: bool = False,
    log_extra: dict[str, Any] | None = None,
    **kwargs: Any,
) -> T:
    """Call `fn` with retry-on-exception semantics.

    Re-raises the original exception after the final attempt unless
    `wrap_final=True`, in which case it raises `RetryExhausted` wrapping it.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    rng = rng or random.Random()
    last: BaseException | None = None
    for attempt in range(attempts):
        try:
            return fn(*args, **kwargs)
        except on as exc:
            last = exc
            if attempt == attempts - 1:
                break
            delay = _compute_delay(attempt, base, cap, jitter, rng)
            _log.warning(
                "retry: %s attempt=%d/%d delay=%.3fs err=%r",
                getattr(fn, "__name__", repr(fn)),
                attempt + 1,
                attempts,
                delay,
                exc,
                extra={**(log_extra or {}), "retry_attempt": attempt + 1,
                       "retry_delay_s": delay},
            )
            sleeper(delay)
    assert last is not None
    if wrap_final:
        raise RetryExhausted(attempts, last) from last
    raise last


def retry(
    *,
    on: tuple[type[BaseException], ...],
    attempts: int = 5,
    base: float = 1.0,
    cap: float = 30.0,
    jitter: float = 0.25,
    sleeper: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    wrap_final: bool = False,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator form of `retry_call`."""

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return retry_call(
                fn,
                *args,
                on=on,
                attempts=attempts,
                base=base,
                cap=cap,
                jitter=jitter,
                sleeper=sleeper,
                rng=rng,
                wrap_final=wrap_final,
                **kwargs,
            )

        return wrapper

    return decorator
