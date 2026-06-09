from __future__ import annotations

import json
import logging

import pytest

from etl.logging_setup import configure_logging


def test_extra_fields_become_json(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")
    logging.getLogger("t").info("hello", extra={"job_id": "abc"})
    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["msg"] == "hello"
    assert payload["job_id"] == "abc"
