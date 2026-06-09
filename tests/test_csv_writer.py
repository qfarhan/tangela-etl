from __future__ import annotations

import csv
import hashlib
from pathlib import Path

import pytest

from etl.csv_writer import write_csv
from etl.errors import CsvWriteError


def test_write_csv_basic(tmp_path: Path) -> None:
    rows = [
        {"id": "1", "name": "alpha", "value": 10},
        {"id": "2", "name": "beta",  "value": 20},
        {"id": "3", "name": "gamma", "value": 30},
    ]
    out = tmp_path / "sub" / "out.csv"
    res = write_csv(rows, ["id", "name", "value"], out)

    assert res.row_count == 3
    assert res.csv_path == out
    assert res.sidecar_path == out.with_suffix(".csv.sha256")
    assert out.exists()
    assert res.sidecar_path.exists()

    # Round-trip
    with out.open("r", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        rows_read = list(reader)
    assert rows_read[0] == ["id", "name", "value"]
    assert rows_read[1] == ["1", "alpha", "10"]
    assert rows_read[3] == ["3", "gamma", "30"]

    # Sidecar matches sha256 of the file on disk.
    actual = hashlib.sha256(out.read_bytes()).hexdigest()
    assert res.sha256_hex == actual
    sidecar_text = res.sidecar_path.read_text(encoding="utf-8")
    assert sidecar_text == f"{actual}  {out.name}\n"


def test_write_csv_handles_missing_fields_and_none(tmp_path: Path) -> None:
    rows = [{"id": "1"}, {"id": "2", "name": None}]
    out = tmp_path / "out.csv"
    res = write_csv(rows, ["id", "name"], out)
    assert res.row_count == 2
    body = out.read_text(encoding="utf-8").splitlines()
    assert body == ["id,name", "1,", "2,"]


def test_write_csv_handles_special_characters(tmp_path: Path) -> None:
    rows = [{"id": "1", "name": 'a,"b"\nc'}]
    out = tmp_path / "out.csv"
    write_csv(rows, ["id", "name"], out)
    # The csv module quotes/escapes the special characters; we round-trip to
    # verify the original value is recovered.
    with out.open(newline="", encoding="utf-8") as fh:
        rows_read = list(csv.reader(fh))
    assert rows_read[1] == ["1", 'a,"b"\nc']


def test_write_csv_empty_columns_raises(tmp_path: Path) -> None:
    with pytest.raises(CsvWriteError, match="columns"):
        write_csv([], [], tmp_path / "out.csv")


def test_write_csv_zero_rows_writes_header_only(tmp_path: Path) -> None:
    out = tmp_path / "out.csv"
    res = write_csv(iter([]), ["a", "b"], out)
    assert res.row_count == 0
    assert out.read_text(encoding="utf-8") == "a,b\n"
