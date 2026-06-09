from __future__ import annotations

import pytest

from etl.errors import TransformError
from etl.transformer import get_by_path, iter_transformed, project


def test_get_by_path_top_level() -> None:
    assert get_by_path({"a": 1}, "a") == 1


def test_get_by_path_nested() -> None:
    assert get_by_path({"a": {"b": {"c": 7}}}, "a.b.c") == 7


def test_get_by_path_list_index() -> None:
    doc = {"users": [{"id": "u1"}, {"id": "u2"}]}
    assert get_by_path(doc, "users[0].id") == "u1"
    assert get_by_path(doc, "users[1].id") == "u2"


def test_get_by_path_missing_returns_empty_string() -> None:
    assert get_by_path({"a": {"b": 1}}, "a.c") == ""
    assert get_by_path({"a": {"b": 1}}, "missing") == ""
    assert get_by_path({"a": [1]}, "a[5]") == ""


def test_get_by_path_none_returns_empty_string() -> None:
    assert get_by_path({"a": None}, "a") == ""
    assert get_by_path({"a": None}, "a.b") == ""


def test_project_uses_paths_then_falls_back_to_column_name() -> None:
    hit = {"user": {"id": "u1", "name": "Alice"}, "value": 42}
    out = project(
        hit,
        columns=["id", "name", "value", "missing"],
        column_paths={"id": "user.id", "name": "user.name"},
        job_id="j",
    )
    assert out == {"id": "u1", "name": "Alice", "value": 42, "missing": ""}


def test_project_empty_path_raises_transform_error() -> None:
    with pytest.raises(TransformError):
        project({"a": 1}, columns=["a"], column_paths={"a": ""}, job_id="j", hit_id="h")


def test_iter_transformed_streams_rows() -> None:
    hits = [
        {"user": {"id": "u1", "name": "Alice"}, "amount": 1},
        {"user": {"id": "u2", "name": "Bob"}},
    ]
    out = list(iter_transformed(
        hits,
        column_paths={"id": "user.id", "name": "user.name", "amount": "amount"},
        columns=["id", "name", "amount"],
        job_id="j",
    ))
    assert out == [
        {"id": "u1", "name": "Alice", "amount": 1},
        {"id": "u2", "name": "Bob", "amount": ""},
    ]


def test_iter_transformed_with_no_mapping_is_pure_top_level_projection() -> None:
    hits = [{"id": "u1", "name": "Alice", "extra": "ignored"}]
    out = list(iter_transformed(hits, {}, ["id", "name"], job_id="j"))
    assert out == [{"id": "u1", "name": "Alice"}]
