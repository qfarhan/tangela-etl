from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from etl.config import RetryConfig, SftpConfig
from etl.errors import SftpUploadError
from etl.sftp_uploader import UploadPlan, _build_batch, upload


def _cfg(tmp_path: Path) -> SftpConfig:
    return SftpConfig(
        host="sftp.example.com",
        port=2222,
        user="etl",
        key_path=tmp_path / "id_ed25519",
        remote_dir="/incoming",
        known_hosts=tmp_path / "known_hosts",
    )


def test_build_batch_quotes_paths() -> None:
    plans = [
        UploadPlan(local=Path("/local/a b.csv"), remote="/remote/a b.csv"),
        UploadPlan(local=Path("/local/sha"), remote="/remote/sha"),
    ]
    text = _build_batch(plans)
    assert "'/local/a b.csv'" in text
    assert "'/remote/a b.csv'" in text
    assert text.strip().endswith("bye")


def test_upload_invokes_sftp_with_strict_host_checking(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, retry_cfg_fast: RetryConfig,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(argv: list[str], **kwargs: Any) -> Any:
        captured["argv"] = argv
        return MagicMock(returncode=0, stderr=b"")

    monkeypatch.setattr(subprocess, "run", fake_run)
    cfg = _cfg(tmp_path)
    upload(
        cfg,
        [UploadPlan(local=tmp_path / "f.csv", remote="/r/f.csv")],
        retry_cfg=retry_cfg_fast,
        sleeper=lambda _: None,
    )
    argv = captured["argv"]
    assert argv[0] == "sftp"
    assert "-b" in argv
    assert "-i" in argv
    assert str(cfg.key_path) in argv
    assert "-P" in argv and "2222" in argv
    assert f"UserKnownHostsFile={cfg.known_hosts}" in argv
    assert "StrictHostKeyChecking=yes" in argv
    assert "BatchMode=yes" in argv
    assert f"{cfg.user}@{cfg.host}" in argv


def test_upload_retries_then_succeeds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, retry_cfg_fast: RetryConfig,
) -> None:
    calls = {"n": 0}

    def fake_run(argv: list[str], **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] < 3:
            return MagicMock(returncode=1, stderr=b"transient")
        return MagicMock(returncode=0, stderr=b"")

    monkeypatch.setattr(subprocess, "run", fake_run)
    upload(
        _cfg(tmp_path),
        [UploadPlan(local=tmp_path / "f", remote="/r/f")],
        retry_cfg=retry_cfg_fast,
        sleeper=lambda _: None,
    )
    assert calls["n"] == 3


def test_upload_raises_after_exhausting_retries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, retry_cfg_fast: RetryConfig,
) -> None:
    calls = {"n": 0}

    def fake_run(argv: list[str], **kwargs: Any) -> Any:
        calls["n"] += 1
        return MagicMock(returncode=1, stderr=b"permanent denial")

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(SftpUploadError, match="permanent denial"):
        upload(
            _cfg(tmp_path),
            [UploadPlan(local=tmp_path / "f", remote="/r/f")],
            retry_cfg=retry_cfg_fast,
            sleeper=lambda _: None,
        )
    assert calls["n"] == retry_cfg_fast.max_attempts


def test_upload_timeout_raises_sftp_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, retry_cfg_fast: RetryConfig,
) -> None:
    def fake_run(argv: list[str], **kwargs: Any) -> Any:
        raise subprocess.TimeoutExpired(cmd=argv, timeout=0.1)

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(SftpUploadError, match="timed out"):
        upload(
            _cfg(tmp_path),
            [UploadPlan(local=tmp_path / "f", remote="/r/f")],
            retry_cfg=retry_cfg_fast,
            sleeper=lambda _: None,
            timeout=0.1,
        )


def test_upload_missing_sftp_binary_raises_sftp_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, retry_cfg_fast: RetryConfig,
) -> None:
    def fake_run(argv: list[str], **kwargs: Any) -> Any:
        raise FileNotFoundError("no sftp")

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(SftpUploadError, match="not found"):
        upload(
            _cfg(tmp_path),
            [UploadPlan(local=tmp_path / "f", remote="/r/f")],
            retry_cfg=retry_cfg_fast,
            sleeper=lambda _: None,
        )
