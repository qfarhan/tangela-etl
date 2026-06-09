"""SFTP upload via the system `sftp` binary.

We intentionally shell out (no `paramiko`) and force strict host-key checking
against a user-supplied `known_hosts` file. The batch file is written into a
temp dir, used with `-b`, then removed.
"""

from __future__ import annotations

import logging
import shlex
import subprocess
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from etl.config import RetryConfig, SftpConfig
from etl.errors import SftpUploadError
from etl.retry import retry_call

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class UploadPlan:
    """Pairs each local file with its remote destination path."""

    local: Path
    remote: str


def _build_batch(plans: list[UploadPlan]) -> str:
    lines: list[str] = []
    for p in plans:
        # `sftp` batch files use whitespace as the separator. The shlex.quote
        # call protects against spaces in paths; bare metacharacters in
        # filenames are otherwise harmless here (no shell involved).
        lines.append(f"put {shlex.quote(str(p.local))} {shlex.quote(p.remote)}")
    lines.append("bye")
    return "\n".join(lines) + "\n"


def _run_sftp(cfg: SftpConfig, batch_text: str, *, timeout: float) -> None:
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sftpbatch", delete=True, encoding="utf-8"
    ) as batch_fh:
        batch_fh.write(batch_text)
        batch_fh.flush()
        argv = [
            "sftp",
            "-b", batch_fh.name,
            "-i", str(cfg.key_path),
            "-P", str(cfg.port),
            "-o", f"UserKnownHostsFile={cfg.known_hosts}",
            "-o", "StrictHostKeyChecking=yes",
            "-o", "BatchMode=yes",
            f"{cfg.user}@{cfg.host}",
        ]
        _log.info("sftp invoking", extra={"argv": argv})
        try:
            proc = subprocess.run(
                argv,
                check=False,
                capture_output=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as e:
            raise SftpUploadError(f"sftp timed out after {timeout}s: {e!r}") from e
        except FileNotFoundError as e:
            raise SftpUploadError(f"sftp binary not found: {e!r}") from e
        if proc.returncode != 0:
            raise SftpUploadError(
                f"sftp exit={proc.returncode} stderr={proc.stderr.decode('utf-8', 'replace')!r}"
            )


def upload(
    cfg: SftpConfig,
    plans: list[UploadPlan],
    *,
    retry_cfg: RetryConfig,
    timeout: float = 300.0,
    sleeper: Callable[[float], None] | None = None,
) -> None:
    """Upload a set of files via sftp with retry-on-failure."""
    batch_text = _build_batch(plans)
    retry_call(
        _run_sftp,
        cfg,
        batch_text,
        timeout=timeout,
        on=(SftpUploadError,),
        attempts=retry_cfg.max_attempts,
        base=retry_cfg.backoff_base,
        cap=retry_cfg.backoff_cap,
        jitter=retry_cfg.jitter,
        sleeper=sleeper if sleeper is not None else time.sleep,
    )
