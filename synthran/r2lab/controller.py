"""Deterministic SSH transport boundary for R2Lab control and FIT hosts."""

from __future__ import annotations

import os
from pathlib import Path
import shlex
import subprocess
from typing import Sequence

from synthran.live_preflight import CommandResult
from synthran.r2lab.provider import R2LabQfitStateError, qfit_node_number
from synthran.utils.ssh import strict_ssh_command
from synthran.workspace.model import WorkspaceError
from synthran.workspace.store import (
    DEFAULT_PROFILE_NAME,
    load_profile,
    resolve_identity_reference,
    verify_profile_identity,
)


R2LAB_GATEWAY = "faraday.inria.fr"
DEFAULT_TIMEOUT_SECONDS = 30


class R2LabResourceError(RuntimeError):
    """Raised when the R2Lab control transport cannot be constructed or executed."""


def _validate_slice(value: str) -> str:
    if not value or len(value) > 64:
        raise R2LabResourceError("R2Lab slice name must contain 1-64 safe characters")
    if any(not (character.isalnum() or character in "._-") for character in value):
        raise R2LabResourceError(
            "R2Lab slice name may contain only letters, numbers, '.', '_', or '-'"
        )
    return value


def _configured_identity(slice_name: str) -> Path | None:
    """Resolve the live R2Lab SSH identity without persisting its path in evidence."""

    slice_name = _validate_slice(slice_name)
    override = os.environ.get("SYNTHRAN_R2LAB_IDENTITY")
    if override:
        return Path(override).expanduser().resolve()

    try:
        profile = load_profile(DEFAULT_PROFILE_NAME)
    except WorkspaceError:
        return None
    if profile.r2lab_identity is None:
        return None
    if profile.r2lab_slice is not None and profile.r2lab_slice != slice_name:
        raise R2LabResourceError(
            "configured R2Lab SSH identity belongs to a different profile slice"
        )
    try:
        verify_profile_identity(profile)
    except WorkspaceError as exc:
        raise R2LabResourceError("configured R2Lab SSH identity could not be verified") from exc
    return resolve_identity_reference(profile.r2lab_identity)


def subprocess_runner(command: Sequence[str], timeout_seconds: int) -> CommandResult:
    """Execute one local command and capture its result."""

    try:
        completed = subprocess.run(
            list(command),
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as exc:
        raise R2LabResourceError("ssh is required for R2Lab control") from exc
    except subprocess.TimeoutExpired as exc:
        raise R2LabResourceError("R2Lab command timed out") from exc
    return CommandResult(completed.returncode, completed.stdout or "", completed.stderr or "")


def gateway_command(slice_name: str, *remote: str) -> tuple[str, ...]:
    """Build direct strict SSH to Faraday, isolated from ambient user SSH routing."""

    slice_name = _validate_slice(slice_name)
    try:
        return strict_ssh_command(
            f"{slice_name}@{R2LAB_GATEWAY}",
            *remote,
            identity=_configured_identity(slice_name),
            isolated_config=True,
        )
    except ValueError as exc:
        raise R2LabResourceError(str(exc)) from exc


def physical_qfit_host(qfit: str) -> str:
    """Map one supported qfit resource to its FIT SSH host."""

    try:
        node = qfit_node_number(qfit)
    except R2LabQfitStateError as exc:
        raise R2LabResourceError("qfit host mapping requires a supported qfit UE") from exc
    return f"fit{node:02d}"


def _qfit_ssh_base(slice_name: str, qfit: str) -> tuple[str, ...]:
    slice_name = _validate_slice(slice_name)
    host = physical_qfit_host(qfit)
    try:
        return strict_ssh_command(
            f"root@{host}",
            known_hosts=f"/home/{slice_name}/.ssh/known_hosts",
            isolated_config=True,
        )
    except ValueError as exc:
        raise R2LabResourceError(str(exc)) from exc


def qfit_host_command(slice_name: str, qfit: str, *remote: str) -> tuple[str, ...]:
    """Build strict SSH from Faraday to one selected FIT host."""

    if not remote:
        raise R2LabResourceError("qfit host command requires one explicit remote command")
    return (*_qfit_ssh_base(slice_name, qfit), shlex.join(remote))


def qfit_gateway_command(slice_name: str, qfit: str, *remote: str) -> tuple[str, ...]:
    """Build local-to-Faraday-to-FIT SSH while preserving the inner argv boundary."""

    if not remote:
        raise R2LabResourceError("qfit gateway command requires one explicit remote command")
    nested = qfit_host_command(slice_name, qfit, *remote)
    return gateway_command(slice_name, shlex.join(nested))
