"""Strict command and SSH primitives for live observation.

5g-Ansible owns deployment preparation. SynthRAN keeps only the narrow process
boundary required to observe an upstream deployment and run experiment-local
commands safely.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
import shlex
import subprocess
from typing import Callable, Sequence

from synthran.fiveg_ansible import InventoryHost


class LivePreflightError(RuntimeError):
    """Raised when a strict live command boundary cannot be established."""


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str = ""


Runner = Callable[[Sequence[str], int], CommandResult]


def subprocess_runner(command: Sequence[str], timeout_seconds: int) -> CommandResult:
    """Execute one argv-only command without a local shell."""

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
        raise LivePreflightError("a required executable was not found") from exc
    except subprocess.TimeoutExpired as exc:
        raise LivePreflightError("a live command timed out") from exc
    return CommandResult(completed.returncode, completed.stdout, completed.stderr)


def ssh_command(host: InventoryHost, *remote_command: str) -> tuple[str, ...]:
    """Build one strict SSH command from upstream inventory facts.

    Host-key policy is never relaxed here. The trusted known-hosts file remains
    an explicit controller input and remote argv is shell-quoted exactly once.
    """

    address = host.variables.get("ansible_host", host.name)
    user = host.variables.get("ansible_user")
    if not user:
        raise LivePreflightError("inventory host is missing ansible_user")

    known_hosts_value = os.environ.get("SYNTHRAN_KNOWN_HOSTS")
    if not known_hosts_value:
        raise LivePreflightError("SYNTHRAN_KNOWN_HOSTS is required for strict SSH")
    known_hosts = Path(known_hosts_value).expanduser().resolve()
    if not known_hosts.is_file():
        raise LivePreflightError("SYNTHRAN_KNOWN_HOSTS does not name an existing file")

    command: list[str] = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
    ]
    port = host.variables.get("ansible_port")
    if port:
        if not port.isdigit() or not 1 <= int(port) <= 65535:
            raise LivePreflightError("inventory ansible_port is invalid")
        command.extend(("-p", port))
    command.append(f"{user}@{address}")
    if remote_command:
        command.append(" ".join(shlex.quote(part) for part in remote_command))
    return tuple(command)
