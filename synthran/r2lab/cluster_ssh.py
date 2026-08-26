from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import shlex
from typing import Iterator

from synthran.fiveg_ansible import InventoryHost
from synthran.live_preflight import LivePreflightError


def strict_cluster_ssh_command(
    host: InventoryHost,
    known_hosts: Path,
    *remote_command: str,
) -> tuple[str, ...]:
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise LivePreflightError("strict SLICES known-hosts file is missing")

    address = host.variables.get("ansible_host", host.name)
    user = host.variables.get("ansible_user")
    if not user:
        raise LivePreflightError("inventory host is missing ansible_user")

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


@contextmanager
def bind_physical_cluster_ssh(known_hosts: Path) -> Iterator[None]:
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise LivePreflightError("strict SLICES known-hosts file is missing")

    from synthran.experiment import r2lab as physical_runtime
    from synthran.experiment import runtime as experiment_runtime

    def bound(host: InventoryHost, *remote_command: str) -> tuple[str, ...]:
        return strict_cluster_ssh_command(host, known_hosts, *remote_command)

    experiment_ssh = experiment_runtime.ssh_command
    physical_ssh = physical_runtime.ssh_command
    experiment_runtime.ssh_command = bound
    physical_runtime.ssh_command = bound
    try:
        yield
    finally:
        experiment_runtime.ssh_command = experiment_ssh
        physical_runtime.ssh_command = physical_ssh
