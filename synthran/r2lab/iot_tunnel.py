"""Strict SSH tunnel primitives for the accepted physical Amber experiment path."""

from __future__ import annotations

from synthran.experiment import ExperimentError
from synthran.experiment.live import _core_address, _remote, _transfer_file
from synthran.fiveg_ansible import NetworkInventory
from synthran.live_preflight import LivePreflightError, ssh_command


REMOTE_EDGE_FORWARD_PORT = 18883


def _remote_path_exists(
    inventory: NetworkInventory,
    path: str,
    *,
    timeout_seconds: int = 10,
) -> bool:
    try:
        command = ssh_command(inventory.core_node, "test", "-e", path)
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    from synthran.experiment.live import _run

    result = _run(command, timeout_seconds=timeout_seconds)
    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False
    raise ExperimentError("remote path existence probe failed")


def _ssh_reverse_tunnel_command(
    inventory: NetworkInventory,
    *,
    remote_port: int,
    local_port: int,
) -> tuple[str, ...]:
    try:
        base = list(ssh_command(inventory.core_node))
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    if not base:
        raise ExperimentError("unable to construct strict SSH reverse tunnel")
    target = base.pop()
    base.extend(
        (
            "-N",
            "-o",
            "ExitOnForwardFailure=yes",
            "-R",
            f"127.0.0.1:{remote_port}:127.0.0.1:{local_port}",
            target,
        )
    )
    return tuple(base)


__all__ = (
    "REMOTE_EDGE_FORWARD_PORT",
    "_core_address",
    "_remote",
    "_remote_path_exists",
    "_ssh_reverse_tunnel_command",
    "_transfer_file",
)
