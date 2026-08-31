"""Run-owned iperf3 server lifecycle for controlled research measurements."""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import PurePosixPath
import re
import time

from synthran.experiment import ExperimentError, validate_run_id
from synthran.experiment import live as base_runtime
from synthran.fiveg_ansible import InventoryHost, NetworkInventory
from synthran.live_preflight import ssh_command
from synthran.research.iperf_toolchain import (
    CONTROL_KEEPALIVE_ARG,
    prepare_locked_iperf_server,
)


_LISTENER_PROBE = r'''
import os
import sys

pidfile = sys.argv[1]
port = int(sys.argv[2])
required = ("-s", "-1", "-p", str(port), "-I", pidfile)
matches = []
for entry in os.listdir("/proc"):
    if not entry.isdigit():
        continue
    pid = int(entry)
    if pid <= 1:
        continue
    try:
        argv = [
            item.decode("utf-8", "replace")
            for item in open(f"/proc/{pid}/cmdline", "rb").read().split(b"\0")
            if item
        ]
    except OSError:
        continue
    if all(token in argv for token in required):
        matches.append(pid)
if len(matches) != 1:
    raise SystemExit(2)
pid = matches[0]
owned = set()
try:
    for name in os.listdir(f"/proc/{pid}/fd"):
        try:
            target = os.readlink(f"/proc/{pid}/fd/{name}")
        except OSError:
            continue
        if target.startswith("socket:[") and target.endswith("]"):
            owned.add(target[8:-1])
except OSError:
    raise SystemExit(3)
port_hex = f"{port:04X}"
for table in ("/proc/net/tcp", "/proc/net/tcp6"):
    try:
        lines = open(table, encoding="utf-8", errors="replace").read().splitlines()[1:]
    except OSError:
        continue
    for line in lines:
        fields = line.split()
        if len(fields) < 10 or fields[3] != "0A":
            continue
        local = fields[1]
        inode = fields[9]
        if local.rsplit(":", 1)[-1].upper() == port_hex and inode in owned:
            print(pid)
            raise SystemExit(0)
raise SystemExit(4)
'''


@dataclass(frozen=True)
class OwnedIperfServer:
    owner_id: str
    server_node: str
    target: str | None
    port: int
    workspace: str
    pidfile: str
    process: base_runtime.ManagedProcess


def _paths(owner_id: str, port: int) -> tuple[str, str]:
    validate_run_id(owner_id)
    if port < 1024 or port > 65535:
        raise ExperimentError("research iperf3 server port must be between 1024 and 65535")
    workspace = str(PurePosixPath("/tmp/synthran-research") / owner_id)
    pidfile = str(PurePosixPath(workspace) / f"iperf3-{port}.pid")
    return workspace, pidfile


def _pattern(pidfile: str, port: int) -> str:
    return re.escape(f"iperf3 -s -1 -p {port} -J -I {pidfile}")


def _measurement_host(
    inventory: NetworkInventory,
    server_node: str,
) -> InventoryHost:
    hosts = {
        inventory.core_node.name: inventory.core_node,
        inventory.ran_node.name: inventory.ran_node,
    }
    host = hosts.get(server_node)
    if host is None:
        raise ExperimentError(
            "research measurement server must be one of the prepared inventory nodes"
        )
    if host.name == inventory.core_node.name:
        raise ExperimentError(
            "research measurement server must be distinct from the 5G core node"
        )
    return host


def _server_inventory(
    inventory: NetworkInventory,
    server_node: str,
) -> NetworkInventory:
    return replace(inventory, core_node=_measurement_host(inventory, server_node))


def prove_measurement_peer(
    inventory: NetworkInventory,
    *,
    server_node: str,
    target: str,
) -> None:
    server_inventory = _server_inventory(inventory, server_node)
    output = base_runtime._remote(
        server_inventory,
        "ip",
        "-4",
        "-o",
        "addr",
        "show",
        label="research measurement peer address proof",
        timeout_seconds=10,
    )
    addresses: set[str] = set()
    for line in output.splitlines():
        fields = line.split()
        try:
            index = fields.index("inet")
        except ValueError:
            continue
        if index + 1 < len(fields):
            addresses.add(fields[index + 1].split("/", 1)[0])
    if target not in addresses:
        raise ExperimentError(
            f"research target {target} is not assigned to measurement server {server_node}"
        )


def _reap(
    inventory: NetworkInventory,
    *,
    server_node: str,
    pidfile: str,
    port: int,
    orphan_only: bool,
    label: str,
) -> None:
    base_runtime._remote_process_reap(
        _server_inventory(inventory, server_node),
        patterns=(_pattern(pidfile, port),),
        orphan_only=orphan_only,
        label=label,
    )


def _listener_ready(
    inventory: NetworkInventory,
    *,
    pidfile: str,
    port: int,
    server_node: str | None = None,
) -> bool:
    remote_inventory = (
        inventory
        if server_node is None
        else _server_inventory(inventory, server_node)
    )
    try:
        base_runtime._remote(
            remote_inventory,
            "python3",
            "-c",
            _LISTENER_PROBE,
            pidfile,
            str(port),
            label="research iperf3 owned listener probe",
            timeout_seconds=5,
        )
    except Exception:
        return False
    return True


def _cleanup_failed_start(
    inventory: NetworkInventory,
    *,
    server_node: str,
    process: base_runtime.ManagedProcess,
    workspace: str,
    pidfile: str,
    port: int,
) -> None:
    server_inventory = _server_inventory(inventory, server_node)
    errors: list[str] = []
    try:
        process.stop()
    except Exception as exc:
        errors.append(f"local SSH process: {exc}")
    try:
        _reap(
            inventory,
            server_node=server_node,
            pidfile=pidfile,
            port=port,
            orphan_only=False,
            label="failed research iperf3 startup cleanup",
        )
    except Exception as exc:
        errors.append(f"remote iperf3 process: {exc}")
    try:
        base_runtime._remote(
            server_inventory,
            "rm",
            "-f",
            pidfile,
            label="failed research iperf3 pidfile cleanup",
            timeout_seconds=10,
        )
        base_runtime._remote(
            server_inventory,
            "rmdir",
            workspace,
            label="failed research iperf3 workspace cleanup",
            timeout_seconds=10,
        )
    except Exception as exc:
        errors.append(f"remote iperf3 workspace: {exc}")
    if errors:
        raise ExperimentError(
            "failed research iperf3 startup cleanup failed closed: "
            + "; ".join(errors)
        )


def start_owned_iperf_server(
    *,
    inventory: NetworkInventory,
    owner_id: str,
    port: int,
    repository_root,
    log_path,
    server_node: str | None = None,
    target: str | None = None,
) -> OwnedIperfServer:
    selected_node = server_node or inventory.ran_node.name
    _measurement_host(inventory, selected_node)
    if target is not None:
        prove_measurement_peer(
            inventory,
            server_node=selected_node,
            target=target,
        )
    iperf_binary = prepare_locked_iperf_server(
        inventory,
        server_node=selected_node,
        repository_root=repository_root,
    )
    server_inventory = _server_inventory(inventory, selected_node)
    workspace, pidfile = _paths(owner_id, port)
    _reap(
        inventory,
        server_node=selected_node,
        pidfile=pidfile,
        port=port,
        orphan_only=True,
        label="stale research iperf3 recovery",
    )
    base_runtime._remote(
        server_inventory,
        "mkdir",
        "-p",
        workspace,
        label="research iperf3 workspace creation",
        timeout_seconds=10,
    )
    base_runtime._remote(
        server_inventory,
        "rm",
        "-f",
        pidfile,
        label="stale research iperf3 pidfile cleanup",
        timeout_seconds=10,
    )
    command = ssh_command(
        _measurement_host(inventory, selected_node),
        iperf_binary,
        "-s",
        "-1",
        "-p",
        str(port),
        "-J",
        "-I",
        pidfile,
        CONTROL_KEEPALIVE_ARG,
    )
    process = base_runtime._start_process(
        "research load server",
        command,
        cwd=repository_root,
        log_path=log_path,
    )
    try:
        deadline = time.monotonic() + 10.0
        published = False
        while time.monotonic() < deadline:
            if process.process.poll() is not None:
                raise ExperimentError(
                    "research iperf3 server exited before becoming ready"
                )
            if not published:
                published = base_runtime._remote_path_exists(
                    server_inventory,
                    pidfile,
                    timeout_seconds=3,
                )
            if published and _listener_ready(
                inventory,
                server_node=selected_node,
                pidfile=pidfile,
                port=port,
            ):
                break
            time.sleep(0.2)
        else:
            if not published:
                raise ExperimentError(
                    "research iperf3 server did not publish its run-owned PID file"
                )
            raise ExperimentError(
                "research iperf3 server did not prove an owned listening socket"
            )
    except Exception as exc:
        try:
            _cleanup_failed_start(
                inventory,
                server_node=selected_node,
                process=process,
                workspace=workspace,
                pidfile=pidfile,
                port=port,
            )
        except Exception as cleanup_exc:
            raise ExperimentError(
                f"research iperf3 startup failed and cleanup failed closed: {exc}; {cleanup_exc}"
            ) from exc
        raise
    return OwnedIperfServer(
        owner_id,
        selected_node,
        target,
        port,
        workspace,
        pidfile,
        process,
    )


def stop_owned_iperf_server(
    inventory: NetworkInventory,
    server: OwnedIperfServer,
) -> None:
    server_inventory = _server_inventory(inventory, server.server_node)
    errors: list[str] = []
    try:
        server.process.stop()
    except Exception as exc:
        errors.append(f"local SSH process: {exc}")
    try:
        _reap(
            inventory,
            server_node=server.server_node,
            pidfile=server.pidfile,
            port=server.port,
            orphan_only=False,
            label="run-owned research iperf3 cleanup",
        )
    except Exception as exc:
        errors.append(f"remote iperf3 process: {exc}")
    try:
        base_runtime._remote(
            server_inventory,
            "rm",
            "-f",
            server.pidfile,
            label="research iperf3 pidfile cleanup",
            timeout_seconds=10,
        )
        base_runtime._remote(
            server_inventory,
            "rmdir",
            server.workspace,
            label="research iperf3 workspace cleanup",
            timeout_seconds=10,
        )
    except Exception as exc:
        errors.append(f"remote iperf3 workspace: {exc}")
    if errors:
        raise ExperimentError(
            "research iperf3 cleanup failed closed: " + "; ".join(errors)
        )
