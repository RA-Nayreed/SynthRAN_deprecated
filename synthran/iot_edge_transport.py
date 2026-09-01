"""Experiment-local transport over an upstream-provisioned physical UE.

5g-Ansible owns reservation, deployment, radio configuration, and UE activation.
This module only exposes one already-active physical UE path to Amber, counts
experiment ingress, and removes exactly the processes/workspace it creates.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shlex
import signal
import time
from typing import Mapping, Sequence

from synthran.experiment import ExperimentError, validate_run_id
from synthran.experiment.live import ManagedProcess, _run, _start_process, _transfer_file
from synthran.fiveg_ansible import NetworkInventory
from synthran.ingress import IngressSnapshot
from synthran.iot_source import MQTTEndpoint
from synthran.live_preflight import LivePreflightError, ssh_command


PHYSICAL_AMBER_INGRESS_PORT = 18886
PHYSICAL_AMBER_LOCAL_PORT = 18886
PHYSICAL_UE_INTERFACE = "wwan0"


def _strict_ssh_base(inventory: NetworkInventory) -> tuple[list[str], str]:
    try:
        base = list(ssh_command(inventory.core_node))
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    if not base:
        raise ExperimentError("unable to construct strict SSH transport")
    return base[:-1], base[-1]


def _local_forward_command(
    inventory: NetworkInventory,
    *,
    local_port: int,
    remote_port: int,
) -> tuple[str, ...]:
    """Forward one controller loopback port to core loopback."""

    base, target = _strict_ssh_base(inventory)
    return tuple(
        base
        + [
            "-N",
            "-o",
            "ExitOnForwardFailure=yes",
            "-L",
            f"127.0.0.1:{local_port}:127.0.0.1:{remote_port}",
            target,
        ]
    )


def _proc_has_listener(port: int) -> bool:
    wanted = f"{port:04X}"
    for path in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        try:
            lines = path.read_text(encoding="ascii").splitlines()[1:]
        except OSError:
            continue
        for line in lines:
            fields = line.split()
            if (
                len(fields) >= 4
                and fields[3] == "0A"
                and fields[1].rsplit(":", 1)[-1].upper() == wanted
            ):
                return True
    return False


def _remote_listener_probe_command(
    inventory: NetworkInventory,
    *,
    port: int,
) -> tuple[str, ...]:
    script = r'''
from pathlib import Path
import sys
want = f"{int(sys.argv[1]):04X}"
for path in (Path('/proc/net/tcp'), Path('/proc/net/tcp6')):
    try:
        lines = path.read_text().splitlines()[1:]
    except OSError:
        continue
    for line in lines:
        fields = line.split()
        if len(fields) >= 4 and fields[3] == '0A' and fields[1].rsplit(':', 1)[-1].upper() == want:
            raise SystemExit(0)
raise SystemExit(1)
'''.strip()
    try:
        return ssh_command(inventory.core_node, "python3", "-c", script, str(port))
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc


def _remote_port_is_closed(inventory: NetworkInventory, port: int) -> bool:
    return _run(
        _remote_listener_probe_command(inventory, port=port),
        timeout_seconds=5,
    ).returncode != 0


def _local_port_is_closed(port: int) -> bool:
    return not _proc_has_listener(port)


def _wait_remote_listener(
    inventory: NetworkInventory,
    *,
    port: int,
    process: ManagedProcess,
    timeout_seconds: int = 30,
) -> None:
    command = _remote_listener_probe_command(inventory, port=port)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise ExperimentError(f"{process.name} exited before its listener became ready")
        if _run(command, timeout_seconds=5).returncode == 0:
            return
        time.sleep(0.25)
    raise ExperimentError(f"remote listener on port {port} did not become ready")


def _wait_local_tcp(
    port: int,
    *,
    process: ManagedProcess,
    timeout_seconds: int = 30,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise ExperimentError(f"{process.name} exited before its local forward became ready")
        if _proc_has_listener(port):
            return
        time.sleep(0.25)
    raise ExperimentError(f"local listener 127.0.0.1:{port} did not become ready")


def _remote_listener_pids(
    inventory: NetworkInventory,
    ports: Sequence[int],
) -> dict[int, tuple[int, ...]]:
    wanted = tuple(sorted({int(port) for port in ports}))
    if not wanted:
        return {}
    script = r'''
import json, os, sys
ports = {int(value) for value in sys.argv[1:]}
inodes = {port: set() for port in ports}
for table in ('/proc/net/tcp', '/proc/net/tcp6'):
    try:
        lines = open(table, encoding='ascii', errors='replace').read().splitlines()[1:]
    except OSError:
        continue
    for line in lines:
        fields = line.split()
        if len(fields) < 10 or fields[3] != '0A':
            continue
        try:
            port = int(fields[1].rsplit(':', 1)[1], 16)
        except (ValueError, IndexError):
            continue
        if port in ports:
            inodes[port].add(fields[9])
result = {port: set() for port in ports}
for entry in os.listdir('/proc'):
    if not entry.isdigit():
        continue
    try:
        names = os.listdir(f'/proc/{entry}/fd')
    except OSError:
        continue
    for name in names:
        try:
            target = os.readlink(f'/proc/{entry}/fd/{name}')
        except OSError:
            continue
        if not target.startswith('socket:[') or not target.endswith(']'):
            continue
        inode = target[8:-1]
        for port, values in inodes.items():
            if inode in values:
                result[port].add(int(entry))
print(json.dumps({str(port): sorted(values) for port, values in result.items()}, sort_keys=True))
'''.strip()
    try:
        command = ssh_command(
            inventory.core_node,
            "python3",
            "-c",
            script,
            *(str(port) for port in wanted),
        )
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    result = _run(command, timeout_seconds=10)
    if result.returncode != 0:
        raise ExperimentError("remote listener ownership observation failed")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ExperimentError("remote listener ownership observation returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ExperimentError("remote listener ownership observation returned malformed data")
    observed: dict[int, tuple[int, ...]] = {}
    for port in wanted:
        values = payload.get(str(port), [])
        if not isinstance(values, list) or not all(
            isinstance(value, int) and value > 1 for value in values
        ):
            raise ExperimentError("remote listener ownership observation returned invalid PIDs")
        observed[port] = tuple(sorted(set(values)))
    return observed


def _require_remote_owner(
    inventory: NetworkInventory,
    port: int,
    label: str,
) -> tuple[int, ...]:
    owners = _remote_listener_pids(inventory, (port,)).get(port, ())
    if not owners:
        raise ExperimentError(f"{label} ownership could not be proven")
    return owners


def _signal_remote_pids(
    inventory: NetworkInventory,
    pids: Sequence[int],
    *,
    signal_number: int,
) -> None:
    targets = tuple(sorted({int(pid) for pid in pids if int(pid) > 1}))
    if not targets:
        return
    script = r'''
import os, sys
signum = int(sys.argv[1])
for value in sys.argv[2:]:
    try:
        os.kill(int(value), signum)
    except (ProcessLookupError, PermissionError, ValueError):
        pass
'''.strip()
    try:
        command = ssh_command(
            inventory.core_node,
            "python3",
            "-c",
            script,
            str(signal_number),
            *(str(pid) for pid in targets),
        )
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    if _run(command, timeout_seconds=10).returncode != 0:
        raise ExperimentError("owned remote listener termination failed")


def _reap_owned_remote_listeners(
    inventory: NetworkInventory,
    owned: Mapping[int, Sequence[int]],
) -> tuple[str, ...]:
    errors: list[str] = []
    ports = tuple(sorted(owned))
    if not ports:
        return ()
    for signum, wait_seconds in ((signal.SIGTERM, 3.0), (signal.SIGKILL, 2.0)):
        current = _remote_listener_pids(inventory, ports)
        targets = {
            pid
            for port in ports
            for pid in set(owned.get(port, ())).intersection(current.get(port, ()))
        }
        if not targets:
            break
        try:
            _signal_remote_pids(
                inventory,
                tuple(sorted(targets)),
                signal_number=int(signum),
            )
        except Exception as exc:
            errors.append(str(exc))
            break
        deadline = time.monotonic() + wait_seconds
        while time.monotonic() < deadline:
            current = _remote_listener_pids(inventory, ports)
            if not any(
                set(owned.get(port, ())).intersection(current.get(port, ()))
                for port in ports
            ):
                break
            time.sleep(0.25)
    current = _remote_listener_pids(inventory, ports)
    for port in ports:
        expected = set(owned.get(port, ()))
        remaining = set(current.get(port, ()))
        if expected.intersection(remaining):
            errors.append(f"owned remote listener on port {port} remained after cleanup")
        if remaining.difference(expected):
            errors.append(f"remote port {port} is owned by an unrecognized process")
    return tuple(errors)


@dataclass(frozen=True)
class PhysicalUeEndpoint:
    """Physical UE management facts sourced exclusively from upstream inventory."""

    name: str
    host: str
    user: str
    mode: str
    ssh_common_args: tuple[str, ...]


def resolve_physical_ue(inventory: NetworkInventory, ue: str) -> PhysicalUeEndpoint:
    host = inventory.ue(ue.strip())
    address = host.variables.get("ansible_host", host.name)
    user = host.variables.get("ansible_user")
    mode = host.variables.get("mode", "")
    common = host.variables.get("ansible_ssh_common_args", "")
    if not address or not user:
        raise ExperimentError("selected physical UE inventory is missing SSH identity")
    if mode not in {"mbim", "qmi"}:
        raise ExperimentError("current physical Amber experiment requires an upstream modem UE")
    try:
        common_args = tuple(shlex.split(common)) if common else ()
    except ValueError as exc:
        raise ExperimentError("upstream UE SSH arguments are malformed") from exc
    return PhysicalUeEndpoint(host.name, address, user, mode, common_args)


def _physical_ue_command(endpoint: PhysicalUeEndpoint, *remote: str) -> tuple[str, ...]:
    if not remote:
        raise ExperimentError("physical UE command requires explicit remote argv")
    return (
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        *endpoint.ssh_common_args,
        f"{endpoint.user}@{endpoint.host}",
        shlex.join(remote),
    )


def prove_physical_ue_route(endpoint: PhysicalUeEndpoint, destination: str) -> None:
    result = _run(
        _physical_ue_command(
            endpoint,
            "ip",
            "-j",
            "route",
            "get",
            destination,
            "oif",
            PHYSICAL_UE_INTERFACE,
        ),
        timeout_seconds=20,
    )
    if result.returncode != 0:
        raise ExperimentError("physical UE route probe failed")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ExperimentError("physical UE route probe returned invalid JSON") from exc
    if not isinstance(payload, list) or not any(
        isinstance(item, dict) and item.get("dev") == PHYSICAL_UE_INTERFACE
        for item in payload
    ):
        raise ExperimentError(
            f"physical experiment destination is not routed through {PHYSICAL_UE_INTERFACE}"
        )


def physical_ue_counter(endpoint: PhysicalUeEndpoint, counter: str) -> int:
    if counter not in {"rx_bytes", "tx_bytes"}:
        raise ExperimentError("unsupported physical UE interface counter")
    result = _run(
        _physical_ue_command(
            endpoint,
            "cat",
            f"/sys/class/net/{PHYSICAL_UE_INTERFACE}/statistics/{counter}",
        ),
        timeout_seconds=10,
    )
    value = result.stdout.strip()
    if result.returncode != 0 or not value.isdigit():
        raise ExperimentError(f"physical UE {counter} counter is unavailable")
    return int(value)


def _physical_local_forward_command(
    endpoint: PhysicalUeEndpoint,
    *,
    local_port: int,
    target_host: str,
    target_port: int,
) -> tuple[str, ...]:
    return (
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        *endpoint.ssh_common_args,
        "-N",
        "-o",
        "ExitOnForwardFailure=yes",
        "-L",
        f"127.0.0.1:{local_port}:{target_host}:{target_port}",
        f"{endpoint.user}@{endpoint.host}",
    )


def _start_core_ingress(
    inventory: NetworkInventory,
    *,
    repository_root: Path,
    remote_workspace: str,
    listen_host: str,
    listen_port: int,
    target_port: int,
    snapshot_path: str,
    log_path: Path,
) -> ManagedProcess:
    command = (
        f"exec python3 {shlex.quote(remote_workspace + '/ingress.py')} "
        f"--listen-host {shlex.quote(listen_host)} "
        f"--listen-port {listen_port} "
        "--target-host 127.0.0.1 "
        f"--target-port {target_port} "
        f"--snapshot-path {shlex.quote(snapshot_path)}"
    )
    try:
        transport = ssh_command(inventory.core_node, "sh", "-c", command)
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    return _start_process(
        "physical Amber counted ingress",
        transport,
        cwd=repository_root,
        log_path=log_path,
    )


class PhysicalEdgeTransportSession:
    def __init__(
        self,
        *,
        inventory: NetworkInventory,
        ue: PhysicalUeEndpoint,
        endpoint: MQTTEndpoint,
        remote_ingress_port: int,
        snapshot_remote_path: str,
        remote_workspace: str,
        processes: list[ManagedProcess],
        owned_remote_pids: Mapping[int, Sequence[int]],
    ) -> None:
        self.inventory = inventory
        self.ue = ue
        self._endpoint = endpoint
        self.remote_ingress_port = remote_ingress_port
        self.snapshot_remote_path = snapshot_remote_path
        self.remote_workspace = remote_workspace
        self.processes = processes
        self.owned_remote_pids = dict(owned_remote_pids)
        self._last_snapshot: IngressSnapshot | None = None
        self._cleanup_errors: list[str] = []
        self._stopped = False

    @property
    def mqtt_endpoint(self) -> MQTTEndpoint:
        return self._endpoint

    def snapshot(self) -> IngressSnapshot:
        try:
            command = ssh_command(self.inventory.core_node, "cat", self.snapshot_remote_path)
        except LivePreflightError as exc:
            raise ExperimentError(str(exc)) from exc
        result = _run(command, timeout_seconds=10)
        if result.returncode != 0:
            raise ExperimentError("physical counted-ingress snapshot is unavailable")
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise ExperimentError("physical counted-ingress snapshot is invalid JSON") from exc
        if not isinstance(payload, dict):
            raise ExperimentError("physical counted-ingress snapshot is malformed")
        self._last_snapshot = IngressSnapshot.from_dict(payload)
        return self._last_snapshot

    def stop(self) -> None:
        if self._stopped:
            return
        try:
            self.snapshot()
        except Exception:
            pass
        for process in reversed(self.processes):
            try:
                process.stop()
            except Exception as exc:
                self._cleanup_errors.append(f"{process.name}: {exc}")
        if not _remote_port_is_closed(self.inventory, self.remote_ingress_port):
            self._cleanup_errors.extend(
                _reap_owned_remote_listeners(self.inventory, self.owned_remote_pids)
            )
        try:
            result = _run(
                ssh_command(self.inventory.core_node, "rm", "-rf", self.remote_workspace),
                timeout_seconds=10,
            )
            if result.returncode != 0:
                self._cleanup_errors.append("remote workspace cleanup failed")
        except Exception as exc:
            self._cleanup_errors.append(f"remote workspace cleanup: {exc}")
        if not _local_port_is_closed(self._endpoint.port):
            self._cleanup_errors.append("local physical publisher forward still listens")
        if not _remote_port_is_closed(self.inventory, self.remote_ingress_port):
            self._cleanup_errors.append("physical counted ingress still listens")
        self._stopped = True

    def evidence(self) -> Mapping[str, object]:
        snapshot = self._last_snapshot
        if snapshot is None and not self._stopped:
            try:
                snapshot = self.snapshot()
            except Exception:
                snapshot = None
        return {
            "backend": "physical",
            "ue": self.ue.name,
            "ue_interface": PHYSICAL_UE_INTERFACE,
            "publisher_endpoint": {
                "host": self._endpoint.host,
                "port": self._endpoint.port,
            },
            "remote_ingress": {
                "port": self.remote_ingress_port,
                "snapshot": snapshot.to_dict() if snapshot is not None else None,
            },
            "stopped": self._stopped,
            "cleanup_errors": list(self._cleanup_errors),
            "cleanup_valid": self._stopped and not self._cleanup_errors,
        }


class PhysicalEdgeTransportAdapter:
    """Expose an upstream-provisioned physical UE path to Amber publishers."""

    def __init__(
        self,
        *,
        inventory: NetworkInventory,
        ue: str,
        repository_root: Path,
        remote_ingress_port: int = PHYSICAL_AMBER_INGRESS_PORT,
        local_port: int = PHYSICAL_AMBER_LOCAL_PORT,
    ) -> None:
        self.inventory = inventory
        self.ue = resolve_physical_ue(inventory, ue)
        self.repository_root = repository_root.resolve()
        self.remote_ingress_port = remote_ingress_port
        self.local_port = local_port

    def start(
        self,
        *,
        run_id: str,
        run_directory: Path,
        core_address: str,
        central_port: int,
    ) -> PhysicalEdgeTransportSession:
        validate_run_id(run_id)
        prove_physical_ue_route(self.ue, core_address)
        if not _local_port_is_closed(self.local_port):
            raise ExperimentError("physical Amber local publisher port is already in use")
        if not _remote_port_is_closed(self.inventory, self.remote_ingress_port):
            raise ExperimentError("physical Amber counted-ingress port is already in use")

        remote_workspace = f"/tmp/synthran/{run_id}"
        logs = run_directory / "logs"
        try:
            result = _run(
                ssh_command(self.inventory.core_node, "mkdir", "-p", remote_workspace),
                timeout_seconds=10,
            )
        except LivePreflightError as exc:
            raise ExperimentError(str(exc)) from exc
        if result.returncode != 0:
            raise ExperimentError("physical Amber workspace creation failed")
        _transfer_file(
            self.inventory,
            self.repository_root / "synthran" / "ingress.py",
            f"{remote_workspace}/ingress.py",
            label="physical counted-ingress helper transfer",
        )

        snapshot_remote = f"{remote_workspace}/amber-ingress-snapshot.json"
        processes: list[ManagedProcess] = []
        owned: dict[int, tuple[int, ...]] = {}
        try:
            ingress = _start_core_ingress(
                self.inventory,
                repository_root=self.repository_root,
                remote_workspace=remote_workspace,
                listen_host=core_address,
                listen_port=self.remote_ingress_port,
                target_port=central_port,
                snapshot_path=snapshot_remote,
                log_path=logs / "physical-ingress.log",
            )
            processes.append(ingress)
            _wait_remote_listener(
                self.inventory,
                port=self.remote_ingress_port,
                process=ingress,
            )
            owned[self.remote_ingress_port] = _require_remote_owner(
                self.inventory,
                self.remote_ingress_port,
                "physical counted ingress",
            )
            publisher_forward = _start_process(
                "physical UE publisher SSH forward",
                _physical_local_forward_command(
                    self.ue,
                    local_port=self.local_port,
                    target_host=core_address,
                    target_port=self.remote_ingress_port,
                ),
                cwd=self.repository_root,
                log_path=logs / "physical-publisher-forward.log",
            )
            processes.append(publisher_forward)
            _wait_local_tcp(self.local_port, process=publisher_forward)
        except Exception:
            for process in reversed(processes):
                try:
                    process.stop()
                except Exception:
                    pass
            if owned:
                try:
                    _reap_owned_remote_listeners(self.inventory, owned)
                except Exception:
                    pass
            try:
                _run(
                    ssh_command(self.inventory.core_node, "rm", "-rf", remote_workspace),
                    timeout_seconds=10,
                )
            except Exception:
                pass
            raise

        return PhysicalEdgeTransportSession(
            inventory=self.inventory,
            ue=self.ue,
            endpoint=MQTTEndpoint("127.0.0.1", self.local_port),
            remote_ingress_port=self.remote_ingress_port,
            snapshot_remote_path=snapshot_remote,
            remote_workspace=remote_workspace,
            processes=processes,
            owned_remote_pids=owned,
        )


__all__ = (
    "PHYSICAL_UE_INTERFACE",
    "PhysicalEdgeTransportAdapter",
    "PhysicalEdgeTransportSession",
    "_local_forward_command",
    "_local_port_is_closed",
    "_remote_port_is_closed",
    "_start_process",
    "_wait_local_tcp",
    "physical_ue_counter",
    "prove_physical_ue_route",
    "resolve_physical_ue",
)
