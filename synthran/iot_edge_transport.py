"""Experiment-side transports for Amber publishers over upstream 5G deployments.

5g-Ansible owns deployment and UE setup.  This module only exposes an already
provisioned UE path to the Amber publishers, counts ingress, and proves exact
experiment-local cleanup.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shlex
import signal
import time
from typing import Any, Mapping, Protocol, Sequence

from synthran.experiment import ExperimentError, validate_run_id
from synthran.experiment.live import ManagedProcess, _run, _start_process, _transfer_file
from synthran.fiveg_ansible import NetworkInventory
from synthran.ingress import IngressSnapshot
from synthran.iot_source import MQTTEndpoint
from synthran.live_preflight import LivePreflightError, ssh_command


RFSIM_EDGE_FORWARD_PORT = 18883
RFSIM_AMBER_INGRESS_PORT = 18886
RFSIM_AMBER_LOCAL_PORT = 18886
PHYSICAL_AMBER_INGRESS_PORT = 18886
PHYSICAL_AMBER_LOCAL_PORT = 18886
PHYSICAL_UE_INTERFACE = "wwan0"
KUBERNETES_NAMESPACE = "open5gs"

# Temporary source compatibility while the last r2lab callers are deleted in
# this PR.  No separate process implementation exists here.
OwnedProcess = ManagedProcess


class EdgeTransportAdapter(Protocol):
    def start(self, **kwargs: Any) -> "EdgeTransportSession": ...


class EdgeTransportSession(Protocol):
    @property
    def mqtt_endpoint(self) -> MQTTEndpoint: ...

    def snapshot(self) -> IngressSnapshot: ...

    def stop(self) -> None: ...

    def evidence(self) -> Mapping[str, Any]: ...


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


def _remote_tcp_probe_command(
    inventory: NetworkInventory,
    *,
    port: int,
) -> tuple[str, ...]:
    script = (
        "import socket,sys; s=socket.socket(); s.settimeout(1); "
        "s.connect(('127.0.0.1',int(sys.argv[1]))); s.close()"
    )
    try:
        return ssh_command(inventory.core_node, "python3", "-c", script, str(port))
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc


def _wait_remote_tcp(
    inventory: NetworkInventory,
    *,
    port: int,
    process: ManagedProcess,
    timeout_seconds: int = 30,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    command = _remote_tcp_probe_command(inventory, port=port)
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise ExperimentError(
                f"{process.name} exited before remote TCP endpoint became ready"
            )
        if _run(command, timeout_seconds=5).returncode == 0:
            return
        time.sleep(0.25)
    raise ExperimentError(f"remote TCP endpoint 127.0.0.1:{port} did not become ready")


def _wait_remote_listener(
    inventory: NetworkInventory,
    *,
    port: int,
    process: ManagedProcess,
    timeout_seconds: int = 30,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    command = _remote_listener_probe_command(inventory, port=port)
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise ExperimentError(
                f"{process.name} exited before remote listener became ready"
            )
        if _run(command, timeout_seconds=5).returncode == 0:
            return
        time.sleep(0.25)
    raise ExperimentError(f"remote listener 127.0.0.1:{port} did not become ready")


def _wait_local_tcp(
    port: int,
    *,
    process: ManagedProcess,
    timeout_seconds: int = 30,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise ExperimentError(
                f"{process.name} exited before local TCP endpoint became ready"
            )
        if _proc_has_listener(port):
            return
        time.sleep(0.25)
    raise ExperimentError(f"local listener 127.0.0.1:{port} did not become ready")


def _remote_port_is_closed(inventory: NetworkInventory, port: int) -> bool:
    return _run(
        _remote_listener_probe_command(inventory, port=port),
        timeout_seconds=5,
    ).returncode != 0


def _local_port_is_closed(port: int) -> bool:
    return not _proc_has_listener(port)


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
    except (FileNotFoundError, PermissionError, OSError):
        continue
    for name in names:
        try:
            target = os.readlink(f'/proc/{entry}/fd/{name}')
        except (FileNotFoundError, PermissionError, OSError):
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
        raise ExperimentError("remote listener ownership probe failed")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ExperimentError("remote listener ownership probe returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ExperimentError("remote listener ownership probe returned invalid data")
    observed: dict[int, tuple[int, ...]] = {}
    for port in wanted:
        values = payload.get(str(port), [])
        if not isinstance(values, list) or not all(
            isinstance(value, int) and value > 1 for value in values
        ):
            raise ExperimentError("remote listener ownership probe returned invalid PIDs")
        observed[port] = tuple(sorted(set(values)))
    return observed


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
        try:
            current = _remote_listener_pids(inventory, ports)
        except Exception as exc:
            return (f"remote listener ownership reproof failed: {exc}",)
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
            errors.append(f"owned remote listener termination failed: {exc}")
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
    try:
        current = _remote_listener_pids(inventory, ports)
    except Exception as exc:
        return tuple(errors + [f"remote listener ownership reproof failed: {exc}"])
    for port in ports:
        expected = set(owned.get(port, ()))
        remaining = set(current.get(port, ()))
        if expected.intersection(remaining):
            errors.append(f"owned remote listener on port {port} remained after cleanup")
        if remaining.difference(expected):
            errors.append(f"remote port {port} is now owned by an unrecognized process")
    return tuple(errors)


class RfsimEdgeTransportSession:
    def __init__(
        self,
        *,
        inventory: NetworkInventory,
        endpoint: MQTTEndpoint,
        edge_forward_port: int,
        remote_ingress_port: int,
        snapshot_remote_path: str,
        processes: list[ManagedProcess],
        owned_remote_pids: Mapping[int, Sequence[int]],
    ) -> None:
        self.inventory = inventory
        self._endpoint = endpoint
        self.edge_forward_port = edge_forward_port
        self.remote_ingress_port = remote_ingress_port
        self.snapshot_remote_path = snapshot_remote_path
        self.processes = processes
        self.owned_remote_pids = {
            int(port): tuple(sorted({int(pid) for pid in pids}))
            for port, pids in owned_remote_pids.items()
        }
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
            raise ExperimentError("RFSIM Amber ingress snapshot is unavailable")
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise ExperimentError("RFSIM Amber ingress snapshot is invalid JSON") from exc
        if not isinstance(value, dict):
            raise ExperimentError("RFSIM Amber ingress snapshot must be a JSON object")
        return IngressSnapshot.from_dict(value)

    def stop(self) -> None:
        if self._stopped:
            return
        for process in reversed(self.processes):
            try:
                process.stop()
            except Exception as exc:
                self._cleanup_errors.append(f"{process.name}: {exc}")
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            if (
                _local_port_is_closed(self._endpoint.port)
                and _remote_port_is_closed(self.inventory, self.remote_ingress_port)
                and _remote_port_is_closed(self.inventory, self.edge_forward_port)
            ):
                break
            time.sleep(0.25)
        if not (
            _remote_port_is_closed(self.inventory, self.remote_ingress_port)
            and _remote_port_is_closed(self.inventory, self.edge_forward_port)
        ):
            self._cleanup_errors.extend(
                _reap_owned_remote_listeners(self.inventory, self.owned_remote_pids)
            )
        if not _local_port_is_closed(self._endpoint.port):
            self._cleanup_errors.append("local publisher forward still listens")
        if not _remote_port_is_closed(self.inventory, self.remote_ingress_port):
            self._cleanup_errors.append("remote counted ingress still listens")
        if not _remote_port_is_closed(self.inventory, self.edge_forward_port):
            self._cleanup_errors.append("remote edge MQTT forward still listens")
        self._stopped = True

    def evidence(self) -> Mapping[str, Any]:
        snapshot = None
        if not self._stopped:
            try:
                snapshot = self.snapshot().to_dict()
            except Exception:
                snapshot = None
        return {
            "backend": "rfsim",
            "publisher_endpoint": {
                "host": self._endpoint.host,
                "port": self._endpoint.port,
            },
            "remote_ingress": {
                "host": "127.0.0.1",
                "port": self.remote_ingress_port,
                "snapshot": snapshot,
            },
            "remote_edge_forward": {
                "host": "127.0.0.1",
                "port": self.edge_forward_port,
            },
            "stopped": self._stopped,
            "cleanup_errors": list(self._cleanup_errors),
            "cleanup_valid": self._stopped and not self._cleanup_errors,
        }


class RfsimEdgeTransportAdapter:
    """Expose the upstream-provisioned RFSIM UE edge broker to Amber."""

    def __init__(
        self,
        *,
        inventory: NetworkInventory,
        repository_root: Path,
        edge_forward_port: int = RFSIM_EDGE_FORWARD_PORT,
        remote_ingress_port: int = RFSIM_AMBER_INGRESS_PORT,
        local_port: int = RFSIM_AMBER_LOCAL_PORT,
    ) -> None:
        self.inventory = inventory
        self.repository_root = repository_root.resolve()
        self.edge_forward_port = edge_forward_port
        self.remote_ingress_port = remote_ingress_port
        self.local_port = local_port

    def start(
        self,
        *,
        run_id: str,
        ue_pod: str,
        remote_workspace: str,
        run_directory: Path,
    ) -> RfsimEdgeTransportSession:
        validate_run_id(run_id)
        if not ue_pod.strip():
            raise ExperimentError("RFSIM edge transport requires the live UE pod")
        if not remote_workspace.startswith("/tmp/synthran/"):
            raise ExperimentError("RFSIM edge workspace is outside the run-owned root")
        for port in (self.remote_ingress_port, self.edge_forward_port):
            if not _remote_port_is_closed(self.inventory, port):
                raise ExperimentError(f"RFSIM experiment port is already in use: {port}")
        if not _local_port_is_closed(self.local_port):
            raise ExperimentError("RFSIM Amber local publisher port is already in use")

        snapshot_remote = f"{remote_workspace}/amber-ingress-snapshot.json"
        logs = run_directory / "logs"
        processes: list[ManagedProcess] = []
        owned: dict[int, tuple[int, ...]] = {}
        edge_forward = _start_process(
            "RFSIM edge MQTT port-forward",
            ssh_command(
                self.inventory.core_node,
                "sh",
                "-c",
                "KUBECONFIG=/etc/kubernetes/admin.conf kubectl port-forward "
                f"-n {KUBERNETES_NAMESPACE} pod/{shlex.quote(ue_pod)} "
                f"{self.edge_forward_port}:1883 --address 127.0.0.1",
            ),
            cwd=self.repository_root,
            log_path=logs / "amber-edge-port-forward.log",
        )
        processes.append(edge_forward)
        try:
            _wait_remote_tcp(
                self.inventory,
                port=self.edge_forward_port,
                process=edge_forward,
            )
            owned[self.edge_forward_port] = _require_remote_owner(
                self.inventory, self.edge_forward_port, "RFSIM edge MQTT forward"
            )
            ingress = _start_core_ingress(
                self.inventory,
                repository_root=self.repository_root,
                remote_workspace=remote_workspace,
                listen_port=self.remote_ingress_port,
                target_host="127.0.0.1",
                target_port=self.edge_forward_port,
                snapshot_path=snapshot_remote,
                log_path=logs / "amber-ingress.log",
            )
            processes.append(ingress)
            _wait_remote_listener(
                self.inventory,
                port=self.remote_ingress_port,
                process=ingress,
            )
            owned[self.remote_ingress_port] = _require_remote_owner(
                self.inventory, self.remote_ingress_port, "RFSIM counted ingress"
            )
            local_forward = _start_process(
                "RFSIM Amber publisher SSH forward",
                _local_forward_command(
                    self.inventory,
                    local_port=self.local_port,
                    remote_port=self.remote_ingress_port,
                ),
                cwd=self.repository_root,
                log_path=logs / "amber-publisher-forward.log",
            )
            processes.append(local_forward)
            _wait_local_tcp(self.local_port, process=local_forward)
        except Exception:
            for process in reversed(processes):
                process.stop()
            if owned:
                _reap_owned_remote_listeners(self.inventory, owned)
            raise
        return RfsimEdgeTransportSession(
            inventory=self.inventory,
            endpoint=MQTTEndpoint("127.0.0.1", self.local_port),
            edge_forward_port=self.edge_forward_port,
            remote_ingress_port=self.remote_ingress_port,
            snapshot_remote_path=snapshot_remote,
            processes=processes,
            owned_remote_pids=owned,
        )


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
        raise ExperimentError(
            "current physical Amber experiment requires an upstream modem UE"
        )
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


def _require_remote_owner(
    inventory: NetworkInventory,
    port: int,
    label: str,
) -> tuple[int, ...]:
    owners = _remote_listener_pids(inventory, (port,)).get(port, ())
    if not owners:
        raise ExperimentError(f"{label} ownership could not be proven")
    return owners


def _start_core_ingress(
    inventory: NetworkInventory,
    *,
    repository_root: Path,
    remote_workspace: str,
    listen_port: int,
    target_host: str,
    target_port: int,
    snapshot_path: str,
    log_path: Path,
) -> ManagedProcess:
    ingress_helper = f"{remote_workspace}/ingress.py"
    command = (
        f"exec python3 {shlex.quote(ingress_helper)} "
        "--listen-host 127.0.0.1 "
        f"--listen-port {listen_port} "
        f"--target-host {shlex.quote(target_host)} "
        f"--target-port {target_port} "
        f"--snapshot-path {shlex.quote(snapshot_path)}"
    )
    return _start_process(
        "Amber counted ingress",
        ssh_command(inventory.core_node, "sh", "-c", command),
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
                ssh_command(
                    self.inventory.core_node,
                    "rm",
                    "-rf",
                    self.remote_workspace,
                ),
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

    def evidence(self) -> Mapping[str, Any]:
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
                "host": "127.0.0.1",
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
                ssh_command(
                    self.inventory.core_node,
                    "mkdir",
                    "-p",
                    remote_workspace,
                ),
                timeout_seconds=10,
            )
            if result.returncode != 0:
                raise ExperimentError("physical Amber workspace creation failed")
            _transfer_file(
                self.inventory,
                self.repository_root / "synthran" / "ingress.py",
                f"{remote_workspace}/ingress.py",
                label="physical counted-ingress helper transfer",
            )
        except Exception:
            raise

        snapshot_remote = f"{remote_workspace}/amber-ingress-snapshot.json"
        processes: list[ManagedProcess] = []
        owned: dict[int, tuple[int, ...]] = {}
        ingress = _start_core_ingress(
            self.inventory,
            repository_root=self.repository_root,
            remote_workspace=remote_workspace,
            listen_port=self.remote_ingress_port,
            target_host="127.0.0.1",
            target_port=central_port,
            snapshot_path=snapshot_remote,
            log_path=logs / "physical-ingress.log",
        )
        processes.append(ingress)
        try:
            _wait_remote_listener(
                self.inventory,
                port=self.remote_ingress_port,
                process=ingress,
            )
            owned[self.remote_ingress_port] = _require_remote_owner(
                self.inventory, self.remote_ingress_port, "physical counted ingress"
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
                process.stop()
            if owned:
                _reap_owned_remote_listeners(self.inventory, owned)
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
