"""Experiment-local Amber transports over upstream-provisioned 5G paths.

5g-Ansible owns deployment and UE setup. This module only creates bounded
transport processes and routes for one experiment, records their ownership, and
removes exactly what it created.
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
RFSIM_UE_RELAY_PORT = 1883
RFSIM_UE_INTERFACE = "tun_srsue1"
PHYSICAL_AMBER_INGRESS_PORT = 18886
PHYSICAL_AMBER_LOCAL_PORT = 18886
PHYSICAL_UE_INTERFACE = "wwan0"
KUBERNETES_NAMESPACE = "open5gs"


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
    dependency: ManagedProcess | None = None,
    timeout_seconds: int = 30,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    command = _remote_tcp_probe_command(inventory, port=port)
    while time.monotonic() < deadline:
        for candidate in (process, dependency):
            if candidate is not None and candidate.process.poll() is not None:
                raise ExperimentError(
                    f"{candidate.name} exited before remote TCP endpoint became ready"
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
    try:
        transport = ssh_command(inventory.core_node, "sh", "-c", command)
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    return _start_process(
        "Amber counted ingress",
        transport,
        cwd=repository_root,
        log_path=log_path,
    )


def _kubectl_exec_command(
    inventory: NetworkInventory,
    pod: str,
    *argv: str,
) -> tuple[str, ...]:
    try:
        return ssh_command(
            inventory.core_node,
            "sh",
            "-c",
            "exec env KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
            f"-n {KUBERNETES_NAMESPACE} {shlex.quote(pod)} -c ue -- "
            + shlex.join(argv),
        )
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc


def _rfsim_route(
    inventory: NetworkInventory,
    pod: str,
    destination: str,
) -> list[Mapping[str, Any]]:
    result = _run(
        _kubectl_exec_command(inventory, pod, "ip", "-j", "route", "get", destination),
        timeout_seconds=15,
    )
    if result.returncode != 0:
        raise ExperimentError("RFSIM UE route probe failed")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ExperimentError("RFSIM UE route probe returned invalid JSON") from exc
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise ExperimentError("RFSIM UE route probe returned malformed data")
    return payload


def _install_rfsim_route(
    inventory: NetworkInventory,
    pod: str,
    *,
    destination: str,
) -> bool:
    current = _rfsim_route(inventory, pod, destination)
    if any(item.get("dev") == RFSIM_UE_INTERFACE for item in current):
        return False
    prefix = f"{destination}/32"
    result = _run(
        _kubectl_exec_command(
            inventory,
            pod,
            "ip",
            "route",
            "add",
            prefix,
            "dev",
            RFSIM_UE_INTERFACE,
        ),
        timeout_seconds=15,
    )
    if result.returncode != 0:
        raise ExperimentError(
            "RFSIM experiment route could not be added without replacing existing state"
        )
    proven = _rfsim_route(inventory, pod, destination)
    if not any(item.get("dev") == RFSIM_UE_INTERFACE for item in proven):
        raise ExperimentError("RFSIM experiment route did not bind to tun_srsue1")
    return True


def _remove_rfsim_route(
    inventory: NetworkInventory,
    pod: str,
    *,
    destination: str,
) -> None:
    prefix = f"{destination}/32"
    result = _run(
        _kubectl_exec_command(
            inventory,
            pod,
            "ip",
            "route",
            "del",
            prefix,
            "dev",
            RFSIM_UE_INTERFACE,
        ),
        timeout_seconds=15,
    )
    if result.returncode != 0:
        raise ExperimentError("RFSIM experiment route cleanup failed")


def _ue_relay_command(
    inventory: NetworkInventory,
    *,
    pod: str,
    pdu_address: str,
    target_host: str,
    target_port: int,
    marker: str,
) -> tuple[str, ...]:
    script = r'''
import socket, sys, threading
listen_port = int(sys.argv[1])
source_ip = sys.argv[2]
target_host = sys.argv[3]
target_port = int(sys.argv[4])
marker = sys.argv[5]

def pump(src, dst):
    try:
        while True:
            data = src.recv(65536)
            if not data:
                break
            dst.sendall(data)
    except OSError:
        pass
    finally:
        try: dst.shutdown(socket.SHUT_WR)
        except OSError: pass

def handle(client):
    upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        upstream.setsockopt(socket.SOL_SOCKET, 25, b'tun_srsue1\0')
        upstream.bind((source_ip, 0))
        upstream.settimeout(10)
        upstream.connect((target_host, target_port))
        upstream.settimeout(None)
        a = threading.Thread(target=pump, args=(client, upstream), daemon=True)
        b = threading.Thread(target=pump, args=(upstream, client), daemon=True)
        a.start(); b.start(); a.join(); b.join()
    finally:
        try: client.close()
        except OSError: pass
        try: upstream.close()
        except OSError: pass

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('127.0.0.1', listen_port))
server.listen(32)
while True:
    client, _ = server.accept()
    threading.Thread(target=handle, args=(client,), daemon=True).start()
'''.strip()
    return _kubectl_exec_command(
        inventory,
        pod,
        "python3",
        "-u",
        "-c",
        script,
        str(RFSIM_UE_RELAY_PORT),
        pdu_address,
        target_host,
        str(target_port),
        marker,
    )


def _cleanup_ue_relay(
    inventory: NetworkInventory,
    *,
    pod: str,
    marker: str,
) -> None:
    script = r'''
import os, signal, sys
marker = sys.argv[1].encode()
for value in os.listdir('/proc'):
    if not value.isdigit() or int(value) <= 1:
        continue
    try:
        command = open(f'/proc/{value}/cmdline', 'rb').read()
    except OSError:
        continue
    if marker in command:
        try: os.kill(int(value), signal.SIGTERM)
        except (ProcessLookupError, PermissionError): pass
'''.strip()
    result = _run(
        _kubectl_exec_command(inventory, pod, "python3", "-c", script, marker),
        timeout_seconds=15,
    )
    if result.returncode != 0:
        raise ExperimentError("RFSIM UE relay cleanup failed")


class RfsimEdgeTransportSession:
    def __init__(
        self,
        *,
        inventory: NetworkInventory,
        ue_pod: str,
        endpoint: MQTTEndpoint,
        edge_forward_port: int,
        remote_ingress_port: int,
        snapshot_remote_path: str,
        processes: list[ManagedProcess],
        owned_remote_pids: Mapping[int, Sequence[int]],
        relay_marker: str,
        route_destination: str,
        route_installed: bool,
    ) -> None:
        self.inventory = inventory
        self.ue_pod = ue_pod
        self._endpoint = endpoint
        self.edge_forward_port = edge_forward_port
        self.remote_ingress_port = remote_ingress_port
        self.snapshot_remote_path = snapshot_remote_path
        self.processes = processes
        self.owned_remote_pids = {
            int(port): tuple(sorted({int(pid) for pid in pids}))
            for port, pids in owned_remote_pids.items()
        }
        self.relay_marker = relay_marker
        self.route_destination = route_destination
        self.route_installed = route_installed
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
            raise ExperimentError("RFSIM Amber ingress snapshot is unavailable")
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise ExperimentError("RFSIM Amber ingress snapshot is invalid JSON") from exc
        if not isinstance(value, dict):
            raise ExperimentError("RFSIM Amber ingress snapshot must be a JSON object")
        self._last_snapshot = IngressSnapshot.from_dict(value)
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
        try:
            _cleanup_ue_relay(
                self.inventory,
                pod=self.ue_pod,
                marker=self.relay_marker,
            )
        except Exception as exc:
            self._cleanup_errors.append(f"UE relay cleanup: {exc}")
        if self.route_installed:
            try:
                _remove_rfsim_route(
                    self.inventory,
                    self.ue_pod,
                    destination=self.route_destination,
                )
            except Exception as exc:
                self._cleanup_errors.append(f"UE route cleanup: {exc}")
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
            self._cleanup_errors.append("remote UE port-forward still listens")
        self._stopped = True

    def evidence(self) -> Mapping[str, Any]:
        snapshot = self._last_snapshot
        if snapshot is None and not self._stopped:
            try:
                snapshot = self.snapshot()
            except Exception:
                snapshot = None
        return {
            "backend": "rfsim",
            "ue_interface": RFSIM_UE_INTERFACE,
            "publisher_endpoint": {
                "host": self._endpoint.host,
                "port": self._endpoint.port,
            },
            "remote_ingress": {
                "host": "127.0.0.1",
                "port": self.remote_ingress_port,
                "snapshot": snapshot.to_dict() if snapshot is not None else None,
            },
            "ue_relay": {"pod": self.ue_pod, "port": RFSIM_UE_RELAY_PORT},
            "route_created": self.route_installed,
            "stopped": self._stopped,
            "cleanup_errors": list(self._cleanup_errors),
            "cleanup_valid": self._stopped and not self._cleanup_errors,
        }


class RfsimEdgeTransportAdapter:
    """Expose an already-ready RFSIM UE without modifying its Deployment."""

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
        pdu_address: str,
        core_address: str,
        central_port: int,
        remote_workspace: str,
        run_directory: Path,
    ) -> RfsimEdgeTransportSession:
        run_id = validate_run_id(run_id)
        if not ue_pod.strip():
            raise ExperimentError("RFSIM transport requires the live UE pod")
        if not remote_workspace.startswith("/tmp/synthran/"):
            raise ExperimentError("RFSIM edge workspace is outside the run-owned root")
        for port in (self.remote_ingress_port, self.edge_forward_port):
            if not _remote_port_is_closed(self.inventory, port):
                raise ExperimentError(f"RFSIM experiment port is already in use: {port}")
        if not _local_port_is_closed(self.local_port):
            raise ExperimentError("RFSIM Amber local publisher port is already in use")

        route_installed = _install_rfsim_route(
            self.inventory,
            ue_pod,
            destination=core_address,
        )
        snapshot_remote = f"{remote_workspace}/amber-ingress-snapshot.json"
        logs = run_directory / "logs"
        processes: list[ManagedProcess] = []
        owned: dict[int, tuple[int, ...]] = {}
        marker = f"synthran-relay-{run_id}"
        try:
            ingress = _start_core_ingress(
                self.inventory,
                repository_root=self.repository_root,
                remote_workspace=remote_workspace,
                listen_port=self.remote_ingress_port,
                target_host="127.0.0.1",
                target_port=central_port,
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

            relay = _start_process(
                "RFSIM UE PDU relay",
                _ue_relay_command(
                    self.inventory,
                    pod=ue_pod,
                    pdu_address=pdu_address,
                    target_host=core_address,
                    target_port=self.remote_ingress_port,
                    marker=marker,
                ),
                cwd=self.repository_root,
                log_path=logs / "amber-ue-relay.log",
            )
            processes.append(relay)

            edge_forward = _start_process(
                "RFSIM UE port-forward",
                ssh_command(
                    self.inventory.core_node,
                    "sh",
                    "-c",
                    "exec env KUBECONFIG=/etc/kubernetes/admin.conf kubectl port-forward "
                    f"-n {KUBERNETES_NAMESPACE} pod/{shlex.quote(ue_pod)} "
                    f"{self.edge_forward_port}:{RFSIM_UE_RELAY_PORT} --address 127.0.0.1",
                ),
                cwd=self.repository_root,
                log_path=logs / "amber-ue-port-forward.log",
            )
            processes.append(edge_forward)
            _wait_remote_tcp(
                self.inventory,
                port=self.edge_forward_port,
                process=edge_forward,
                dependency=relay,
            )
            owned[self.edge_forward_port] = _require_remote_owner(
                self.inventory, self.edge_forward_port, "RFSIM UE port-forward"
            )

            local_forward = _start_process(
                "RFSIM Amber publisher SSH forward",
                _local_forward_command(
                    self.inventory,
                    local_port=self.local_port,
                    remote_port=self.edge_forward_port,
                ),
                cwd=self.repository_root,
                log_path=logs / "amber-publisher-forward.log",
            )
            processes.append(local_forward)
            _wait_local_tcp(self.local_port, process=local_forward)
        except Exception:
            for process in reversed(processes):
                try:
                    process.stop()
                except Exception:
                    pass
            try:
                _cleanup_ue_relay(self.inventory, pod=ue_pod, marker=marker)
            except Exception:
                pass
            if route_installed:
                try:
                    _remove_rfsim_route(
                        self.inventory,
                        ue_pod,
                        destination=core_address,
                    )
                except Exception:
                    pass
            if owned:
                _reap_owned_remote_listeners(self.inventory, owned)
            raise

        return RfsimEdgeTransportSession(
            inventory=self.inventory,
            ue_pod=ue_pod,
            endpoint=MQTTEndpoint("127.0.0.1", self.local_port),
            edge_forward_port=self.edge_forward_port,
            remote_ingress_port=self.remote_ingress_port,
            snapshot_remote_path=snapshot_remote,
            processes=processes,
            owned_remote_pids=owned,
            relay_marker=marker,
            route_destination=core_address,
            route_installed=route_installed,
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
                try:
                    process.stop()
                except Exception:
                    pass
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
