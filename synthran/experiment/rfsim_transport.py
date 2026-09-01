"""Experiment-local Amber transport over an upstream-provisioned RFSIM UE.

The network deployment is immutable from this module's point of view.  It may
create only run-owned transport processes plus one exact /32 route when the
current UE route does not already select ``tun_srsue1``.  It never patches a
Deployment, restarts the gNB/UE, or invokes 5g-Ansible mutation verbs.
"""

from __future__ import annotations

import json
from pathlib import Path
import shlex
import signal
import time
from typing import Any, Mapping, Sequence

from synthran.experiment import ExperimentError, validate_run_id
from synthran.experiment.live import ManagedProcess, _run, _start_process, _transfer_file
from synthran.fiveg_ansible import NetworkInventory
from synthran.ingress import IngressSnapshot
from synthran.iot_source import MQTTEndpoint
from synthran.live_preflight import LivePreflightError, ssh_command


KUBERNETES_NAMESPACE = "open5gs"
UE_INTERFACE = "tun_srsue1"
UE_RELAY_PORT = 1883
CORE_UE_FORWARD_PORT = 18883
CORE_INGRESS_PORT = 18886
LOCAL_PUBLISHER_PORT = 18886


def _kubectl_exec_command(
    inventory: NetworkInventory,
    ue_pod: str,
    *argv: str,
) -> tuple[str, ...]:
    try:
        return ssh_command(
            inventory.core_node,
            "sh",
            "-c",
            "exec env KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
            f"-n {KUBERNETES_NAMESPACE} {shlex.quote(ue_pod)} -c ue -- "
            + shlex.join(argv),
        )
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc


def _route(inventory: NetworkInventory, ue_pod: str, destination: str) -> list[Mapping[str, Any]]:
    result = _run(
        _kubectl_exec_command(inventory, ue_pod, "ip", "-j", "route", "get", destination),
        timeout_seconds=15,
    )
    if result.returncode != 0:
        raise ExperimentError("RFSIM UE route observation failed")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ExperimentError("RFSIM UE route observation returned invalid JSON") from exc
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise ExperimentError("RFSIM UE route observation returned malformed data")
    return list(payload)


def _install_route(inventory: NetworkInventory, ue_pod: str, destination: str) -> bool:
    if any(item.get("dev") == UE_INTERFACE for item in _route(inventory, ue_pod, destination)):
        return False
    prefix = f"{destination}/32"
    result = _run(
        _kubectl_exec_command(
            inventory,
            ue_pod,
            "ip",
            "route",
            "add",
            prefix,
            "dev",
            UE_INTERFACE,
        ),
        timeout_seconds=15,
    )
    if result.returncode != 0:
        raise ExperimentError(
            "RFSIM experiment route could not be added without replacing existing state"
        )
    if not any(item.get("dev") == UE_INTERFACE for item in _route(inventory, ue_pod, destination)):
        try:
            _remove_route(inventory, ue_pod, destination)
        except Exception:
            pass
        raise ExperimentError("RFSIM experiment route did not bind to tun_srsue1")
    return True


def _remove_route(inventory: NetworkInventory, ue_pod: str, destination: str) -> None:
    result = _run(
        _kubectl_exec_command(
            inventory,
            ue_pod,
            "ip",
            "route",
            "del",
            f"{destination}/32",
            "dev",
            UE_INTERFACE,
        ),
        timeout_seconds=15,
    )
    if result.returncode != 0:
        raise ExperimentError("RFSIM experiment route cleanup failed")


def _listener_probe_command(inventory: NetworkInventory, port: int) -> tuple[str, ...]:
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


def _remote_port_closed(inventory: NetworkInventory, port: int) -> bool:
    return _run(_listener_probe_command(inventory, port), timeout_seconds=5).returncode != 0


def _local_port_closed(port: int) -> bool:
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
                return False
    return True


def _remote_listener_pids(
    inventory: NetworkInventory,
    ports: Sequence[int],
) -> dict[int, tuple[int, ...]]:
    wanted = tuple(sorted({int(port) for port in ports}))
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
        raise ExperimentError("RFSIM remote listener ownership observation failed")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ExperimentError("RFSIM remote listener ownership returned invalid JSON") from exc
    if not isinstance(payload, Mapping):
        raise ExperimentError("RFSIM remote listener ownership returned malformed data")
    observed: dict[int, tuple[int, ...]] = {}
    for port in wanted:
        values = payload.get(str(port), [])
        if not isinstance(values, list) or not all(isinstance(value, int) and value > 1 for value in values):
            raise ExperimentError("RFSIM remote listener ownership returned invalid PIDs")
        observed[port] = tuple(sorted(set(values)))
    return observed


def _require_owner(inventory: NetworkInventory, port: int, label: str) -> tuple[int, ...]:
    owners = _remote_listener_pids(inventory, (port,)).get(port, ())
    if not owners:
        raise ExperimentError(f"{label} ownership could not be proven")
    return owners


def _signal_remote_pids(
    inventory: NetworkInventory,
    pids: Sequence[int],
    signum: int,
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
            str(signum),
            *(str(pid) for pid in targets),
        )
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    if _run(command, timeout_seconds=10).returncode != 0:
        raise ExperimentError("RFSIM owned listener termination failed")


def _reap_owned_listeners(
    inventory: NetworkInventory,
    owned: Mapping[int, Sequence[int]],
) -> tuple[str, ...]:
    errors: list[str] = []
    ports = tuple(sorted(owned))
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
            _signal_remote_pids(inventory, tuple(sorted(targets)), int(signum))
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


def _wait_remote_listener(
    inventory: NetworkInventory,
    port: int,
    process: ManagedProcess,
    *,
    timeout_seconds: int = 30,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    command = _listener_probe_command(inventory, port)
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise ExperimentError(f"{process.name} exited before its listener became ready")
        if _run(command, timeout_seconds=5).returncode == 0:
            return
        time.sleep(0.25)
    raise ExperimentError(f"remote listener on port {port} did not become ready")


def _wait_remote_tcp(
    inventory: NetworkInventory,
    port: int,
    process: ManagedProcess,
    dependency: ManagedProcess,
    *,
    timeout_seconds: int = 30,
) -> None:
    script = (
        "import socket,sys; s=socket.socket(); s.settimeout(1); "
        "s.connect(('127.0.0.1',int(sys.argv[1]))); s.close()"
    )
    try:
        command = ssh_command(inventory.core_node, "python3", "-c", script, str(port))
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        for candidate in (process, dependency):
            if candidate.process.poll() is not None:
                raise ExperimentError(f"{candidate.name} exited before the UE relay became reachable")
        if _run(command, timeout_seconds=5).returncode == 0:
            return
        time.sleep(0.25)
    raise ExperimentError("RFSIM UE relay did not become reachable")


def _wait_local_listener(port: int, process: ManagedProcess, *, timeout_seconds: int = 30) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise ExperimentError(f"{process.name} exited before its local forward became ready")
        if not _local_port_closed(port):
            return
        time.sleep(0.25)
    raise ExperimentError(f"local listener on port {port} did not become ready")


def _ue_relay_command(
    inventory: NetworkInventory,
    ue_pod: str,
    pdu_address: str,
    core_address: str,
    ingress_port: int,
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
        ue_pod,
        "python3",
        "-u",
        "-c",
        script,
        str(UE_RELAY_PORT),
        pdu_address,
        core_address,
        str(ingress_port),
        marker,
    )


def _cleanup_ue_relay(inventory: NetworkInventory, ue_pod: str, marker: str) -> None:
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
        _kubectl_exec_command(inventory, ue_pod, "python3", "-c", script, marker),
        timeout_seconds=15,
    )
    if result.returncode != 0:
        raise ExperimentError("RFSIM UE relay cleanup failed")


def _start_counted_ingress(
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
        "RFSIM counted ingress",
        transport,
        cwd=repository_root,
        log_path=log_path,
    )


def _core_local_forward(
    inventory: NetworkInventory,
    *,
    local_port: int,
    remote_port: int,
) -> tuple[str, ...]:
    try:
        base = list(ssh_command(inventory.core_node))
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    target = base.pop()
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


class RfsimTransportSession:
    def __init__(
        self,
        *,
        inventory: NetworkInventory,
        ue_pod: str,
        endpoint: MQTTEndpoint,
        snapshot_remote_path: str,
        remote_workspace: str,
        processes: list[ManagedProcess],
        owned_remote_pids: Mapping[int, Sequence[int]],
        relay_marker: str,
        route_destination: str,
        route_created: bool,
    ) -> None:
        self.inventory = inventory
        self.ue_pod = ue_pod
        self._endpoint = endpoint
        self.snapshot_remote_path = snapshot_remote_path
        self.remote_workspace = remote_workspace
        self.processes = processes
        self.owned_remote_pids = {
            int(port): tuple(sorted({int(pid) for pid in pids}))
            for port, pids in owned_remote_pids.items()
        }
        self.relay_marker = relay_marker
        self.route_destination = route_destination
        self.route_created = route_created
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
            raise ExperimentError("RFSIM counted-ingress snapshot is unavailable")
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise ExperimentError("RFSIM counted-ingress snapshot is invalid JSON") from exc
        if not isinstance(payload, Mapping):
            raise ExperimentError("RFSIM counted-ingress snapshot is malformed")
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
        try:
            _cleanup_ue_relay(self.inventory, self.ue_pod, self.relay_marker)
        except Exception as exc:
            self._cleanup_errors.append(f"UE relay cleanup: {exc}")
        if self.route_created:
            try:
                _remove_route(self.inventory, self.ue_pod, self.route_destination)
            except Exception as exc:
                self._cleanup_errors.append(f"UE route cleanup: {exc}")
        if any(not _remote_port_closed(self.inventory, port) for port in self.owned_remote_pids):
            self._cleanup_errors.extend(_reap_owned_listeners(self.inventory, self.owned_remote_pids))
        try:
            result = _run(
                ssh_command(self.inventory.core_node, "rm", "-rf", self.remote_workspace),
                timeout_seconds=10,
            )
            if result.returncode != 0:
                self._cleanup_errors.append("remote workspace cleanup failed")
        except Exception as exc:
            self._cleanup_errors.append(f"remote workspace cleanup: {exc}")
        if not _local_port_closed(self._endpoint.port):
            self._cleanup_errors.append("local RFSIM publisher forward still listens")
        for port in self.owned_remote_pids:
            if not _remote_port_closed(self.inventory, port):
                self._cleanup_errors.append(f"remote RFSIM port {port} still listens")
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
            "ue_pod": self.ue_pod,
            "ue_interface": UE_INTERFACE,
            "publisher_endpoint": {
                "host": self._endpoint.host,
                "port": self._endpoint.port,
            },
            "ingress": snapshot.to_dict() if snapshot is not None else None,
            "route_created": self.route_created,
            "stopped": self._stopped,
            "cleanup_errors": list(self._cleanup_errors),
            "cleanup_valid": self._stopped and not self._cleanup_errors,
        }


class RfsimTransportAdapter:
    """Expose one existing RFSIM UE to Amber without deployment mutation."""

    def __init__(self, *, inventory: NetworkInventory, repository_root: Path) -> None:
        self.inventory = inventory
        self.repository_root = repository_root.resolve()

    def start(
        self,
        *,
        run_id: str,
        ue_pod: str,
        pdu_address: str,
        core_address: str,
        central_port: int,
        run_directory: Path,
    ) -> RfsimTransportSession:
        run_id = validate_run_id(run_id)
        if not ue_pod:
            raise ExperimentError("RFSIM transport requires an observed UE pod")
        if not _local_port_closed(LOCAL_PUBLISHER_PORT):
            raise ExperimentError("RFSIM publisher port is already in use")
        for port in (CORE_UE_FORWARD_PORT, CORE_INGRESS_PORT):
            if not _remote_port_closed(self.inventory, port):
                raise ExperimentError(f"RFSIM experiment port is already in use: {port}")

        remote_workspace = f"/tmp/synthran/{run_id}"
        try:
            mkdir = _run(
                ssh_command(self.inventory.core_node, "mkdir", "-p", remote_workspace),
                timeout_seconds=10,
            )
            if mkdir.returncode != 0:
                raise ExperimentError("RFSIM remote workspace creation failed")
            _transfer_file(
                self.inventory,
                self.repository_root / "synthran" / "ingress.py",
                f"{remote_workspace}/ingress.py",
                label="RFSIM counted-ingress helper transfer",
            )
        except LivePreflightError as exc:
            raise ExperimentError(str(exc)) from exc

        route_created = False
        processes: list[ManagedProcess] = []
        owned: dict[int, tuple[int, ...]] = {}
        marker = f"synthran-rfsim-relay-{run_id}"
        snapshot_remote = f"{remote_workspace}/amber-ingress-snapshot.json"
        logs = run_directory / "logs"
        try:
            route_created = _install_route(self.inventory, ue_pod, core_address)
            ingress = _start_counted_ingress(
                self.inventory,
                repository_root=self.repository_root,
                remote_workspace=remote_workspace,
                listen_host=core_address,
                listen_port=CORE_INGRESS_PORT,
                target_port=central_port,
                snapshot_path=snapshot_remote,
                log_path=logs / "amber-ingress.log",
            )
            processes.append(ingress)
            _wait_remote_listener(self.inventory, CORE_INGRESS_PORT, ingress)
            owned[CORE_INGRESS_PORT] = _require_owner(
                self.inventory, CORE_INGRESS_PORT, "RFSIM counted ingress"
            )

            relay = _start_process(
                "RFSIM UE PDU relay",
                _ue_relay_command(
                    self.inventory,
                    ue_pod,
                    pdu_address,
                    core_address,
                    CORE_INGRESS_PORT,
                    marker,
                ),
                cwd=self.repository_root,
                log_path=logs / "amber-ue-relay.log",
            )
            processes.append(relay)

            try:
                port_forward_command = ssh_command(
                    self.inventory.core_node,
                    "sh",
                    "-c",
                    "exec env KUBECONFIG=/etc/kubernetes/admin.conf kubectl port-forward "
                    f"-n {KUBERNETES_NAMESPACE} pod/{shlex.quote(ue_pod)} "
                    f"{CORE_UE_FORWARD_PORT}:{UE_RELAY_PORT} --address 127.0.0.1",
                )
            except LivePreflightError as exc:
                raise ExperimentError(str(exc)) from exc
            port_forward = _start_process(
                "RFSIM UE port-forward",
                port_forward_command,
                cwd=self.repository_root,
                log_path=logs / "amber-ue-port-forward.log",
            )
            processes.append(port_forward)
            _wait_remote_tcp(
                self.inventory,
                CORE_UE_FORWARD_PORT,
                port_forward,
                relay,
            )
            owned[CORE_UE_FORWARD_PORT] = _require_owner(
                self.inventory, CORE_UE_FORWARD_PORT, "RFSIM UE port-forward"
            )

            publisher_forward = _start_process(
                "RFSIM Amber publisher forward",
                _core_local_forward(
                    self.inventory,
                    local_port=LOCAL_PUBLISHER_PORT,
                    remote_port=CORE_UE_FORWARD_PORT,
                ),
                cwd=self.repository_root,
                log_path=logs / "amber-publisher-forward.log",
            )
            processes.append(publisher_forward)
            _wait_local_listener(LOCAL_PUBLISHER_PORT, publisher_forward)
        except Exception:
            for process in reversed(processes):
                try:
                    process.stop()
                except Exception:
                    pass
            try:
                _cleanup_ue_relay(self.inventory, ue_pod, marker)
            except Exception:
                pass
            if route_created:
                try:
                    _remove_route(self.inventory, ue_pod, core_address)
                except Exception:
                    pass
            if owned:
                try:
                    _reap_owned_listeners(self.inventory, owned)
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

        return RfsimTransportSession(
            inventory=self.inventory,
            ue_pod=ue_pod,
            endpoint=MQTTEndpoint("127.0.0.1", LOCAL_PUBLISHER_PORT),
            snapshot_remote_path=snapshot_remote,
            remote_workspace=remote_workspace,
            processes=processes,
            owned_remote_pids=owned,
            relay_marker=marker,
            route_destination=core_address,
            route_created=route_created,
        )
