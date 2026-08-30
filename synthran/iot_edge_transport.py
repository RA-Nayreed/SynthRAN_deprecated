"""Backend-specific edge transports for portable IoT publishers."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import time
from typing import Any, Mapping, Protocol, Sequence

from synthran.experiment import ExperimentError, validate_run_id
from synthran.fiveg_ansible import NetworkInventory
from synthran.ingress import IngressSnapshot
from synthran.iot_source import MQTTEndpoint
from synthran.live_preflight import LivePreflightError, ssh_command


RFSIM_EDGE_FORWARD_PORT = 18883
RFSIM_AMBER_INGRESS_PORT = 18886
RFSIM_AMBER_LOCAL_PORT = 18886
KUBERNETES_NAMESPACE = "open5gs"


class EdgeTransportAdapter(Protocol):
    def start(
        self,
        *,
        run_id: str,
        ue_pod: str,
        remote_workspace: str,
        run_directory: Path,
    ) -> "EdgeTransportSession": ...


class EdgeTransportSession(Protocol):
    @property
    def mqtt_endpoint(self) -> MQTTEndpoint: ...

    def snapshot(self) -> IngressSnapshot: ...

    def stop(self) -> None: ...

    def evidence(self) -> Mapping[str, Any]: ...


@dataclass
class OwnedProcess:
    name: str
    process: subprocess.Popen[str]
    log_path: Path
    log_stream: Any

    def stop(self) -> None:
        if self.process.poll() is None:
            pid = getattr(self.process, "pid", None)
            if isinstance(pid, int) and pid > 1 and hasattr(os, "killpg"):
                try:
                    os.killpg(pid, signal.SIGTERM)
                except (ProcessLookupError, PermissionError):
                    pass
                try:
                    self.process.wait(timeout=8)
                except Exception:
                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except (ProcessLookupError, PermissionError):
                        pass
                    try:
                        self.process.wait(timeout=5)
                    except Exception:
                        pass
            else:
                try:
                    self.process.terminate()
                except Exception:
                    pass
                try:
                    self.process.wait(timeout=8)
                except Exception:
                    try:
                        self.process.kill()
                    except Exception:
                        pass
        try:
            self.log_stream.close()
        except Exception:
            pass


def _start_process(
    name: str,
    command: Sequence[str],
    *,
    cwd: Path,
    log_path: Path,
) -> OwnedProcess:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    stream = log_path.open("w", encoding="utf-8", newline="\n")
    try:
        process = subprocess.Popen(
            list(command),
            cwd=cwd,
            text=True,
            stdin=subprocess.DEVNULL,
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except (FileNotFoundError, OSError) as exc:
        stream.close()
        raise ExperimentError(f"unable to start {name}") from exc
    return OwnedProcess(name, process, log_path, stream)


def _run(command: Sequence[str], *, timeout_seconds: int = 20) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(command),
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        raise ExperimentError("edge transport command failed") from exc


def _strict_ssh_base(inventory: NetworkInventory) -> tuple[list[str], str]:
    try:
        base = list(ssh_command(inventory.core_node))
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    if not base:
        raise ExperimentError("unable to construct strict SSH transport")
    target = base.pop()
    return base, target


def _local_forward_command(
    inventory: NetworkInventory,
    *,
    local_port: int,
    remote_port: int,
) -> tuple[str, ...]:
    base, target = _strict_ssh_base(inventory)
    base.extend(
        (
            "-N",
            "-o",
            "ExitOnForwardFailure=yes",
            "-L",
            f"127.0.0.1:{local_port}:127.0.0.1:{remote_port}",
            target,
        )
    )
    return tuple(base)


def _proc_has_listener(port: int) -> bool:
    port_hex = f"{port:04X}"
    for path in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        try:
            lines = path.read_text(encoding="ascii").splitlines()[1:]
        except OSError:
            continue
        for line in lines:
            fields = line.split()
            if len(fields) < 4 or fields[3] != "0A":
                continue
            local = fields[1]
            _, _, encoded_port = local.rpartition(":")
            if encoded_port.upper() == port_hex:
                return True
    return False


def _remote_listener_probe_command(
    inventory: NetworkInventory,
    *,
    port: int,
) -> tuple[str, ...]:
    script = (
        "from pathlib import Path; "
        f"want='{port:04X}'; found=False; "
        "paths=(Path('/proc/net/tcp'),Path('/proc/net/tcp6')); "
        "\nfor path in paths:\n"
        "    try: lines=path.read_text().splitlines()[1:]\n"
        "    except OSError: continue\n"
        "    for line in lines:\n"
        "        fields=line.split()\n"
        "        if len(fields)>=4 and fields[3]=='0A' and fields[1].rsplit(':',1)[-1].upper()==want:\n"
        "            found=True; break\n"
        "    if found: break\n"
        "raise SystemExit(0 if found else 1)"
    )
    try:
        return ssh_command(inventory.core_node, "python3", "-c", script)
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc


def _remote_tcp_probe_command(
    inventory: NetworkInventory,
    *,
    port: int,
) -> tuple[str, ...]:
    script = (
        "import socket; "
        "s=socket.socket(socket.AF_INET,socket.SOCK_STREAM); "
        "s.settimeout(1); "
        f"s.connect(('127.0.0.1',{port})); s.close()"
    )
    try:
        return ssh_command(inventory.core_node, "python3", "-c", script)
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc


def _wait_remote_tcp(
    inventory: NetworkInventory,
    *,
    port: int,
    process: OwnedProcess,
    timeout_seconds: int = 30,
) -> None:
    """Probe a non-counted upstream endpoint by connecting to it."""

    deadline = time.monotonic() + timeout_seconds
    command = _remote_tcp_probe_command(inventory, port=port)
    while time.monotonic() < deadline:
        if process.process.poll() is not None:
            raise ExperimentError(
                f"{process.name} exited before remote TCP endpoint became ready"
            )
        result = _run(command, timeout_seconds=5)
        if result.returncode == 0:
            return
        time.sleep(0.25)
    raise ExperimentError(f"remote TCP endpoint 127.0.0.1:{port} did not become ready")


def _wait_remote_listener(
    inventory: NetworkInventory,
    *,
    port: int,
    process: OwnedProcess,
    timeout_seconds: int = 30,
) -> None:
    """Wait for a listener without opening a connection or changing its counters."""

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
    process: OwnedProcess,
    timeout_seconds: int = 30,
) -> None:
    """Wait for the local SSH listener without traversing counted ingress."""

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


class RfsimEdgeTransportSession:
    def __init__(
        self,
        *,
        inventory: NetworkInventory,
        endpoint: MQTTEndpoint,
        remote_ingress_port: int,
        snapshot_remote_path: str,
        processes: list[OwnedProcess],
    ) -> None:
        self.inventory = inventory
        self._endpoint = endpoint
        self.remote_ingress_port = remote_ingress_port
        self.snapshot_remote_path = snapshot_remote_path
        self.processes = processes
        self._cleanup_errors: list[str] = []
        self._stopped = False

    @property
    def mqtt_endpoint(self) -> MQTTEndpoint:
        return self._endpoint

    def snapshot(self) -> IngressSnapshot:
        try:
            command = ssh_command(
                self.inventory.core_node,
                "cat",
                self.snapshot_remote_path,
            )
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
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if _local_port_is_closed(self._endpoint.port) and _remote_port_is_closed(
                self.inventory, self.remote_ingress_port
            ):
                break
            time.sleep(0.25)
        if not _local_port_is_closed(self._endpoint.port):
            self._cleanup_errors.append("local publisher forward still listens")
        if not _remote_port_is_closed(self.inventory, self.remote_ingress_port):
            self._cleanup_errors.append("remote counted ingress still listens")
        self._stopped = True

    def evidence(self) -> Mapping[str, Any]:
        snapshot: dict[str, Any] | None = None
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
            "owned_processes": [process.name for process in self.processes],
            "stopped": self._stopped,
            "cleanup_errors": list(self._cleanup_errors),
            "cleanup_valid": self._stopped and not self._cleanup_errors,
        }


class RfsimEdgeTransportAdapter:
    """Expose the srsUE edge broker to Amber through counted loopback hops."""

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
        if not _remote_port_is_closed(self.inventory, self.remote_ingress_port):
            raise ExperimentError(
                "RFSIM Amber ingress port is already owned by another process"
            )
        if not _local_port_is_closed(self.local_port):
            raise ExperimentError(
                "RFSIM Amber local publisher port is already owned by another process"
            )

        ingress_helper = f"{remote_workspace}/ingress.py"
        snapshot_remote = f"{remote_workspace}/amber-ingress-snapshot.json"
        logs = run_directory / "logs"
        processes: list[OwnedProcess] = []

        edge_command = ssh_command(
            self.inventory.core_node,
            "sh",
            "-c",
            "KUBECONFIG=/etc/kubernetes/admin.conf kubectl port-forward "
            f"-n {KUBERNETES_NAMESPACE} pod/{shlex.quote(ue_pod)} "
            f"{self.edge_forward_port}:1883 --address 127.0.0.1",
        )
        edge_forward = _start_process(
            "RFSIM edge MQTT port-forward",
            edge_command,
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

            ingress_command = (
                f"exec python3 {shlex.quote(ingress_helper)} "
                "--listen-host 127.0.0.1 "
                f"--listen-port {self.remote_ingress_port} "
                "--target-host 127.0.0.1 "
                f"--target-port {self.edge_forward_port} "
                f"--snapshot-path {shlex.quote(snapshot_remote)}"
            )
            ingress = _start_process(
                "RFSIM counted Amber ingress",
                ssh_command(
                    self.inventory.core_node,
                    "sh",
                    "-c",
                    ingress_command,
                ),
                cwd=self.repository_root,
                log_path=logs / "amber-ingress.log",
            )
            processes.append(ingress)
            _wait_remote_listener(
                self.inventory,
                port=self.remote_ingress_port,
                process=ingress,
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
            _wait_local_tcp(
                self.local_port,
                process=local_forward,
            )
        except Exception:
            for process in reversed(processes):
                process.stop()
            raise

        return RfsimEdgeTransportSession(
            inventory=self.inventory,
            endpoint=MQTTEndpoint("127.0.0.1", self.local_port),
            remote_ingress_port=self.remote_ingress_port,
            snapshot_remote_path=snapshot_remote,
            processes=processes,
        )
