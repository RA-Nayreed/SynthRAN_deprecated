"""Experiment-side Amber transport helpers for an upstream-provisioned R2Lab UE.

This module owns no reservation, radio, core, RAN, or UE activation lifecycle.
The selected UE and its management transport come from the inventory generated
by 5g-Ansible. SynthRAN only binds experiment traffic to the already-provisioned
UE data interface and observes experiment-local state.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import ipaddress
import json
import shlex
import socketserver
import subprocess
import threading
import time
from typing import Any, Mapping, Sequence

from synthran.dependencies import DependencyLock
from synthran.experiment import ExperimentError, validate_run_id
from synthran.experiment.live import _remote, _run
from synthran.experiment.resources import (
    CENTRAL_PORT,
    MOSQUITTO_BINARY,
    ROLE_LABEL,
    RUN_LABEL,
    _mosquitto_image,
)
from synthran.fiveg_ansible import NetworkInventory
from synthran.live_preflight import CommandResult


LOCAL_UE_RELAY_PORT = 18887
KUBERNETES_NAMESPACE = "open5gs"
UE_INTERFACE = "wwan0"
_RELAY_MARKER = "SYNTHRAN_R2LAB_RELAY"


class R2LabIoTLiveError(ExperimentError):
    """Raised when the accepted physical experiment path cannot be used safely."""


@dataclass(frozen=True)
class PhysicalUeEndpoint:
    """Management facts for one physical UE, sourced only from upstream inventory."""

    name: str
    host: str
    user: str
    mode: str
    ssh_common_args: tuple[str, ...]


def _suffix(run_id: str) -> str:
    validate_run_id(run_id)
    return hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:12]


def physical_central_name(run_id: str) -> str:
    return f"synthran-r2lab-central-{_suffix(run_id)}"


def render_physical_central_objects(
    *,
    run_id: str,
    lock: DependencyLock,
    core_node: str,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    """Render only the run-owned host-network central MQTT resources."""

    validate_run_id(run_id)
    name = physical_central_name(run_id)
    labels = {
        "app.kubernetes.io/name": "synthran-experiment",
        "app.kubernetes.io/component": "mqtt",
        RUN_LABEL: run_id,
    }
    config = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": KUBERNETES_NAMESPACE,
            "labels": labels,
        },
        "data": {
            "mosquitto.conf": "\n".join(
                (
                    "per_listener_settings true",
                    f"listener {CENTRAL_PORT} 0.0.0.0",
                    "allow_anonymous true",
                    "persistence false",
                    "log_type all",
                    "",
                )
            )
        },
    }
    listener_probe = (
        f"awk '$2 ~ /:{CENTRAL_PORT:04X}$/ && $4 == \"0A\" "
        "{ found=1 } END { exit !found }' /proc/net/tcp"
    )
    deployment = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": KUBERNETES_NAMESPACE,
            "labels": labels,
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": {
                    RUN_LABEL: run_id,
                    ROLE_LABEL: "central-mqtt",
                }
            },
            "template": {
                "metadata": {
                    "labels": {
                        RUN_LABEL: run_id,
                        ROLE_LABEL: "central-mqtt",
                    }
                },
                "spec": {
                    "hostNetwork": True,
                    "dnsPolicy": "ClusterFirstWithHostNet",
                    "nodeSelector": {"kubernetes.io/hostname": core_node},
                    "containers": [
                        {
                            "name": "central-mqtt",
                            "image": _mosquitto_image(lock),
                            "imagePullPolicy": "IfNotPresent",
                            "args": [MOSQUITTO_BINARY, "-c", "/synthran/mosquitto.conf"],
                            "ports": [
                                {
                                    "name": "mqtt-central",
                                    "containerPort": CENTRAL_PORT,
                                    "hostPort": CENTRAL_PORT,
                                }
                            ],
                            "volumeMounts": [
                                {
                                    "name": "config",
                                    "mountPath": "/synthran",
                                    "readOnly": True,
                                }
                            ],
                            "readinessProbe": {
                                "exec": {"command": ["/bin/sh", "-ec", listener_probe]},
                                "initialDelaySeconds": 2,
                                "periodSeconds": 2,
                                "timeoutSeconds": 2,
                            },
                        }
                    ],
                    "volumes": [
                        {"name": "config", "configMap": {"name": name}}
                    ],
                },
            },
        },
    }
    return config, deployment


def _validate_ue(inventory: NetworkInventory, ue: str) -> PhysicalUeEndpoint:
    """Resolve one wwan0-capable experiment endpoint from upstream inventory facts."""

    value = ue.strip()
    host = inventory.ue(value)
    user = host.variables.get("ansible_user")
    address = host.variables.get("ansible_host", host.name)
    mode = host.variables.get("mode", "")
    common = host.variables.get("ansible_ssh_common_args", "")
    if not user or not address:
        raise R2LabIoTLiveError("selected physical UE inventory is missing SSH identity")
    if mode not in {"mbim", "qmi"}:
        raise R2LabIoTLiveError(
            "physical Amber workload requires an upstream UE with a wwan0 modem mode"
        )
    try:
        common_args = tuple(shlex.split(common)) if common else ()
    except ValueError as exc:
        raise R2LabIoTLiveError(
            "selected physical UE inventory contains malformed SSH arguments"
        ) from exc
    return PhysicalUeEndpoint(
        name=host.name,
        host=address,
        user=user,
        mode=mode,
        ssh_common_args=common_args,
    )


def _ue_command(endpoint: PhysicalUeEndpoint, *remote: str) -> tuple[str, ...]:
    if not remote:
        raise R2LabIoTLiveError("physical UE command requires explicit remote argv")
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


_RELAY_SCRIPT = r'''
import os, select, socket, sys
MARKER = "SYNTHRAN_R2LAB_RELAY"
run_id, host, port_text, interface = sys.argv[1:5]
port = int(port_text)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE, (interface + "\0").encode("ascii"))
sock.settimeout(10)
sock.connect((host, port))
sock.settimeout(None)
stdin_open = True
while True:
    readers = [sock]
    if stdin_open:
        readers.append(0)
    ready, _, _ = select.select(readers, [], [])
    if stdin_open and 0 in ready:
        data = os.read(0, 65536)
        if data:
            sock.sendall(data)
        else:
            stdin_open = False
            try:
                sock.shutdown(socket.SHUT_WR)
            except OSError:
                pass
    if sock in ready:
        data = sock.recv(65536)
        if not data:
            break
        os.write(1, data)
sock.close()
'''.strip()


def build_physical_ue_stdio_relay_command(
    *,
    inventory: NetworkInventory,
    ue: str,
    run_id: str,
    central_address: str,
    central_port: int = CENTRAL_PORT,
    interface: str = UE_INTERFACE,
) -> tuple[str, ...]:
    validate_run_id(run_id)
    endpoint = _validate_ue(inventory, ue)
    try:
        address = ipaddress.ip_address(central_address)
    except ValueError as exc:
        raise R2LabIoTLiveError("central broker address must be a literal IP") from exc
    if not isinstance(address, ipaddress.IPv4Address):
        raise R2LabIoTLiveError("current physical workload is IPv4-only")
    if central_port < 1 or central_port > 65535:
        raise R2LabIoTLiveError("central broker port is invalid")
    if interface != UE_INTERFACE:
        raise R2LabIoTLiveError("physical relay must bind to wwan0")
    return _ue_command(
        endpoint,
        "python3",
        "-c",
        _RELAY_SCRIPT,
        run_id,
        str(address),
        str(central_port),
        interface,
    )


def route_uses_wwan0(text: str, destination: str) -> bool:
    try:
        wanted = str(ipaddress.ip_address(destination))
        payload = json.loads(text)
    except (ValueError, json.JSONDecodeError):
        return False
    if not isinstance(payload, list) or not payload:
        return False
    return any(
        isinstance(item, dict)
        and item.get("dev") == UE_INTERFACE
        and str(item.get("dst", wanted)) == wanted
        for item in payload
    )


def _ue_read(
    *,
    endpoint: PhysicalUeEndpoint,
    command: Sequence[str],
    label: str = "physical UE workload precondition",
    timeout_seconds: int = 30,
) -> CommandResult:
    result = _run(
        _ue_command(endpoint, *tuple(command)),
        timeout_seconds=timeout_seconds,
    )
    if result.returncode != 0:
        raise R2LabIoTLiveError(f"{label} returned nonzero")
    return result


def _ue_counter(endpoint: PhysicalUeEndpoint, counter: str) -> int:
    if counter not in {"rx_bytes", "tx_bytes"}:
        raise R2LabIoTLiveError("unsupported physical UE interface counter")
    result = _ue_read(
        endpoint=endpoint,
        command=("cat", f"/sys/class/net/{UE_INTERFACE}/statistics/{counter}"),
        label=f"physical UE {counter} counter probe",
    )
    value = result.stdout.strip()
    if not value.isdigit():
        raise R2LabIoTLiveError("physical UE interface counter returned malformed data")
    return int(value)


def _prove_ue_route(endpoint: PhysicalUeEndpoint, central_address: str) -> None:
    route = _ue_read(
        endpoint=endpoint,
        command=(
            "ip",
            "-j",
            "route",
            "get",
            central_address,
            "oif",
            UE_INTERFACE,
        ),
        label="physical UE interface-bound route probe",
    )
    if not route_uses_wwan0(route.stdout, central_address):
        raise R2LabIoTLiveError(
            "central MQTT destination is not selectable through wwan0"
        )


def _ue_relay_process_count(endpoint: PhysicalUeEndpoint, run_id: str) -> int:
    validate_run_id(run_id)
    probe = r'''
import os, sys
marker, run_id = sys.argv[1:3]
self_pid = os.getpid()
count = 0
for entry in os.listdir('/proc'):
    if not entry.isdigit() or int(entry) == self_pid:
        continue
    try:
        raw = open(f'/proc/{entry}/cmdline', 'rb').read()
    except (FileNotFoundError, PermissionError, OSError):
        continue
    text = raw.replace(b'\0', b' ').decode('utf-8', 'replace')
    if marker in text and run_id in text:
        count += 1
print(count)
'''.strip()
    result = _ue_read(
        endpoint=endpoint,
        command=("python3", "-c", probe, _RELAY_MARKER, run_id),
        label="physical UE relay cleanup probe",
    )
    value = result.stdout.strip()
    if not value.isdigit():
        raise R2LabIoTLiveError(
            "physical UE relay cleanup probe returned malformed data"
        )
    return int(value)


def _stop_relay_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        try:
            process.terminate()
            process.wait(timeout=3)
        except (OSError, subprocess.TimeoutExpired):
            try:
                process.kill()
                process.wait(timeout=3)
            except (OSError, subprocess.TimeoutExpired):
                pass
    for stream in (process.stdin, process.stdout):
        if stream is not None and not stream.closed:
            stream.close()


class _RelayTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = False
    daemon_threads = True

    def __init__(self, address: tuple[str, int], command: tuple[str, ...]) -> None:
        self.command = command
        self.children: set[subprocess.Popen[bytes]] = set()
        self.children_lock = threading.Lock()
        super().__init__(address, _RelayHandler)

    def register_child(self, process: subprocess.Popen[bytes]) -> None:
        with self.children_lock:
            self.children.add(process)

    def unregister_child(self, process: subprocess.Popen[bytes]) -> None:
        with self.children_lock:
            self.children.discard(process)

    def terminate_children(self) -> None:
        with self.children_lock:
            children = list(self.children)
        for process in children:
            _stop_relay_process(process)


class _RelayHandler(socketserver.BaseRequestHandler):
    server: _RelayTCPServer

    def handle(self) -> None:
        try:
            process = subprocess.Popen(
                self.server.command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                bufsize=0,
                start_new_session=True,
            )
        except (FileNotFoundError, OSError):
            return
        self.server.register_child(process)
        assert process.stdin is not None
        assert process.stdout is not None

        def to_process() -> None:
            try:
                while True:
                    data = self.request.recv(65536)
                    if not data:
                        break
                    process.stdin.write(data)
                    process.stdin.flush()
            except (BrokenPipeError, ConnectionError, OSError):
                pass
            finally:
                try:
                    process.stdin.close()
                except OSError:
                    pass

        def to_client() -> None:
            try:
                while True:
                    data = process.stdout.read(65536)
                    if not data:
                        break
                    self.request.sendall(data)
            except (BrokenPipeError, ConnectionError, OSError):
                pass
            finally:
                try:
                    self.request.shutdown(1)
                except OSError:
                    pass

        outgoing = threading.Thread(target=to_process, daemon=True)
        incoming = threading.Thread(target=to_client, daemon=True)
        outgoing.start()
        incoming.start()
        outgoing.join()
        incoming.join()
        _stop_relay_process(process)
        self.server.unregister_child(process)


class ManagedPhysicalUeRelay:
    """Loopback endpoint creating one selected-UE stdio relay per client."""

    def __init__(self, *, port: int, command: tuple[str, ...]) -> None:
        self.server = _RelayTCPServer(("127.0.0.1", port), command)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.server.terminate_children()
        self.thread.join(timeout=5)


def _wait_remote_tcp(
    inventory: NetworkInventory,
    *,
    host: str,
    port: int,
    timeout_seconds: int,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    probe = (
        "import socket,sys; "
        "s=socket.socket(); s.settimeout(1); "
        "s.connect((sys.argv[1], int(sys.argv[2]))); s.close()"
    )
    while time.monotonic() < deadline:
        try:
            _remote(
                inventory,
                "python3",
                "-c",
                probe,
                host,
                str(port),
                label="physical central MQTT readiness probe",
                timeout_seconds=5,
            )
            return
        except Exception:
            time.sleep(0.5)
    raise R2LabIoTLiveError("physical central MQTT endpoint did not become ready")


def _central_rollout(inventory: NetworkInventory, deployment: str) -> None:
    try:
        _remote(
            inventory,
            "sh",
            "-c",
            "KUBECONFIG=/etc/kubernetes/admin.conf kubectl rollout status deployment/"
            f"{shlex.quote(deployment)} -n {KUBERNETES_NAMESPACE} --timeout=180s",
            label="physical central MQTT rollout",
            timeout_seconds=200,
        )
    except Exception as exc:
        diagnostics: list[str] = []
        for label, command in (
            (
                "deployment",
                "KUBECONFIG=/etc/kubernetes/admin.conf kubectl get deployment/"
                f"{shlex.quote(deployment)} -n {KUBERNETES_NAMESPACE} "
                "-o jsonpath='{.status.readyReplicas}/{.status.replicas} ready; "
                "{.status.unavailableReplicas} unavailable'",
            ),
            (
                "broker-log",
                "KUBECONFIG=/etc/kubernetes/admin.conf kubectl logs -n "
                f"{KUBERNETES_NAMESPACE} deployment/{shlex.quote(deployment)} "
                "-c central-mqtt --tail=40",
            ),
        ):
            try:
                output = _remote(
                    inventory,
                    "sh",
                    "-c",
                    command,
                    label=f"physical central MQTT {label} diagnostics",
                    timeout_seconds=20,
                ).strip()
            except Exception:
                continue
            if output:
                diagnostics.append(f"{label}: {' '.join(output.split())[:800]}")
        detail = "; ".join(diagnostics) or "no broker diagnostics were available"
        raise R2LabIoTLiveError(
            f"physical central MQTT rollout failed: {detail}"
        ) from exc

    _wait_remote_tcp(
        inventory,
        host="127.0.0.1",
        port=CENTRAL_PORT,
        timeout_seconds=15,
    )
