"""Physical R2Lab adapter for the deterministic SynthRAN IoT workload.

The accepted virtual experiment deliberately owns an srsUE Deployment and
``tun_srsue1``.  A COTS qfit UE has neither.  This module therefore reuses the
stable Cooja/tunslip/collector contracts but replaces the virtual UE sidecar
with a transient stdio relay that executes on the selected qfit and binds its
outbound TCP socket to ``wwan0``.

The relay is an integration adapter, not a substitute for physical-path proof.
The caller must enter through the R2Lab workload handoff after user-plane
acceptance; this executor additionally proves the exact central destination is
routed through ``wwan0`` and requires the qfit interface counters to increase.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Mapping, Sequence, TextIO

from synthran.dependencies import DependencyLock
from synthran.experiment import (
    ExperimentCheck,
    ExperimentError,
    SENSOR_COUNT,
    build_data_evidence,
    load_jsonl,
    save_experiment_evidence,
    sha256_file,
    validate_run_id,
    write_parquet,
)
from synthran.experiment.resources import (
    CENTRAL_PORT,
    MOSQUITTO_BINARY,
    ROLE_LABEL,
    RUN_LABEL,
    _mosquitto_image,
)
from synthran.experiment.runtime import (
    DEFAULT_COLLECTION_SECONDS,
    DEFAULT_MINIMUM_PER_SENSOR,
    LOCAL_CENTRAL_FORWARD_PORT,
    REMOTE_EDGE_FORWARD_PORT,
    ManagedProcess,
    _cleanup_remote_run_processes,
    _copy_sensor_source,
    _core_address,
    _delete_experiment_objects,
    _kubectl_apply_object,
    _prepare_cooja_checkout,
    _probe_experiment_host,
    _probe_ssh_forwarding,
    _remote,
    _remote_json,
    _remote_path_exists,
    _run,
    _save_manifest,
    _ssh_reverse_tunnel_command,
    _ssh_tunnel_command,
    _start_process,
    _transfer_directory,
    _transfer_file,
    _validate_contiki_checkout,
    _validate_java_runtime,
    _wait_remote_tcp,
    _wait_tcp,
)
from synthran.fiveg_ansible import NetworkInventory
from synthran.ingress import IngressSnapshot
from synthran.iot import write_run_inputs
from synthran.live_preflight import CommandResult, LivePreflightError, ssh_command
from synthran.mqtt_collector import collect_mqtt
from synthran.r2lab.controller import gateway_command
from synthran.r2lab.ue import PhysicalWorkloadContext, PhysicalWorkloadResult


DEFAULT_PHYSICAL_RUN_ROOT = Path(".synthran/experiments-r2lab")
LOCAL_QFIT_RELAY_PORT = 18887
KUBERNETES_NAMESPACE = "open5gs"
_QFIT_INTERFACE = "wwan0"
_QFIT_RE = re.compile(r"^qfit(?:07|09|18|29|32|34)$")
_SAFE_NAME_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_RELAY_MARKER = "SYNTHRAN_R2LAB_RELAY"


class R2LabPhysicalExperimentError(ExperimentError):
    """Raised when the physical deterministic workload cannot be proven safely."""


@dataclass(frozen=True)
class PhysicalExperimentScenario:
    """Cooja-compatible scenario without persisting a physical UE address."""

    run_id: str
    network_run_id: str
    sensor_count: int = SENSOR_COUNT
    sensor_period_seconds: int = 10
    cooja_seed: int = 424242
    serial_socket_port: int = 60001
    topic_prefix: str = "synthran"

    def __post_init__(self) -> None:
        validate_run_id(self.run_id)
        validate_run_id(self.network_run_id)
        if self.sensor_count != SENSOR_COUNT:
            raise R2LabPhysicalExperimentError("physical golden path requires exactly 10 sensors")
        if self.sensor_period_seconds <= 0 or self.sensor_period_seconds > 3600:
            raise R2LabPhysicalExperimentError("sensor period must be between 1 and 3600 seconds")
        if self.cooja_seed < 0:
            raise R2LabPhysicalExperimentError("Cooja seed must be non-negative")
        if self.serial_socket_port < 1024 or self.serial_socket_port > 65535:
            raise R2LabPhysicalExperimentError("serial socket port is outside the allowed range")
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,31}", self.topic_prefix):
            raise R2LabPhysicalExperimentError("topic prefix contains unsupported characters")

    @property
    def topic_root(self) -> str:
        return f"{self.topic_prefix}/{self.run_id}"

    @property
    def sensor_topic(self) -> str:
        return f"{self.topic_root}/sensor/+"

    @property
    def exact_sensor_topics(self) -> tuple[str, ...]:
        return tuple(
            f"{self.topic_root}/sensor/sensor-{index:02d}"
            for index in range(1, self.sensor_count + 1)
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/iot-experiment-r2lab/v1alpha1",
            "run_id": self.run_id,
            "network_run_id": self.network_run_id,
            "backend": "r2lab",
            "ue_interface": _QFIT_INTERFACE,
            "sensor_count": self.sensor_count,
            "sensor_period_seconds": self.sensor_period_seconds,
            "cooja_seed": self.cooja_seed,
            "serial_socket_port": self.serial_socket_port,
            "topic_root": self.topic_root,
            "rpl_prefix": "fd00::/64",
            "edge_adapter": "qfit-stdio-tcp-relay",
            "raw_pdu_address_persisted": False,
            "data_contract": "synthran/telemetry/v1alpha1",
        }


@dataclass(frozen=True)
class PhysicalExperimentConfig:
    slice_name: str
    inventory: NetworkInventory
    lock: DependencyLock
    dependency_root: Path
    repository_root: Path
    workload_id: str
    run_root: Path = DEFAULT_PHYSICAL_RUN_ROOT
    collection_seconds: int = DEFAULT_COLLECTION_SECONDS
    minimum_per_sensor: int = DEFAULT_MINIMUM_PER_SENSOR
    cooja_seed: int = 424242
    sensor_period_seconds: int = 10
    local_qfit_relay_port: int = LOCAL_QFIT_RELAY_PORT
    progress: TextIO | None = None

    def validate(self) -> "PhysicalExperimentConfig":
        validate_run_id(self.workload_id)
        if not self.slice_name or len(self.slice_name) > 64:
            raise R2LabPhysicalExperimentError("R2Lab slice name is malformed")
        if self.collection_seconds < 30 or self.collection_seconds > 3600:
            raise R2LabPhysicalExperimentError("collection duration must be between 30 and 3600 seconds")
        if self.minimum_per_sensor < 1 or self.minimum_per_sensor > 100:
            raise R2LabPhysicalExperimentError("minimum events per sensor must be between 1 and 100")
        if self.local_qfit_relay_port < 1024 or self.local_qfit_relay_port > 65535:
            raise R2LabPhysicalExperimentError("local qfit relay port is invalid")
        return self


def _suffix(run_id: str) -> str:
    import hashlib

    return hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:12]


def physical_central_name(run_id: str) -> str:
    validate_run_id(run_id)
    return f"synthran-r2lab-central-{_suffix(run_id)}"


def render_physical_central_objects(
    scenario: PhysicalExperimentScenario,
    *,
    lock: DependencyLock,
    core_node: str,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    """Render only the run-owned host-network central broker resources."""

    name = physical_central_name(scenario.run_id)
    labels = {
        "app.kubernetes.io/name": "synthran-experiment",
        "app.kubernetes.io/component": "mqtt",
        RUN_LABEL: scenario.run_id,
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
                    RUN_LABEL: scenario.run_id,
                    ROLE_LABEL: "central-mqtt",
                }
            },
            "template": {
                "metadata": {
                    "labels": {
                        RUN_LABEL: scenario.run_id,
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
                                "tcpSocket": {"port": CENTRAL_PORT},
                                "initialDelaySeconds": 2,
                                "periodSeconds": 2,
                            },
                        }
                    ],
                    "volumes": [
                        {
                            "name": "config",
                            "configMap": {"name": name},
                        }
                    ],
                },
            },
        },
    }
    return config, deployment


def _validate_qfit(qfit: str) -> str:
    value = qfit.strip().lower()
    if not _QFIT_RE.fullmatch(value):
        raise R2LabPhysicalExperimentError("physical workload requires one reviewed qfit UE")
    return value


def _nested_qfit_command(slice_name: str, qfit: str, *remote: str) -> tuple[str, ...]:
    qfit = _validate_qfit(qfit)
    inner = (
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        "--",
        f"root@{qfit}",
        shlex.join(remote),
    )
    return gateway_command(slice_name, shlex.join(inner))


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


def build_qfit_stdio_relay_command(
    *,
    slice_name: str,
    qfit: str,
    run_id: str,
    central_address: str,
    central_port: int = CENTRAL_PORT,
    interface: str = _QFIT_INTERFACE,
) -> tuple[str, ...]:
    """Build one strict SSH command whose remote socket is bound to wwan0."""

    validate_run_id(run_id)
    _validate_qfit(qfit)
    try:
        address = ipaddress.ip_address(central_address)
    except ValueError as exc:
        raise R2LabPhysicalExperimentError("central broker address must be a literal IP") from exc
    if not isinstance(address, ipaddress.IPv4Address):
        raise R2LabPhysicalExperimentError("current physical workload is IPv4-only")
    if central_port < 1 or central_port > 65535:
        raise R2LabPhysicalExperimentError("central broker port is invalid")
    if interface != _QFIT_INTERFACE:
        raise R2LabPhysicalExperimentError("physical relay must bind to wwan0")
    return _nested_qfit_command(
        slice_name,
        qfit,
        "python3",
        "-c",
        _RELAY_SCRIPT,
        run_id,
        str(address),
        str(central_port),
        interface,
    )


def route_uses_wwan0(text: str, destination: str) -> bool:
    """Accept only an exact JSON route observation through wwan0."""

    try:
        wanted = str(ipaddress.ip_address(destination))
        payload = json.loads(text)
    except (ValueError, json.JSONDecodeError):
        return False
    if not isinstance(payload, list) or not payload:
        return False
    matching = [
        item
        for item in payload
        if isinstance(item, dict)
        and item.get("dev") == _QFIT_INTERFACE
        and str(item.get("dst", wanted)) == wanted
    ]
    return bool(matching)


def _qfit_read(
    *,
    slice_name: str,
    qfit: str,
    command: Sequence[str],
    timeout_seconds: int = 30,
) -> CommandResult:
    result = _run(
        _nested_qfit_command(slice_name, qfit, *tuple(command)),
        timeout_seconds=timeout_seconds,
    )
    if result.returncode != 0:
        raise R2LabPhysicalExperimentError("qfit workload precondition could not be observed")
    return result


def _qfit_counter(slice_name: str, qfit: str, counter: str) -> int:
    if counter not in {"rx_bytes", "tx_bytes"}:
        raise R2LabPhysicalExperimentError("unsupported qfit interface counter")
    result = _qfit_read(
        slice_name=slice_name,
        qfit=qfit,
        command=("cat", f"/sys/class/net/{_QFIT_INTERFACE}/statistics/{counter}"),
    )
    value = result.stdout.strip()
    if not value.isdigit():
        raise R2LabPhysicalExperimentError("qfit interface counter returned malformed data")
    return int(value)


def _prove_qfit_route(slice_name: str, qfit: str, central_address: str) -> None:
    route = _qfit_read(
        slice_name=slice_name,
        qfit=qfit,
        command=("ip", "-j", "route", "get", central_address),
    )
    if not route_uses_wwan0(route.stdout, central_address):
        raise R2LabPhysicalExperimentError(
            "central MQTT destination is not currently routed through wwan0"
        )


def _prove_qfit_python(slice_name: str, qfit: str) -> None:
    result = _qfit_read(
        slice_name=slice_name,
        qfit=qfit,
        command=("python3", "-c", "import socket; print('ok')"),
    )
    if result.stdout.strip() != "ok":
        raise R2LabPhysicalExperimentError("qfit python3 relay capability is unavailable")


def _qfit_relay_process_count(slice_name: str, qfit: str, run_id: str) -> int:
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
    result = _qfit_read(
        slice_name=slice_name,
        qfit=qfit,
        command=("python3", "-c", probe, _RELAY_MARKER, run_id),
    )
    value = result.stdout.strip()
    if not value.isdigit():
        raise R2LabPhysicalExperimentError("qfit relay cleanup probe returned malformed data")
    return int(value)


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
            if process.poll() is None:
                try:
                    process.terminate()
                    process.wait(timeout=3)
                except Exception:
                    try:
                        process.kill()
                    except Exception:
                        pass


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
                    self.request.shutdown(socket.SHUT_WR)
                except OSError:
                    pass

        outgoing = threading.Thread(target=to_process, daemon=True)
        incoming = threading.Thread(target=to_client, daemon=True)
        outgoing.start()
        incoming.start()
        outgoing.join()
        incoming.join()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                process.terminate()
            except OSError:
                pass
        finally:
            self.server.unregister_child(process)


class ManagedQfitRelay:
    """Local TCP endpoint that creates one strict qfit stdio relay per client."""

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


def _physical_manifest(
    *,
    scenario: PhysicalExperimentScenario,
    status: str,
    cleanup_proven: bool,
    failure: str | None,
    data_evidence_sha256: str | None,
    qfit_tx_delta: int | None,
    qfit_rx_delta: int | None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema": "synthran/r2lab-physical-iot-workload/v1alpha1",
        "run_id": scenario.run_id,
        "physical_run_id": scenario.network_run_id,
        "backend": "r2lab",
        "ue_interface": _QFIT_INTERFACE,
        "status": status,
        "cleanup_proven": cleanup_proven,
        "data_evidence_sha256": data_evidence_sha256,
        "qfit_tx_delta_bytes": qfit_tx_delta,
        "qfit_rx_delta_bytes": qfit_rx_delta,
        "raw_pdu_address_persisted": False,
        "raw_qfit_route_persisted": False,
        "updated_at_utc": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
    }
    if failure is not None:
        payload["failure"] = "physical workload failed; inspect run-scoped logs/evidence"
    return payload


def _save_physical_manifest(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(dict(payload), indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        delete=False,
    ) as temporary:
        temporary.write(content)
        temporary_path = Path(temporary.name)
    temporary_path.replace(path)


def _central_rollout(inventory: NetworkInventory, deployment: str) -> None:
    _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl rollout status deployment/"
        f"{shlex.quote(deployment)} -n {KUBERNETES_NAMESPACE} --timeout=180s",
        label="physical central MQTT rollout",
        timeout_seconds=200,
    )


def _remote_tun_cleanup(inventory: NetworkInventory) -> list[str]:
    errors: list[str] = []
    try:
        exists = _remote_path_exists(inventory, "/sys/class/net/tun0", timeout_seconds=5)
    except Exception as exc:
        return [f"remote tun0 existence check: {exc}"]
    if exists:
        try:
            _remote(
                inventory,
                "ip",
                "link",
                "delete",
                "dev",
                "tun0",
                label="physical remote tun0 cleanup",
                timeout_seconds=10,
            )
        except Exception as exc:
            errors.append(f"remote tun0 cleanup: {exc}")
    try:
        if _remote_path_exists(inventory, "/sys/class/net/tun0", timeout_seconds=5):
            errors.append("remote tun0 cleanup postcondition: tun0 still exists")
    except Exception as exc:
        errors.append(f"remote tun0 cleanup postcondition: {exc}")
    return errors


def execute_physical_iot_workload(
    context: PhysicalWorkloadContext,
    *,
    config: PhysicalExperimentConfig,
) -> PhysicalWorkloadResult:
    """Run the deterministic 10-sensor workload through qfit/wwan0.

    This function assumes it is called by ``execute_physical_workload_handoff``
    after the ordered physical user-plane stage has passed.  It performs no R2Lab
    booking, radio power, modem attach, or gNB lifecycle mutation.
    """

    config.validate()
    if sys.platform != "linux":
        raise R2LabPhysicalExperimentError("physical workload execution requires Linux")
    if os.environ.get("CONDA_DEFAULT_ENV") != "synthran":
        raise R2LabPhysicalExperimentError(
            "physical workload execution requires the active synthran Conda environment"
        )
    if context.interface != _QFIT_INTERFACE:
        raise R2LabPhysicalExperimentError("physical workload context must use wwan0")
    qfit = _validate_qfit(context.qfit)
    scenario = PhysicalExperimentScenario(
        run_id=config.workload_id,
        network_run_id=context.run_id,
        sensor_period_seconds=config.sensor_period_seconds,
        cooja_seed=config.cooja_seed,
    )

    inventory = config.inventory
    lock = config.lock
    repository_root = config.repository_root.resolve()
    dependency_root = config.dependency_root.resolve()
    run_root = config.run_root.resolve()
    core_address = _core_address(inventory)
    core_host = inventory.core_node

    def report(message: str) -> None:
        if config.progress is not None:
            print(f"[synthran] {message}", file=config.progress, flush=True)

    # Fail before run-scoped mutation if the physical path cannot host the
    # workload adapter or the destination route is not currently cellular.
    _prove_qfit_python(config.slice_name, qfit)
    _prove_qfit_route(config.slice_name, qfit, core_address)
    if _qfit_relay_process_count(config.slice_name, qfit, scenario.run_id) != 0:
        raise R2LabPhysicalExperimentError(
            "an existing qfit relay still owns this workload ID"
        )
    _probe_experiment_host(
        inventory,
        required_ports=(
            scenario.serial_socket_port,
            REMOTE_EDGE_FORWARD_PORT,
            LOCAL_CENTRAL_FORWARD_PORT,
            CENTRAL_PORT,
        ),
    )
    _probe_ssh_forwarding(inventory)
    contiki = _validate_contiki_checkout(lock, dependency_root)
    java_home = _validate_java_runtime()

    run_root.mkdir(parents=True, exist_ok=True)
    run_directory = run_root / scenario.run_id
    try:
        run_directory.mkdir()
    except FileExistsError as exc:
        raise R2LabPhysicalExperimentError(
            "physical workload run directory already exists; choose a new workload ID"
        ) from exc
    logs = run_directory / "logs"
    logs.mkdir()
    _prepare_cooja_checkout(contiki)
    _, csc, scenario_path = write_run_inputs(
        scenario,  # type: ignore[arg-type]
        run_directory=run_directory,
        contiki_directory=contiki,
    )
    _copy_sensor_source(repository_root, run_directory)

    manifest_path = run_directory / "manifest.json"
    experiment_evidence_path = run_directory / "experiment-evidence.json"
    physical_summary_path = run_directory / "physical-workload.json"
    jsonl_path = run_directory / "telemetry.jsonl"
    rejected_path = run_directory / "rejected-events.jsonl"
    parquet_path = run_directory / "telemetry.parquet"
    _save_manifest(
        manifest_path,
        {
            "schema": "synthran/experiment-run-r2lab/v1alpha1",
            "run_id": scenario.run_id,
            "physical_run_id": context.run_id,
            "status": "running",
            "backend": "r2lab",
            "ue_interface": _QFIT_INTERFACE,
            "scenario": scenario_path.name,
            "reservation_action": "none",
            "network_deployment_action": "none",
        },
    )

    processes: list[ManagedProcess] = []
    relay: ManagedQfitRelay | None = None
    central_deployment = physical_central_name(scenario.run_id)
    remote_workspace = f"/tmp/synthran/{scenario.run_id}"
    remote_workspace_created = False
    tun_started = False
    failure: str | None = None
    cleanup_errors: list[str] = []
    checks: list[ExperimentCheck] = []
    tx_before: int | None = None
    rx_before: int | None = None
    tx_after: int | None = None
    rx_after: int | None = None
    data_ready = False

    try:
        report("physical workload: staging central MQTT broker...")
        for index, obj in enumerate(
            render_physical_central_objects(
                scenario,
                lock=lock,
                core_node=core_host.name,
            ),
            start=1,
        ):
            _kubectl_apply_object(
                inventory,
                obj,
                label=f"physical central MQTT object {index}",
            )
        _central_rollout(inventory, central_deployment)

        # Re-prove the destination route after the central endpoint exists and
        # before any sensor traffic is accepted.
        _prove_qfit_route(config.slice_name, qfit, core_address)
        tx_before = _qfit_counter(config.slice_name, qfit, "tx_bytes")
        rx_before = _qfit_counter(config.slice_name, qfit, "rx_bytes")

        relay_command = build_qfit_stdio_relay_command(
            slice_name=config.slice_name,
            qfit=qfit,
            run_id=scenario.run_id,
            central_address=core_address,
        )
        relay = ManagedQfitRelay(
            port=config.local_qfit_relay_port,
            command=relay_command,
        )
        relay.start()

        reverse_edge = _start_process(
            "physical qfit edge reverse tunnel",
            _ssh_reverse_tunnel_command(
                inventory,
                remote_port=REMOTE_EDGE_FORWARD_PORT,
                local_port=relay.port,
            ),
            cwd=repository_root,
            log_path=logs / "qfit-edge-reverse-tunnel.log",
        )
        processes.append(reverse_edge)
        _wait_remote_tcp(
            inventory,
            host="127.0.0.1",
            port=REMOTE_EDGE_FORWARD_PORT,
            timeout_seconds=30,
            process=reverse_edge,
        )

        central_forward = _start_process(
            "physical central MQTT port-forward",
            _ssh_tunnel_command(
                inventory,
                local_port=LOCAL_CENTRAL_FORWARD_PORT,
                remote_port=LOCAL_CENTRAL_FORWARD_PORT,
                remote_command=(
                    "KUBECONFIG=/etc/kubernetes/admin.conf kubectl port-forward "
                    f"-n {KUBERNETES_NAMESPACE} deployment/{central_deployment} "
                    f"{LOCAL_CENTRAL_FORWARD_PORT}:{CENTRAL_PORT} --address 127.0.0.1"
                ),
            ),
            cwd=repository_root,
            log_path=logs / "central-port-forward.log",
        )
        processes.append(central_forward)
        _wait_tcp(
            "127.0.0.1",
            LOCAL_CENTRAL_FORWARD_PORT,
            timeout_seconds=30,
            process=central_forward,
        )

        _remote(
            inventory,
            "mkdir",
            "-p",
            f"{remote_workspace}/serial-io",
            label="physical remote workspace creation",
        )
        remote_workspace_created = True
        _transfer_directory(
            inventory,
            contiki / "tools" / "serial-io",
            f"{remote_workspace}/serial-io",
            label="physical serial-io transfer",
        )
        _transfer_file(
            inventory,
            repository_root / "synthran" / "ingress.py",
            f"{remote_workspace}/ingress.py",
            label="physical ingress helper transfer",
        )
        _remote(
            inventory,
            "make",
            "-C",
            f"{remote_workspace}/serial-io",
            "tunslip6",
            label="physical remote tunslip6 build",
            timeout_seconds=180,
        )

        cooja_env = os.environ.copy()
        cooja_env["JAVA_HOME"] = str(java_home)
        cooja = _start_process(
            "physical Cooja",
            (
                str(contiki / "tools" / "cooja" / "gradlew"),
                "--no-daemon",
                "--console=plain",
                "run",
                f"--args=--no-gui {csc}",
            ),
            cwd=contiki / "tools" / "cooja",
            log_path=logs / "cooja.log",
            env=cooja_env,
        )
        processes.append(cooja)
        _wait_tcp(
            "127.0.0.1",
            scenario.serial_socket_port,
            timeout_seconds=180,
            process=cooja,
        )
        checks.append(
            ExperimentCheck(
                "cooja",
                True,
                "deterministic 10-sensor simulation exposed its serial socket",
            )
        )

        reverse_serial = _start_process(
            "physical SerialSocket reverse SSH tunnel",
            _ssh_reverse_tunnel_command(
                inventory,
                remote_port=scenario.serial_socket_port,
                local_port=scenario.serial_socket_port,
            ),
            cwd=repository_root,
            log_path=logs / "serial-reverse-tunnel.log",
        )
        processes.append(reverse_serial)
        time.sleep(1)
        if reverse_serial.process.poll() is not None:
            raise R2LabPhysicalExperimentError("SerialSocket reverse SSH tunnel failed to start")

        tunslip = _start_process(
            "physical tunslip6",
            ssh_command(
                inventory.core_node,
                "sh",
                "-c",
                f"exec {shlex.quote(remote_workspace)}/serial-io/tunslip6 "
                "-a 127.0.0.1 "
                f"-p {scenario.serial_socket_port} "
                "-t tun0 fd00::1/64",
            ),
            cwd=repository_root,
            log_path=logs / "tunslip6.log",
        )
        processes.append(tunslip)
        tun_started = True

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if tunslip.process.poll() is not None:
                raise R2LabPhysicalExperimentError("tunslip6 exited before tun0 became ready")
            result = _run(
                ssh_command(
                    inventory.core_node,
                    "ip",
                    "-j",
                    "address",
                    "show",
                    "dev",
                    "tun0",
                ),
                timeout_seconds=5,
            )
            if result.returncode == 0 and "fd00::1" in result.stdout:
                break
            time.sleep(1)
        else:
            raise R2LabPhysicalExperimentError("tun0 did not become ready on the core node")
        checks.append(
            ExperimentCheck(
                "rpl-border-router",
                True,
                "Cooja SerialSocket is bridged through remote tunslip6/tun0",
            )
        )

        snapshot_remote_path = f"{remote_workspace}/ingress-snapshot.json"
        ingress_command = (
            f"exec python3 {shlex.quote(remote_workspace)}/ingress.py "
            "--listen-host fd00::1 --listen-port 1883 "
            "--target-host 127.0.0.1 "
            f"--target-port {REMOTE_EDGE_FORWARD_PORT} "
            f"--snapshot-path {shlex.quote(snapshot_remote_path)}"
        )
        ingress = _start_process(
            "physical CountedTcpIngress",
            ssh_command(inventory.core_node, "sh", "-c", ingress_command),
            cwd=repository_root,
            log_path=logs / "ingress.log",
        )
        processes.append(ingress)
        time.sleep(1)
        if ingress.process.poll() is not None:
            raise R2LabPhysicalExperimentError("physical CountedTcpIngress failed to start")

        collection = collect_mqtt(
            scenario,  # type: ignore[arg-type]
            host="127.0.0.1",
            port=LOCAL_CENTRAL_FORWARD_PORT,
            jsonl_path=jsonl_path,
            rejected_path=rejected_path,
            minimum_per_sensor=config.minimum_per_sensor,
            timeout_seconds=config.collection_seconds,
        )
        if not collection.completed:
            raise R2LabPhysicalExperimentError(
                "physical collector did not observe all ten deterministic sensor streams"
            )
        checks.append(
            ExperimentCheck(
                "central-mqtt",
                True,
                f"central broker delivered {collection.records} events from 10 sensors",
            )
        )

        snapshot_data = _remote_json(
            inventory,
            f"cat {shlex.quote(snapshot_remote_path)}",
            label="physical ingress snapshot probe",
        )
        ingress_snapshot = IngressSnapshot.from_dict(snapshot_data)
        if (
            ingress_snapshot.accepted_connections < scenario.sensor_count
            or ingress_snapshot.upstream_bytes <= 0
        ):
            raise R2LabPhysicalExperimentError("Cooja MQTT ingress was not proven through tun0")
        checks.append(
            ExperimentCheck(
                "edge-adapter",
                True,
                "ten-sensor MQTT ingress crossed the qfit stdio relay adapter",
            )
        )

        tx_after = _qfit_counter(config.slice_name, qfit, "tx_bytes")
        rx_after = _qfit_counter(config.slice_name, qfit, "rx_bytes")
        if tx_before is None or tx_after <= tx_before:
            raise R2LabPhysicalExperimentError(
                "wwan0 TX counter did not increase during physical telemetry delivery"
            )
        checks.append(
            ExperimentCheck(
                "5g-egress",
                True,
                f"wwan0 counters increased (tx +{tx_after - tx_before}, "
                f"rx +{max(0, rx_after - (rx_before or 0))})",
            )
        )
        _prove_qfit_route(config.slice_name, qfit, core_address)
        checks.append(
            ExperimentCheck(
                "physical-route",
                True,
                "central MQTT destination remained routed through wwan0",
            )
        )

        records = load_jsonl(jsonl_path, expected_run_id=scenario.run_id)
        write_parquet(records, parquet_path)
        evidence = build_data_evidence(
            scenario=scenario,  # type: ignore[arg-type]
            scenario_path=scenario_path,
            jsonl_path=jsonl_path,
            parquet_path=parquet_path,
            minimum_per_sensor=config.minimum_per_sensor,
            extra_checks=checks,
        )
        if not evidence.ready:
            raise R2LabPhysicalExperimentError("physical experiment data evidence is incomplete")
        save_experiment_evidence(evidence, experiment_evidence_path)
        data_ready = True
    except Exception as exc:
        failure = str(exc)
        report("physical workload failed; preserving run-scoped evidence")
    finally:
        for process in reversed(processes):
            try:
                process.stop()
            except Exception as exc:
                cleanup_errors.append(f"local process cleanup: {exc}")
        if relay is not None:
            try:
                relay.stop()
            except Exception as exc:
                cleanup_errors.append(f"qfit relay cleanup: {exc}")

        try:
            _cleanup_remote_run_processes(
                inventory,
                remote_workspace=remote_workspace,
                ue_pod=None,
                central_deployment=central_deployment,
            )
        except Exception as exc:
            cleanup_errors.append(f"remote process cleanup: {exc}")

        if tun_started:
            cleanup_errors.extend(_remote_tun_cleanup(inventory))

        if remote_workspace_created:
            try:
                _remote(
                    inventory,
                    "rm",
                    "-rf",
                    remote_workspace,
                    label="physical remote workspace cleanup",
                    timeout_seconds=10,
                )
            except Exception as exc:
                cleanup_errors.append(f"remote workspace cleanup: {exc}")
            try:
                if _remote_path_exists(inventory, remote_workspace, timeout_seconds=5):
                    cleanup_errors.append("remote workspace still exists after cleanup")
            except Exception as exc:
                cleanup_errors.append(f"remote workspace cleanup postcondition: {exc}")

        try:
            _delete_experiment_objects(inventory, scenario.run_id)
        except Exception as exc:
            cleanup_errors.append(f"run-scoped Kubernetes cleanup: {exc}")

        try:
            if _qfit_relay_process_count(config.slice_name, qfit, scenario.run_id) != 0:
                cleanup_errors.append("qfit relay processes remain after cleanup")
        except Exception as exc:
            cleanup_errors.append(f"qfit relay cleanup postcondition: {exc}")

        try:
            _probe_experiment_host(
                inventory,
                required_ports=(
                    scenario.serial_socket_port,
                    REMOTE_EDGE_FORWARD_PORT,
                    LOCAL_CENTRAL_FORWARD_PORT,
                    CENTRAL_PORT,
                ),
                timeout_seconds=30,
            )
        except Exception as exc:
            cleanup_errors.append(f"core runtime cleanup postcondition: {exc}")

    cleanup_proven = not cleanup_errors
    if failure is None and not cleanup_proven:
        failure = "workload cleanup did not satisfy all exact postconditions"
    accepted = failure is None and data_ready and cleanup_proven
    data_digest = (
        sha256_file(experiment_evidence_path)
        if experiment_evidence_path.is_file()
        else None
    )
    tx_delta = (
        tx_after - tx_before
        if tx_before is not None and tx_after is not None and tx_after >= tx_before
        else None
    )
    rx_delta = (
        rx_after - rx_before
        if rx_before is not None and rx_after is not None and rx_after >= rx_before
        else None
    )
    _save_physical_manifest(
        physical_summary_path,
        _physical_manifest(
            scenario=scenario,
            status="iot-to-5g-path-proven" if accepted else "failed",
            cleanup_proven=cleanup_proven,
            failure=failure,
            data_evidence_sha256=data_digest,
            qfit_tx_delta=tx_delta,
            qfit_rx_delta=rx_delta,
        ),
    )
    _save_manifest(
        manifest_path,
        {
            "schema": "synthran/experiment-run-r2lab/v1alpha1",
            "run_id": scenario.run_id,
            "physical_run_id": context.run_id,
            "status": "iot-to-5g-path-proven" if accepted else "failed",
            "backend": "r2lab",
            "ue_interface": _QFIT_INTERFACE,
            "scenario": scenario_path.name,
            "physical_workload": physical_summary_path.name,
            "reservation_action": "none",
            "network_deployment_action": "none",
            "failure": None if accepted else "physical workload not proven",
        },
    )
    return PhysicalWorkloadResult(
        run_id=context.run_id,
        workload_id=scenario.run_id,
        backend="r2lab",
        interface=_QFIT_INTERFACE,
        evidence_sha256=sha256_file(physical_summary_path),
        accepted=accepted,
        cleanup_proven=cleanup_proven,
    )


def build_physical_workload_executor(
    config: PhysicalExperimentConfig,
):
    """Return the concrete executor expected by the R2Lab workload handoff."""

    config.validate()

    def execute(context: PhysicalWorkloadContext) -> PhysicalWorkloadResult:
        return execute_physical_iot_workload(context, config=config)

    return execute
