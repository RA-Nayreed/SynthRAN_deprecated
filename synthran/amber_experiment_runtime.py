"""Live Amber workload execution over the accepted RFSIM 5G path."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shlex
import socket
import sys
import time
from typing import Any, Mapping, Protocol, TextIO

from synthran.dependencies import DependencyLock
from synthran.experiment import (
    ExperimentError,
    build_scenario,
    render_edge_mosquitto_config,
    validate_run_id,
    write_parquet,
)
from synthran.experiment_resources import (
    CENTRAL_PORT,
    EDGE_CONTAINER,
    names,
    render_edge_patch,
    render_experiment_objects,
)
from synthran.experiment.runtime import (
    DEFAULT_COLLECTION_SECONDS,
    DEFAULT_MINIMUM_PER_SENSOR,
    LOCAL_CENTRAL_FORWARD_PORT,
    ExperimentRunResult,
    _add_ue_route,
    _cleanup_live_resources,
    _collect_rollout_diagnostics,
    _core_address,
    _discover_ue_deployment,
    _interface_counter,
    _kubectl_apply_object,
    _kubectl_patch_deployment,
    _probe_ssh_forwarding,
    _remote,
    _replace_edge_runtime_config,
    _restart_edge_sidecar,
    _ssh_tunnel_command,
    _start_process,
    _transfer_file,
    _wait_rollout,
    _wait_tcp,
)
from synthran.fiveg_ansible import NetworkInventory
from synthran.iot_collector import PortableMqttCollectorSession
from synthran.iot_edge_transport import (
    RFSIM_AMBER_INGRESS_PORT,
    RFSIM_EDGE_FORWARD_PORT,
    RfsimEdgeTransportAdapter,
    RfsimEdgeTransportSession,
)
from synthran.iot_publisher import AmberReplaySession
from synthran.iot_source import (
    AMBER_SOURCE_ID,
    DEFAULT_IOT_SEED,
    TRANSPORT_PROFILE,
    AmberSourceAdapter,
    IoTSourceSpec,
    PreparedIoTPlan,
    reconcile_source_and_transport,
)
from synthran.network_runtime import verify_network_path
from synthran.rfsim_runtime import reconcile_rfsim_runtime


AMBER_EXPERIMENT_SCHEMA = "synthran/experiment-run/v2alpha1"
KUBERNETES_NAMESPACE = "open5gs"
REMOTE_AMBER_PORTS = (
    RFSIM_EDGE_FORWARD_PORT,
    LOCAL_CENTRAL_FORWARD_PORT,
    RFSIM_AMBER_INGRESS_PORT,
)
LOCAL_AMBER_PORTS = (LOCAL_CENTRAL_FORWARD_PORT, RFSIM_AMBER_INGRESS_PORT)


@dataclass(frozen=True)
class AmberRuntimeContext:
    """Live identity handed explicitly to optional measurement instrumentation."""

    run_id: str
    network_run_id: str
    ue_pod: str
    pdu_address: str
    run_directory: Path
    source_plan: PreparedIoTPlan


class AmberMeasurementLifecycle(Protocol):
    """Optional synchronous lifecycle that runs while Amber replays in background."""

    def run(self, context: AmberRuntimeContext) -> Mapping[str, Any]: ...

    def stop(self) -> None: ...


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(dict(value), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    temporary.replace(path)


def _manifest(
    *,
    run_id: str,
    network_run_id: str,
    profile: str,
    seed: int,
    period: int,
    status: str,
    scenario_name: str = "iot-scenario-v2.json",
    failure: str | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema": AMBER_EXPERIMENT_SCHEMA,
        "run_id": run_id,
        "network_run_id": network_run_id,
        "status": status,
        "iot_source": AMBER_SOURCE_ID,
        "iot_profile": profile,
        "iot_seed": seed,
        "sensor_period_seconds": period,
        "scenario": scenario_name,
        "updated_at_utc": _utc_now(),
        "reservation_action": "none",
        "network_deployment_action": "none",
    }
    if failure:
        value["failure"] = failure
    return value


def _local_port_free(port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def _edge_sidecar_status(
    inventory: NetworkInventory,
    pod: str,
) -> tuple[int, bool, bool, bool]:
    raw = _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl get pod "
        f"-n {KUBERNETES_NAMESPACE} {shlex.quote(pod)} -o json",
        label="Amber edge MQTT sidecar status",
    )
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ExperimentError("Amber edge MQTT sidecar status returned invalid JSON") from exc
    status = payload.get("status") if isinstance(payload, dict) else None
    if not isinstance(status, Mapping):
        raise ExperimentError("Amber edge MQTT sidecar status is unavailable")
    statuses = status.get("containerStatuses")
    if not isinstance(statuses, list):
        raise ExperimentError("Amber edge MQTT sidecar container status is unavailable")
    matches = [
        item
        for item in statuses
        if isinstance(item, Mapping) and item.get("name") == EDGE_CONTAINER
    ]
    if len(matches) != 1:
        raise ExperimentError("Amber edge MQTT sidecar container status is ambiguous")
    sidecar = matches[0]
    restart_count = sidecar.get("restartCount")
    if not isinstance(restart_count, int) or isinstance(restart_count, bool):
        raise ExperimentError("Amber edge MQTT sidecar restart count is unavailable")
    state = sidecar.get("state")
    running = isinstance(state, Mapping) and isinstance(state.get("running"), Mapping)
    container_ready = sidecar.get("ready") is True
    conditions = status.get("conditions")
    pod_ready = isinstance(conditions, list) and any(
        isinstance(item, Mapping)
        and item.get("type") == "Ready"
        and item.get("status") == "True"
        for item in conditions
    )
    return restart_count, container_ready, pod_ready, running


def _restart_edge_sidecar_and_wait(
    inventory: NetworkInventory,
    pod: str,
    *,
    timeout_seconds: int = 60,
) -> None:
    before, _, _, _ = _edge_sidecar_status(inventory, pod)
    _restart_edge_sidecar(inventory, pod)
    deadline = time.monotonic() + timeout_seconds
    latest = "restart not yet observed"
    while time.monotonic() < deadline:
        try:
            count, container_ready, pod_ready, running = _edge_sidecar_status(
                inventory, pod
            )
        except Exception as exc:
            latest = str(exc)
        else:
            latest = (
                f"restartCount={count}, containerReady={container_ready}, "
                f"podReady={pod_ready}, running={running}"
            )
            if count > before and container_ready and pod_ready and running:
                return
        time.sleep(1)
    raise ExperimentError(
        "Amber edge MQTT sidecar restart did not reach a new Ready container instance "
        f"within {timeout_seconds}s ({latest})"
    )


def _edge_bridge_connected(
    inventory: NetworkInventory,
    pod: str,
    *,
    pdu_address: str,
    central_address: str,
    central_port: int,
) -> bool:
    probe = r'''
import socket, sys
local_ip, remote_ip, remote_port = sys.argv[1], sys.argv[2], int(sys.argv[3])

def decode_ipv4(raw):
    try:
        return socket.inet_ntoa(bytes.fromhex(raw)[::-1])
    except Exception:
        return None

connected = False
try:
    lines = open('/proc/net/tcp', encoding='ascii', errors='replace').read().splitlines()[1:]
except OSError:
    lines = []
for line in lines:
    fields = line.split()
    if len(fields) < 4 or fields[3] != '01':
        continue
    local_raw, _ = fields[1].split(':', 1)
    remote_raw, remote_port_raw = fields[2].split(':', 1)
    if (
        decode_ipv4(local_raw) == local_ip
        and decode_ipv4(remote_raw) == remote_ip
        and int(remote_port_raw, 16) == remote_port
    ):
        connected = True
        break
print('1' if connected else '0')
'''
    output = _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
        f"-n {KUBERNETES_NAMESPACE} {shlex.quote(pod)} -c ue -- "
        "python3 -c "
        f"{shlex.quote(probe)} {shlex.quote(pdu_address)} "
        f"{shlex.quote(central_address)} {int(central_port)}",
        label="Amber edge MQTT bridge connection probe",
    ).strip()
    if output not in {"0", "1"}:
        raise ExperimentError("Amber edge MQTT bridge connection probe returned invalid data")
    return output == "1"


def _wait_edge_bridge_connected(
    inventory: NetworkInventory,
    pod: str,
    *,
    pdu_address: str,
    central_address: str,
    central_port: int,
    timeout_seconds: int = 60,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    latest = "bridge connection not yet observed"
    while time.monotonic() < deadline:
        try:
            if _edge_bridge_connected(
                inventory,
                pod,
                pdu_address=pdu_address,
                central_address=central_address,
                central_port=central_port,
            ):
                return
            latest = "no established bridge socket"
        except Exception as exc:
            latest = str(exc)
        time.sleep(1)
    raise ExperimentError(
        "Amber edge MQTT bridge did not establish the accepted PDU-to-central connection "
        f"within {timeout_seconds}s ({latest})"
    )


def _probe_amber_host(inventory: NetworkInventory) -> None:
    """Check only capabilities required by the Amber source transport."""

    probe = (
        "import json, os, socket\n"
        f"ports={list(REMOTE_AMBER_PORTS)!r}\n"
        "busy=[]\n"
        "for port in ports:\n"
        "    s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)\n"
        "    try:\n"
        "        s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)\n"
        "        s.bind(('127.0.0.1',port))\n"
        "    except OSError:\n"
        "        busy.append(port)\n"
        "    finally:\n"
        "        s.close()\n"
        "print(json.dumps({'uid':os.geteuid(),'busy_ports':busy}))\n"
    )
    raw = _remote(
        inventory,
        "python3",
        "-c",
        probe,
        label="Amber experiment-host capability probe",
    )
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ExperimentError("Amber experiment-host probe returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise ExperimentError("Amber experiment-host probe returned invalid data")
    if value.get("uid") != 0:
        raise ExperimentError("Amber experiment host must be accessed as root")
    busy = value.get("busy_ports")
    if busy:
        raise ExperimentError(
            f"Amber experiment remote loopback ports are already in use: {busy}"
        )
    local_busy = [port for port in LOCAL_AMBER_PORTS if not _local_port_free(port)]
    if local_busy:
        raise ExperimentError(
            f"Amber experiment controller loopback ports are already in use: {local_busy}"
        )


def _write_failure_artifacts(
    *,
    manifest_path: Path,
    wrapper_evidence_path: Path,
    run_id: str,
    network_run_id: str,
    profile: str,
    seed: int,
    period: int,
    failure: str,
) -> None:
    _atomic_json(
        manifest_path,
        _manifest(
            run_id=run_id,
            network_run_id=network_run_id,
            profile=profile,
            seed=seed,
            period=period,
            status="failed",
            failure=failure,
        ),
    )
    _atomic_json(
        wrapper_evidence_path,
        {
            "schema": "synthran/experiment-evidence/v2alpha1",
            "run_id": run_id,
            "network_run_id": network_run_id,
            "iot_source": AMBER_SOURCE_ID,
            "iot_profile": profile,
            "iot_seed": seed,
            "ready": False,
            "failure": failure,
            "updated_at_utc": _utc_now(),
        },
    )


def execute_amber_experiment(
    *,
    inventory: NetworkInventory,
    lock: DependencyLock,
    dependency_root: Path,
    network_manifest: Path,
    network_evidence: Path,
    run_id: str,
    repository_root: Path,
    run_root: Path,
    collection_seconds: int = DEFAULT_COLLECTION_SECONDS,
    minimum_per_sensor: int = DEFAULT_MINIMUM_PER_SENSOR,
    iot_profile: str = TRANSPORT_PROFILE,
    iot_seed: int = DEFAULT_IOT_SEED,
    sensor_period_seconds: int = 10,
    measurement_lifecycle: AmberMeasurementLifecycle | None = None,
    progress: TextIO | None = None,
) -> ExperimentRunResult:
    """Run an immutable Amber plan through the live RFSIM user plane."""

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] {message}", file=progress, flush=True)

    if sys.platform != "linux":
        raise ExperimentError("live experiment execution requires Linux")
    if os.environ.get("CONDA_DEFAULT_ENV") != "synthran":
        raise ExperimentError(
            "live experiment execution requires the active synthran Conda environment"
        )
    if collection_seconds < 30 or collection_seconds > 3600:
        raise ExperimentError("collection duration must be between 30 and 3600 seconds")
    if minimum_per_sensor < 1 or minimum_per_sensor > 100:
        raise ExperimentError("minimum events per sensor must be between 1 and 100")

    run_id = validate_run_id(run_id)
    transport_context = build_scenario(
        run_id=run_id,
        network_manifest=network_manifest,
        network_evidence=network_evidence,
    )
    source_spec = IoTSourceSpec(
        run_id=run_id,
        network_run_id=transport_context.network_run_id,
        source=AMBER_SOURCE_ID,
        profile=iot_profile,
        seed=iot_seed,
        sensor_period_seconds=sensor_period_seconds,
    )

    report(f"experiment: {run_id}")
    report("network prerequisite: verifying path-proven baseline...")
    base = verify_network_path(
        inventory=inventory,
        lock=lock,
        run_id=transport_context.network_run_id,
        timeout_seconds=120,
    )
    if not base.ready:
        raise ExperimentError("accepted network no longer satisfies path proof")
    _probe_amber_host(inventory)
    _probe_ssh_forwarding(inventory)
    report("network and Amber transport prerequisites: OK")

    run_root = run_root.resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    run_directory = run_root / run_id
    try:
        run_directory.mkdir()
    except FileExistsError as exc:
        raise ExperimentError("experiment run directory already exists; choose a new run ID") from exc
    logs = run_directory / "logs"
    logs.mkdir()

    manifest_path = run_directory / "manifest.json"
    wrapper_evidence_path = run_directory / "experiment-evidence.json"
    jsonl_path = run_directory / "telemetry.jsonl"
    rejected_path = run_directory / "rejected-events.jsonl"
    parquet_path = run_directory / "telemetry.parquet"
    _atomic_json(
        manifest_path,
        _manifest(
            run_id=run_id,
            network_run_id=transport_context.network_run_id,
            profile=iot_profile,
            seed=iot_seed,
            period=sensor_period_seconds,
            status="preparing",
        ),
    )

    source_adapter = AmberSourceAdapter(
        repository_root=repository_root,
        dependency_root=dependency_root,
    )
    plan: PreparedIoTPlan | None = None
    try:
        report("Amber source: preparing immutable event plan...")
        plan = source_adapter.prepare(source_spec, collection_seconds, run_directory)
    except Exception as exc:
        failure = str(exc)
        _write_failure_artifacts(
            manifest_path=manifest_path,
            wrapper_evidence_path=wrapper_evidence_path,
            run_id=run_id,
            network_run_id=transport_context.network_run_id,
            profile=iot_profile,
            seed=iot_seed,
            period=sensor_period_seconds,
            failure=failure,
        )
        raise
    report(
        f"Amber source: {plan.planned_count} opportunities, "
        f"{plan.decoded_count} decoded, {plan.source_loss_count} classified source loss"
    )
    _atomic_json(
        manifest_path,
        _manifest(
            run_id=run_id,
            network_run_id=transport_context.network_run_id,
            profile=iot_profile,
            seed=iot_seed,
            period=sensor_period_seconds,
            status="running",
            scenario_name=plan.scenario_path.name,
        ),
    )

    remote_workspace = f"/tmp/synthran/{run_id}"
    remote_workspace_created = False
    ue_deployment: str | None = None
    ue_pod: str | None = None
    central_forward = None
    edge_session: RfsimEdgeTransportSession | None = None
    collector: PortableMqttCollectorSession | None = None
    publisher: AmberReplaySession | None = None
    failure: str | None = None
    cleanup_errors: list[str] = []
    transport_payload: dict[str, Any] = {}
    lifecycle_payload: dict[str, Any] | None = None
    lifecycle_started = False
    run_accepted = False

    try:
        _remote(
            inventory,
            "mkdir",
            "-p",
            remote_workspace,
            label="Amber remote workspace creation",
        )
        remote_workspace_created = True
        _transfer_file(
            inventory,
            repository_root.resolve() / "synthran" / "ingress.py",
            f"{remote_workspace}/ingress.py",
            label="Amber counted ingress transfer",
        )

        core_address = _core_address(inventory)
        ue_deployment = _discover_ue_deployment(
            inventory, transport_context.network_run_id
        )
        resource_names = names(transport_context)
        for index, value in enumerate(
            render_experiment_objects(
                transport_context,
                lock=lock,
                core_node=inventory.core_node.name,
                core_address=core_address,
            ),
            start=1,
        ):
            _kubectl_apply_object(
                inventory,
                value,
                label=f"Amber experiment Kubernetes object {index}",
            )
        _remote(
            inventory,
            "sh",
            "-c",
            "KUBECONFIG=/etc/kubernetes/admin.conf kubectl rollout status deployment/"
            f"{resource_names['central_deployment']} -n {KUBERNETES_NAMESPACE} "
            "--timeout=180s",
            label="Amber central MQTT rollout",
            timeout_seconds=200,
        )

        _kubectl_patch_deployment(
            inventory,
            ue_deployment,
            render_edge_patch(
                transport_context,
                lock=lock,
                core_address=core_address,
            ),
            label="Amber srsUE MQTT sidecar patch",
        )
        try:
            _wait_rollout(inventory, ue_deployment, label="Amber srsUE MQTT rollout")
        except Exception as exc:
            _collect_rollout_diagnostics(
                inventory,
                network_run_id=transport_context.network_run_id,
                log_path=logs / "amber-srsue-mqtt-rollout-diagnostics.log",
                private_paths=(repository_root, dependency_root, run_directory, inventory.path),
            )
            raise ExperimentError(
                "Amber edge MQTT sidecar did not become Ready; diagnostics saved"
            ) from exc

        runtime_state = reconcile_rfsim_runtime(
            inventory,
            network_run_id=transport_context.network_run_id,
        )
        ue_pod = runtime_state.ue_pod
        transport_context = replace(
            transport_context,
            pdu_address=runtime_state.pdu_address,
        )
        _add_ue_route(inventory, ue_pod, core_address)
        edge_config = render_edge_mosquitto_config(
            transport_context,
            central_broker_address=core_address,
            central_broker_port=CENTRAL_PORT,
        )
        _replace_edge_runtime_config(inventory, ue_pod, edge_config)
        try:
            _restart_edge_sidecar_and_wait(inventory, ue_pod)
        except Exception as exc:
            _collect_rollout_diagnostics(
                inventory,
                network_run_id=transport_context.network_run_id,
                log_path=logs / "amber-srsue-mqtt-restart-diagnostics.log",
                private_paths=(repository_root, dependency_root, run_directory, inventory.path),
            )
            raise ExperimentError(
                "Amber edge MQTT sidecar restart did not become Ready; diagnostics saved"
            ) from exc
        try:
            _wait_edge_bridge_connected(
                inventory,
                ue_pod,
                pdu_address=transport_context.pdu_address,
                central_address=core_address,
                central_port=CENTRAL_PORT,
            )
        except Exception as exc:
            _collect_rollout_diagnostics(
                inventory,
                network_run_id=transport_context.network_run_id,
                log_path=logs / "amber-edge-bridge-diagnostics.log",
                private_paths=(repository_root, dependency_root, run_directory, inventory.path),
            )
            raise ExperimentError(
                "Amber edge MQTT bridge did not connect over the accepted 5G path; diagnostics saved"
            ) from exc

        after_patch = verify_network_path(
            inventory=inventory,
            lock=lock,
            run_id=transport_context.network_run_id,
            timeout_seconds=120,
        )
        if not after_patch.ready:
            _collect_rollout_diagnostics(
                inventory,
                network_run_id=transport_context.network_run_id,
                log_path=logs / "amber-srsue-mqtt-network-reproof-diagnostics.log",
                private_paths=(repository_root, dependency_root, run_directory, inventory.path),
                verification=after_patch,
            )
            failures = "; ".join(
                f"{check.name}: {check.detail}"
                for check in after_patch.checks
                if not check.passed
            )
            raise ExperimentError(
                "srsUE sidecar patch failed network reproof"
                + (f" ({failures})" if failures else "")
                + "; diagnostics saved"
            )
        tx_before = _interface_counter(inventory, ue_pod, "tun_srsue1", "tx_bytes")
        rx_before = _interface_counter(inventory, ue_pod, "tun_srsue1", "rx_bytes")

        central_forward = _start_process(
            "Amber central MQTT port-forward",
            _ssh_tunnel_command(
                inventory,
                local_port=LOCAL_CENTRAL_FORWARD_PORT,
                remote_port=LOCAL_CENTRAL_FORWARD_PORT,
                remote_command=(
                    "KUBECONFIG=/etc/kubernetes/admin.conf kubectl port-forward "
                    f"-n {KUBERNETES_NAMESPACE} "
                    f"deployment/{resource_names['central_deployment']} "
                    f"{LOCAL_CENTRAL_FORWARD_PORT}:{CENTRAL_PORT} --address 127.0.0.1"
                ),
            ),
            cwd=repository_root,
            log_path=logs / "amber-central-port-forward.log",
        )
        _wait_tcp(
            "127.0.0.1",
            LOCAL_CENTRAL_FORWARD_PORT,
            timeout_seconds=30,
            process=central_forward,
        )

        collector = PortableMqttCollectorSession(
            run_id=run_id,
            sensor_count=source_spec.sensor_count,
            topic_root=source_spec.topic_root,
            host="127.0.0.1",
            port=LOCAL_CENTRAL_FORWARD_PORT,
            jsonl_path=jsonl_path,
            rejected_path=rejected_path,
            default_timeout_seconds=max(30.0, float(collection_seconds) + 30.0),
        ).start()
        collector.wait_ready(timeout=30.0)

        edge_session = RfsimEdgeTransportAdapter(
            inventory=inventory,
            repository_root=repository_root,
        ).start(
            run_id=run_id,
            ue_pod=ue_pod,
            remote_workspace=remote_workspace,
            run_directory=run_directory,
        )

        publisher = AmberReplaySession(
            plan=plan,
            endpoint=edge_session.mqtt_endpoint,
            collector_barrier=collector,
        ).start()

        if measurement_lifecycle is not None:
            lifecycle_started = True
            lifecycle_payload = dict(
                measurement_lifecycle.run(
                    AmberRuntimeContext(
                        run_id=run_id,
                        network_run_id=transport_context.network_run_id,
                        ue_pod=ue_pod,
                        pdu_address=transport_context.pdu_address,
                        run_directory=run_directory,
                        source_plan=plan,
                    )
                )
            )

        publisher_evidence = publisher.wait(timeout=float(collection_seconds) + 60.0)
        collector.wait_end_canaries(source_spec.sensor_ids, timeout=30.0)

        decoded_pairs = tuple(event.key for event in plan.events if event.decoded)
        records = collector.wait_expected_pairs(decoded_pairs, timeout=30.0)
        central_pairs = [
            (str(record["sensor_id"]), int(record["sequence"])) for record in records
        ]
        reconciliation = reconcile_source_and_transport(
            plan.events,
            published_pairs=publisher.published_pairs(),
            central_pairs=central_pairs,
        )
        if not reconciliation.valid:
            raise ExperimentError(
                "Amber source/transport reconciliation found loss, duplicates, or unexpected telemetry"
            )
        if iot_profile == TRANSPORT_PROFILE and plan.source_loss_count:
            raise ExperimentError("transport-v1 produced scientific source loss")
        if iot_profile == TRANSPORT_PROFILE:
            counts: dict[str, int] = {}
            for record in records:
                sensor_id = str(record["sensor_id"])
                counts[sensor_id] = counts.get(sensor_id, 0) + 1
            if len(counts) != source_spec.sensor_count or any(
                count < minimum_per_sensor for count in counts.values()
            ):
                raise ExperimentError(
                    "transport-v1 did not satisfy the minimum telemetry window"
                )

        ingress_snapshot = edge_session.snapshot()
        if (
            ingress_snapshot.accepted_connections < source_spec.sensor_count
            or ingress_snapshot.upstream_bytes <= 0
        ):
            raise ExperimentError(
                "Amber counted ingress did not prove all ten publisher connections"
            )

        tx_after = _interface_counter(inventory, ue_pod, "tun_srsue1", "tx_bytes")
        rx_after = _interface_counter(inventory, ue_pod, "tun_srsue1", "rx_bytes")
        if tx_after <= tx_before:
            raise ExperimentError(
                "tun_srsue1 TX counter did not increase during Amber delivery"
            )

        live_network = verify_network_path(
            inventory=inventory,
            lock=lock,
            run_id=transport_context.network_run_id,
            timeout_seconds=120,
        )
        if not live_network.ready:
            raise ExperimentError(
                "accepted UPF path was not valid after Amber telemetry delivery"
            )

        write_parquet(records, parquet_path)
        transport_payload = {
            "publisher": publisher_evidence.to_dict(),
            "collector": collector.evidence().to_dict(),
            "ingress": ingress_snapshot.to_dict(),
            "reconciliation": reconciliation.to_dict(),
            "ue": {
                "interface": "tun_srsue1",
                "tx_before": tx_before,
                "tx_after": tx_after,
                "tx_delta": tx_after - tx_before,
                "rx_before": rx_before,
                "rx_after": rx_after,
                "rx_delta": max(0, rx_after - rx_before),
            },
            "network_reproof_ready": True,
            "measurement": lifecycle_payload,
        }
        run_accepted = True
    except Exception as exc:
        failure = str(exc)
        report(f"error: {failure}")
    finally:
        if measurement_lifecycle is not None and lifecycle_started:
            try:
                measurement_lifecycle.stop()
            except Exception as exc:
                cleanup_errors.append(f"measurement lifecycle cleanup: {exc}")
        if publisher is not None:
            try:
                publisher.stop()
            except Exception as exc:
                cleanup_errors.append(f"publisher cleanup: {exc}")
        if collector is not None:
            try:
                collector.stop()
            except Exception as exc:
                cleanup_errors.append(f"collector cleanup: {exc}")
        if edge_session is not None:
            try:
                edge_session.stop()
                edge_cleanup = edge_session.evidence()
                if not edge_cleanup.get("cleanup_valid"):
                    details = edge_cleanup.get("cleanup_errors")
                    if isinstance(details, list) and details:
                        cleanup_errors.extend(
                            f"RFSIM Amber edge transport: {detail}"
                            for detail in details
                        )
                    else:
                        cleanup_errors.append(
                            "RFSIM Amber edge transport cleanup was incomplete"
                        )
            except Exception as exc:
                cleanup_errors.append(f"edge transport cleanup: {exc}")
        if central_forward is not None:
            try:
                central_forward.stop()
            except Exception as exc:
                cleanup_errors.append(f"central forward cleanup: {exc}")
        if not _local_port_free(LOCAL_CENTRAL_FORWARD_PORT):
            cleanup_errors.append("central MQTT controller port remained in use")
        if remote_workspace_created:
            try:
                _remote(
                    inventory,
                    "rm",
                    "-rf",
                    remote_workspace,
                    label="Amber remote workspace cleanup",
                )
            except Exception as exc:
                cleanup_errors.append(f"remote workspace cleanup: {exc}")

        cleanup_check = _cleanup_live_resources(
            inventory=inventory,
            lock=lock,
            scenario=transport_context,
            ue_deployment=ue_deployment,
            cleanup_errors=cleanup_errors,
        )
        cleanup_valid = cleanup_check.passed
        ready = run_accepted and cleanup_valid and failure is None

        try:
            iot_evidence = json.loads(plan.evidence_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            iot_evidence = {
                "schema": "synthran/iot-evidence/v2alpha1",
                "run_id": run_id,
            }
        iot_evidence["live_transport"] = transport_payload or None
        iot_evidence["cleanup"] = {
            "valid": cleanup_valid,
            "detail": cleanup_check.detail,
            "errors": cleanup_errors,
        }
        iot_evidence["ready"] = ready
        if failure:
            iot_evidence["failure"] = failure
        _atomic_json(plan.evidence_path, iot_evidence)

        wrapper = {
            "schema": "synthran/experiment-evidence/v2alpha1",
            "run_id": run_id,
            "network_run_id": transport_context.network_run_id,
            "iot_source": AMBER_SOURCE_ID,
            "iot_profile": iot_profile,
            "iot_seed": iot_seed,
            "profile_digest": plan.profile_digest,
            "ready": ready,
            "iot_evidence": plan.evidence_path.name,
            "telemetry_jsonl": jsonl_path.name if jsonl_path.is_file() else None,
            "telemetry_parquet": parquet_path.name if parquet_path.is_file() else None,
            "updated_at_utc": _utc_now(),
        }
        if failure:
            wrapper["failure"] = failure
        if not cleanup_valid:
            wrapper["cleanup_failure"] = cleanup_check.detail
        _atomic_json(wrapper_evidence_path, wrapper)
        _atomic_json(
            manifest_path,
            _manifest(
                run_id=run_id,
                network_run_id=transport_context.network_run_id,
                profile=iot_profile,
                seed=iot_seed,
                period=sensor_period_seconds,
                status="accepted" if ready else "failed",
                scenario_name=plan.scenario_path.name,
                failure=failure or (None if cleanup_valid else cleanup_check.detail),
            ),
        )

    if failure:
        raise ExperimentError(failure)
    if not ready:
        raise ExperimentError("Amber experiment failed its cleanup or acceptance gate")
    return ExperimentRunResult(
        run_id=run_id,
        run_directory=run_directory,
        evidence_path=wrapper_evidence_path,
        ready=True,
    )
