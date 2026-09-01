"""Amber execution over an upstream-provisioned RFSIM deployment.

5g-Ansible owns the 5G lifecycle.  This module observes the accepted UE/PDU
identity, creates run-scoped MQTT/transport resources, executes Amber, measures
the path, removes only its own resources, and re-proves the upstream network.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping, Protocol, TextIO

from synthran.ambient_contract import (
    DEFAULT_ENERGY_NODE_VARIATION,
    DEFAULT_ENERGY_POWER_SCALE,
)
from synthran.dependencies import DependencyLock
from synthran.experiment import ExperimentError, build_scenario, validate_run_id, write_parquet
from synthran.experiment.live import (
    DEFAULT_COLLECTION_SECONDS,
    DEFAULT_MINIMUM_PER_SENSOR,
    ExperimentRunResult,
    LOCAL_CENTRAL_FORWARD_PORT,
    _delete_experiment_objects,
    _kubectl_apply_object,
    _probe_ssh_forwarding,
    _start_process,
    _wait_rollout,
)
from synthran.experiment.observe import (
    RFSIM_UE_INTERFACE,
    core_address,
    discover_rfsim_ue_pod,
    interface_counter,
    rfsim_pdu_address,
)
from synthran.experiment.resources import CENTRAL_PORT, central_names, render_central_objects
from synthran.experiment.rfsim_transport import (
    RfsimTransportAdapter,
    RfsimTransportSession,
    _core_local_forward,
    _local_port_closed,
    _remote_port_closed,
    _wait_local_listener,
)
from synthran.fiveg_ansible import NetworkInventory
from synthran.iot_collector import PortableMqttCollectorSession
from synthran.iot_publisher import AmberReplaySession
from synthran.iot_source import (
    AMBER_SOURCE_ID,
    AMBIENT_PROFILE,
    DEFAULT_IOT_SEED,
    TRANSPORT_PROFILE,
    AmberSourceAdapter,
    IoTSourceSpec,
    PreparedIoTPlan,
    reconcile_source_and_transport,
)
from synthran.network.runtime import verify_network_path


RFSIM_EXPERIMENT_SCHEMA = "synthran/experiment-run/v2alpha1"


@dataclass(frozen=True)
class AmberRuntimeContext:
    """Observed live identity handed to optional measurement instrumentation."""

    run_id: str
    network_run_id: str
    ue_pod: str
    pdu_address: str
    run_directory: Path
    source_plan: PreparedIoTPlan


class AmberMeasurementLifecycle(Protocol):
    def run(self, context: AmberRuntimeContext) -> Mapping[str, Any]: ...

    def stop(self) -> None: ...


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(dict(payload), indent=2, sort_keys=True) + "\n",
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
    failure: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": RFSIM_EXPERIMENT_SCHEMA,
        "run_id": run_id,
        "network_run_id": network_run_id,
        "backend": "rfsim",
        "status": status,
        "iot_source": AMBER_SOURCE_ID,
        "iot_profile": profile,
        "iot_seed": seed,
        "sensor_period_seconds": period,
        "updated_at_utc": _utc_now(),
        "network_deployment_action": "none",
    }
    if failure:
        payload["failure"] = failure
    return payload


def _minimum_window_ok(
    records: list[Mapping[str, Any]],
    *,
    sensor_count: int,
    minimum_per_sensor: int,
) -> bool:
    counts: dict[str, int] = {}
    for record in records:
        sensor = str(record["sensor_id"])
        counts[sensor] = counts.get(sensor, 0) + 1
    return len(counts) == sensor_count and all(
        count >= minimum_per_sensor for count in counts.values()
    )


def execute_rfsim_amber_experiment(
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
    energy_power_scale: float = DEFAULT_ENERGY_POWER_SCALE,
    energy_node_variation: float = DEFAULT_ENERGY_NODE_VARIATION,
    measurement_lifecycle: AmberMeasurementLifecycle | None = None,
    progress: TextIO | None = None,
) -> ExperimentRunResult:
    """Run Amber without repairing or redeploying any RFSIM infrastructure."""

    if sys.platform != "linux":
        raise ExperimentError("RFSIM Amber execution requires Linux")
    if os.environ.get("CONDA_DEFAULT_ENV") != "synthran":
        raise ExperimentError("RFSIM Amber execution requires the synthran Conda environment")
    if not 30 <= collection_seconds <= 3600:
        raise ExperimentError("collection duration must be between 30 and 3600 seconds")
    if not 1 <= minimum_per_sensor <= 100:
        raise ExperimentError("minimum events per sensor must be between 1 and 100")

    run_id = validate_run_id(run_id)
    scenario = build_scenario(
        run_id=run_id,
        network_manifest=network_manifest,
        network_evidence=network_evidence,
    )
    source_spec = IoTSourceSpec(
        run_id=run_id,
        network_run_id=scenario.network_run_id,
        source=AMBER_SOURCE_ID,
        profile=iot_profile,
        seed=iot_seed,
        sensor_period_seconds=sensor_period_seconds,
        energy_power_scale=energy_power_scale,
        energy_node_variation=energy_node_variation,
    )

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] {message}", file=progress, flush=True)

    report(f"experiment: {run_id}")
    if iot_profile == AMBIENT_PROFILE:
        report(
            "Amber energy treatment: "
            f"external-power-scale={source_spec.energy_power_scale:g}, "
            f"node-variation={source_spec.energy_node_variation:g}"
        )

    network = verify_network_path(
        inventory=inventory,
        lock=lock,
        run_id=scenario.network_run_id,
        timeout_seconds=120,
    )
    if not network.ready or network.pdu_address != scenario.pdu_address:
        raise ExperimentError("RFSIM experiment requires the persisted upstream path to remain ready")
    ue_pod = discover_rfsim_ue_pod(inventory, scenario.network_run_id)
    live_pdu = rfsim_pdu_address(inventory, ue_pod)
    if live_pdu != scenario.pdu_address:
        raise ExperimentError("RFSIM UE PDU identity changed after network acceptance")
    core_ip = core_address(inventory)
    _probe_ssh_forwarding(inventory)

    run_directory = run_root.expanduser().resolve() / run_id
    try:
        run_directory.mkdir(parents=True)
    except FileExistsError as exc:
        raise ExperimentError("experiment run directory already exists; choose a new run ID") from exc
    logs = run_directory / "logs"
    logs.mkdir()
    manifest_path = run_directory / "manifest.json"
    evidence_path = run_directory / "experiment-evidence.json"
    jsonl_path = run_directory / "telemetry.jsonl"
    rejected_path = run_directory / "rejected-events.jsonl"
    parquet_path = run_directory / "telemetry.parquet"

    _write_json(
        manifest_path,
        _manifest(
            run_id=run_id,
            network_run_id=scenario.network_run_id,
            profile=iot_profile,
            seed=iot_seed,
            period=sensor_period_seconds,
            status="preparing",
        ),
    )

    plan: PreparedIoTPlan | None = None
    transport: RfsimTransportSession | None = None
    collector: PortableMqttCollectorSession | None = None
    publisher: AmberReplaySession | None = None
    central_forward = None
    lifecycle_started = False
    lifecycle_payload: dict[str, Any] | None = None
    failure: str | None = None
    cleanup_errors: list[str] = []
    transport_payload: dict[str, Any] = {}
    accepted_delivery = False
    tx_before = rx_before = tx_after = rx_after = 0

    try:
        report("Amber source: preparing immutable event plan...")
        plan = AmberSourceAdapter(
            repository_root=repository_root,
            dependency_root=dependency_root,
        ).prepare(source_spec, collection_seconds, run_directory)
        _write_json(
            manifest_path,
            _manifest(
                run_id=run_id,
                network_run_id=scenario.network_run_id,
                profile=iot_profile,
                seed=iot_seed,
                period=sensor_period_seconds,
                status="running",
            ),
        )

        central = central_names(run_id)["central_deployment"]
        for index, obj in enumerate(
            render_central_objects(
                run_id=run_id,
                lock=lock,
                core_node=inventory.core_node.name,
            ),
            start=1,
        ):
            _kubectl_apply_object(
                inventory,
                obj,
                label=f"RFSIM Amber central object {index}",
            )
        _wait_rollout(inventory, central, label="RFSIM Amber central MQTT rollout")

        current = verify_network_path(
            inventory=inventory,
            lock=lock,
            run_id=scenario.network_run_id,
            timeout_seconds=120,
        )
        if not current.ready or current.pdu_address != live_pdu:
            raise ExperimentError("RFSIM upstream path changed while creating experiment resources")

        tx_before = interface_counter(inventory, ue_pod, RFSIM_UE_INTERFACE, "tx_bytes")
        rx_before = interface_counter(inventory, ue_pod, RFSIM_UE_INTERFACE, "rx_bytes")

        central_forward = _start_process(
            "RFSIM Amber central MQTT forward",
            _core_local_forward(
                inventory,
                local_port=LOCAL_CENTRAL_FORWARD_PORT,
                remote_port=CENTRAL_PORT,
            ),
            cwd=repository_root,
            log_path=logs / "amber-central-forward.log",
        )
        _wait_local_listener(LOCAL_CENTRAL_FORWARD_PORT, central_forward)

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

        transport = RfsimTransportAdapter(
            inventory=inventory,
            repository_root=repository_root,
        ).start(
            run_id=run_id,
            ue_pod=ue_pod,
            pdu_address=live_pdu,
            core_address=core_ip,
            central_port=CENTRAL_PORT,
            run_directory=run_directory,
        )

        publisher = AmberReplaySession(
            plan=plan,
            endpoint=transport.mqtt_endpoint,
            collector_barrier=collector,
        ).start()

        if measurement_lifecycle is not None:
            lifecycle_started = True
            lifecycle_payload = dict(
                measurement_lifecycle.run(
                    AmberRuntimeContext(
                        run_id=run_id,
                        network_run_id=scenario.network_run_id,
                        ue_pod=ue_pod,
                        pdu_address=live_pdu,
                        run_directory=run_directory,
                        source_plan=plan,
                    )
                )
            )

        publisher_evidence = publisher.wait(timeout=float(collection_seconds) + 60.0)
        collector.wait_end_canaries(source_spec.sensor_ids, timeout=30.0)
        decoded_pairs = tuple(event.key for event in plan.events if event.decoded)
        records = collector.wait_expected_pairs(decoded_pairs, timeout=30.0)
        reconciliation = reconcile_source_and_transport(
            plan.events,
            published_pairs=publisher.published_pairs(),
            central_pairs=[
                (str(record["sensor_id"]), int(record["sequence"])) for record in records
            ],
        )
        if not reconciliation.valid:
            raise ExperimentError("Amber source/transport reconciliation failed")
        if iot_profile == TRANSPORT_PROFILE and plan.source_loss_count:
            raise ExperimentError("transport-v1 produced source loss")
        if iot_profile == TRANSPORT_PROFILE and not _minimum_window_ok(
            records,
            sensor_count=source_spec.sensor_count,
            minimum_per_sensor=minimum_per_sensor,
        ):
            raise ExperimentError("transport-v1 did not satisfy the telemetry window")

        ingress = transport.snapshot()
        if (
            ingress.accepted_connections < source_spec.sensor_count
            or ingress.upstream_bytes <= 0
        ):
            raise ExperimentError("counted ingress did not prove all publisher connections")

        tx_after = interface_counter(inventory, ue_pod, RFSIM_UE_INTERFACE, "tx_bytes")
        rx_after = interface_counter(inventory, ue_pod, RFSIM_UE_INTERFACE, "rx_bytes")
        if tx_after <= tx_before:
            raise ExperimentError("tun_srsue1 TX counter did not increase during Amber delivery")

        final_network = verify_network_path(
            inventory=inventory,
            lock=lock,
            run_id=scenario.network_run_id,
            timeout_seconds=120,
        )
        if not final_network.ready or final_network.pdu_address != live_pdu:
            raise ExperimentError("RFSIM upstream path changed during Amber delivery")

        write_parquet(records, parquet_path)
        transport_payload = {
            "publisher": publisher_evidence.to_dict(),
            "collector": collector.evidence().to_dict(),
            "ingress": ingress.to_dict(),
            "reconciliation": reconciliation.to_dict(),
            "ue": {
                "pod": ue_pod,
                "interface": RFSIM_UE_INTERFACE,
                "pdu_address": live_pdu,
                "tx_before": tx_before,
                "tx_after": tx_after,
                "tx_delta": tx_after - tx_before,
                "rx_before": rx_before,
                "rx_after": rx_after,
                "rx_delta": max(0, rx_after - rx_before),
            },
            "measurement": lifecycle_payload,
        }
        accepted_delivery = True
    except Exception as exc:
        failure = str(exc)
        report(f"error: {failure}")
    finally:
        if measurement_lifecycle is not None and lifecycle_started:
            try:
                measurement_lifecycle.stop()
            except Exception as exc:
                cleanup_errors.append(f"measurement cleanup: {exc}")
        for label, session in (
            ("publisher", publisher),
            ("collector", collector),
            ("RFSIM transport", transport),
            ("central forward", central_forward),
        ):
            if session is None:
                continue
            try:
                session.stop()
            except Exception as exc:
                cleanup_errors.append(f"{label} cleanup: {exc}")
        if transport is not None:
            evidence = transport.evidence()
            if not evidence.get("cleanup_valid"):
                cleanup_errors.extend(str(item) for item in evidence.get("cleanup_errors", []))
        if not _local_port_closed(LOCAL_CENTRAL_FORWARD_PORT):
            cleanup_errors.append("central MQTT controller port remained in use")
        try:
            _delete_experiment_objects(inventory, run_id)
        except Exception as exc:
            cleanup_errors.append(f"run-scoped Kubernetes cleanup: {exc}")
        if not _remote_port_closed(inventory, CENTRAL_PORT):
            cleanup_errors.append("central MQTT host port remained in use")
        try:
            restored = verify_network_path(
                inventory=inventory,
                lock=lock,
                run_id=scenario.network_run_id,
                timeout_seconds=120,
            )
            if not restored.ready or restored.pdu_address != live_pdu:
                cleanup_errors.append("upstream RFSIM path did not revalidate after cleanup")
            elif discover_rfsim_ue_pod(inventory, scenario.network_run_id) != ue_pod:
                cleanup_errors.append("upstream RFSIM UE identity changed during experiment")
        except Exception as exc:
            cleanup_errors.append(f"upstream RFSIM reproof: {exc}")

    ready = accepted_delivery and failure is None and not cleanup_errors
    if plan is not None:
        try:
            iot_evidence = json.loads(plan.evidence_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            iot_evidence = {"schema": "synthran/iot-evidence/v2alpha1", "run_id": run_id}
        iot_evidence["live_transport"] = transport_payload or None
        iot_evidence["cleanup"] = {"valid": not cleanup_errors, "errors": cleanup_errors}
        iot_evidence["ready"] = ready
        if failure:
            iot_evidence["failure"] = failure
        _write_json(plan.evidence_path, iot_evidence)

    wrapper = {
        "schema": "synthran/experiment-evidence/v2alpha1",
        "run_id": run_id,
        "network_run_id": scenario.network_run_id,
        "backend": "rfsim",
        "ue_pod": ue_pod,
        "ue_interface": RFSIM_UE_INTERFACE,
        "pdu_address": live_pdu,
        "iot_source": AMBER_SOURCE_ID,
        "iot_profile": iot_profile,
        "iot_seed": iot_seed,
        "profile_digest": plan.profile_digest if plan is not None else None,
        "ready": ready,
        "iot_evidence": plan.evidence_path.name if plan is not None else None,
        "telemetry_jsonl": jsonl_path.name if jsonl_path.is_file() else None,
        "telemetry_parquet": parquet_path.name if parquet_path.is_file() else None,
        "cleanup_errors": cleanup_errors,
        "updated_at_utc": _utc_now(),
    }
    if failure:
        wrapper["failure"] = failure
    _write_json(evidence_path, wrapper)
    _write_json(
        manifest_path,
        _manifest(
            run_id=run_id,
            network_run_id=scenario.network_run_id,
            profile=iot_profile,
            seed=iot_seed,
            period=sensor_period_seconds,
            status="accepted" if ready else "failed",
            failure=failure or (None if ready else "experiment cleanup was not proven"),
        ),
    )
    if failure:
        raise ExperimentError(failure)
    if not ready:
        raise ExperimentError("RFSIM Amber experiment failed its cleanup or acceptance gate")
    return ExperimentRunResult(run_id, run_directory, evidence_path, True)
