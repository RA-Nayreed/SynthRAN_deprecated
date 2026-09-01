"""Amber execution over an upstream-provisioned physical UE path.

All deployment, radio, and UE activation state comes from 5g-Ansible.  This
module owns only experiment-local MQTT resources, Amber replay, measurement,
and exact cleanup.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Mapping, TextIO

from synthran.dependencies import DependencyLock
from synthran.experiment import ExperimentError, validate_run_id, write_parquet
from synthran.experiment.live import (
    DEFAULT_COLLECTION_SECONDS,
    DEFAULT_MINIMUM_PER_SENSOR,
    ExperimentRunResult,
    LOCAL_CENTRAL_FORWARD_PORT,
    _core_address,
    _delete_experiment_objects,
    _kubectl_apply_object,
    _probe_ssh_forwarding,
    _wait_rollout,
)
from synthran.experiment.resources import CENTRAL_PORT, central_names, render_central_objects
from synthran.fiveg_ansible import NetworkInventory
from synthran.iot_collector import PortableMqttCollectorSession
from synthran.iot_edge_transport import (
    PHYSICAL_UE_INTERFACE,
    PhysicalEdgeTransportAdapter,
    PhysicalEdgeTransportSession,
    _local_forward_command,
    _local_port_is_closed,
    _remote_port_is_closed,
    _start_process,
    physical_ue_counter,
    prove_physical_ue_route,
    resolve_physical_ue,
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


PHYSICAL_EXPERIMENT_SCHEMA = "synthran/experiment-run/v2alpha1"


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
    profile: str,
    seed: int,
    period: int,
    status: str,
    failure: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": PHYSICAL_EXPERIMENT_SCHEMA,
        "run_id": run_id,
        "network_run_id": run_id,
        "backend": "physical",
        "status": status,
        "iot_source": AMBER_SOURCE_ID,
        "iot_profile": profile,
        "iot_seed": seed,
        "sensor_period_seconds": period,
        "updated_at_utc": _utc_now(),
        "reservation_action": "none",
        "network_deployment_action": "none",
    }
    if failure:
        payload["failure"] = failure
    return payload


def _central_port_closed(inventory: NetworkInventory) -> bool:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if _remote_port_is_closed(inventory, CENTRAL_PORT):
            return True
        time.sleep(0.25)
    return False


def execute_physical_amber_experiment(
    *,
    inventory: NetworkInventory,
    lock: DependencyLock,
    dependency_root: Path,
    run_id: str,
    ue: str,
    repository_root: Path,
    run_root: Path,
    collection_seconds: int = DEFAULT_COLLECTION_SECONDS,
    minimum_per_sensor: int = DEFAULT_MINIMUM_PER_SENSOR,
    iot_profile: str = TRANSPORT_PROFILE,
    iot_seed: int = DEFAULT_IOT_SEED,
    sensor_period_seconds: int = 10,
    progress: TextIO | None = None,
) -> ExperimentRunResult:
    """Run Amber through one physical UE described by upstream inventory facts."""

    if sys.platform != "linux":
        raise ExperimentError("physical Amber execution requires Linux")
    if os.environ.get("CONDA_DEFAULT_ENV") != "synthran":
        raise ExperimentError("physical Amber execution requires the synthran Conda environment")
    if not 30 <= collection_seconds <= 3600:
        raise ExperimentError("collection duration must be between 30 and 3600 seconds")
    if not 1 <= minimum_per_sensor <= 100:
        raise ExperimentError("minimum events per sensor must be between 1 and 100")

    run_id = validate_run_id(run_id)
    endpoint = resolve_physical_ue(inventory, ue)
    core_address = _core_address(inventory)
    prove_physical_ue_route(endpoint, core_address)
    _probe_ssh_forwarding(inventory)

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] {message}", file=progress, flush=True)

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

    source_spec = IoTSourceSpec(
        run_id=run_id,
        network_run_id=run_id,
        source=AMBER_SOURCE_ID,
        profile=iot_profile,
        seed=iot_seed,
        sensor_period_seconds=sensor_period_seconds,
    )
    _write_json(
        manifest_path,
        _manifest(
            run_id=run_id,
            profile=iot_profile,
            seed=iot_seed,
            period=sensor_period_seconds,
            status="preparing",
        ),
    )

    plan: PreparedIoTPlan | None = None
    transport: PhysicalEdgeTransportSession | None = None
    collector: PortableMqttCollectorSession | None = None
    publisher: AmberReplaySession | None = None
    central_forward = None
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
                label=f"physical Amber central object {index}",
            )
        _wait_rollout(inventory, central, label="physical Amber central MQTT rollout")

        prove_physical_ue_route(endpoint, core_address)
        tx_before = physical_ue_counter(endpoint, "tx_bytes")
        rx_before = physical_ue_counter(endpoint, "rx_bytes")

        central_forward = _start_process(
            "physical Amber central MQTT forward",
            _local_forward_command(
                inventory,
                local_port=LOCAL_CENTRAL_FORWARD_PORT,
                remote_port=CENTRAL_PORT,
            ),
            cwd=repository_root,
            log_path=logs / "physical-central-forward.log",
        )
        from synthran.iot_edge_transport import _wait_local_tcp
        _wait_local_tcp(LOCAL_CENTRAL_FORWARD_PORT, process=central_forward)

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

        transport = PhysicalEdgeTransportAdapter(
            inventory=inventory,
            ue=ue,
            repository_root=repository_root,
        ).start(
            run_id=run_id,
            run_directory=run_directory,
            core_address=core_address,
            central_port=CENTRAL_PORT,
        )
        publisher = AmberReplaySession(
            plan=plan,
            endpoint=transport.mqtt_endpoint,
            collector_barrier=collector,
        ).start()
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
        if iot_profile == TRANSPORT_PROFILE:
            counts: dict[str, int] = {}
            for record in records:
                sensor = str(record["sensor_id"])
                counts[sensor] = counts.get(sensor, 0) + 1
            if len(counts) != source_spec.sensor_count or any(
                count < minimum_per_sensor for count in counts.values()
            ):
                raise ExperimentError("transport-v1 did not satisfy the telemetry window")

        ingress = transport.snapshot()
        if (
            ingress.accepted_connections < source_spec.sensor_count
            or ingress.upstream_bytes <= 0
        ):
            raise ExperimentError("counted ingress did not prove all publisher connections")
        tx_after = physical_ue_counter(endpoint, "tx_bytes")
        rx_after = physical_ue_counter(endpoint, "rx_bytes")
        if tx_after <= tx_before:
            raise ExperimentError(f"{PHYSICAL_UE_INTERFACE} TX counter did not increase")
        prove_physical_ue_route(endpoint, core_address)
        write_parquet(records, parquet_path)
        transport_payload = {
            "publisher": publisher_evidence.to_dict(),
            "collector": collector.evidence().to_dict(),
            "ingress": ingress.to_dict(),
            "reconciliation": reconciliation.to_dict(),
            "ue": {
                "name": endpoint.name,
                "interface": PHYSICAL_UE_INTERFACE,
                "tx_before": tx_before,
                "tx_after": tx_after,
                "tx_delta": tx_after - tx_before,
                "rx_before": rx_before,
                "rx_after": rx_after,
                "rx_delta": max(0, rx_after - rx_before),
            },
        }
        accepted_delivery = True
    except Exception as exc:
        failure = str(exc)
        report(f"error: {failure}")
    finally:
        for label, session in (
            ("publisher", publisher),
            ("collector", collector),
            ("physical transport", transport),
            ("central forward", central_forward),
        ):
            if session is None:
                continue
            try:
                session.stop()
            except Exception as exc:
                cleanup_errors.append(f"{label} cleanup: {exc}")
        if transport is not None and not transport.evidence().get("cleanup_valid"):
            cleanup_errors.extend(
                str(item) for item in transport.evidence().get("cleanup_errors", [])
            )
        if not _local_port_is_closed(LOCAL_CENTRAL_FORWARD_PORT):
            cleanup_errors.append("central MQTT controller port remained in use")
        try:
            _delete_experiment_objects(inventory, run_id)
        except Exception as exc:
            cleanup_errors.append(f"run-scoped Kubernetes cleanup: {exc}")
        if not _central_port_closed(inventory):
            cleanup_errors.append("central MQTT host port remained in use")
        try:
            prove_physical_ue_route(endpoint, core_address)
        except Exception as exc:
            cleanup_errors.append(f"physical path reproof: {exc}")

    ready = accepted_delivery and failure is None and not cleanup_errors
    if plan is not None:
        try:
            iot_evidence = json.loads(plan.evidence_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            iot_evidence = {"schema": "synthran/iot-evidence/v2alpha1", "run_id": run_id}
        iot_evidence["live_transport"] = transport_payload or None
        iot_evidence["cleanup"] = {"valid": not cleanup_errors, "errors": cleanup_errors}
        iot_evidence["ready"] = ready
        _write_json(plan.evidence_path, iot_evidence)

    wrapper = {
        "schema": "synthran/experiment-evidence/v2alpha1",
        "run_id": run_id,
        "network_run_id": run_id,
        "backend": "physical",
        "ue": endpoint.name,
        "ue_interface": PHYSICAL_UE_INTERFACE,
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
        raise ExperimentError("physical Amber experiment failed its cleanup or acceptance gate")
    return ExperimentRunResult(run_id, run_directory, evidence_path, True)
