"""Amber IoT workload execution through an upstream-provisioned physical path."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Mapping, TextIO

from synthran.dependencies import DependencyLock
from synthran.experiment import ExperimentError, sha256_file, validate_run_id, write_parquet
from synthran.experiment.live import (
    LOCAL_CENTRAL_FORWARD_PORT,
    _core_address,
    _delete_experiment_objects,
    _kubectl_apply_object,
    _probe_ssh_forwarding,
)
from synthran.experiment.resources import CENTRAL_PORT
from synthran.fiveg_ansible import NetworkInventory
from synthran.iot_collector import PortableMqttCollectorSession
from synthran.iot_edge_transport import (
    OwnedProcess,
    _local_forward_command,
    _local_port_is_closed,
    _remote_port_is_closed,
    _start_process,
    _wait_local_tcp,
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
from synthran.r2lab.iot_live import (
    LOCAL_UE_RELAY_PORT,
    _central_rollout,
    _prove_ue_route,
    _ue_counter,
    _ue_relay_process_count,
    _validate_ue,
    physical_central_name,
    render_physical_central_objects,
)
from synthran.r2lab.iot_transport import (
    R2LAB_AMBER_INGRESS_PORT,
    R2LabIoTTransportAdapter,
    R2LabIoTTransportSession,
)
from synthran.r2lab.ue import PhysicalWorkloadContext, PhysicalWorkloadResult


PHYSICAL_IOT_SCHEMA = "synthran/r2lab-iot-workload/v2alpha1"
PHYSICAL_RUN_SCHEMA = "synthran/experiment-run-r2lab/v2alpha1"


class R2LabIoTWorkloadError(ExperimentError):
    """Raised when Amber delivery through the physical UE cannot be proven."""


@dataclass(frozen=True)
class PhysicalIoTConfig:
    slice_name: str
    inventory: NetworkInventory
    lock: DependencyLock
    dependency_root: Path
    repository_root: Path
    known_hosts: Path
    workload_id: str
    run_root: Path
    collection_seconds: int = 180
    minimum_per_sensor: int = 3
    iot_profile: str = TRANSPORT_PROFILE
    iot_seed: int = DEFAULT_IOT_SEED
    sensor_period_seconds: int = 10
    local_ue_relay_port: int = LOCAL_UE_RELAY_PORT
    progress: TextIO | None = None

    def validate(self) -> "PhysicalIoTConfig":
        validate_run_id(self.workload_id)
        if not self.slice_name or len(self.slice_name) > 64:
            raise R2LabIoTWorkloadError("R2Lab slice name is malformed")
        if not self.known_hosts.expanduser().resolve().is_file():
            raise R2LabIoTWorkloadError("strict SLICES known-hosts file is missing")
        if not 30 <= self.collection_seconds <= 3600:
            raise R2LabIoTWorkloadError(
                "collection duration must be between 30 and 3600 seconds"
            )
        if not 1 <= self.minimum_per_sensor <= 100:
            raise R2LabIoTWorkloadError(
                "minimum events per sensor must be between 1 and 100"
            )
        if not 1 <= self.sensor_period_seconds <= 3600:
            raise R2LabIoTWorkloadError(
                "sensor period must be between 1 and 3600 seconds"
            )
        if self.iot_seed < 0:
            raise R2LabIoTWorkloadError("IoT seed must be non-negative")
        return self


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
    source_spec: IoTSourceSpec,
    physical_run_id: str,
    status: str,
    failure: str | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema": PHYSICAL_RUN_SCHEMA,
        "run_id": source_spec.run_id,
        "physical_run_id": physical_run_id,
        "backend": "r2lab",
        "ue_interface": "wwan0",
        "iot_source": AMBER_SOURCE_ID,
        "iot_profile": source_spec.profile,
        "iot_seed": source_spec.seed,
        "sensor_period_seconds": source_spec.sensor_period_seconds,
        "status": status,
        "scenario": "iot-scenario-v2.json",
        "reservation_action": "none",
        "network_deployment_action": "none",
        "updated_at_utc": _utc_now(),
    }
    if failure is not None:
        value["failure"] = "physical IoT workload was not proven"
    return value


def _cleanup_central_port(
    inventory: NetworkInventory,
    cleanup_errors: list[str],
) -> None:
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if _remote_port_is_closed(inventory, CENTRAL_PORT):
            return
        time.sleep(0.25)
    cleanup_errors.append("physical central MQTT port remained in use")


def _network_reproof(
    *,
    config: PhysicalIoTConfig,
    profile: Any,
    central_address: str,
) -> bool:
    """Re-prove only the experiment-side UE route; deployment health is upstream."""

    _prove_ue_route(config.slice_name, profile, central_address)
    return True


def execute_physical_iot_workload(
    context: PhysicalWorkloadContext,
    *,
    config: PhysicalIoTConfig,
) -> PhysicalWorkloadResult:
    """Run one Amber source plan through the selected physical UE and wwan0."""

    config.validate()
    if sys.platform != "linux":
        raise R2LabIoTWorkloadError("physical IoT execution requires Linux")
    if os.environ.get("CONDA_DEFAULT_ENV") != "synthran":
        raise R2LabIoTWorkloadError(
            "physical IoT execution requires the active synthran Conda environment"
        )
    if context.interface != "wwan0":
        raise R2LabIoTWorkloadError("physical IoT context must use wwan0")

    profile = _validate_ue(context.ue)
    source_spec = IoTSourceSpec(
        run_id=config.workload_id,
        network_run_id=context.run_id,
        source=AMBER_SOURCE_ID,
        profile=config.iot_profile,
        seed=config.iot_seed,
        sensor_period_seconds=config.sensor_period_seconds,
    )
    run_root = config.run_root.expanduser().resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    run_directory = run_root / source_spec.run_id
    try:
        run_directory.mkdir()
    except FileExistsError as exc:
        raise R2LabIoTWorkloadError(
            "physical IoT run directory already exists; choose a new workload ID"
        ) from exc
    logs = run_directory / "logs"
    logs.mkdir()

    manifest_path = run_directory / "manifest.json"
    physical_summary_path = run_directory / "physical-workload.json"
    wrapper_evidence_path = run_directory / "experiment-evidence.json"
    jsonl_path = run_directory / "telemetry.jsonl"
    rejected_path = run_directory / "rejected-events.jsonl"
    parquet_path = run_directory / "telemetry.parquet"
    _atomic_json(
        manifest_path,
        _manifest(
            source_spec=source_spec,
            physical_run_id=context.run_id,
            status="preparing",
        ),
    )

    def report(message: str) -> None:
        if config.progress is not None:
            print(f"[synthran] {message}", file=config.progress, flush=True)

    plan: PreparedIoTPlan | None = None
    transport: R2LabIoTTransportSession | None = None
    collector: PortableMqttCollectorSession | None = None
    publisher: AmberReplaySession | None = None
    central_forward: OwnedProcess | None = None
    transport_payload: dict[str, Any] = {}
    cleanup_errors: list[str] = []
    failure: str | None = None
    run_accepted = False
    delivery_reproof = False
    cleanup_reproof = False
    tx_before: int | None = None
    tx_after: int | None = None
    rx_before: int | None = None
    rx_after: int | None = None
    central_address = _core_address(config.inventory)

    try:
        _probe_ssh_forwarding(config.inventory)
        _prove_ue_route(config.slice_name, profile, central_address)
        if _ue_relay_process_count(
            config.slice_name,
            profile,
            source_spec.run_id,
        ) != 0:
            raise R2LabIoTWorkloadError(
                "an existing physical UE relay already owns this workload ID"
            )
        for port in (CENTRAL_PORT, 18883, R2LAB_AMBER_INGRESS_PORT):
            if not _remote_port_is_closed(config.inventory, port):
                raise R2LabIoTWorkloadError(
                    f"required physical core port is already in use: {port}"
                )
        for port in (
            LOCAL_CENTRAL_FORWARD_PORT,
            R2LAB_AMBER_INGRESS_PORT,
            config.local_ue_relay_port,
        ):
            if not _local_port_is_closed(port):
                raise R2LabIoTWorkloadError(
                    f"required controller loopback port is already in use: {port}"
                )

        report("Amber source: preparing immutable event plan...")
        source_adapter = AmberSourceAdapter(
            repository_root=config.repository_root,
            dependency_root=config.dependency_root,
        )
        plan = source_adapter.prepare(
            source_spec,
            config.collection_seconds,
            run_directory,
        )
        _atomic_json(
            manifest_path,
            _manifest(
                source_spec=source_spec,
                physical_run_id=context.run_id,
                status="running",
            ),
        )

        central_deployment = physical_central_name(source_spec.run_id)
        for index, obj in enumerate(
            render_physical_central_objects(
                run_id=source_spec.run_id,
                lock=config.lock,
                core_node=config.inventory.core_node.name,
            ),
            start=1,
        ):
            _kubectl_apply_object(
                config.inventory,
                obj,
                label=f"physical central MQTT object {index}",
            )
        _central_rollout(config.inventory, central_deployment)

        _prove_ue_route(config.slice_name, profile, central_address)
        tx_before = _ue_counter(config.slice_name, profile, "tx_bytes")
        rx_before = _ue_counter(config.slice_name, profile, "rx_bytes")

        central_forward = _start_process(
            "physical central MQTT forward",
            _local_forward_command(
                config.inventory,
                local_port=LOCAL_CENTRAL_FORWARD_PORT,
                remote_port=CENTRAL_PORT,
            ),
            cwd=config.repository_root,
            log_path=logs / "physical-central-mqtt-forward.log",
        )
        _wait_local_tcp(
            LOCAL_CENTRAL_FORWARD_PORT,
            process=central_forward,
        )

        collector = PortableMqttCollectorSession(
            run_id=source_spec.run_id,
            sensor_count=source_spec.sensor_count,
            topic_root=source_spec.topic_root,
            host="127.0.0.1",
            port=LOCAL_CENTRAL_FORWARD_PORT,
            jsonl_path=jsonl_path,
            rejected_path=rejected_path,
            default_timeout_seconds=max(
                30.0,
                float(config.collection_seconds) + 30.0,
            ),
        ).start()
        collector.wait_ready(timeout=30.0)

        transport = R2LabIoTTransportAdapter(
            inventory=config.inventory,
            slice_name=config.slice_name,
            ue=context.ue,
            repository_root=config.repository_root,
            relay_port=config.local_ue_relay_port,
        ).start(
            run_id=source_spec.run_id,
            run_directory=run_directory,
        )

        publisher = AmberReplaySession(
            plan=plan,
            endpoint=transport.mqtt_endpoint,
            collector_barrier=collector,
        ).start()
        publisher_evidence = publisher.wait(
            timeout=float(config.collection_seconds) + 60.0
        )
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
            raise R2LabIoTWorkloadError(
                "Amber source/transport reconciliation found loss, duplicates, or unexpected telemetry"
            )
        if source_spec.profile == TRANSPORT_PROFILE and plan.source_loss_count:
            raise R2LabIoTWorkloadError("transport-v1 produced source loss")
        if source_spec.profile == TRANSPORT_PROFILE:
            counts: dict[str, int] = {}
            for record in records:
                sensor_id = str(record["sensor_id"])
                counts[sensor_id] = counts.get(sensor_id, 0) + 1
            if len(counts) != source_spec.sensor_count or any(
                count < config.minimum_per_sensor for count in counts.values()
            ):
                raise R2LabIoTWorkloadError(
                    "transport-v1 did not satisfy the minimum telemetry window"
                )

        ingress_snapshot = transport.snapshot()
        if (
            ingress_snapshot.accepted_connections < source_spec.sensor_count
            or ingress_snapshot.upstream_bytes <= 0
        ):
            raise R2LabIoTWorkloadError(
                "counted ingress did not prove all ten publisher connections"
            )

        tx_after = _ue_counter(config.slice_name, profile, "tx_bytes")
        rx_after = _ue_counter(config.slice_name, profile, "rx_bytes")
        if tx_before is None or tx_after <= tx_before:
            raise R2LabIoTWorkloadError(
                "wwan0 TX counter did not increase during Amber delivery"
            )
        delivery_reproof = _network_reproof(
            config=config,
            profile=profile,
            central_address=central_address,
        )
        if not delivery_reproof:
            raise R2LabIoTWorkloadError(
                "physical UE route was not valid after Amber telemetry delivery"
            )

        write_parquet(records, parquet_path)
        transport_payload = {
            "publisher": publisher_evidence.to_dict(),
            "collector": collector.evidence().to_dict(),
            "ingress": ingress_snapshot.to_dict(),
            "reconciliation": reconciliation.to_dict(),
            "ue": {
                "interface": "wwan0",
                "tx_before": tx_before,
                "tx_after": tx_after,
                "tx_delta": tx_after - tx_before,
                "rx_before": rx_before,
                "rx_after": rx_after,
                "rx_delta": max(0, rx_after - (rx_before or 0)),
            },
            "network_reproof_ready": True,
        }
        run_accepted = True
    except Exception as exc:
        failure = str(exc)
        report(f"physical IoT workload failed: {failure}")
    finally:
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
        if transport is not None:
            try:
                transport.stop()
                if not transport.evidence().get("cleanup_valid"):
                    cleanup_errors.append("physical IoT transport cleanup was incomplete")
            except Exception as exc:
                cleanup_errors.append(f"physical IoT transport cleanup: {exc}")
        if central_forward is not None:
            try:
                central_forward.stop()
            except Exception as exc:
                cleanup_errors.append(f"central MQTT forward cleanup: {exc}")
        if not _local_port_is_closed(LOCAL_CENTRAL_FORWARD_PORT):
            cleanup_errors.append("central MQTT controller port remained in use")
        try:
            _delete_experiment_objects(config.inventory, source_spec.run_id)
        except Exception as exc:
            cleanup_errors.append(f"run-scoped Kubernetes cleanup: {exc}")
        _cleanup_central_port(config.inventory, cleanup_errors)

        try:
            cleanup_reproof = _network_reproof(
                config=config,
                profile=profile,
                central_address=central_address,
            )
            if not cleanup_reproof:
                cleanup_errors.append("physical UE route was not valid after cleanup")
        except Exception as exc:
            cleanup_errors.append(f"physical path cleanup reproof: {exc}")

    cleanup_proven = not cleanup_errors and cleanup_reproof
    accepted = run_accepted and failure is None and cleanup_proven
    if failure is None and not cleanup_proven:
        failure = "physical IoT cleanup did not satisfy all postconditions"

    if plan is not None:
        try:
            iot_evidence = json.loads(plan.evidence_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            iot_evidence = {
                "schema": "synthran/iot-evidence/v2alpha1",
                "run_id": source_spec.run_id,
            }
        iot_evidence["live_transport"] = transport_payload or None
        iot_evidence["cleanup"] = {
            "valid": cleanup_proven,
            "errors": cleanup_errors,
            "network_reproof_ready": cleanup_reproof,
        }
        iot_evidence["ready"] = accepted
        if failure is not None:
            iot_evidence["failure"] = "physical IoT workload was not proven"
        _atomic_json(plan.evidence_path, iot_evidence)

    summary = {
        "schema": PHYSICAL_IOT_SCHEMA,
        "run_id": source_spec.run_id,
        "physical_run_id": context.run_id,
        "backend": "r2lab",
        "ue": context.ue,
        "ue_interface": "wwan0",
        "iot_source": AMBER_SOURCE_ID,
        "iot_profile": source_spec.profile,
        "iot_seed": source_spec.seed,
        "sensor_period_seconds": source_spec.sensor_period_seconds,
        "profile_digest": plan.profile_digest if plan is not None else None,
        "amber_commit": plan.amber_commit if plan is not None else None,
        "transport": transport_payload or None,
        "delivery_network_reproof_ready": delivery_reproof,
        "cleanup_network_reproof_ready": cleanup_reproof,
        "cleanup_proven": cleanup_proven,
        "accepted": accepted,
        "failure": None if accepted else "physical IoT workload was not proven",
        "updated_at_utc": _utc_now(),
    }
    _atomic_json(physical_summary_path, summary)
    _atomic_json(
        wrapper_evidence_path,
        {
            "schema": "synthran/experiment-evidence/v2alpha1",
            "run_id": source_spec.run_id,
            "network_run_id": context.run_id,
            "backend": "r2lab",
            "iot_source": AMBER_SOURCE_ID,
            "iot_profile": source_spec.profile,
            "iot_seed": source_spec.seed,
            "profile_digest": plan.profile_digest if plan is not None else None,
            "ready": accepted,
            "physical_workload": physical_summary_path.name,
            "updated_at_utc": _utc_now(),
        },
    )
    _atomic_json(
        manifest_path,
        _manifest(
            source_spec=source_spec,
            physical_run_id=context.run_id,
            status="iot-to-5g-path-proven" if accepted else "failed",
            failure=failure,
        ),
    )
    return PhysicalWorkloadResult(
        run_id=context.run_id,
        workload_id=source_spec.run_id,
        backend="r2lab",
        interface="wwan0",
        evidence_sha256=sha256_file(physical_summary_path),
        accepted=accepted,
        cleanup_proven=cleanup_proven,
    )


def build_physical_iot_executor(config: PhysicalIoTConfig):
    """Return the physical Amber executor for one upstream deployment inventory."""

    config.validate()

    def execute(context: PhysicalWorkloadContext) -> PhysicalWorkloadResult:
        return execute_physical_iot_workload(context, config=config)

    return execute