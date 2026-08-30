"""Explicit controlled-research execution for Amber over RFSIM."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any, Mapping, TextIO

from synthran.amber_experiment_runtime import (
    AmberMeasurementLifecycle,
    AmberRuntimeContext,
    execute_amber_experiment,
)
from synthran.dependencies import DependencyLock
from synthran.experiment import load_jsonl as load_telemetry_jsonl
from synthran.fiveg_ansible import NetworkInventory
from synthran.research import (
    LOAD_RESULT_SCHEMA,
    NETWORK_SAMPLE_SCHEMA,
    PROBE_SCHEMA,
    ResearchError,
    atomic_json,
    load_jsonl,
    write_records_parquet,
)
from synthran.research.instrumentation import (
    _check_research_tools,
    _install_target_route,
    _parse_load_log,
    _parse_probe_log,
    _prove_target_reachability,
    _prove_target_route,
    _remove_target_route,
    _start_load_client,
    _start_probe,
    _wait_load_client_connected,
)
from synthran.research.iperf import (
    OwnedIperfServer,
    start_owned_iperf_server,
    stop_owned_iperf_server,
)
from synthran.research.runtime import _require_network_ready
from synthran.research.sampling import ResearchNetworkSampler
from synthran.research.v2 import (
    AmberResearchSpec,
    research_summary_artifact,
    save_research_experiment_v2,
    save_research_summary_v2,
    select_measurement_telemetry,
)


MEASUREMENT_WINDOW_SCHEMA_V2 = "synthran/research-measurement-window/v2alpha1"
MEASUREMENT_PATH_SCHEMA_V2 = "synthran/research-measurement-path/v2alpha1"
_LOAD_RUNTIME_HEADROOM_SECONDS = 15


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class AmberResearchMeasurementLifecycle(AmberMeasurementLifecycle):
    """Run warmup, fixed measurement instrumentation, and load explicitly."""

    def __init__(
        self,
        *,
        spec: AmberResearchSpec,
        inventory: NetworkInventory,
        lock: DependencyLock,
        repository_root: Path,
        progress: TextIO | None = None,
    ) -> None:
        if spec.probe_target is None:
            raise ResearchError("live controlled experiment requires a probe/load target")
        self.spec = spec
        self.inventory = inventory
        self.lock = lock
        self.repository_root = repository_root.resolve()
        self.progress = progress
        self.context: AmberRuntimeContext | None = None
        self.sampler: ResearchNetworkSampler | None = None
        self.probe_process: Any | None = None
        self.load_process: Any | None = None
        self.load_server: OwnedIperfServer | None = None
        self.route_installed = False
        self.instrumentation_errors: list[str] = []
        self.path_errors: list[str] = []
        self.window_started_at: datetime | None = None
        self.window_ended_at: datetime | None = None
        self.pre_report: Any | None = None
        self.post_report: Any | None = None
        self.pre_target_ready = False
        self._instrumentation_stopped = False
        self._stopped = False

    def _report(self, message: str) -> None:
        if self.progress is not None:
            print(f"[synthran] research: {message}", file=self.progress, flush=True)

    @property
    def _run_directory(self) -> Path:
        if self.context is None:
            raise ResearchError("Amber research runtime has no live context")
        return self.context.run_directory

    def _paths(self) -> dict[str, Path]:
        root = self._run_directory
        logs = root / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        return {
            "window": root / "measurement-window.json",
            "measurement_path": root / "measurement-path.json",
            "probe": root / "probe.jsonl",
            "network": root / "network-samples.jsonl",
            "load": root / "load.jsonl",
            "probe_log": logs / "research-probe.log",
            "load_client_log": logs / "research-load-client.log",
            "load_server_log": logs / "research-load-server.log",
        }

    def _health_check(self) -> None:
        if self.sampler is not None:
            self.sampler.check()
        if self.probe_process is not None:
            exit_code = self.probe_process.process.poll()
            if exit_code is not None:
                raise ResearchError(
                    f"research RTT probe exited unexpectedly with code {exit_code}"
                )
        if self.load_process is not None:
            exit_code = self.load_process.process.poll()
            if exit_code is not None:
                raise ResearchError(
                    f"research background load exited unexpectedly with code {exit_code}"
                )
        if self.load_server is not None:
            exit_code = self.load_server.process.process.poll()
            if exit_code is not None:
                raise ResearchError(
                    f"research load server exited unexpectedly with code {exit_code}"
                )

    def _write_path_evidence(self, *, cleanup_reproved: bool | None = None) -> None:
        assert self.context is not None
        path = self._paths()["measurement_path"]
        atomic_json(
            path,
            {
                "schema": MEASUREMENT_PATH_SCHEMA_V2,
                "run_id": self.context.run_id,
                "network_run_id": self.context.network_run_id,
                "ue_pod": self.context.ue_pod,
                "pdu_address": self.context.pdu_address,
                "target": self.spec.probe_target,
                "pre_window": {
                    "network_ready": bool(
                        self.pre_report is not None
                        and getattr(self.pre_report, "ready", False)
                    ),
                    "target_ready": self.pre_target_ready,
                },
                "post_window": {
                    "network_ready": bool(
                        self.post_report is not None
                        and getattr(self.post_report, "ready", False)
                    ),
                },
                "cleanup_reproved": cleanup_reproved,
                "errors": list(self.path_errors),
            },
        )

    def _start_loaded_transport(self, paths: Mapping[str, Path]) -> None:
        assert self.context is not None
        target_bps = self.spec.load.resolved_target_bps
        assert target_bps is not None
        self.load_server = start_owned_iperf_server(
            inventory=self.inventory,
            owner_id=self.spec.run_id,
            port=self.spec.load.server_port,
            repository_root=self.repository_root,
            log_path=paths["load_server_log"],
        )
        self._report(f"load server: ready on port {self.spec.load.server_port}")
        per_stream_bps = max(1, target_bps // self.spec.load.parallel_flows)
        self.load_process = _start_load_client(
            inventory=self.inventory,
            ue_pod=self.context.ue_pod,
            pdu_address=self.context.pdu_address,
            target=self.spec.probe_target or "",
            port=self.spec.load.server_port,
            target_bps=per_stream_bps,
            protocol=self.spec.load.protocol,
            parallel_flows=self.spec.load.parallel_flows,
            duration_seconds=(
                self.spec.measurement.duration_seconds + _LOAD_RUNTIME_HEADROOM_SECONDS
            ),
            repository_root=self.repository_root,
            log_path=paths["load_client_log"],
        )
        _wait_load_client_connected(
            inventory=self.inventory,
            ue_pod=self.context.ue_pod,
            pdu_address=self.context.pdu_address,
            target=self.spec.probe_target or "",
            port=self.spec.load.server_port,
            process=self.load_process,
        )
        self._report("load client: connected")

    def _prove_pre_window(self, paths: Mapping[str, Path]) -> None:
        assert self.context is not None
        self.pre_report = _require_network_ready(
            inventory=self.inventory,
            lock=self.lock,
            network_run_id=self.context.network_run_id,
            ue_pod=self.context.ue_pod,
            pdu_address=self.context.pdu_address,
        )
        _prove_target_route(
            self.inventory,
            self.context.ue_pod,
            pdu_address=self.context.pdu_address,
            target=self.spec.probe_target or "",
        )
        if self.spec.load.enabled:
            self._start_loaded_transport(paths)
        else:
            _prove_target_reachability(
                self.inventory,
                self.context.ue_pod,
                target=self.spec.probe_target or "",
            )
        self.pre_target_ready = True
        self._write_path_evidence()

    def _start_instrumentation(self, paths: Mapping[str, Path]) -> None:
        assert self.context is not None
        _check_research_tools(
            self.inventory,
            self.context.ue_pod,
            load_enabled=self.spec.load.enabled,
        )
        self.route_installed = _install_target_route(
            self.inventory,
            self.context.ue_pod,
            pdu_address=self.context.pdu_address,
            target=self.spec.probe_target or "",
        )
        self._prove_pre_window(paths)

        self.sampler = ResearchNetworkSampler(
            inventory=self.inventory,
            network_run_id=self.context.network_run_id,
            experiment_run_id=self.context.run_id,
            ue_pod=self.context.ue_pod,
            interval_seconds=self.spec.measurement.sample_interval_seconds,
            destination=paths["network"],
        )
        self.sampler.start()
        self.probe_process = _start_probe(
            inventory=self.inventory,
            ue_pod=self.context.ue_pod,
            target=self.spec.probe_target or "",
            duration_seconds=(
                self.spec.measurement.duration_seconds + _LOAD_RUNTIME_HEADROOM_SECONDS
            ),
            interval_seconds=self.spec.measurement.probe_interval_seconds,
            repository_root=self.repository_root,
            log_path=paths["probe_log"],
        )

    def run(self, context: AmberRuntimeContext) -> Mapping[str, Any]:
        self.context = context
        paths = self._paths()
        if self.spec.measurement.warmup_seconds:
            self._report(f"warmup: {self.spec.measurement.warmup_seconds}s")
            warmup_deadline = time.monotonic() + self.spec.measurement.warmup_seconds
            while time.monotonic() < warmup_deadline:
                time.sleep(min(0.25, warmup_deadline - time.monotonic()))

        try:
            self._report("measurement path: verifying...")
            self._start_instrumentation(paths)
            self._report("measurement path: ready")
        except Exception as exc:
            self.path_errors.append(str(exc))
            self._write_path_evidence()
            raise

        self.window_started_at = _utc_now()
        self._report(f"measurement window: {self.spec.measurement.duration_seconds}s")
        deadline = time.monotonic() + self.spec.measurement.duration_seconds
        try:
            while time.monotonic() < deadline:
                self._health_check()
                time.sleep(min(0.25, deadline - time.monotonic()))
            self.window_ended_at = _utc_now()
            self._health_check()
            assert self.context is not None
            self.post_report = _require_network_ready(
                inventory=self.inventory,
                lock=self.lock,
                network_run_id=self.context.network_run_id,
                ue_pod=self.context.ue_pod,
                pdu_address=self.context.pdu_address,
            )
            self._write_path_evidence()
        except Exception as exc:
            self.path_errors.append(str(exc))
            self._write_path_evidence()
            raise
        finally:
            self._stop_instrumentation(paths)

        atomic_json(
            paths["window"],
            {
                "schema": MEASUREMENT_WINDOW_SCHEMA_V2,
                "run_id": context.run_id,
                "warmup_seconds": self.spec.measurement.warmup_seconds,
                "requested_duration_seconds": self.spec.measurement.duration_seconds,
                "started_at_utc": _utc_text(self.window_started_at),
                "ended_at_utc": _utc_text(self.window_ended_at),
                "source_start_ms": self.spec.measurement.warmup_seconds * 1000,
                "source_end_ms": self.spec.total_source_seconds * 1000,
            },
        )
        return {
            "window": paths["window"].name,
            "measurement_path": paths["measurement_path"].name,
            "probe": paths["probe"].name if paths["probe"].is_file() else None,
            "network_samples": (
                paths["network"].name if paths["network"].is_file() else None
            ),
            "load": paths["load"].name if paths["load"].is_file() else None,
            "pre_window_network_ready": bool(
                self.pre_report is not None and getattr(self.pre_report, "ready", False)
            ),
            "pre_window_target_ready": self.pre_target_ready,
            "post_window_network_ready": bool(
                self.post_report is not None and getattr(self.post_report, "ready", False)
            ),
            "instrumentation_errors": list(self.instrumentation_errors),
        }

    def _stop_instrumentation(self, paths: Mapping[str, Path]) -> None:
        if self._instrumentation_stopped:
            return
        self._instrumentation_stopped = True

        if self.sampler is not None:
            try:
                self.sampler.stop()
            except Exception as exc:
                self.instrumentation_errors.append(str(exc))
            self.sampler = None
        if self.probe_process is not None:
            try:
                self.probe_process.stop()
            except Exception as exc:
                self.instrumentation_errors.append(
                    f"{self.probe_process.name}: {exc}"
                )
            self.probe_process = None
        if self.load_process is not None:
            try:
                self.load_process.stop()
            except Exception as exc:
                self.instrumentation_errors.append(f"load client: {exc}")
            self.load_process = None
        if self.load_server is not None:
            try:
                stop_owned_iperf_server(self.inventory, self.load_server)
            except Exception as exc:
                self.instrumentation_errors.append(str(exc))
            self.load_server = None
        if self.context is not None and self.route_installed:
            try:
                _remove_target_route(
                    self.inventory,
                    self.context.ue_pod,
                    pdu_address=self.context.pdu_address,
                    target=self.spec.probe_target or "",
                )
            except Exception as exc:
                self.instrumentation_errors.append(str(exc))
            self.route_installed = False

        try:
            _parse_probe_log(
                paths["probe_log"],
                paths["probe"],
                interval_seconds=self.spec.measurement.probe_interval_seconds,
                window_started_at_utc=self.window_started_at,
                window_ended_at_utc=self.window_ended_at,
            )
        except Exception as exc:
            self.instrumentation_errors.append(str(exc))
        if self.spec.load.enabled:
            target_bps = self.spec.load.resolved_target_bps
            assert target_bps is not None
            try:
                _parse_load_log(
                    paths["load_client_log"],
                    paths["load"],
                    target_bps=target_bps,
                    protocol=self.spec.load.protocol,
                )
            except Exception as exc:
                self.instrumentation_errors.append(str(exc))

    def stop(self) -> None:
        if self._stopped:
            return
        if self.context is not None:
            self._stop_instrumentation(self._paths())
        self._stopped = True


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def _measurement_metrics(run_directory: Path) -> dict[str, Any]:
    probe_records = load_jsonl(run_directory / "probe.jsonl", schema=PROBE_SCHEMA)
    network_records = load_jsonl(
        run_directory / "network-samples.jsonl", schema=NETWORK_SAMPLE_SCHEMA
    )
    load_records = load_jsonl(run_directory / "load.jsonl", schema=LOAD_RESULT_SCHEMA)
    rtts = [
        float(record["rtt_ms"])
        for record in probe_records
        if record.get("rtt_ms") is not None
    ]
    loads = [
        float(record["achieved_bps"])
        for record in load_records
        if record.get("achieved_bps") is not None
    ]
    return {
        "probe_records": len(probe_records),
        "mean_rtt_ms": _mean(rtts),
        "network_samples": len(network_records),
        "load_records": len(load_records),
        "mean_achieved_load_bps": _mean(loads),
    }


def execute_amber_research_experiment(
    *,
    spec: AmberResearchSpec,
    inventory: NetworkInventory,
    lock: DependencyLock,
    dependency_root: Path,
    network_manifest: Path,
    network_evidence: Path,
    repository_root: Path,
    run_root: Path,
    progress: TextIO | None = None,
) -> Path:
    """Execute Amber source + research instrumentation without runtime overrides."""

    lifecycle = AmberResearchMeasurementLifecycle(
        spec=spec,
        inventory=inventory,
        lock=lock,
        repository_root=repository_root,
        progress=progress,
    )
    result = execute_amber_experiment(
        inventory=inventory,
        lock=lock,
        dependency_root=dependency_root,
        network_manifest=network_manifest,
        network_evidence=network_evidence,
        run_id=spec.run_id,
        repository_root=repository_root,
        run_root=run_root,
        collection_seconds=spec.total_source_seconds,
        minimum_per_sensor=1,
        iot_profile=spec.iot_profile,
        iot_seed=spec.iot_seed,
        sensor_period_seconds=spec.sensor_period_seconds,
        measurement_lifecycle=lifecycle,
        progress=progress,
    )
    run_directory = result.run_directory

    try:
        iot_evidence = json.loads(
            (run_directory / "iot-evidence-v2.json").read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise ResearchError("Amber IoT evidence is unavailable after research run") from exc
    transport = iot_evidence.get("live_transport")
    if not isinstance(transport, Mapping):
        raise ResearchError("Amber research run is missing live transport evidence")
    reconciliation = transport.get("reconciliation")
    if not isinstance(reconciliation, Mapping):
        raise ResearchError("Amber research run is missing transport reconciliation")

    telemetry = load_telemetry_jsonl(
        run_directory / "telemetry.jsonl", expected_run_id=spec.run_id
    )
    measurement_telemetry = select_measurement_telemetry(spec, telemetry)
    measurement_jsonl = run_directory / "measurement-telemetry.jsonl"
    with measurement_jsonl.open("w", encoding="utf-8", newline="\n") as stream:
        for record in measurement_telemetry:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
    write_records_parquet(
        measurement_telemetry,
        run_directory / "measurement-telemetry.parquet",
    )

    profile_digest = str(iot_evidence.get("profile_digest", ""))
    amber_commit = str(iot_evidence.get("amber_commit", ""))
    save_research_experiment_v2(
        spec,
        run_directory / "experiment-spec-v2.json",
        profile_digest=profile_digest,
        amber_commit=amber_commit,
        energy_trace_sha256=(
            str(iot_evidence["energy_trace_sha256"])
            if iot_evidence.get("energy_trace_sha256") is not None
            else None
        ),
    )

    source_loss = int(reconciliation.get("source_loss_count", 0))
    transport_loss = int(reconciliation.get("transport_loss_count", 0))
    duplicate_count = int(reconciliation.get("duplicate_count", 0))
    planned = int(reconciliation.get("planned_count", 0))
    decoded = int(reconciliation.get("decoded_count", 0))
    published = int(reconciliation.get("published_count", 0))
    received = int(reconciliation.get("central_received_count", 0))
    measurement_evidence = transport.get("measurement")
    path_valid = bool(
        isinstance(measurement_evidence, Mapping)
        and measurement_evidence.get("pre_window_network_ready") is True
        and measurement_evidence.get("pre_window_target_ready") is True
        and measurement_evidence.get("post_window_network_ready") is True
        and not measurement_evidence.get("instrumentation_errors")
    )
    infrastructure_valid = bool(
        iot_evidence.get("ready") is True
        and transport_loss == 0
        and duplicate_count == 0
        and path_valid
    )
    scientific_valid = infrastructure_valid and (
        spec.iot_profile != "transport-v1" or len(measurement_telemetry) > 0
    )
    summary = research_summary_artifact(
        spec,
        profile_digest=profile_digest,
        planned_opportunities=planned,
        decoded_opportunities=decoded,
        published_events=published,
        received_events=received,
        source_loss=source_loss,
        transport_loss=transport_loss,
        duplicate_count=duplicate_count,
        measurement_received_events=len(measurement_telemetry),
        infrastructure_valid=infrastructure_valid,
        scientific_valid=scientific_valid,
        extra=_measurement_metrics(run_directory),
    )
    summary_path = run_directory / "research-summary-v2.json"
    save_research_summary_v2(summary_path, summary)
    if not infrastructure_valid:
        raise ResearchError("Amber research run failed its infrastructure validity gate")
    return summary_path
