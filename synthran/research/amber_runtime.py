"""Explicit controlled-research execution for Amber over RFSIM."""

from __future__ import annotations

from collections import Counter
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
from synthran.iot_publisher import (
    install_replay_start_gate,
    release_replay_start_gate,
    remove_replay_start_gate,
    wait_replay_start_origin,
)
from synthran.iot_source import load_source_events
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
from synthran.research.iperf_window import parse_measurement_load_log
from synthran.research.runtime import _require_network_ready
from synthran.research.sampling import ResearchNetworkSampler
from synthran.research.v2 import (
    AmberResearchSpec,
    research_summary_artifact,
    save_research_experiment_v2,
    save_research_summary_v2,
)


MEASUREMENT_WINDOW_SCHEMA_V2 = "synthran/research-measurement-window/v2alpha1"
MEASUREMENT_PATH_SCHEMA_V2 = "synthran/research-measurement-path/v2alpha1"
_LOAD_RUNTIME_HEADROOM_SECONDS = 15


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError as exc:
        raise ResearchError("research artifact contains an invalid UTC timestamp") from exc


class AmberResearchMeasurementLifecycle(AmberMeasurementLifecycle):
    """Prepare instrumentation, release source time zero, then measure exactly."""

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
        self.load_started_monotonic_s: float | None = None
        self.route_installed = False
        self.instrumentation_errors: list[str] = []
        self.path_errors: list[str] = []
        self.window_started_at: datetime | None = None
        self.window_ended_at: datetime | None = None
        self.pre_report: Any | None = None
        self.post_report: Any | None = None
        self.pre_target_ready = False
        self.replay_origin_monotonic_s: float | None = None
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

    def _wait_until(self, deadline: float) -> None:
        while time.monotonic() < deadline:
            self._health_check()
            time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
        self._health_check()

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
        self.load_started_monotonic_s = time.monotonic()
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
                self.spec.total_source_seconds + _LOAD_RUNTIME_HEADROOM_SECONDS
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
                self.spec.total_source_seconds + _LOAD_RUNTIME_HEADROOM_SECONDS
            ),
            interval_seconds=self.spec.measurement.probe_interval_seconds,
            repository_root=self.repository_root,
            log_path=paths["probe_log"],
        )

    def run(self, context: AmberRuntimeContext) -> Mapping[str, Any]:
        self.context = context
        paths = self._paths()
        try:
            self._report("measurement path: preparing before source time zero...")
            self._start_instrumentation(paths)
            self._report("measurement path: ready")
        except Exception as exc:
            self.path_errors.append(str(exc))
            self._write_path_evidence()
            raise

        release_replay_start_gate(context.run_id)
        self.replay_origin_monotonic_s = wait_replay_start_origin(context.run_id)

        try:
            if self.spec.measurement.warmup_seconds:
                self._report(f"warmup: {self.spec.measurement.warmup_seconds}s")
                self._wait_until(
                    self.replay_origin_monotonic_s
                    + self.spec.measurement.warmup_seconds
                )

            self.window_started_at = _utc_now()
            self._report(f"measurement window: {self.spec.measurement.duration_seconds}s")
            measurement_deadline = (
                self.replay_origin_monotonic_s + self.spec.total_source_seconds
            )
            self._wait_until(measurement_deadline)
            self.window_ended_at = _utc_now()

            self.post_report = _require_network_ready(
                inventory=self.inventory,
                lock=self.lock,
                network_run_id=context.network_run_id,
                ue_pod=context.ue_pod,
                pdu_address=context.pdu_address,
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
                "alignment": "publisher-start-gate",
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
            "source_clock_alignment": "publisher-start-gate",
            "source_start_ms": self.spec.measurement.warmup_seconds * 1000,
            "source_end_ms": self.spec.total_source_seconds * 1000,
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
                if (
                    self.load_started_monotonic_s is None
                    or self.replay_origin_monotonic_s is None
                ):
                    raise ResearchError(
                        "background-load source-clock alignment is unavailable"
                    )
                measurement_start_offset = (
                    self.replay_origin_monotonic_s
                    + self.spec.measurement.warmup_seconds
                    - self.load_started_monotonic_s
                )
                parse_measurement_load_log(
                    paths["load_client_log"],
                    paths["load"],
                    target_bps=target_bps,
                    protocol=self.spec.load.protocol,
                    measurement_start_offset_seconds=measurement_start_offset,
                    measurement_duration_seconds=float(
                        self.spec.measurement.duration_seconds
                    ),
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


def _load_window(run_directory: Path) -> tuple[datetime, datetime, int, int]:
    try:
        value = json.loads(
            (run_directory / "measurement-window.json").read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise ResearchError("measurement-window evidence is unavailable") from exc
    if not isinstance(value, Mapping) or value.get("alignment") != "publisher-start-gate":
        raise ResearchError("measurement window is not source-clock aligned")
    try:
        started = _parse_utc(str(value["started_at_utc"]))
        ended = _parse_utc(str(value["ended_at_utc"]))
        source_start = int(value["source_start_ms"])
        source_end = int(value["source_end_ms"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ResearchError("measurement-window evidence is malformed") from exc
    if ended <= started or source_end <= source_start:
        raise ResearchError("measurement-window bounds are invalid")
    return started, ended, source_start, source_end


def _filter_utc_records(
    records: list[dict[str, Any]],
    *,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for record in records:
        raw = record.get("observed_at_utc")
        if not isinstance(raw, str):
            continue
        observed = _parse_utc(raw)
        if start <= observed <= end:
            selected.append(record)
    return selected


def _measurement_metrics(run_directory: Path) -> dict[str, Any]:
    started, ended, _, _ = _load_window(run_directory)
    probe_records = load_jsonl(run_directory / "probe.jsonl", schema=PROBE_SCHEMA)
    network_records = _filter_utc_records(
        load_jsonl(run_directory / "network-samples.jsonl", schema=NETWORK_SAMPLE_SCHEMA),
        start=started,
        end=ended,
    )
    load_records = load_jsonl(run_directory / "load.jsonl", schema=LOAD_RESULT_SCHEMA)
    rtts = [
        float(record["rtt_ms"])
        for record in probe_records
        if record.get("rtt_ms") is not None
    ]
    loads = [
        float(record["bits_per_second"])
        for record in load_records
        if isinstance(record.get("bits_per_second"), (int, float))
    ]
    return {
        "probe_records": len(probe_records),
        "mean_rtt_ms": _mean(rtts),
        "network_samples": len(network_records),
        "load_records": len(load_records),
        "mean_achieved_load_bps": _mean(loads),
    }


def _publisher_pairs(run_directory: Path) -> list[tuple[str, int]]:
    path = run_directory / "publisher-events.jsonl"
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ResearchError("publisher event evidence is unavailable") from exc
    pairs: list[tuple[str, int]] = []
    for number, line in enumerate(lines, start=1):
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ResearchError(f"publisher event line {number} is invalid JSON") from exc
        if (
            isinstance(value, Mapping)
            and value.get("kind") == "telemetry"
            and value.get("success") is True
            and isinstance(value.get("sensor_id"), str)
            and isinstance(value.get("sequence"), int)
        ):
            pairs.append((str(value["sensor_id"]), int(value["sequence"])))
    return pairs


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
    """Execute one source-clock-aligned Amber controlled experiment."""

    lifecycle = AmberResearchMeasurementLifecycle(
        spec=spec,
        inventory=inventory,
        lock=lock,
        repository_root=repository_root,
        progress=progress,
    )
    install_replay_start_gate(spec.run_id)
    try:
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
            energy_power_scale=spec.energy_power_scale,
            energy_node_variation=spec.energy_node_variation,
            measurement_lifecycle=lifecycle,
            progress=progress,
        )
    finally:
        remove_replay_start_gate(spec.run_id)
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

    _, _, source_start_ms, source_end_ms = _load_window(run_directory)
    source_events = load_source_events(run_directory / "amber-source-events.jsonl")
    measurement_source = [
        event
        for event in source_events
        if source_start_ms <= event.planned_at_ms < source_end_ms
    ]
    duration_ms = spec.measurement.duration_seconds * 1000
    period_ms = spec.sensor_period_seconds * 1000
    expected_measurement_opportunities = (
        (duration_ms + period_ms - 1) // period_ms
    ) * spec.sensor_count
    if len(measurement_source) != expected_measurement_opportunities:
        raise ResearchError(
            "measurement source window does not contain the expected opportunity count"
        )

    telemetry = load_telemetry_jsonl(
        run_directory / "telemetry.jsonl", expected_run_id=spec.run_id
    )
    measurement_telemetry = [
        record
        for record in telemetry
        if source_start_ms <= int(record["sensor_time_ms"]) < source_end_ms
    ]
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

    planned_keys = {event.key for event in measurement_source}
    decoded_keys = {event.key for event in measurement_source if event.decoded}
    published_pairs_all = _publisher_pairs(run_directory)
    published_pairs = [pair for pair in published_pairs_all if pair in planned_keys]
    published_keys = set(published_pairs)
    central_pairs = [
        (str(record["sensor_id"]), int(record["sequence"]))
        for record in measurement_telemetry
    ]
    central_keys = set(central_pairs)
    source_loss = len(planned_keys - decoded_keys)
    transport_loss = len(decoded_keys - central_keys)
    duplicate_count = len(central_pairs) - len(central_keys)
    unexpected_count = len(central_keys - decoded_keys)
    published_missing = len(decoded_keys - published_keys)
    outcome_counts = Counter(event.outcome for event in measurement_source)

    full_transport_valid = bool(
        reconciliation.get("valid") is True
        and int(reconciliation.get("transport_loss_count", 0)) == 0
        and int(reconciliation.get("duplicate_count", 0)) == 0
        and int(reconciliation.get("unexpected_central_count", 0)) == 0
    )
    measurement_evidence = transport.get("measurement")
    path_valid = bool(
        isinstance(measurement_evidence, Mapping)
        and measurement_evidence.get("source_clock_alignment") == "publisher-start-gate"
        and measurement_evidence.get("pre_window_network_ready") is True
        and measurement_evidence.get("pre_window_target_ready") is True
        and measurement_evidence.get("post_window_network_ready") is True
        and not measurement_evidence.get("instrumentation_errors")
    )
    infrastructure_valid = bool(
        iot_evidence.get("ready") is True
        and full_transport_valid
        and transport_loss == 0
        and duplicate_count == 0
        and unexpected_count == 0
        and published_missing == 0
        and path_valid
    )

    metrics = _measurement_metrics(run_directory)
    target_bps = spec.load.resolved_target_bps
    achieved_bps = metrics.get("mean_achieved_load_bps")
    if spec.load.enabled:
        load_target_ratio = (
            float(achieved_bps) / target_bps
            if isinstance(achieved_bps, (int, float)) and target_bps
            else None
        )
        load_target_valid = bool(
            isinstance(load_target_ratio, float)
            and 0.90 <= load_target_ratio <= 1.10
        )
    else:
        load_target_ratio = None
        load_target_valid = True

    scientific_valid = bool(
        infrastructure_valid
        and len(measurement_source) == expected_measurement_opportunities
        and (spec.iot_profile != "transport-v1" or source_loss == 0)
        and load_target_valid
    )

    measurement_transmitted = sum(event.transmitted for event in measurement_source)
    metrics.update(
        {
            "source_window_start_ms": source_start_ms,
            "source_window_end_ms": source_end_ms,
            "source_outcomes": dict(sorted(outcome_counts.items())),
            "measurement_transmitted": measurement_transmitted,
            "measurement_decoded": len(decoded_keys),
            "measurement_unexpected_central": unexpected_count,
            "measurement_published_missing": published_missing,
            "load_target_ratio": load_target_ratio,
            "load_target_valid": load_target_valid,
            "full_run_reconciliation": dict(reconciliation),
        }
    )
    summary = research_summary_artifact(
        spec,
        profile_digest=profile_digest,
        planned_opportunities=len(planned_keys),
        decoded_opportunities=len(decoded_keys),
        published_events=len(published_keys),
        received_events=len(central_keys),
        source_loss=source_loss,
        transport_loss=transport_loss,
        duplicate_count=duplicate_count,
        measurement_received_events=len(measurement_telemetry),
        infrastructure_valid=infrastructure_valid,
        scientific_valid=scientific_valid,
        extra=metrics,
    )
    summary_path = run_directory / "research-summary-v2.json"
    save_research_summary_v2(summary_path, summary)

    if progress is not None:
        print(
            "[synthran] research: measurement source: "
            f"planned={len(planned_keys)}, transmitted={measurement_transmitted}, "
            f"decoded={len(decoded_keys)}, source-loss={source_loss}",
            file=progress,
            flush=True,
        )
        print(
            "[synthran] research: measurement outcomes: "
            + ", ".join(
                f"{name}={outcome_counts[name]}" for name in sorted(outcome_counts)
            ),
            file=progress,
            flush=True,
        )
        print(
            "[synthran] research: transport: "
            f"published={len(published_keys)}, received={len(central_keys)}, "
            f"loss={transport_loss}, duplicates={duplicate_count}",
            file=progress,
            flush=True,
        )

    if not infrastructure_valid:
        raise ResearchError("Amber research run failed its infrastructure validity gate")
    if not scientific_valid:
        raise ResearchError("Amber research run failed its scientific validity gate")
    return summary_path
