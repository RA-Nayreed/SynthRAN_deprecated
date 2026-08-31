"""Controlled research execution on top of the accepted SynthRAN lifecycle."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import threading
from typing import Any, Callable, Mapping, TextIO

from synthran.dependencies import DependencyLock
from synthran.experiment import live as base_runtime
from synthran.fiveg_ansible import NetworkInventory
from synthran.network.rfsim import reconcile_rfsim_runtime
from synthran.network.runtime import verify_network_path
from synthran.research import (
    CAPACITY_SCHEMA,
    LOAD_RESULT_SCHEMA,
    MEASUREMENT_WINDOW_SCHEMA,
    NETWORK_SAMPLE_SCHEMA,
    PROBE_SCHEMA,
    ResearchError,
    ResearchExperimentSpec,
    atomic_json,
    build_run_summary,
    load_jsonl,
    save_research_spec,
    save_run_summary,
    write_records_parquet,
)
from synthran.research.collector import collect_mqtt_window
from synthran.research.instrumentation import (
    DEFAULT_RESEARCH_RUN_ROOT,
    ResearchRunResult,
    _base_cleanup_reproved,
    _check_research_tools,
    _extract_iperf_bps,
    _install_target_route,
    _kubectl_exec_command,
    _parse_load_log,
    _parse_probe_log,
    _prove_target_reachability,
    _prove_target_route,
    _remove_target_route,
    _runtime_overrides,
    _start_load_client,
    _start_probe,
    _wait_load_client_connected,
)
from synthran.research.iperf import (
    OwnedIperfServer,
    start_owned_iperf_server,
    stop_owned_iperf_server,
)
from synthran.research.sampling import ResearchNetworkSampler

_RUNTIME_OVERRIDE_LOCK = threading.Lock()
_MEASUREMENT_PATH_SCHEMA = "synthran/research-measurement-path/v1alpha1"
_LOAD_RUNTIME_HEADROOM_SECONDS = 15


class _ResearchProgressStream:
    """Filter base-run lines that would misstate controlled research results."""

    _SUPPRESSED = (
        "[synthran] collector: OK",
        "[synthran] error: experiment data evidence is incomplete",
        "[synthran] IOT-TO-5G PATH PROVEN",
        "[synthran] experiment path NOT PROVEN",
    )

    def __init__(self, target: TextIO | None) -> None:
        self.target = target
        self._buffer = ""

    def write(self, text: str) -> int:
        if self.target is None:
            return len(text)
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if not line.startswith(self._SUPPRESSED):
                self.target.write(line + "\n")
        return len(text)

    def flush(self) -> None:
        if self.target is None:
            self._buffer = ""
            return
        if self._buffer and not self._buffer.startswith(self._SUPPRESSED):
            self.target.write(self._buffer)
        self._buffer = ""
        self.target.flush()


def _measurement_runtime_handoff(
    inventory: NetworkInventory,
    scenario: Any,
) -> tuple[str, str]:
    ue_pod = base_runtime._discover_ue_pod(
        inventory,
        scenario.network_run_id,
    )
    pdu_address = scenario.pdu_address
    if not isinstance(pdu_address, str) or not pdu_address:
        raise ResearchError("measurement runtime does not contain a live UE PDU address")
    return ue_pod, pdu_address


def _report_checks(report: Any) -> list[dict[str, Any]]:
    checks = []
    for check in getattr(report, "checks", ()):
        checks.append(
            {
                "name": str(check.name),
                "passed": bool(check.passed),
                "detail": str(check.detail),
            }
        )
    return checks


def _require_network_ready(
    *,
    inventory: NetworkInventory,
    lock: DependencyLock,
    network_run_id: str,
    ue_pod: str,
    pdu_address: str,
) -> Any:
    report = verify_network_path(
        inventory=inventory,
        lock=lock,
        run_id=network_run_id,
        timeout_seconds=120,
    )
    if not report.ready:
        failing = [
            f"{check.name}: {check.detail}"
            for check in report.checks
            if not check.passed
        ]
        detail = "; ".join(failing) if failing else "network verification failed"
        raise ResearchError(
            "controlled measurement requires a currently path-proven network: "
            + detail
        )
    current_ue = base_runtime._discover_ue_pod(inventory, network_run_id)
    if current_ue != ue_pod:
        raise ResearchError(
            "controlled measurement UE pod changed after runtime handoff"
        )
    current_pdu = getattr(report, "pdu_address", None)
    if not isinstance(current_pdu, str) or current_pdu != pdu_address:
        raise ResearchError(
            "controlled measurement UE PDU changed after runtime handoff"
        )
    return report


def _prove_pre_window_target(
    *,
    spec: ResearchExperimentSpec,
    prove_icmp: Callable[[], None],
    prove_transport: Callable[[], None],
) -> None:
    """Prove target readiness using the transport that defines the condition.

    Baseline runs have no load transport, so they retain the bounded ICMP
    reachability proof. Loaded runs prove the actual iperf3 transport instead:
    the run-owned server must be listening and the UE client's TCP control
    connection must become established before the measurement window opens.
    """

    if spec.load.enabled:
        prove_transport()
        return
    prove_icmp()


def _write_measurement_path(
    path: Path,
    *,
    run_id: str,
    network_run_id: str,
    ue_pod: str,
    pdu_address: str,
    target: str,
    pre_report: Any | None,
    pre_target_ready: bool,
    post_report: Any | None,
    cleanup_reproved: bool | None,
    errors: list[str],
) -> None:
    atomic_json(
        path,
        {
            "schema": _MEASUREMENT_PATH_SCHEMA,
            "run_id": run_id,
            "network_run_id": network_run_id,
            "ue_pod": ue_pod,
            "pdu_address": pdu_address,
            "target": target,
            "pre_window": {
                "network_ready": bool(
                    pre_report is not None and getattr(pre_report, "ready", False)
                ),
                "target_ready": pre_target_ready,
                "checks": _report_checks(pre_report) if pre_report is not None else [],
            },
            "post_window": {
                "network_ready": bool(
                    post_report is not None and getattr(post_report, "ready", False)
                ),
                "checks": _report_checks(post_report) if post_report is not None else [],
            },
            "cleanup_reproved": cleanup_reproved,
            "errors": list(errors),
        },
    )


def _finalize_validity(
    *,
    summary: Mapping[str, Any],
    spec: ResearchExperimentSpec,
    telemetry_artifact_present: bool,
    window_present: bool,
    pre_network_ready: bool,
    pre_target_ready: bool,
    post_network_ready: bool,
    instrumentation_clean: bool,
    cleanup_reproved: bool,
) -> tuple[dict[str, Any], bool]:
    validity = dict(summary["validity"])
    received_events = int(summary["telemetry"]["received_events"])
    validity["telemetry_present"] = telemetry_artifact_present
    validity["baseline_delivery_observed"] = (
        spec.load.enabled or received_events > 0
    )
    validity["measurement_window_present"] = window_present
    validity["pre_window_network_ready"] = pre_network_ready
    validity["pre_window_target_ready"] = pre_target_ready
    validity["post_window_network_ready"] = post_network_ready
    validity["instrumentation_clean"] = instrumentation_clean
    validity["base_cleanup_reproved"] = cleanup_reproved
    path_ready = (
        pre_network_ready
        and pre_target_ready
        and post_network_ready
        and cleanup_reproved
    )
    return validity, path_ready


def execute_research_experiment(
    *,
    spec: ResearchExperimentSpec,
    inventory: NetworkInventory,
    lock: DependencyLock,
    dependency_root: Path,
    network_manifest: Path,
    network_evidence: Path,
    repository_root: Path,
    run_root: Path = DEFAULT_RESEARCH_RUN_ROOT,
    progress: TextIO | None = None,
) -> ResearchRunResult:
    if spec.probe_target is None:
        raise ResearchError("live controlled experiment requires a probe/load target")
    run_directory = run_root.resolve() / spec.run_id
    summary_path = run_directory / "research-summary.json"
    window_path = run_directory / "measurement-window.json"
    measurement_path = run_directory / "measurement-path.json"
    probe_path = run_directory / "probe.jsonl"
    network_path = run_directory / "network-samples.jsonl"
    load_path = run_directory / "load.jsonl"
    probe_log = run_directory / "logs" / "research-probe.log"
    load_client_log = run_directory / "logs" / "research-load-client.log"
    load_server_log = run_directory / "logs" / "research-load-server.log"
    instrumentation_errors: list[str] = []
    path_errors: list[str] = []
    pre_report: Any | None = None
    post_report: Any | None = None
    pre_target_ready = False
    runtime_identity: dict[str, str] = {}

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] research: {message}", file=progress, flush=True)

    def collector(
        scenario: Any,
        *,
        host: str,
        port: int,
        jsonl_path: Path,
        rejected_path: Path,
        minimum_per_sensor: int,
        timeout_seconds: int,
    ) -> Any:
        del minimum_per_sensor, timeout_seconds
        nonlocal pre_report, post_report, pre_target_ready
        ue_pod, runtime_pdu = _measurement_runtime_handoff(inventory, scenario)
        runtime_identity.update({"ue_pod": ue_pod, "pdu_address": runtime_pdu})
        report(f"runtime UE: {ue_pod}")
        report(f"runtime PDU: {runtime_pdu}")
        _check_research_tools(inventory, ue_pod, load_enabled=spec.load.enabled)
        route_installed = _install_target_route(
            inventory,
            ue_pod,
            pdu_address=runtime_pdu,
            target=spec.probe_target or "",
        )
        report(f"target route: proven through tun_srsue1 to {spec.probe_target}")
        sampler: ResearchNetworkSampler | None = None
        probe_process: base_runtime.ManagedProcess | None = None
        load_process: base_runtime.ManagedProcess | None = None
        load_server: OwnedIperfServer | None = None
        result = None
        sampler_stopped = False

        def save_path_state(cleanup_reproved: bool | None = None) -> None:
            _write_measurement_path(
                measurement_path,
                run_id=scenario.run_id,
                network_run_id=scenario.network_run_id,
                ue_pod=ue_pod,
                pdu_address=runtime_pdu,
                target=spec.probe_target or "",
                pre_report=pre_report,
                pre_target_ready=pre_target_ready,
                post_report=post_report,
                cleanup_reproved=cleanup_reproved,
                errors=path_errors,
            )

        def start_loaded_transport() -> None:
            nonlocal load_server, load_process
            target_bps = spec.load.resolved_target_bps
            assert target_bps is not None
            load_server = start_owned_iperf_server(
                inventory=inventory,
                owner_id=spec.run_id,
                port=spec.load.server_port,
                repository_root=repository_root,
                log_path=load_server_log,
            )
            report(f"load server: ready on port {spec.load.server_port}")
            per_stream_bps = max(1, target_bps // spec.load.parallel_flows)
            load_process = _start_load_client(
                inventory=inventory,
                ue_pod=ue_pod,
                pdu_address=runtime_pdu,
                target=spec.probe_target or "",
                port=spec.load.server_port,
                target_bps=per_stream_bps,
                protocol=spec.load.protocol,
                parallel_flows=spec.load.parallel_flows,
                duration_seconds=(
                    spec.measurement.duration_seconds
                    + _LOAD_RUNTIME_HEADROOM_SECONDS
                ),
                repository_root=repository_root,
                log_path=load_client_log,
            )
            _wait_load_client_connected(
                inventory=inventory,
                ue_pod=ue_pod,
                pdu_address=runtime_pdu,
                target=spec.probe_target or "",
                port=spec.load.server_port,
                process=load_process,
            )
            report("load client: connected")
            report(
                "UDP load: target "
                f"{target_bps / 1_000_000:.2f} Mbps, "
                f"{spec.load.parallel_flows} flow(s)"
            )

        def start_instrumentation() -> None:
            nonlocal pre_report, pre_target_ready
            nonlocal sampler, probe_process
            report("measurement path: verifying...")
            try:
                pre_report = _require_network_ready(
                    inventory=inventory,
                    lock=lock,
                    network_run_id=scenario.network_run_id,
                    ue_pod=ue_pod,
                    pdu_address=runtime_pdu,
                )
                _prove_target_route(
                    inventory,
                    ue_pod,
                    pdu_address=runtime_pdu,
                    target=spec.probe_target or "",
                )
                _prove_pre_window_target(
                    spec=spec,
                    prove_icmp=lambda: _prove_target_reachability(
                        inventory,
                        ue_pod,
                        target=spec.probe_target or "",
                    ),
                    prove_transport=start_loaded_transport,
                )
                pre_target_ready = True
            except Exception as exc:
                path_errors.append(str(exc))
                save_path_state()
                report(f"measurement path: failed: {exc}")
                raise
            save_path_state()
            report("measurement path: ready")

            sampler = ResearchNetworkSampler(
                inventory=inventory,
                network_run_id=scenario.network_run_id,
                experiment_run_id=scenario.run_id,
                ue_pod=ue_pod,
                interval_seconds=spec.measurement.sample_interval_seconds,
                destination=network_path,
            )
            sampler.start()
            report(
                f"network sampler: ready ({spec.measurement.sample_interval_seconds:g}s target interval)"
            )

            probe_process = _start_probe(
                inventory=inventory,
                ue_pod=ue_pod,
                target=spec.probe_target or "",
                duration_seconds=(
                    spec.measurement.duration_seconds
                    + _LOAD_RUNTIME_HEADROOM_SECONDS
                ),
                interval_seconds=spec.measurement.probe_interval_seconds,
                repository_root=repository_root,
                log_path=probe_log,
            )
            report(
                f"RTT probe: ready ({spec.measurement.probe_interval_seconds:g}s interval)"
            )
            report(f"measurement window: {spec.measurement.duration_seconds}s")

        def health_check() -> None:
            if sampler is not None:
                sampler.check()
            if probe_process is not None:
                exit_code = probe_process.process.poll()
                if exit_code is not None:
                    raise ResearchError(
                        f"research RTT probe exited unexpectedly with code {exit_code}"
                    )
            if load_process is not None:
                exit_code = load_process.process.poll()
                if exit_code is not None:
                    raise ResearchError(
                        f"research background load exited unexpectedly with code {exit_code}"
                    )
            if load_server is not None:
                exit_code = load_server.process.process.poll()
                if exit_code is not None:
                    raise ResearchError(
                        f"research load server exited unexpectedly with code {exit_code}"
                    )

        try:
            if spec.measurement.warmup_seconds:
                report(f"warmup: {spec.measurement.warmup_seconds}s")
            result = collect_mqtt_window(
                scenario,
                host=host,
                port=port,
                jsonl_path=jsonl_path,
                rejected_path=rejected_path,
                duration_seconds=spec.measurement.duration_seconds,
                warmup_seconds=spec.measurement.warmup_seconds,
                on_window_start=start_instrumentation,
                health_check=health_check,
            )
            atomic_json(
                window_path,
                {
                    "schema": MEASUREMENT_WINDOW_SCHEMA,
                    "run_id": scenario.run_id,
                    "warmup_seconds": spec.measurement.warmup_seconds,
                    "requested_duration_seconds": spec.measurement.duration_seconds,
                    "started_at_utc": result.started_at_utc.astimezone(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "ended_at_utc": result.ended_at_utc.astimezone(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                },
            )
            if sampler is not None:
                sampler.stop()
                sampler_stopped = True
            report("measurement path: verifying after window...")
            try:
                post_report = _require_network_ready(
                    inventory=inventory,
                    lock=lock,
                    network_run_id=scenario.network_run_id,
                    ue_pod=ue_pod,
                    pdu_address=runtime_pdu,
                )
            except Exception as exc:
                path_errors.append(str(exc))
                save_path_state()
                report(f"measurement path: failed after window: {exc}")
                raise
            save_path_state()
            report("measurement path: ready after window")
            report(
                f"measurement complete: {result.records} telemetry event(s) "
                f"from {result.sensors} sensor(s)"
            )
            return result
        except Exception as exc:
            report(f"measurement aborted: {exc}")
            raise
        finally:
            if sampler is not None and not sampler_stopped:
                try:
                    sampler.stop()
                except Exception as exc:
                    instrumentation_errors.append(str(exc))
            if probe_process is not None:
                try:
                    probe_process.stop()
                except Exception as exc:
                    instrumentation_errors.append(f"{probe_process.name}: {exc}")
            if load_process is not None:
                try:
                    if load_process.process.poll() is None:
                        load_process.process.wait(
                            timeout=_LOAD_RUNTIME_HEADROOM_SECONDS + 5
                        )
                except Exception:
                    pass
                try:
                    load_process.stop()
                except Exception as exc:
                    instrumentation_errors.append(f"{load_process.name}: {exc}")
            if load_server is not None:
                try:
                    stop_owned_iperf_server(inventory, load_server)
                    report("load server: stopped")
                except Exception as exc:
                    instrumentation_errors.append(str(exc))
            if route_installed:
                try:
                    _remove_target_route(
                        inventory,
                        ue_pod,
                        pdu_address=runtime_pdu,
                        target=spec.probe_target or "",
                    )
                    report("target route: restored")
                except Exception as exc:
                    instrumentation_errors.append(str(exc))
            try:
                _parse_probe_log(
                    probe_log,
                    probe_path,
                    interval_seconds=spec.measurement.probe_interval_seconds,
                    window_started_at_utc=(
                        result.started_at_utc if result is not None else None
                    ),
                    window_ended_at_utc=(
                        result.ended_at_utc if result is not None else None
                    ),
                )
            except Exception as exc:
                instrumentation_errors.append(str(exc))
            if spec.load.enabled:
                target_bps = spec.load.resolved_target_bps
                assert target_bps is not None
                try:
                    _parse_load_log(
                        load_client_log,
                        load_path,
                        target_bps=target_bps,
                        protocol=spec.load.protocol,
                    )
                except Exception as exc:
                    instrumentation_errors.append(str(exc))

    with _RUNTIME_OVERRIDE_LOCK:
        with _runtime_overrides(spec=spec, collector=collector):
            base_result = base_runtime.execute_experiment(
                inventory=inventory,
                lock=lock,
                dependency_root=dependency_root,
                network_manifest=network_manifest,
                network_evidence=network_evidence,
                run_id=spec.run_id,
                repository_root=repository_root,
                run_root=run_root,
                collection_seconds=max(30, spec.measurement.duration_seconds),
                minimum_per_sensor=1,
                progress=_ResearchProgressStream(progress),
            )

    save_research_spec(spec, run_directory / "experiment-spec.json")
    telemetry_file = run_directory / "telemetry.jsonl"
    telemetry_records = load_jsonl(telemetry_file)
    probe_records = load_jsonl(probe_path, schema=PROBE_SCHEMA)
    network_records = load_jsonl(network_path, schema=NETWORK_SAMPLE_SCHEMA)
    load_records = load_jsonl(load_path, schema=LOAD_RESULT_SCHEMA)
    summary = build_run_summary(
        spec=spec,
        run_directory=run_directory,
        telemetry_records=telemetry_records,
        probe_records=probe_records,
        network_records=network_records,
        load_records=load_records,
    )
    cleanup_reproved = _base_cleanup_reproved(run_directory)
    if measurement_path.is_file() and runtime_identity:
        _write_measurement_path(
            measurement_path,
            run_id=spec.run_id,
            network_run_id=spec.network_run_id,
            ue_pod=runtime_identity["ue_pod"],
            pdu_address=runtime_identity["pdu_address"],
            target=spec.probe_target or "",
            pre_report=pre_report,
            pre_target_ready=pre_target_ready,
            post_report=post_report,
            cleanup_reproved=cleanup_reproved,
            errors=path_errors,
        )
    validity, path_ready = _finalize_validity(
        summary=summary,
        spec=spec,
        telemetry_artifact_present=telemetry_file.is_file(),
        window_present=window_path.is_file(),
        pre_network_ready=bool(pre_report is not None and pre_report.ready),
        pre_target_ready=pre_target_ready,
        post_network_ready=bool(post_report is not None and post_report.ready),
        instrumentation_clean=not instrumentation_errors,
        cleanup_reproved=cleanup_reproved,
    )
    summary["validity"] = validity
    summary["instrumentation_errors"] = instrumentation_errors
    summary["path_errors"] = path_errors
    summary["ready_for_campaign_analysis"] = all(validity.values())
    summary["path_acceptance_ready"] = path_ready
    summary["base_experiment_ready"] = base_result.ready
    if measurement_path.is_file():
        hashes = dict(summary.get("artifact_sha256", {}))
        hashes[measurement_path.name] = hashlib.sha256(
            measurement_path.read_bytes()
        ).hexdigest()
        summary["artifact_sha256"] = hashes
    save_run_summary(summary, summary_path)

    for source, destination in (
        (telemetry_records, run_directory / "telemetry.parquet"),
        (probe_records, run_directory / "probe.parquet"),
        (network_records, run_directory / "network-samples.parquet"),
        (load_records, run_directory / "load.parquet"),
    ):
        if source or (
            destination.name == "telemetry.parquet" and telemetry_file.is_file()
        ):
            write_records_parquet(source, destination)

    report(
        "telemetry: "
        f"{summary['telemetry']['received_events']}/"
        f"{summary['telemetry']['expected_events']}"
    )
    report(
        "RTT: "
        f"{summary['probe']['samples']} attempt(s), "
        f"{summary['probe']['timeouts']} timeout(s)"
    )
    if spec.load.enabled:
        measured = summary["load_result"]["measured_bps"]["mean"]
        target = summary["load_result"]["target_bps"]
        ratio = summary["load_result"]["target_ratio"]
        if measured is None or target is None or ratio is None:
            report("load: no valid measured goodput")
        else:
            report(
                f"load: {measured / 1_000_000:.2f}/"
                f"{target / 1_000_000:.2f} Mbps ({ratio:.3f} target ratio)"
            )

    return ResearchRunResult(
        run_id=spec.run_id,
        run_directory=run_directory,
        summary_path=summary_path,
        ready_for_campaign_analysis=(summary["ready_for_campaign_analysis"] is True),
        path_acceptance_ready=path_ready,
    )


def calibrate_capacity(
    *,
    inventory: NetworkInventory,
    lock: DependencyLock,
    network_run_id: str,
    target: str,
    repository_root: Path,
    output_path: Path,
    duration_seconds: int = 10,
    server_port: int = 5201,
) -> Mapping[str, Any]:
    if duration_seconds < 5 or duration_seconds > 120:
        raise ResearchError("calibration duration must be between 5 and 120 seconds")
    base = verify_network_path(
        inventory=inventory,
        lock=lock,
        run_id=network_run_id,
        timeout_seconds=120,
    )
    if not base.ready:
        raise ResearchError(
            "capacity calibration requires a currently path-proven network"
        )
    state = reconcile_rfsim_runtime(inventory, network_run_id=network_run_id)
    ue_pod = state.ue_pod
    _check_research_tools(inventory, ue_pod, load_enabled=True)
    route_installed = _install_target_route(
        inventory,
        ue_pod,
        pdu_address=state.pdu_address,
        target=target,
    )
    owner_id = "cal-" + hashlib.sha256(
        f"{network_run_id}:{target}:{server_port}".encode("utf-8")
    ).hexdigest()[:16]
    server: OwnedIperfServer | None = None
    failure: Exception | None = None
    payload: Mapping[str, Any] | None = None
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        server_log = output_path.with_suffix(".server.log")
        server = start_owned_iperf_server(
            inventory=inventory,
            owner_id=owner_id,
            port=server_port,
            repository_root=repository_root,
            log_path=server_log,
        )
        command = _kubectl_exec_command(
            inventory,
            ue_pod,
            "iperf3",
            "-c",
            target,
            "-B",
            state.pdu_address,
            "-p",
            str(server_port),
            "--connect-timeout",
            "5000",
            "-t",
            str(duration_seconds),
            "-J",
        )
        result = base_runtime._run(command, timeout_seconds=duration_seconds + 20)
        if result.returncode != 0:
            raise ResearchError("capacity calibration client failed")
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise ResearchError(
                "capacity calibration produced invalid iperf3 JSON"
            ) from exc
        if not isinstance(value, Mapping):
            raise ResearchError("capacity calibration result is malformed")
        capacity = _extract_iperf_bps(value)
        if capacity is None or capacity <= 0:
            raise ResearchError(
                "capacity calibration did not report positive throughput"
            )
        payload = {
            "schema": CAPACITY_SCHEMA,
            "network_run_id": network_run_id,
            "ue_pod": ue_pod,
            "ue_interface": "tun_srsue1",
            "pdu_address": state.pdu_address,
            "target": target,
            "duration_seconds": duration_seconds,
            "reference_capacity_bps": round(capacity),
            "measured_at_utc": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        }
        output_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    except Exception as exc:
        failure = exc

    cleanup_errors: list[str] = []
    if server is not None:
        try:
            stop_owned_iperf_server(inventory, server)
        except Exception as exc:
            cleanup_errors.append(f"iperf3 server: {exc}")
    if route_installed:
        try:
            _remove_target_route(
                inventory,
                ue_pod,
                pdu_address=state.pdu_address,
                target=target,
            )
        except Exception as exc:
            cleanup_errors.append(f"target route: {exc}")
    if failure is not None and cleanup_errors:
        raise ResearchError(
            "capacity calibration failed and cleanup failed closed: "
            f"{failure}; {'; '.join(cleanup_errors)}"
        ) from failure
    if failure is not None:
        raise failure
    if cleanup_errors:
        raise ResearchError(
            "capacity calibration cleanup failed closed: " + "; ".join(cleanup_errors)
        )
    assert payload is not None
    return payload
