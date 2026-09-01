"""UE-path instrumentation helpers for controlled research experiments."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import ipaddress
import json
import math
from pathlib import Path
import re
import shlex
import time
from typing import Any, Callable, Iterator, Mapping

from synthran.experiment import build_scenario as build_base_scenario
from synthran.experiment import live as base_runtime
from synthran.fiveg_ansible import NetworkInventory
from synthran.live_preflight import ssh_command
from synthran.research import (
    LOAD_RESULT_SCHEMA,
    PROBE_SCHEMA,
    ResearchError,
    ResearchExperimentSpec,
    append_jsonl,
)
from synthran.research.iperf_toolchain import (
    CONTROL_KEEPALIVE_ARG,
    UE_IPERF_PATH,
    prepare_locked_iperf_client,
)

DEFAULT_RESEARCH_RUN_ROOT = Path(".synthran/experiments")
_PING_TIME_RE = re.compile(r"time[=<]([0-9]+(?:\.[0-9]+)?)\s*ms")
_PING_SEQ_RE = re.compile(r"icmp_seq[= ]([0-9]+)")
_PING_EPOCH_RE = re.compile(r"^\[([0-9]+(?:\.[0-9]+)?)\]")
_PING_SUMMARY_RE = re.compile(r"(\d+)\s+packets transmitted")
# RFSIM reconciliation can hand off a fresh PDU before the external user plane
# has fully converged. Keep this strictly pre-window and bounded, but allow more
# than the previous 5 seconds for the iperf3 TCP control handshake to establish.
_IPERF_CONNECT_TIMEOUT_MS = 15000

_PROBE_SCRIPT = r'''
import datetime, json, re, subprocess, sys, time

target = sys.argv[1]
duration = float(sys.argv[2])
interval = float(sys.argv[3])
schema = sys.argv[4]
match_time = re.compile(r"time[=<]([0-9]+(?:\.[0-9]+)?)\s*ms")
started = time.monotonic()
end_at = started + duration
next_at = started
sequence = 0
while True:
    now = time.monotonic()
    if now < next_at:
        time.sleep(next_at - now)
    attempt_at = time.monotonic()
    if attempt_at >= end_at:
        break
    sequence += 1
    observed = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    output = ""
    returncode = 1
    try:
        completed = subprocess.run(
            ["ping", "-n", "-I", "tun_srsue1", "-c", "1", "-W", "1", target],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=max(0.1, min(1.0, interval * 0.8)),
            check=False,
        )
        output = completed.stdout
        returncode = completed.returncode
    except subprocess.TimeoutExpired as exc:
        if isinstance(exc.stdout, bytes):
            output = exc.stdout.decode("utf-8", "replace")
        elif isinstance(exc.stdout, str):
            output = exc.stdout
    match = match_time.search(output)
    rtt = float(match.group(1)) if returncode == 0 and match is not None else None
    print(json.dumps({
        "schema": schema,
        "sequence": sequence,
        "elapsed_seconds": attempt_at - started,
        "observed_at_utc": observed,
        "rtt_ms": rtt,
        "timeout": rtt is None,
    }, separators=(",", ":"), sort_keys=True), flush=True)
    next_at = started + sequence * interval
    after = time.monotonic()
    if next_at <= after:
        next_at = started + (int((after - started) // interval) + 1) * interval
'''

_LOAD_CONNECTION_SCRIPT = r'''
import socket, sys

local_address, target_address, target_port = sys.argv[1], sys.argv[2], int(sys.argv[3])
def ipv4(raw):
    try:
        return socket.inet_ntoa(bytes.fromhex(raw)[::-1])
    except Exception:
        return None
for table in ("/proc/net/tcp", "/proc/net/tcp6"):
    try:
        lines = open(table, encoding="utf-8", errors="replace").read().splitlines()[1:]
    except OSError:
        continue
    for line in lines:
        fields = line.split()
        if len(fields) < 4 or fields[3] != "01":
            continue
        local_raw = fields[1].split(":", 1)[0]
        remote_raw, remote_port_raw = fields[2].split(":", 1)
        local_ip = ipv4(local_raw) if len(local_raw) == 8 else None
        remote_ip = ipv4(remote_raw) if len(remote_raw) == 8 else None
        if local_ip == local_address and remote_ip == target_address and int(remote_port_raw, 16) == target_port:
            raise SystemExit(0)
raise SystemExit(1)
'''


@dataclass(frozen=True)
class ResearchRunResult:
    run_id: str
    run_directory: Path
    summary_path: Path
    ready_for_campaign_analysis: bool
    path_acceptance_ready: bool


def _edge_sidecar_status(inventory: NetworkInventory, pod: str) -> tuple[int, bool, bool, bool]:
    payload = base_runtime._remote_json(
        inventory,
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl get pod "
        f"-n {base_runtime.KUBERNETES_NAMESPACE} {shlex.quote(pod)} -o json",
        label="edge MQTT sidecar status",
    )
    status = payload.get("status")
    if not isinstance(status, Mapping):
        raise ResearchError("edge MQTT sidecar status is unavailable")
    statuses = status.get("containerStatuses")
    if not isinstance(statuses, list):
        raise ResearchError("edge MQTT sidecar container status is unavailable")
    matches = [item for item in statuses if isinstance(item, Mapping) and item.get("name") == base_runtime.EDGE_CONTAINER]
    if len(matches) != 1:
        raise ResearchError("edge MQTT sidecar container status is ambiguous")
    sidecar = matches[0]
    restart_count = sidecar.get("restartCount")
    if not isinstance(restart_count, int) or isinstance(restart_count, bool):
        raise ResearchError("edge MQTT sidecar restart count is unavailable")
    state = sidecar.get("state")
    running = isinstance(state, Mapping) and isinstance(state.get("running"), Mapping)
    container_ready = sidecar.get("ready") is True
    conditions = status.get("conditions")
    pod_ready = isinstance(conditions, list) and any(
        isinstance(item, Mapping) and item.get("type") == "Ready" and item.get("status") == "True"
        for item in conditions
    )
    return restart_count, container_ready, pod_ready, running


def _restart_edge_sidecar_and_wait(
    inventory: NetworkInventory,
    pod: str,
    *,
    restart: Callable[[NetworkInventory, str], None],
    timeout_seconds: int = 60,
) -> None:
    before, _, _, _ = _edge_sidecar_status(inventory, pod)
    restart(inventory, pod)
    deadline = time.monotonic() + timeout_seconds
    latest = "restart not yet observed"
    while time.monotonic() < deadline:
        try:
            count, container_ready, pod_ready, running = _edge_sidecar_status(inventory, pod)
        except Exception as exc:
            latest = str(exc)
        else:
            latest = f"restartCount={count}, containerReady={container_ready}, podReady={pod_ready}, running={running}"
            if count > before and container_ready and pod_ready and running:
                return
        time.sleep(1)
    raise ResearchError(
        "edge MQTT sidecar restart did not reach a new Ready container instance "
        f"within {timeout_seconds}s ({latest})"
    )


def _kubectl_exec_command(inventory: NetworkInventory, ue_pod: str, *command: str) -> tuple[str, ...]:
    return tuple(ssh_command(
        inventory.core_node,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
        f"-n {base_runtime.KUBERNETES_NAMESPACE} {shlex.quote(ue_pod)} -c ue -- "
        + " ".join(shlex.quote(part) for part in command),
    ))


def _check_research_tools(inventory: NetworkInventory, ue_pod: str, *, load_enabled: bool) -> None:
    tools = ["ip", "ping", "python3"]
    script = "for x in " + " ".join(tools) + '; do command -v "$x" >/dev/null || exit 7; done'
    result = base_runtime._run(
        _kubectl_exec_command(inventory, ue_pod, "sh", "-c", script),
        timeout_seconds=15,
    )
    if result.returncode != 0:
        raise ResearchError("UE container is missing required research measurement tools")
    if not load_enabled:
        return
    try:
        prepare_locked_iperf_client(inventory, ue_pod)
    except Exception as exc:
        raise ResearchError(f"locked research iperf3 preparation failed: {exc}") from exc
    help_result = base_runtime._run(
        _kubectl_exec_command(inventory, ue_pod, UE_IPERF_PATH, "--help"),
        timeout_seconds=10,
    )
    help_text = help_result.stdout + help_result.stderr
    if (
        help_result.returncode != 0
        or "--connect-timeout" not in help_text
        or "--cntl-ka" not in help_text
    ):
        raise ResearchError(
            "locked UE iperf3 does not support required connection timeout/keepalive options"
        )


def _target_prefix(target: str) -> str:
    try:
        address = ipaddress.ip_address(target)
    except ValueError as exc:
        raise ResearchError("research target must be a literal IPv4 address") from exc
    if not isinstance(address, ipaddress.IPv4Address):
        raise ResearchError("research target must be a literal IPv4 address")
    return f"{address}/32"


def _target_route_uses_tunnel(
    inventory: NetworkInventory, ue_pod: str, *, pdu_address: str, target: str
) -> bool:
    result = base_runtime._run(
        _kubectl_exec_command(inventory, ue_pod, "ip", "route", "get", target, "from", pdu_address),
        timeout_seconds=10,
    )
    if result.returncode != 0:
        raise ResearchError("research target route could not be inspected")
    return "dev tun_srsue1" in result.stdout


def _prove_target_route(inventory: NetworkInventory, ue_pod: str, *, pdu_address: str, target: str) -> None:
    if not _target_route_uses_tunnel(inventory, ue_pod, pdu_address=pdu_address, target=target):
        raise ResearchError("research target route is not proven through tun_srsue1")


def _prove_target_reachability(inventory: NetworkInventory, ue_pod: str, *, target: str) -> None:
    result = base_runtime._run(
        _kubectl_exec_command(inventory, ue_pod, "ping", "-n", "-I", "tun_srsue1", "-c", "3", "-W", "1", target),
        timeout_seconds=6,
    )
    if result.returncode != 0:
        raise ResearchError("research target is not reachable through tun_srsue1 before measurement")


def _remove_target_route(
    inventory: NetworkInventory, ue_pod: str, *, pdu_address: str, target: str
) -> None:
    result = base_runtime._run(
        _kubectl_exec_command(inventory, ue_pod, "ip", "route", "del", _target_prefix(target), "dev", "tun_srsue1"),
        timeout_seconds=10,
    )
    if result.returncode != 0:
        raise ResearchError("owned research target route cleanup failed")
    if _target_route_uses_tunnel(inventory, ue_pod, pdu_address=pdu_address, target=target):
        raise ResearchError("owned research target route remained after cleanup")


def _install_target_route(
    inventory: NetworkInventory, ue_pod: str, *, pdu_address: str, target: str
) -> bool:
    if _target_route_uses_tunnel(inventory, ue_pod, pdu_address=pdu_address, target=target):
        return False
    result = base_runtime._run(
        _kubectl_exec_command(inventory, ue_pod, "ip", "route", "add", _target_prefix(target), "dev", "tun_srsue1"),
        timeout_seconds=10,
    )
    if result.returncode != 0:
        raise ResearchError("unable to install exact research target route without replacing existing state")
    try:
        _prove_target_route(inventory, ue_pod, pdu_address=pdu_address, target=target)
    except Exception as exc:
        try:
            _remove_target_route(inventory, ue_pod, pdu_address=pdu_address, target=target)
        except Exception as cleanup_exc:
            raise ResearchError(
                "research target route proof failed and route cleanup failed closed: "
                f"{exc}; {cleanup_exc}"
            ) from exc
        raise
    return True


def _start_probe(
    *,
    inventory: NetworkInventory,
    ue_pod: str,
    target: str,
    duration_seconds: int,
    interval_seconds: float,
    repository_root: Path,
    log_path: Path,
) -> base_runtime.ManagedProcess:
    command = _kubectl_exec_command(
        inventory, ue_pod, "python3", "-u", "-c", _PROBE_SCRIPT,
        target, str(duration_seconds), f"{interval_seconds:.6f}", PROBE_SCHEMA,
    )
    return base_runtime._start_process(
        "research RTT probe", command, cwd=repository_root, log_path=log_path
    )


def _append_structured_probe_records(
    lines: list[str],
    destination: Path,
    *,
    window_started_at_utc: datetime | None,
    window_ended_at_utc: datetime | None,
) -> bool:
    records: list[dict[str, Any]] = []
    for line in lines:
        text = line.strip()
        if not text.startswith("{"):
            continue
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(value, dict) or value.get("schema") != PROBE_SCHEMA:
            continue
        sequence, elapsed = value.get("sequence"), value.get("elapsed_seconds")
        observed_at, timeout, rtt = value.get("observed_at_utc"), value.get("timeout"), value.get("rtt_ms")
        if (
            not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1
            or not isinstance(elapsed, (int, float)) or isinstance(elapsed, bool)
            or not isinstance(observed_at, str) or not isinstance(timeout, bool)
            or (rtt is not None and (not isinstance(rtt, (int, float)) or isinstance(rtt, bool) or float(rtt) < 0))
        ):
            raise ResearchError("RTT probe produced a malformed structured record")
        try:
            observed = datetime.fromisoformat(observed_at.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError as exc:
            raise ResearchError("RTT probe produced an invalid timestamp") from exc
        if window_started_at_utc is not None and observed < window_started_at_utc:
            continue
        if window_ended_at_utc is not None and observed > window_ended_at_utc:
            continue
        records.append(dict(value))
    if not records:
        return False
    first_elapsed = float(records[0]["elapsed_seconds"])
    for value in records:
        value["elapsed_seconds"] = max(0.0, float(value["elapsed_seconds"]) - first_elapsed)
        append_jsonl(destination, value)
    return True


def _parse_probe_log(
    path: Path,
    destination: Path,
    *,
    interval_seconds: float,
    window_started_at_utc: datetime | None = None,
    window_ended_at_utc: datetime | None = None,
) -> None:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        raise ResearchError("unable to read RTT probe log") from exc
    if _append_structured_probe_records(
        lines,
        destination,
        window_started_at_utc=window_started_at_utc,
        window_ended_at_utc=window_ended_at_utc,
    ):
        return

    seen: dict[int, tuple[float, float | None]] = {}
    for line in lines:
        seq_match, time_match = _PING_SEQ_RE.search(line), _PING_TIME_RE.search(line)
        if seq_match is None or time_match is None:
            continue
        epoch_match = _PING_EPOCH_RE.search(line)
        seen[int(seq_match.group(1))] = (
            float(time_match.group(1)),
            float(epoch_match.group(1)) if epoch_match is not None else None,
        )
    if not seen:
        transmitted = next(
            (int(match.group(1)) for line in lines if (match := _PING_SUMMARY_RE.search(line)) is not None),
            0,
        )
        if transmitted <= 0:
            raise ResearchError("RTT probe produced no attempted samples")
        if window_started_at_utc is not None and window_ended_at_utc is not None:
            span = max(0.0, (window_ended_at_utc - window_started_at_utc).total_seconds())
            transmitted = min(transmitted, max(1, math.floor(span / interval_seconds) + 1))
        for index in range(transmitted):
            observed = (
                window_started_at_utc + index * timedelta(seconds=interval_seconds)
                if window_started_at_utc is not None else None
            )
            append_jsonl(destination, {
                "schema": PROBE_SCHEMA,
                "sequence": index + 1,
                "elapsed_seconds": index * interval_seconds,
                "observed_at_utc": observed.isoformat().replace("+00:00", "Z") if observed is not None else None,
                "rtt_ms": None,
                "timeout": True,
            })
        return

    first_seen = min(seen)
    anchor_epoch = seen[first_seen][1]
    if (window_started_at_utc is not None or window_ended_at_utc is not None) and anchor_epoch is None:
        raise ResearchError("RTT probe timestamps are missing from ping output")
    if window_started_at_utc is not None and window_ended_at_utc is not None:
        first_sequence = max(1, math.ceil(first_seen + (window_started_at_utc.timestamp() - anchor_epoch) / interval_seconds))
        last_sequence = math.floor(first_seen + (window_ended_at_utc.timestamp() - anchor_epoch) / interval_seconds)
        if last_sequence < first_sequence:
            raise ResearchError("RTT probe does not overlap the measurement window")
    else:
        first_sequence, last_sequence = min(seen), max(seen)
    for sequence in range(first_sequence, last_sequence + 1):
        observed = seen.get(sequence)
        rtt = observed[0] if observed is not None else None
        expected_epoch = anchor_epoch + (sequence - first_seen) * interval_seconds if anchor_epoch is not None else None
        append_jsonl(destination, {
            "schema": PROBE_SCHEMA,
            "sequence": sequence,
            "elapsed_seconds": (sequence - first_sequence) * interval_seconds,
            "observed_at_utc": datetime.fromtimestamp(expected_epoch, timezone.utc).isoformat().replace("+00:00", "Z") if expected_epoch is not None else None,
            "rtt_ms": rtt,
            "timeout": rtt is None,
        })


def _start_load_client(
    *,
    inventory: NetworkInventory,
    ue_pod: str,
    pdu_address: str,
    target: str,
    port: int,
    target_bps: int,
    protocol: str,
    parallel_flows: int,
    duration_seconds: int,
    repository_root: Path,
    log_path: Path,
) -> base_runtime.ManagedProcess:
    arguments = [
        UE_IPERF_PATH, "-c", target, "-B", pdu_address, "-p", str(port),
        "--connect-timeout", str(_IPERF_CONNECT_TIMEOUT_MS),
        CONTROL_KEEPALIVE_ARG,
        "-t", str(duration_seconds), "-P", str(parallel_flows), "-J",
    ]
    if protocol == "udp":
        arguments.extend(("-u", "-b", str(target_bps)))
    return base_runtime._start_process(
        "research background load",
        _kubectl_exec_command(inventory, ue_pod, *arguments),
        cwd=repository_root,
        log_path=log_path,
    )


def _wait_load_client_connected(
    *,
    inventory: NetworkInventory,
    ue_pod: str,
    pdu_address: str,
    target: str,
    port: int,
    process: base_runtime.ManagedProcess,
    timeout_seconds: float = 20.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        exit_code = process.process.poll()
        if exit_code is not None:
            raise ResearchError(f"research background load exited with code {exit_code} before its control connection became ready")
        result = base_runtime._run(
            _kubectl_exec_command(inventory, ue_pod, "python3", "-c", _LOAD_CONNECTION_SCRIPT, pdu_address, target, str(port)),
            timeout_seconds=5,
        )
        if result.returncode == 0:
            return
        time.sleep(0.2)
    exit_code = process.process.poll()
    if exit_code is not None:
        raise ResearchError(f"research background load exited with code {exit_code} before its control connection became ready")
    raise ResearchError("research background load control connection did not become ready")


def _extract_iperf_bps(
    value: Mapping[str, Any],
    *,
    protocol: str | None = None,
) -> float | None:
    end = value.get("end")
    if not isinstance(end, Mapping):
        return None
    order = (
        ("sum_sent", "sum_received", "sum")
        if protocol == "udp"
        else ("sum_received", "sum_sent", "sum")
    )
    for name in order:
        candidate = end.get(name)
        if isinstance(candidate, Mapping):
            bps = candidate.get("bits_per_second")
            if isinstance(bps, (int, float)) and not isinstance(bps, bool):
                return float(bps)
    streams = end.get("streams")
    totals: list[float] = []
    if isinstance(streams, list):
        for stream in streams:
            if not isinstance(stream, Mapping):
                continue
            receiver, sender = stream.get("receiver"), stream.get("sender")
            if protocol == "udp":
                candidate = sender if isinstance(sender, Mapping) else receiver
            else:
                candidate = receiver if isinstance(receiver, Mapping) else sender
            if isinstance(candidate, Mapping):
                bps = candidate.get("bits_per_second")
                if isinstance(bps, (int, float)) and not isinstance(bps, bool):
                    totals.append(float(bps))
    return sum(totals) if totals else None


def _parse_load_log(path: Path, destination: Path, *, target_bps: int, protocol: str) -> None:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise ResearchError("unable to read background load log") from exc
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end < start:
        raise ResearchError("background load did not produce iperf3 JSON")
    try:
        value = json.loads(text[start : end + 1])
    except json.JSONDecodeError as exc:
        raise ResearchError("background load produced invalid iperf3 JSON") from exc
    if not isinstance(value, Mapping):
        raise ResearchError("background load result is malformed")
    error = value.get("error")
    if isinstance(error, str) and error.strip():
        raise ResearchError(f"background load iperf3 failed: {error.strip()}")
    bps = _extract_iperf_bps(value, protocol=protocol)
    if bps is None or bps <= 0:
        raise ResearchError("background load result does not contain positive measured throughput")
    append_jsonl(destination, {
        "schema": LOAD_RESULT_SCHEMA,
        "protocol": protocol,
        "target_bps": target_bps,
        "bits_per_second": bps,
    })


def _base_cleanup_reproved(run_directory: Path) -> bool:
    try:
        value = json.loads((run_directory / "experiment-evidence.json").read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    checks = value.get("checks") if isinstance(value, Mapping) else None
    return isinstance(checks, list) and any(
        isinstance(check, Mapping) and check.get("name") == "cleanup-base-network" and check.get("passed") is True
        for check in checks
    )


@contextmanager
def _runtime_overrides(
    *,
    spec: ResearchExperimentSpec,
    collector: Callable[..., Any],
) -> Iterator[None]:
    original_builder = base_runtime.build_scenario
    original_collector = base_runtime.collect_mqtt
    original_restart = base_runtime._restart_edge_sidecar

    def research_builder(**kwargs: Any) -> Any:
        return build_base_scenario(
            **kwargs,
            sensor_period_seconds=spec.sensor_period_seconds,
            cooja_seed=spec.cooja_seed,
        )

    def research_restart(inventory: NetworkInventory, pod: str) -> None:
        _restart_edge_sidecar_and_wait(inventory, pod, restart=original_restart)

    base_runtime.build_scenario = research_builder
    base_runtime.collect_mqtt = collector
    base_runtime._restart_edge_sidecar = research_restart
    try:
        yield
    finally:
        base_runtime.build_scenario = original_builder
        base_runtime.collect_mqtt = original_collector
        base_runtime._restart_edge_sidecar = original_restart
