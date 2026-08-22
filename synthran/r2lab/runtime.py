"""Read-only live verification for the physical R2Lab path.

This module contains the runtime observation boundary that follows a reviewed,
artifact-bound singleton gNB start. It deliberately performs no radio power,
UE attach, Helm, or Kubernetes scale mutation. Raw modem, gNB, AMF, and qfit
output is reduced immediately to sanitized evidence before it can enter
persistent state.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import json
from pathlib import Path
import re
import shlex
from typing import Callable, Sequence

from synthran.live_preflight import CommandResult
from synthran.r2lab.acceptance import (
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
)
from synthran.r2lab.deployment import (
    CORE_NODE,
    DEPLOYMENT_PACKAGE_ANNOTATION,
    DEPLOYMENT_RENDER_ANNOTATION,
    DEPLOYMENT_RUN_ANNOTATION,
    DEPLOYMENT_RUN_LABEL,
    GNB_DEPLOYMENT,
    GNB_NAMESPACE,
    GNB_SELECTOR,
    PhysicalStartAuthority,
    parse_gnb_pods_json,
)
from synthran.r2lab.n2 import build_amf_n2_evidence
from synthran.r2lab.radio import (
    QfitRuntimeEvidence,
    UserPlaneProbeEvidence,
    classify_qfit_runtime,
    execute_user_plane_probe,
)
from synthran.r2lab.readiness import (
    QfitReadinessEvidence,
    R2LabQfitReadinessError,
    execute_qfit_readiness,
)


Runner = Callable[[Sequence[str], int], CommandResult]
_SAFE_QFIT_RE = re.compile(r"^qfit(?:07|09|18|29|32|34)$")
_SAFE_POD_RE = re.compile(r"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_QFIT_AT_DEVICE = "/dev/ttyUSB2"
_QFIT_MBIM_DEVICE = "/dev/cdc-wdm0"
_QFIT_INTERFACE = "wwan0"
_AMF_NAMESPACE = "open5gs"
_AMF_SELECTOR = "nf=amf"

# The two AT commands below are intentionally allow-listed. In particular, the
# stock R2Lab `check-ue` helper is not used because it also queries IMSI.
_ALLOWED_AT_PROBES = frozenset({"AT+QNWINFO", "AT+C5GREG?"})
_SERIAL_PROBE_SCRIPT = (
    "import serial,sys,time;"
    f"p=serial.Serial('{_QFIT_AT_DEVICE}',460800,timeout=2);"
    "p.reset_input_buffer();"
    "p.write((sys.argv[1]+'\\r').encode('ascii'));"
    "time.sleep(0.25);"
    "sys.stdout.write(p.read_all().decode('utf-8','replace'));"
    "p.close()"
)


class R2LabRuntimeVerificationError(RuntimeError):
    """Raised when a runtime verification request crosses a safety boundary."""


class N2State(str, Enum):
    ESTABLISHED = "established"
    NOT_OBSERVED = "not-observed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class GnbN2Evidence:
    """Sanitized current-state proof for the run-bound singleton gNB and N2."""

    namespace_owned: bool
    deployment_bound: bool
    desired_replicas: int | None
    pod_count: int | None
    ready_running_count: int | None
    n2_state: N2State
    n2_source: str
    peer_fingerprint: str | None
    log_observed: bool
    transport_error: bool

    @property
    def singleton_ready(self) -> bool:
        return (
            self.desired_replicas == 1
            and self.pod_count == 1
            and self.ready_running_count == 1
        )

    @property
    def proven(self) -> bool:
        return (
            not self.transport_error
            and self.namespace_owned
            and self.deployment_bound
            and self.singleton_ready
            and self.log_observed
            and self.n2_state is N2State.ESTABLISHED
            and self.n2_source in {"gnb-log", "amf-exact-peer"}
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "namespace_owned": self.namespace_owned,
            "deployment_bound": self.deployment_bound,
            "desired_replicas": self.desired_replicas,
            "pod_count": self.pod_count,
            "ready_running_count": self.ready_running_count,
            "n2_state": self.n2_state.value,
            "n2_source": self.n2_source,
            "peer_fingerprint": self.peer_fingerprint,
            "log_observed": self.log_observed,
            "transport_error": self.transport_error,
            "proven": self.proven,
        }


@dataclass(frozen=True)
class PhysicalRuntimeVerificationResult:
    """In-memory summary; persistent truth remains PhysicalRunEvidence."""

    evidence: PhysicalRunEvidence
    gnb_n2: GnbN2Evidence
    qfit_readiness: QfitReadinessEvidence | None
    qfit_runtime: QfitRuntimeEvidence | None
    user_plane: UserPlaneProbeEvidence | None


def _validate_digest(value: str, label: str) -> str:
    if not _SHA256_RE.fullmatch(value):
        raise R2LabRuntimeVerificationError(f"{label} is not a SHA-256 digest")
    return value


def _validate_qfit(value: str) -> str:
    qfit = value.strip().lower()
    if not _SAFE_QFIT_RE.fullmatch(qfit):
        raise R2LabRuntimeVerificationError("runtime verification requires one reviewed qfit UE")
    return qfit


def _cluster_ssh(known_hosts: Path, *remote: str) -> tuple[str, ...]:
    """Strict SLICES SSH; the remote command is one shell-escaped argv string."""

    return (
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        f"root@{CORE_NODE}",
        shlex.join(remote),
    )


def _gateway_command(slice_name: str, *remote: str) -> tuple[str, ...]:
    from synthran.r2lab.controller import gateway_command

    return gateway_command(slice_name, *remote)


def _qfit_gateway_command(slice_name: str, qfit: str, *remote: str) -> tuple[str, ...]:
    """Strict nested SSH from Faraday to exactly one reviewed qfit host."""

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
    return _gateway_command(slice_name, shlex.join(inner))


def _serial_probe_command(command: str) -> tuple[str, ...]:
    if command not in _ALLOWED_AT_PROBES:
        raise R2LabRuntimeVerificationError("AT probe is not in the sanitized allow-list")
    return ("python3", "-c", _SERIAL_PROBE_SCRIPT, command)


def qfit_runtime_probe_commands() -> tuple[tuple[str, ...], ...]:
    return (
        _serial_probe_command("AT+QNWINFO"),
        _serial_probe_command("AT+C5GREG?"),
        ("mbimcli", "-p", "-d", _QFIT_MBIM_DEVICE, "--query-packet-service-state"),
        ("ip", "-o", "link", "show", "dev", _QFIT_INTERFACE),
        ("ip", "-o", "-4", "addr", "show", "dev", _QFIT_INTERFACE),
    )


def _safe_qfit_read(
    *, slice_name: str, qfit: str, command: Sequence[str], runner: Runner, timeout_seconds: int
) -> CommandResult | None:
    try:
        result = runner(_qfit_gateway_command(slice_name, qfit, *tuple(command)), timeout_seconds)
    except (RuntimeError, OSError):
        return None
    return result if result.returncode == 0 else None


def execute_qfit_runtime_probe(
    *, slice_name: str, qfit: str, runner: Runner, timeout_seconds: int = 15
) -> QfitRuntimeEvidence:
    qfit = _validate_qfit(qfit)
    if timeout_seconds < 5 or timeout_seconds > 60:
        raise R2LabRuntimeVerificationError("qfit probe timeout must be between 5 and 60 seconds")
    commands = qfit_runtime_probe_commands()
    qnwinfo = _safe_qfit_read(slice_name=slice_name, qfit=qfit, command=commands[0], runner=runner, timeout_seconds=timeout_seconds)
    registration = _safe_qfit_read(slice_name=slice_name, qfit=qfit, command=commands[1], runner=runner, timeout_seconds=timeout_seconds)
    packet = _safe_qfit_read(slice_name=slice_name, qfit=qfit, command=commands[2], runner=runner, timeout_seconds=timeout_seconds)
    link = _safe_qfit_read(slice_name=slice_name, qfit=qfit, command=commands[3], runner=runner, timeout_seconds=timeout_seconds)
    address = None
    if link is not None:
        address = _safe_qfit_read(slice_name=slice_name, qfit=qfit, command=commands[4], runner=runner, timeout_seconds=timeout_seconds)
    return classify_qfit_runtime(
        qnwinfo_output=qnwinfo.stdout if qnwinfo is not None else "",
        c5greg_output=registration.stdout if registration is not None else "",
        packet_service_output=packet.stdout if packet is not None else "",
        ipv4_output=address.stdout if address is not None else "",
        interface_present=link is not None,
    )


def execute_qfit_management_probe(
    *, slice_name: str, qfit: str, runner: Runner, timeout_seconds: int = 15
) -> bool:
    """Legacy reachability probe; insufficient for UE-management acceptance."""
    qfit = _validate_qfit(qfit)
    try:
        result = runner(_gateway_command(slice_name, "ping", "-c", "1", "-W", "1", qfit), timeout_seconds)
    except (RuntimeError, OSError):
        return False
    return result.returncode == 0


def _deployment_binding(text: str, *, evidence: PhysicalRunEvidence) -> tuple[bool, int | None]:
    if evidence.staged is None:
        return False, None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return False, None
    if not isinstance(payload, dict):
        return False, None
    metadata = payload.get("metadata")
    spec = payload.get("spec")
    if not isinstance(metadata, dict) or not isinstance(spec, dict):
        return False, None
    labels = metadata.get("labels")
    annotations = metadata.get("annotations")
    desired = spec.get("replicas")
    if not isinstance(labels, dict) or not isinstance(annotations, dict):
        return False, None
    if not isinstance(desired, int) or isinstance(desired, bool):
        return False, None
    expected = evidence.staged
    bound = (
        labels.get(DEPLOYMENT_RUN_LABEL) == evidence.run_id
        and annotations.get(DEPLOYMENT_RUN_ANNOTATION) == evidence.run_id
        and annotations.get(DEPLOYMENT_PACKAGE_ANNOTATION) == expected.package_sha256
        and annotations.get(DEPLOYMENT_RENDER_ANNOTATION) == expected.render_sha256
    )
    return bound, desired


def _one_pod_name(text: str) -> str | None:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or len(items) != 1:
        return None
    item = items[0]
    metadata = item.get("metadata") if isinstance(item, dict) else None
    name = metadata.get("name") if isinstance(metadata, dict) else None
    if not isinstance(name, str) or not _SAFE_POD_RE.fullmatch(name):
        return None
    return name


def _one_ready_pod_name(text: str) -> str | None:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or len(items) != 1:
        return None
    item = items[0]
    if not isinstance(item, dict):
        return None
    metadata = item.get("metadata")
    status = item.get("status")
    if not isinstance(metadata, dict) or not isinstance(status, dict):
        return None
    name = metadata.get("name")
    if not isinstance(name, str) or not _SAFE_POD_RE.fullmatch(name):
        return None
    if status.get("phase") != "Running":
        return None
    statuses = status.get("containerStatuses")
    if not isinstance(statuses, list) or not statuses:
        return None
    if any(not isinstance(item, dict) or item.get("ready") is not True for item in statuses):
        return None
    return name


def parse_n2_log_state(text: str) -> N2State:
    if not text.strip():
        return N2State.NOT_OBSERVED
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in lines:
        lowered = line.lower()
        if any(word in lowered for word in ("failed", "failure", "error", "timeout", "disconnected")):
            continue
        if re.search(r"\bamf\b.*\b(?:connection|association)\b.*\b(?:established|connected|successful)\b", lowered):
            return N2State.ESTABLISHED
        if re.search(r"\b(?:ngap|ng[- ]?setup|n2)\b.*\b(?:established|connected|successful|success|response)\b", lowered):
            return N2State.ESTABLISHED
    all_text = "\n".join(lines).lower()
    if ("amf" in all_text or "ngap" in all_text) and re.search(r"\bsctp\b.*\b(?:established|connected)\b", all_text):
        return N2State.ESTABLISHED
    return N2State.NOT_OBSERVED


def verify_gnb_n2(
    *,
    evidence: PhysicalRunEvidence,
    known_hosts: Path,
    runner: Runner,
    expected_gnb_n2_peer: str | None = None,
    timeout_seconds: int = 30,
) -> GnbN2Evidence:
    if evidence.staged is None or evidence.gnb_start is None:
        raise R2LabRuntimeVerificationError("gNB/N2 verification requires staged and started evidence")
    if evidence.gnb_start.run_id != evidence.run_id:
        raise R2LabRuntimeVerificationError("gNB start evidence belongs to another run")
    _validate_digest(evidence.gnb_start.package_sha256, "package digest")
    _validate_digest(evidence.gnb_start.render_sha256, "render digest")
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabRuntimeVerificationError("strict SLICES known-hosts file is missing")
    if timeout_seconds < 5 or timeout_seconds > 60:
        raise R2LabRuntimeVerificationError("gNB/N2 probe timeout must be between 5 and 60 seconds")

    def read(*remote: str) -> CommandResult | None:
        try:
            result = runner(_cluster_ssh(known_hosts, *remote), timeout_seconds)
        except (RuntimeError, OSError):
            return None
        return result if result.returncode == 0 else None

    namespace = read("kubectl", "get", "namespace", GNB_NAMESPACE, "-o", "jsonpath={.metadata.labels.synthran\\.run/id}")
    namespace_owned = namespace is not None and namespace.stdout.strip() == evidence.run_id
    deployment = read("kubectl", "get", f"deployment/{GNB_DEPLOYMENT}", "-n", GNB_NAMESPACE, "-o", "json")
    deployment_bound = False
    desired: int | None = None
    if deployment is not None:
        deployment_bound, desired = _deployment_binding(deployment.stdout, evidence=evidence)
    pods_result = read("kubectl", "get", "pods", "-n", GNB_NAMESPACE, "-l", GNB_SELECTOR, "-o", "json")
    pod_count: int | None = None
    ready_count: int | None = None
    pod_name: str | None = None
    if pods_result is not None:
        try:
            observation = parse_gnb_pods_json(pods_result.stdout)
        except RuntimeError:
            observation = None
        if observation is not None:
            pod_count = observation.total_count
            ready_count = observation.ready_running_count
            if observation.exactly_one_ready:
                pod_name = _one_pod_name(pods_result.stdout)
    logs = None
    if pod_name is not None:
        logs = read("kubectl", "logs", f"pod/{pod_name}", "-n", GNB_NAMESPACE, "--tail=400")
    n2_state = parse_n2_log_state(logs.stdout) if logs is not None else N2State.UNKNOWN
    n2_source = "gnb-log" if n2_state is N2State.ESTABLISHED else "not-observed"
    peer_fingerprint: str | None = None
    log_observed = logs is not None
    n2_transport_error = pod_name is not None and logs is None

    if n2_state is not N2State.ESTABLISHED and expected_gnb_n2_peer is not None:
        amf_pods = read("kubectl", "get", "pods", "-n", _AMF_NAMESPACE, "-l", _AMF_SELECTOR, "-o", "json")
        amf_name = _one_ready_pod_name(amf_pods.stdout) if amf_pods is not None else None
        amf_logs = None
        if amf_name is not None:
            amf_logs = read("kubectl", "logs", f"pod/{amf_name}", "-n", _AMF_NAMESPACE, "--tail=400")
        if amf_logs is not None:
            amf = build_amf_n2_evidence(text=amf_logs.stdout, expected_peer=expected_gnb_n2_peer)
            log_observed = log_observed or amf.log_observed
            if amf.proven:
                n2_state = N2State.ESTABLISHED
                n2_source = "amf-exact-peer"
                peer_fingerprint = amf.peer_fingerprint
                n2_transport_error = False
            elif logs is None:
                n2_transport_error = False
        elif logs is None:
            n2_transport_error = True

    base_transport_error = any(item is None for item in (namespace, deployment, pods_result))
    return GnbN2Evidence(
        namespace_owned=namespace_owned,
        deployment_bound=deployment_bound,
        desired_replicas=desired,
        pod_count=pod_count,
        ready_running_count=ready_count,
        n2_state=n2_state,
        n2_source=n2_source,
        peer_fingerprint=peer_fingerprint,
        log_observed=log_observed,
        transport_error=base_transport_error or n2_transport_error,
    )


def _same_start_authority(authority: PhysicalStartAuthority, evidence: PhysicalRunEvidence) -> bool:
    if evidence.gnb_start is None:
        return False
    return (
        authority.run_id == evidence.run_id
        and authority.radio == "n300"
        and authority.ue_kind == "qfit"
        and authority.claim_sha256 == evidence.gnb_start.claim_sha256
        and authority.lease_verified is True
        and authority.radio_state == "on"
    )


def _persist(evidence: PhysicalRunEvidence, path: Path | None) -> None:
    if path is not None:
        evidence.write_json(path)


def execute_physical_runtime_verification(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    run_root: Path,
    known_hosts: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    qfit_known_hosts_remote: str | None = None,
    expected_gnb_n2_peer: str | None = None,
    user_plane_peer: str | None = None,
    evidence_path: Path | None = None,
    timeout_seconds: int = 30,
) -> PhysicalRuntimeVerificationResult:
    if evidence.gnb_start is None:
        raise R2LabRuntimeVerificationError("runtime verification requires bound gNB start evidence")
    if evidence.acceptance.next_stage is not PhysicalAcceptanceStage.GNB_N2:
        raise R2LabRuntimeVerificationError("runtime verification must begin at the gNB/N2 stage")

    from synthran.r2lab.controller import authorize_physical_start

    authority = authorize_physical_start(run_id=evidence.run_id, slice_name=slice_name, run_root=run_root, runner=r2lab_runner, timeout_seconds=timeout_seconds).validate()
    if not _same_start_authority(authority, evidence):
        raise R2LabRuntimeVerificationError("R2Lab claim or selected-resource authority changed")
    gnb = verify_gnb_n2(evidence=evidence, known_hosts=known_hosts, runner=cluster_runner, expected_gnb_n2_peer=expected_gnb_n2_peer, timeout_seconds=timeout_seconds)
    state = evidence
    gnb_source = "sanitized-gnb-n2:" + f"bound-{int(gnb.deployment_bound)}:" + f"one-ready-{int(gnb.singleton_ready)}:" + f"n2-{gnb.n2_state.value}:source-{gnb.n2_source}"
    if gnb.proven:
        state = state.pass_stage(PhysicalAcceptanceStage.GNB_N2, source=gnb_source)
    else:
        state = state.fail_stage(PhysicalAcceptanceStage.GNB_N2, source=gnb_source)
        _persist(state, evidence_path)
        return PhysicalRuntimeVerificationResult(state, gnb, None, None, None)
    _persist(state, evidence_path)

    authority = authorize_physical_start(run_id=evidence.run_id, slice_name=slice_name, run_root=run_root, runner=r2lab_runner, timeout_seconds=timeout_seconds).validate()
    if not _same_start_authority(authority, state):
        raise R2LabRuntimeVerificationError("R2Lab authority changed before qfit verification")
    if qfit_known_hosts_remote is None:
        raise R2LabRuntimeVerificationError("qfit readiness requires an explicit strict remote known-hosts path")

    def faraday_runner(command: Sequence[str], command_timeout: int) -> CommandResult:
        return r2lab_runner(_gateway_command(slice_name, *tuple(command)), command_timeout)

    try:
        readiness = execute_qfit_readiness(qfit=authority.ue, remote_known_hosts=qfit_known_hosts_remote, runner=faraday_runner, timeout_seconds=min(timeout_seconds, 30))
    except R2LabQfitReadinessError as exc:
        raise R2LabRuntimeVerificationError(str(exc)) from exc
    if readiness.ready:
        state = state.pass_stage(PhysicalAcceptanceStage.UE_MANAGEMENT, source="sanitized-qfit-management:ready")
    else:
        state = state.fail_stage(PhysicalAcceptanceStage.UE_MANAGEMENT, source="sanitized-qfit-management:not-ready")
        _persist(state, evidence_path)
        return PhysicalRuntimeVerificationResult(state, gnb, readiness, None, None)
    _persist(state, evidence_path)

    qfit = execute_qfit_runtime_probe(slice_name=slice_name, qfit=authority.ue, runner=r2lab_runner, timeout_seconds=min(timeout_seconds, 30))
    state = state.record_qfit_runtime(qfit)
    _persist(state, evidence_path)
    if state.acceptance.failed_stage is not None:
        return PhysicalRuntimeVerificationResult(state, gnb, readiness, qfit, None)

    user_plane = None
    if user_plane_peer is not None:
        authority = authorize_physical_start(run_id=evidence.run_id, slice_name=slice_name, run_root=run_root, runner=r2lab_runner, timeout_seconds=timeout_seconds).validate()
        if not _same_start_authority(authority, state):
            raise R2LabRuntimeVerificationError("R2Lab authority changed before user-plane proof")

        def qfit_runner(command: Sequence[str], command_timeout: int) -> CommandResult:
            return r2lab_runner(_qfit_gateway_command(slice_name, authority.ue, *tuple(command)), command_timeout)

        user_plane = execute_user_plane_probe(peer=user_plane_peer, runner=qfit_runner, command_timeout_seconds=min(timeout_seconds, 60))
        state = state.record_user_plane(user_plane)
        _persist(state, evidence_path)
    return PhysicalRuntimeVerificationResult(state, gnb, readiness, qfit, user_plane)
