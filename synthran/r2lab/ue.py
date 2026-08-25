"""Selected-topology FR1 UE lifecycle for the physical R2Lab backend.

The same ordered UE boundary is used for FIT-attached qfits and Raspberry-Pi
qhats.  MBIM devices use libmbim; RedCap RG255C qhats use one run-owned
``quectel-CM`` QMI process.  Every mutation and traffic boundary refreshes the
current R2Lab claim/lease, selected SLICES allocation, singleton gNB, and N2.
Deployment hashes remain provenance and are deliberately not runtime authority.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import shlex
import tempfile
import time
from typing import Callable, Mapping, Protocol, Sequence

from synthran.live_preflight import CommandResult, subprocess_runner
from synthran.r2lab.acceptance import (
    AcceptanceOutcome,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
    R2LabAcceptanceError,
)
from synthran.r2lab.hardware import PhysicalTopology
from synthran.r2lab.n2 import build_amf_n2_evidence
from synthran.r2lab.radio import (
    CellAcquisitionState,
    Ipv4State,
    PacketServiceState,
    RegistrationState,
    UserPlaneProbeEvidence,
    execute_user_plane_probe,
    parse_c5greg,
    parse_ipv4_state,
    parse_packet_service,
    parse_qnwinfo,
)
from synthran.r2lab.resources import (
    R2LabTopologyResourceError,
    load_topology,
    ue_gateway_command,
    verify_physical_authority,
    verify_selected_allocation,
)
from synthran.r2lab.runtime import N2State, parse_n2_log_state


Runner = Callable[[Sequence[str], int], CommandResult]
Sleeper = Callable[[float], None]
NAMESPACE = "open5gs"
RELEASE = "srsran-gnb"
GNB_SELECTOR = "app=srsran,component=gnb"
RUN_LABEL = "synthran.run/id"
RUN_ANNOTATION = "synthran.io/run-id"
UE_INTERFACE = "wwan0"
AT_DEVICE = "/dev/ttyUSB2"
MBIM_DEVICE = "/dev/cdc-wdm0"
_ALLOWED_AT = frozenset({"AT+QNWINFO", "AT+C5GREG?"})
_SERIAL_SCRIPT = (
    "import serial,sys,time;"
    f"p=serial.Serial('{AT_DEVICE}',460800,timeout=2);"
    "p.reset_input_buffer();"
    "p.write((sys.argv[1]+'\\r').encode('ascii'));"
    "time.sleep(0.25);"
    "sys.stdout.write(p.read_all().decode('utf-8','replace'));"
    "p.close()"
)
_SAFE_POD = re.compile(r"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$")


class R2LabPhysicalUeError(RuntimeError):
    """Raised when the selected physical UE path cannot be proven safely."""


@dataclass(frozen=True)
class PhysicalUeRuntimeEvidence:
    ue: str
    mode: str
    interface: str
    cell: CellAcquisitionState
    registration: RegistrationState
    packet_service: PacketServiceState
    ipv4: Ipv4State
    manager_running: bool
    transport_error: bool

    @property
    def cell_acquired(self) -> bool:
        return self.cell is CellAcquisitionState.ACQUIRED_NR_SA

    @property
    def registered(self) -> bool:
        return self.cell_acquired and self.registration is RegistrationState.REGISTERED

    @property
    def pdu_session_established(self) -> bool:
        return (
            self.registered
            and self.packet_service is PacketServiceState.ATTACHED
            and self.ipv4 is Ipv4State.PRESENT
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-ue-runtime/v1alpha1",
            "ue": self.ue,
            "mode": self.mode,
            "interface": self.interface,
            "cell": self.cell.value,
            "registration": self.registration.value,
            "packet_service": self.packet_service.value,
            "ipv4": self.ipv4.value,
            "manager_running": self.manager_running,
            "transport_error": self.transport_error,
            "cell_acquired": self.cell_acquired,
            "registered": self.registered,
            "pdu_session_established": self.pdu_session_established,
        }


@dataclass(frozen=True)
class PhysicalUeActivationSummary:
    run_id: str
    ue: str
    mode: str
    status: str
    runtime: PhysicalUeRuntimeEvidence
    evidence_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-ue-activation/v1alpha1",
            "run_id": self.run_id,
            "ue": self.ue,
            "mode": self.mode,
            "status": self.status,
            "runtime": self.runtime.to_dict(),
            "evidence_path": str(self.evidence_path),
        }


@dataclass(frozen=True)
class PhysicalUeUserPlaneSummary:
    evidence: PhysicalRunEvidence
    probe: UserPlaneProbeEvidence


@dataclass(frozen=True)
class PhysicalWorkloadContext:
    run_id: str
    ue: str
    interface: str
    backend: str = "r2lab"

    def __post_init__(self) -> None:
        if self.backend != "r2lab":
            raise R2LabPhysicalUeError("physical workload context must use the R2Lab backend")
        if self.interface != UE_INTERFACE:
            raise R2LabPhysicalUeError("physical workload context must use wwan0")
        if not self.ue:
            raise R2LabPhysicalUeError("physical workload context UE is missing")

    def to_dict(self) -> dict[str, str]:
        return {
            "run_id": self.run_id,
            "ue": self.ue,
            "interface": self.interface,
            "backend": self.backend,
        }


@dataclass(frozen=True)
class PhysicalWorkloadResult:
    run_id: str
    workload_id: str
    backend: str
    interface: str
    evidence_sha256: str
    accepted: bool
    cleanup_proven: bool

    def __post_init__(self) -> None:
        if self.backend != "r2lab" or self.interface != UE_INTERFACE:
            raise R2LabPhysicalUeError("physical workload result is not bound to R2Lab/wwan0")
        if not re.fullmatch(r"[0-9a-f]{64}", self.evidence_sha256):
            raise R2LabPhysicalUeError("physical workload evidence digest is malformed")

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "workload_id": self.workload_id,
            "backend": self.backend,
            "interface": self.interface,
            "evidence_sha256": self.evidence_sha256,
            "accepted": self.accepted,
            "cleanup_proven": self.cleanup_proven,
        }


class PhysicalWorkloadExecutor(Protocol):
    def __call__(self, context: PhysicalWorkloadContext) -> PhysicalWorkloadResult: ...


def _write_json(path: Path, payload: Mapping[str, object]) -> Path:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(dict(payload), indent=2, sort_keys=True) + "\n"
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=path.parent,
            delete=False,
        ) as temporary:
            temporary.write(text)
            temporary_path = Path(temporary.name)
        temporary_path.replace(path)
    except OSError as exc:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise R2LabPhysicalUeError("physical UE evidence could not be persisted") from exc
    return path


def _cluster_ssh(topology: PhysicalTopology, known_hosts: Path, *remote: str) -> tuple[str, ...]:
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
        f"root@{topology.core_node}",
        shlex.join(remote),
    )


def _checked(
    runner: Runner,
    command: Sequence[str],
    timeout_seconds: int,
    label: str,
) -> CommandResult:
    try:
        result = runner(command, timeout_seconds)
    except Exception as exc:
        raise R2LabPhysicalUeError(f"{label} could not complete") from exc
    if result.returncode != 0:
        raise R2LabPhysicalUeError(f"{label} returned nonzero")
    return result


def _ue_read(
    *,
    topology: PhysicalTopology,
    slice_name: str,
    runner: Runner,
    remote: Sequence[str],
    timeout_seconds: int,
) -> CommandResult | None:
    try:
        result = runner(
            ue_gateway_command(slice_name, topology.ue_profile, *tuple(remote)),
            timeout_seconds,
        )
    except Exception:
        return None
    return result if result.returncode == 0 else None


def _ue_required(
    *,
    topology: PhysicalTopology,
    slice_name: str,
    runner: Runner,
    remote: Sequence[str],
    timeout_seconds: int,
    label: str,
) -> CommandResult:
    try:
        return _checked(
            runner,
            ue_gateway_command(slice_name, topology.ue_profile, *tuple(remote)),
            timeout_seconds,
            label,
        )
    except R2LabTopologyResourceError as exc:
        raise R2LabPhysicalUeError(str(exc)) from exc


def _serial(command: str) -> tuple[str, ...]:
    if command not in _ALLOWED_AT:
        raise R2LabPhysicalUeError("AT command is outside the sanitized UE probe allow-list")
    return ("python3", "-c", _SERIAL_SCRIPT, command)


def _qmi_pid_path(run_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", run_id):
        raise R2LabPhysicalUeError("physical run ID is unsafe for QMI process ownership")
    return f"/run/synthran/{run_id}.qmi.pid"


def _qmi_log_path(run_id: str) -> str:
    _qmi_pid_path(run_id)
    return f"/run/synthran/{run_id}.qmi.log"


def _qmi_running(
    *, topology: PhysicalTopology, slice_name: str, run_id: str, runner: Runner, timeout_seconds: int
) -> bool:
    pid = _qmi_pid_path(run_id)
    script = (
        f"test -s {shlex.quote(pid)} && "
        f"kill -0 \"$(cat {shlex.quote(pid)})\" 2>/dev/null"
    )
    result = _ue_read(
        topology=topology,
        slice_name=slice_name,
        runner=runner,
        remote=("sh", "-c", script),
        timeout_seconds=timeout_seconds,
    )
    return result is not None


def _start_qmi_manager(
    *, topology: PhysicalTopology, slice_name: str, run_id: str, runner: Runner, timeout_seconds: int
) -> None:
    if topology.ue_profile.mode != "qmi":
        raise R2LabPhysicalUeError("QMI manager requested for a non-QMI UE")
    pid = _qmi_pid_path(run_id)
    log = _qmi_log_path(run_id)
    script = " ; ".join(
        (
            "mkdir -p /run/synthran",
            (
                f"if test -s {shlex.quote(pid)} && "
                f"kill -0 \"$(cat {shlex.quote(pid)})\" 2>/dev/null; "
                "then exit 0; fi"
            ),
            (
                "nohup quectel-CM -4 -s internet -i wwan0 "
                f">{shlex.quote(log)} 2>&1 & echo $! >{shlex.quote(pid)}"
            ),
        )
    )
    _ue_required(
        topology=topology,
        slice_name=slice_name,
        runner=runner,
        remote=("sh", "-c", script),
        timeout_seconds=timeout_seconds,
        label="run-owned QMI manager start",
    )


def _stop_qmi_manager(
    *, topology: PhysicalTopology, slice_name: str, run_id: str, runner: Runner, timeout_seconds: int
) -> bool:
    pid = _qmi_pid_path(run_id)
    log = _qmi_log_path(run_id)
    script = " ; ".join(
        (
            (
                f"if test -s {shlex.quote(pid)}; then p=$(cat {shlex.quote(pid)}); "
                "if kill -0 \"$p\" 2>/dev/null; then kill \"$p\" 2>/dev/null || true; "
                "i=0; while kill -0 \"$p\" 2>/dev/null && test $i -lt 20; "
                "do sleep 0.25; i=$((i+1)); done; "
                "if kill -0 \"$p\" 2>/dev/null; then kill -9 \"$p\" 2>/dev/null || true; fi; fi; fi"
            ),
            f"rm -f {shlex.quote(pid)} {shlex.quote(log)}",
        )
    )
    result = _ue_read(
        topology=topology,
        slice_name=slice_name,
        runner=runner,
        remote=("sh", "-c", script),
        timeout_seconds=timeout_seconds,
    )
    return result is not None and not _qmi_running(
        topology=topology,
        slice_name=slice_name,
        run_id=run_id,
        runner=runner,
        timeout_seconds=timeout_seconds,
    )


def observe_physical_ue_runtime(
    *,
    run_id: str,
    slice_name: str,
    run_root: Path = Path(".synthran/r2lab"),
    runner: Runner = subprocess_runner,
    timeout_seconds: int = 30,
) -> PhysicalUeRuntimeEvidence:
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    profile = topology.ue_profile
    qnwinfo = _ue_read(
        topology=topology,
        slice_name=slice_name,
        runner=runner,
        remote=_serial("AT+QNWINFO"),
        timeout_seconds=timeout_seconds,
    )
    registration = _ue_read(
        topology=topology,
        slice_name=slice_name,
        runner=runner,
        remote=_serial("AT+C5GREG?"),
        timeout_seconds=timeout_seconds,
    )
    link = _ue_read(
        topology=topology,
        slice_name=slice_name,
        runner=runner,
        remote=("ip", "-o", "link", "show", "dev", UE_INTERFACE),
        timeout_seconds=timeout_seconds,
    )
    address = None
    if link is not None:
        address = _ue_read(
            topology=topology,
            slice_name=slice_name,
            runner=runner,
            remote=("ip", "-o", "-4", "addr", "show", "dev", UE_INTERFACE),
            timeout_seconds=timeout_seconds,
        )
    ipv4 = parse_ipv4_state(
        address.stdout if address is not None else "",
        interface_present=link is not None,
    )
    manager_running = True
    packet = PacketServiceState.UNKNOWN
    if profile.mode == "mbim":
        packet_result = _ue_read(
            topology=topology,
            slice_name=slice_name,
            runner=runner,
            remote=("mbimcli", "-p", "-d", MBIM_DEVICE, "--query-packet-service-state"),
            timeout_seconds=timeout_seconds,
        )
        packet = parse_packet_service(packet_result.stdout if packet_result is not None else "")
        manager_running = packet_result is not None
    elif profile.mode == "qmi":
        manager_running = _qmi_running(
            topology=topology,
            slice_name=slice_name,
            run_id=run_id,
            runner=runner,
            timeout_seconds=timeout_seconds,
        )
        # A current IPv4 address on wwan0 plus 5G registration is the PDU
        # postcondition.  The manager is recorded separately because an already
        # established QMI session may have been created before this invocation.
        packet = (
            PacketServiceState.ATTACHED
            if ipv4 is Ipv4State.PRESENT
            else PacketServiceState.DETACHED
        )
    else:
        raise R2LabPhysicalUeError("selected UE mode is not supported by the canonical path")
    transport_error = any(item is None for item in (qnwinfo, registration, link))
    return PhysicalUeRuntimeEvidence(
        ue=topology.ue,
        mode=profile.mode,
        interface=UE_INTERFACE,
        cell=parse_qnwinfo(qnwinfo.stdout if qnwinfo is not None else ""),
        registration=parse_c5greg(registration.stdout if registration is not None else ""),
        packet_service=packet,
        ipv4=ipv4,
        manager_running=manager_running,
        transport_error=transport_error,
    )


def _load_expected_peer(run_root: Path, run_id: str) -> str:
    path = run_root.expanduser().resolve() / run_id / "physical" / "n3xx-artifact.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2LabPhysicalUeError("N3xx artifact metadata is unavailable") from exc
    peer = payload.get("expected_gnb_peer") if isinstance(payload, dict) else None
    try:
        address = ipaddress.ip_address(peer) if isinstance(peer, str) else None
    except ValueError as exc:
        raise R2LabPhysicalUeError("stored expected gNB N2 peer is malformed") from exc
    if not isinstance(address, ipaddress.IPv4Address):
        raise R2LabPhysicalUeError("stored expected gNB N2 peer is missing")
    return str(address)


def verify_current_n3xx_n2(
    *,
    run_id: str,
    known_hosts: Path,
    run_root: Path = Path(".synthran/r2lab"),
    runner: Runner = subprocess_runner,
    timeout_seconds: int = 30,
) -> bool:
    """Fresh current-state proof used immediately before UE mutation/traffic."""

    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabPhysicalUeError("strict SLICES known-hosts file is missing")
    namespace = _checked(
        runner,
        _cluster_ssh(
            topology,
            known_hosts,
            "kubectl",
            "get",
            "namespace",
            NAMESPACE,
            "-o",
            "jsonpath={.metadata.labels.synthran\\.run/id}",
        ),
        timeout_seconds,
        "current Open5GS ownership query",
    ).stdout.strip()
    if namespace != run_id:
        return False
    deployment_result = _checked(
        runner,
        _cluster_ssh(
            topology,
            known_hosts,
            "kubectl",
            "get",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            "-o",
            "json",
        ),
        timeout_seconds,
        "current physical gNB query",
    )
    try:
        deployment = json.loads(deployment_result.stdout)
    except json.JSONDecodeError:
        return False
    metadata = deployment.get("metadata") if isinstance(deployment, dict) else None
    spec = deployment.get("spec") if isinstance(deployment, dict) else None
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
    if (
        not isinstance(labels, dict)
        or not isinstance(annotations, dict)
        or not isinstance(spec, dict)
        or labels.get(RUN_LABEL) != run_id
        or annotations.get(RUN_ANNOTATION) != run_id
        or spec.get("replicas") != 1
    ):
        return False
    pods_result = _checked(
        runner,
        _cluster_ssh(
            topology,
            known_hosts,
            "kubectl",
            "get",
            "pods",
            "-n",
            NAMESPACE,
            "-l",
            GNB_SELECTOR,
            "-o",
            "json",
        ),
        timeout_seconds,
        "current physical gNB pod query",
    )
    try:
        pods = json.loads(pods_result.stdout)
    except json.JSONDecodeError:
        return False
    items = pods.get("items") if isinstance(pods, dict) else None
    if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict):
        return False
    pod = items[0]
    pod_metadata = pod.get("metadata")
    pod_status = pod.get("status")
    name = pod_metadata.get("name") if isinstance(pod_metadata, dict) else None
    statuses = pod_status.get("containerStatuses") if isinstance(pod_status, dict) else None
    gnb = next(
        (item for item in statuses if isinstance(item, dict) and item.get("name") == "gnb"),
        None,
    ) if isinstance(statuses, list) else None
    if (
        not isinstance(name, str)
        or not _SAFE_POD.fullmatch(name)
        or not isinstance(pod_status, dict)
        or pod_status.get("phase") != "Running"
        or not isinstance(gnb, dict)
        or gnb.get("ready") is not True
    ):
        return False
    logs = _checked(
        runner,
        _cluster_ssh(
            topology,
            known_hosts,
            "kubectl",
            "logs",
            f"pod/{name}",
            "-n",
            NAMESPACE,
            "-c",
            "gnb",
            "--tail=400",
        ),
        timeout_seconds,
        "current physical gNB log query",
    )
    if parse_n2_log_state(logs.stdout) is N2State.ESTABLISHED:
        return True
    expected_peer = _load_expected_peer(run_root, run_id)
    amf_pods = _checked(
        runner,
        _cluster_ssh(
            topology,
            known_hosts,
            "kubectl",
            "get",
            "pods",
            "-n",
            NAMESPACE,
            "-l",
            "nf=amf",
            "-o",
            "json",
        ),
        timeout_seconds,
        "current AMF pod query",
    )
    try:
        amf_payload = json.loads(amf_pods.stdout)
    except json.JSONDecodeError:
        return False
    amf_items = amf_payload.get("items") if isinstance(amf_payload, dict) else None
    if not isinstance(amf_items, list) or len(amf_items) != 1 or not isinstance(amf_items[0], dict):
        return False
    amf_name = amf_items[0].get("metadata", {}).get("name")
    if not isinstance(amf_name, str) or not _SAFE_POD.fullmatch(amf_name):
        return False
    amf_logs = _checked(
        runner,
        _cluster_ssh(
            topology,
            known_hosts,
            "kubectl",
            "logs",
            f"pod/{amf_name}",
            "-n",
            NAMESPACE,
            "--tail=400",
        ),
        timeout_seconds,
        "current AMF N2 log query",
    )
    return build_amf_n2_evidence(text=amf_logs.stdout, expected_peer=expected_peer).proven


def _refresh_boundary(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    run_root: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    timeout_seconds: int,
) -> PhysicalTopology:
    authority = verify_physical_authority(
        run_id=run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    topology = authority.topology
    verify_selected_allocation(
        topology=topology,
        runner=cluster_runner,
        owner=owner,
        allocation_id=allocation_id,
        timeout_seconds=min(timeout_seconds, 300),
    )
    if not verify_current_n3xx_n2(
        run_id=run_id,
        known_hosts=known_hosts,
        run_root=run_root,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 60),
    ):
        raise R2LabPhysicalUeError("current singleton gNB/N2 path is not proven")
    return topology


def _record_runtime(
    evidence: PhysicalRunEvidence,
    runtime: PhysicalUeRuntimeEvidence,
    *,
    source: str,
) -> PhysicalRunEvidence:
    state = evidence
    if state.acceptance.next_stage is PhysicalAcceptanceStage.CELL_ACQUISITION:
        if not runtime.cell_acquired:
            return state.fail_stage(
                PhysicalAcceptanceStage.CELL_ACQUISITION,
                source=f"{source}:cell-{runtime.cell.value}",
            )
        state = state.pass_stage(
            PhysicalAcceptanceStage.CELL_ACQUISITION,
            source=f"{source}:cell-{runtime.cell.value}",
        )
    if state.acceptance.next_stage is PhysicalAcceptanceStage.REGISTRATION:
        if not runtime.registered:
            return state.fail_stage(
                PhysicalAcceptanceStage.REGISTRATION,
                source=f"{source}:registration-{runtime.registration.value}",
            )
        state = state.pass_stage(
            PhysicalAcceptanceStage.REGISTRATION,
            source=f"{source}:registration-{runtime.registration.value}",
        )
    if state.acceptance.next_stage is PhysicalAcceptanceStage.PDU_SESSION:
        detail = f"{source}:{runtime.mode}:ipv4-{runtime.ipv4.value}:packet-{runtime.packet_service.value}"
        if not runtime.pdu_session_established:
            return state.fail_stage(PhysicalAcceptanceStage.PDU_SESSION, source=detail)
        state = state.pass_stage(PhysicalAcceptanceStage.PDU_SESSION, source=detail)
    return state


def _mbim_activate(
    *, topology: PhysicalTopology, slice_name: str, runner: Runner, timeout_seconds: int
) -> None:
    for command, label in (
        (("command", "-v", "mbim-set-ip.sh"), "MBIM IP helper probe"),
        (("mbimcli", "-p", "-d", MBIM_DEVICE, "--set-radio-state=on"), "MBIM radio enable"),
        (("mbimcli", "-p", "-d", MBIM_DEVICE, "--attach-packet-service"), "MBIM packet attach"),
        (("mbimcli", "-p", "-d", MBIM_DEVICE, "--connect=session-id=0,apn=internet,ip-type=ipv4"), "MBIM PDU connect"),
        (("mbim-set-ip.sh", MBIM_DEVICE, UE_INTERFACE, "0"), "MBIM IPv4 configuration"),
    ):
        _ue_required(
            topology=topology,
            slice_name=slice_name,
            runner=runner,
            remote=command,
            timeout_seconds=timeout_seconds,
            label=label,
        )


def _mbim_rollback(
    *, topology: PhysicalTopology, slice_name: str, runner: Runner, timeout_seconds: int
) -> None:
    for command in (
        ("mbimcli", "-p", "-d", MBIM_DEVICE, "--disconnect=session-id=0"),
        ("mbimcli", "-p", "-d", MBIM_DEVICE, "--detach-packet-service"),
        ("ip", "addr", "flush", "dev", UE_INTERFACE),
    ):
        _ue_read(
            topology=topology,
            slice_name=slice_name,
            runner=runner,
            remote=command,
            timeout_seconds=timeout_seconds,
        )


def activate_physical_ue(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    run_root: Path = Path(".synthran/r2lab"),
    r2lab_runner: Runner = subprocess_runner,
    cluster_runner: Runner = subprocess_runner,
    evidence_path: Path | None = None,
    activation_evidence_path: Path | None = None,
    sleeper: Sleeper = time.sleep,
    timeout_seconds: int = 30,
) -> tuple[PhysicalRunEvidence, PhysicalUeActivationSummary]:
    """Advance UE-management/cell/registration/PDU under fresh selected authority."""

    if evidence.gnb_start is None:
        raise R2LabPhysicalUeError("physical UE activation requires a started gNB")
    if evidence.acceptance.next_stage not in {
        PhysicalAcceptanceStage.UE_MANAGEMENT,
        PhysicalAcceptanceStage.CELL_ACQUISITION,
        PhysicalAcceptanceStage.REGISTRATION,
        PhysicalAcceptanceStage.PDU_SESSION,
    }:
        raise R2LabPhysicalUeError("physical UE activation is not the next lifecycle boundary")
    state = evidence
    topology = _refresh_boundary(
        run_id=state.run_id,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        run_root=run_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        timeout_seconds=timeout_seconds,
    )
    if state.acceptance.next_stage is PhysicalAcceptanceStage.UE_MANAGEMENT:
        state = state.pass_stage(
            PhysicalAcceptanceStage.UE_MANAGEMENT,
            source=f"current-management:{topology.ue}:{topology.ue_profile.mode}",
        )
        if evidence_path is not None:
            state.write_json(evidence_path)

    before = observe_physical_ue_runtime(
        run_id=state.run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 60),
    )
    if before.pdu_session_established:
        state = _record_runtime(state, before, source="preexisting-current-ue")
        if evidence_path is not None:
            state.write_json(evidence_path)
        summary = PhysicalUeActivationSummary(
            run_id=state.run_id,
            ue=topology.ue,
            mode=topology.ue_profile.mode,
            status="already-ready",
            runtime=before,
            evidence_path=evidence_path or Path("physical-run.json"),
        )
        if activation_evidence_path is not None:
            _write_json(activation_evidence_path, summary.to_dict())
        return state, summary

    # Refresh again immediately before mutation.
    _refresh_boundary(
        run_id=state.run_id,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        run_root=run_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        timeout_seconds=timeout_seconds,
    )
    started_qmi = False
    try:
        if topology.ue_profile.mode == "mbim":
            _mbim_activate(
                topology=topology,
                slice_name=slice_name,
                runner=r2lab_runner,
                timeout_seconds=max(timeout_seconds, 60),
            )
        elif topology.ue_profile.mode == "qmi":
            _start_qmi_manager(
                topology=topology,
                slice_name=slice_name,
                run_id=state.run_id,
                runner=r2lab_runner,
                timeout_seconds=max(timeout_seconds, 60),
            )
            started_qmi = True
        else:
            raise R2LabPhysicalUeError("selected UE transport mode is unsupported")

        runtime = before
        for attempt in range(60):
            runtime = observe_physical_ue_runtime(
                run_id=state.run_id,
                slice_name=slice_name,
                run_root=run_root,
                runner=r2lab_runner,
                timeout_seconds=min(timeout_seconds, 60),
            )
            if runtime.pdu_session_established:
                break
            if attempt < 59:
                sleeper(2.0)
        state = _record_runtime(state, runtime, source="physical-ue-activation")
        if evidence_path is not None:
            state.write_json(evidence_path)
        status = "activated" if runtime.pdu_session_established else "not-proven"
        summary = PhysicalUeActivationSummary(
            run_id=state.run_id,
            ue=topology.ue,
            mode=topology.ue_profile.mode,
            status=status,
            runtime=runtime,
            evidence_path=evidence_path or Path("physical-run.json"),
        )
        if activation_evidence_path is not None:
            _write_json(activation_evidence_path, summary.to_dict())
        return state, summary
    except Exception:
        if topology.ue_profile.mode == "mbim":
            _mbim_rollback(
                topology=topology,
                slice_name=slice_name,
                runner=r2lab_runner,
                timeout_seconds=min(timeout_seconds, 60),
            )
        elif started_qmi:
            _stop_qmi_manager(
                topology=topology,
                slice_name=slice_name,
                run_id=state.run_id,
                runner=r2lab_runner,
                timeout_seconds=min(timeout_seconds, 60),
            )
        raise


def prove_physical_user_plane(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    peer: str,
    run_root: Path = Path(".synthran/r2lab"),
    r2lab_runner: Runner = subprocess_runner,
    cluster_runner: Runner = subprocess_runner,
    evidence_path: Path | None = None,
    timeout_seconds: int = 30,
) -> PhysicalUeUserPlaneSummary:
    if evidence.acceptance.next_stage is not PhysicalAcceptanceStage.USER_PLANE:
        raise R2LabPhysicalUeError("physical user-plane probe is not the next lifecycle boundary")
    topology = _refresh_boundary(
        run_id=evidence.run_id,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        run_root=run_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        timeout_seconds=timeout_seconds,
    )

    def ue_runner(command: Sequence[str], command_timeout: int) -> CommandResult:
        return r2lab_runner(
            ue_gateway_command(slice_name, topology.ue_profile, *tuple(command)),
            command_timeout,
        )

    probe = execute_user_plane_probe(
        peer=peer,
        runner=ue_runner,
        interface=UE_INTERFACE,
        command_timeout_seconds=min(timeout_seconds, 60),
    )
    state = (
        evidence.pass_stage(
            PhysicalAcceptanceStage.USER_PLANE,
            source=f"current-{topology.ue}:wwan0:{probe.received_packets}-of-{probe.requested_packets}",
        )
        if probe.proven
        else evidence.fail_stage(
            PhysicalAcceptanceStage.USER_PLANE,
            source=f"current-{topology.ue}:wwan0:probe-failed",
        )
    )
    if evidence_path is not None:
        state.write_json(evidence_path)
    return PhysicalUeUserPlaneSummary(evidence=state, probe=probe)


def execute_physical_workload_handoff(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    run_root: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    executor: PhysicalWorkloadExecutor,
    evidence_path: Path | None = None,
    workload_evidence_path: Path | None = None,
    timeout_seconds: int = 30,
) -> tuple[PhysicalRunEvidence, PhysicalWorkloadResult | None]:
    """Run one deterministic physical workload after refreshing the entire path."""

    if evidence.acceptance.next_stage is not PhysicalAcceptanceStage.WORKLOAD:
        raise R2LabPhysicalUeError("physical workload is not the next lifecycle boundary")
    topology = _refresh_boundary(
        run_id=evidence.run_id,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        run_root=run_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        timeout_seconds=timeout_seconds,
    )
    context = PhysicalWorkloadContext(
        run_id=evidence.run_id,
        ue=topology.ue,
        interface=UE_INTERFACE,
    )
    result = executor(context)
    if result.run_id != evidence.run_id:
        raise R2LabPhysicalUeError("physical workload result belongs to another physical run")
    accepted = result.accepted and result.cleanup_proven
    source = (
        f"physical-iot:{result.workload_id}:accepted"
        if accepted
        else f"physical-iot:{result.workload_id}:not-proven"
    )
    state = (
        evidence.pass_stage(PhysicalAcceptanceStage.WORKLOAD, source=source)
        if accepted
        else evidence.fail_stage(PhysicalAcceptanceStage.WORKLOAD, source=source)
    )
    if evidence_path is not None:
        state.write_json(evidence_path)
    if workload_evidence_path is not None:
        _write_json(workload_evidence_path, result.to_dict())
    return state, result
