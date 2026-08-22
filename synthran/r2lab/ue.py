"""Mutating qfit UE/PDU lifecycle and physical-workload handoff for R2Lab.

The read-only observation path lives in :mod:`synthran.r2lab.runtime`.  This
module owns the complementary mutating boundary after a run-bound singleton gNB
has been proven: controlled MBIM activation, fail-closed rollback, user-plane
proof, and the explicit handoff to a physical-only workload executor.

It deliberately does not call the upstream ``prepare-ue``, ``config-ue``,
``check-ue``, ``start.sh``, or ``stop.sh`` wrappers.  The exact reviewed MBIM
operations are executed one at a time and accepted from independently observed
postconditions rather than process return codes.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import json
from pathlib import Path
import re
import shlex
import tempfile
import time
from typing import Callable, Mapping, Sequence

from synthran.live_preflight import CommandResult
from synthran.network.runtime import validate_run_id
from synthran.r2lab.acceptance import (
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
)
from synthran.r2lab.deployment import PhysicalStartAuthority
from synthran.r2lab.radio import (
    CellAcquisitionState,
    Ipv4State,
    PacketServiceState,
    QfitRuntimeEvidence,
    RegistrationState,
    UserPlaneProbeEvidence,
    execute_user_plane_probe,
)


Runner = Callable[[Sequence[str], int], CommandResult]
Sleeper = Callable[[float], None]
RuntimeObserver = Callable[[], QfitRuntimeEvidence]
WorkloadExecutor = Callable[["PhysicalWorkloadContext"], "PhysicalWorkloadResult"]

_QFIT_DEVICE = "/dev/cdc-wdm0"
_QFIT_INTERFACE = "wwan0"
_QFIT_DNN = "internet"
_QFIT_SESSION_ID = 0
_QFIT_IP_TYPE = "ipv4"
_SAFE_QFIT_RE = re.compile(r"^qfit(?:07|09|18|29|32|34)$")
_SAFE_WORKLOAD_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RADIO_RE = re.compile(
    r"software\s+radio\s+state\s*:\s*['\"]?(on|off)['\"]?",
    re.IGNORECASE,
)


class R2LabQfitActivationError(RuntimeError):
    """Raised when the controlled physical qfit path cannot proceed safely."""


class SoftwareRadioState(str, Enum):
    ON = "on"
    OFF = "off"
    UNKNOWN = "unknown"


def parse_mbim_radio_state(text: str) -> SoftwareRadioState:
    """Reduce MBIM radio-state output to one conservative software state."""

    states = {match.group(1).lower() for match in _RADIO_RE.finditer(text)}
    if len(states) != 1:
        return SoftwareRadioState.UNKNOWN
    return SoftwareRadioState.ON if "on" in states else SoftwareRadioState.OFF


def _validate_qfit(value: str) -> str:
    qfit = value.strip().lower()
    if not _SAFE_QFIT_RE.fullmatch(qfit):
        raise R2LabQfitActivationError("qfit activation requires one reviewed qfit UE")
    return qfit


def _validate_digest(value: str, label: str) -> str:
    if not _SHA256_RE.fullmatch(value):
        raise R2LabQfitActivationError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _unknown_runtime() -> QfitRuntimeEvidence:
    return QfitRuntimeEvidence(
        cell=CellAcquisitionState.UNKNOWN,
        registration=RegistrationState.UNKNOWN,
        packet_service=PacketServiceState.UNKNOWN,
        ipv4=Ipv4State.UNKNOWN,
    )


@dataclass(frozen=True)
class MutationStepEvidence:
    name: str
    returncode: int | None
    transport_error: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "returncode": self.returncode,
            "transport_error": self.transport_error,
        }


@dataclass(frozen=True)
class QfitActivationRequest:
    run_id: str
    qfit: str
    dnn: str = _QFIT_DNN
    interface: str = _QFIT_INTERFACE
    device: str = _QFIT_DEVICE
    session_id: int = _QFIT_SESSION_ID
    ip_type: str = _QFIT_IP_TYPE

    def validate(self) -> "QfitActivationRequest":
        try:
            validated = validate_run_id(self.run_id)
        except Exception as exc:
            raise R2LabQfitActivationError(str(exc)) from exc
        if validated != self.run_id:
            raise R2LabQfitActivationError("qfit activation run ID is not canonical")
        _validate_qfit(self.qfit)
        if self.dnn != _QFIT_DNN:
            raise R2LabQfitActivationError(
                "current physical checkpoint requires the reviewed internet DNN"
            )
        if self.interface != _QFIT_INTERFACE:
            raise R2LabQfitActivationError("current physical checkpoint requires wwan0")
        if self.device != _QFIT_DEVICE:
            raise R2LabQfitActivationError(
                "current physical checkpoint requires /dev/cdc-wdm0"
            )
        if self.session_id != _QFIT_SESSION_ID:
            raise R2LabQfitActivationError("current physical checkpoint requires MBIM session 0")
        if self.ip_type != _QFIT_IP_TYPE:
            raise R2LabQfitActivationError("current physical checkpoint is IPv4-only")
        return self


@dataclass(frozen=True)
class QfitActivationResult:
    run_id: str
    qfit: str
    dnn: str
    interface: str
    device: str
    session_id: int
    status: str
    initial_runtime: QfitRuntimeEvidence
    final_runtime: QfitRuntimeEvidence
    final_radio_state: SoftwareRadioState
    rollback_proven: bool
    steps: tuple[MutationStepEvidence, ...]

    @property
    def accepted(self) -> bool:
        return (
            self.status in {"already-established", "pdu-established"}
            and self.final_runtime.pdu_session_established
        )

    @property
    def hardware_mutation(self) -> bool:
        return bool(self.steps)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-qfit-activation/v1alpha1",
            "run_id": self.run_id,
            "qfit": self.qfit,
            "dnn": self.dnn,
            "interface": self.interface,
            "device": self.device,
            "session_id": self.session_id,
            "status": self.status,
            "accepted": self.accepted,
            "hardware_mutation": self.hardware_mutation,
            "initial_runtime": self.initial_runtime.to_dict(),
            "final_runtime": self.final_runtime.to_dict(),
            "final_radio_state": self.final_radio_state.value,
            "rollback_proven": self.rollback_proven,
            "steps": [step.to_dict() for step in self.steps],
            "raw_modem_output_persisted": False,
            "subscriber_identity_queried": False,
        }

    def write_json(self, path: Path) -> Path:
        path = path.expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n"
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                dir=path.parent,
                delete=False,
            ) as temporary:
                temporary.write(content)
                temporary_path = Path(temporary.name)
            temporary_path.replace(path)
        except OSError as exc:
            raise R2LabQfitActivationError("qfit activation evidence could not be persisted") from exc
        return path


def qfit_activation_commands(
    request: QfitActivationRequest,
) -> Mapping[str, tuple[str, ...]]:
    """Return the fixed reviewed mutation set for audit/tests.

    These are direct commands for the selected qfit host.  They contain no
    generic shell, subscriber query, modem reset, APN reconfiguration helper, or
    automatic retry that changes hardware selection.
    """

    request.validate()
    return {
        "link-up": ("ip", "link", "set", "dev", request.interface, "up"),
        "radio-on": (
            "mbimcli",
            "-p",
            "-d",
            request.device,
            "--set-radio-state=on",
        ),
        "radio-query": (
            "mbimcli",
            "-p",
            "-d",
            request.device,
            "--query-radio-state",
        ),
        "attach": (
            "mbimcli",
            "-p",
            "-d",
            request.device,
            "--attach-packet-service",
        ),
        "connect": (
            "mbimcli",
            "-p",
            "-d",
            request.device,
            (
                f"--connect=session-id={request.session_id},"
                f"apn={request.dnn},ip-type={request.ip_type}"
            ),
        ),
        "set-ip": (
            "mbim-set-ip.sh",
            request.device,
            request.interface,
            str(request.session_id),
        ),
        "radio-off": (
            "mbimcli",
            "-p",
            "-d",
            request.device,
            "--set-radio-state=off",
        ),
        "link-down": ("ip", "link", "set", "dev", request.interface, "down"),
    }


def _run_mutation(
    *,
    name: str,
    command: Sequence[str],
    runner: Runner,
    timeout_seconds: int,
) -> MutationStepEvidence:
    try:
        result = runner(tuple(command), timeout_seconds)
    except (RuntimeError, OSError):
        return MutationStepEvidence(name=name, returncode=None, transport_error=True)
    return MutationStepEvidence(
        name=name,
        returncode=result.returncode,
        transport_error=False,
    )


def _observe_runtime(observer: RuntimeObserver) -> QfitRuntimeEvidence:
    try:
        return observer()
    except (RuntimeError, OSError, ValueError):
        return _unknown_runtime()


def _query_radio_state(
    *,
    command: Sequence[str],
    runner: Runner,
    timeout_seconds: int,
) -> SoftwareRadioState:
    try:
        result = runner(tuple(command), timeout_seconds)
    except (RuntimeError, OSError):
        return SoftwareRadioState.UNKNOWN
    text = "\n".join(part for part in (result.stdout, result.stderr) if part)
    return parse_mbim_radio_state(text)


def _wait_radio_state(
    *,
    expected: SoftwareRadioState,
    command: Sequence[str],
    runner: Runner,
    sleeper: Sleeper,
    timeout_seconds: int,
    attempts: int,
    poll_interval_seconds: float,
) -> SoftwareRadioState:
    observed = SoftwareRadioState.UNKNOWN
    for attempt in range(attempts):
        observed = _query_radio_state(
            command=command,
            runner=runner,
            timeout_seconds=timeout_seconds,
        )
        if observed is expected:
            return observed
        if attempt + 1 < attempts:
            sleeper(poll_interval_seconds)
    return observed


def _wait_runtime(
    *,
    observer: RuntimeObserver,
    predicate: Callable[[QfitRuntimeEvidence], bool],
    sleeper: Sleeper,
    attempts: int,
    poll_interval_seconds: float,
) -> QfitRuntimeEvidence:
    observed = _unknown_runtime()
    for attempt in range(attempts):
        observed = _observe_runtime(observer)
        if predicate(observed):
            return observed
        if attempt + 1 < attempts:
            sleeper(poll_interval_seconds)
    return observed


def _rollback_activation(
    *,
    commands: Mapping[str, tuple[str, ...]],
    runner: Runner,
    observer: RuntimeObserver,
    sleeper: Sleeper,
    timeout_seconds: int,
    attempts: int,
    poll_interval_seconds: float,
    steps: list[MutationStepEvidence],
) -> tuple[bool, QfitRuntimeEvidence, SoftwareRadioState]:
    steps.append(
        _run_mutation(
            name="rollback-radio-off",
            command=commands["radio-off"],
            runner=runner,
            timeout_seconds=timeout_seconds,
        )
    )
    steps.append(
        _run_mutation(
            name="rollback-link-down",
            command=commands["link-down"],
            runner=runner,
            timeout_seconds=timeout_seconds,
        )
    )
    final_runtime = _unknown_runtime()
    final_radio = SoftwareRadioState.UNKNOWN
    for attempt in range(attempts):
        final_radio = _query_radio_state(
            command=commands["radio-query"],
            runner=runner,
            timeout_seconds=timeout_seconds,
        )
        final_runtime = _observe_runtime(observer)
        clean = (
            final_radio is SoftwareRadioState.OFF
            and final_runtime.packet_service is PacketServiceState.DETACHED
            and final_runtime.ipv4 is Ipv4State.ABSENT
        )
        if clean:
            return True, final_runtime, final_radio
        if attempt + 1 < attempts:
            sleeper(poll_interval_seconds)
    return False, final_runtime, final_radio


def execute_qfit_activation(
    *,
    request: QfitActivationRequest,
    runner: Runner,
    observer: RuntimeObserver,
    sleeper: Sleeper = time.sleep,
    timeout_seconds: int = 30,
    registration_attempts: int = 12,
    packet_attempts: int = 8,
    pdu_attempts: int = 8,
    rollback_attempts: int = 6,
    poll_interval_seconds: float = 2.0,
) -> QfitActivationResult:
    """Establish one MBIM PDU session from independently verified postconditions.

    A non-zero mutation return code is diagnostic, not state truth.  After radio
    enable and packet attach, the code checks current modem state before moving
    forward.  Any unresolved failure requests the exact reviewed rollback
    (software radio off + wwan0 down) and records whether that cleanup could be
    proven.
    """

    request.validate()
    if timeout_seconds < 5 or timeout_seconds > 120:
        raise R2LabQfitActivationError("qfit activation timeout must be between 5 and 120 seconds")
    if min(registration_attempts, packet_attempts, pdu_attempts, rollback_attempts) < 1:
        raise R2LabQfitActivationError("qfit activation poll attempts must be positive")
    if poll_interval_seconds < 0 or poll_interval_seconds > 30:
        raise R2LabQfitActivationError("qfit activation poll interval is out of range")

    commands = qfit_activation_commands(request)
    initial = _observe_runtime(observer)
    if initial.pdu_session_established:
        radio = _query_radio_state(
            command=commands["radio-query"],
            runner=runner,
            timeout_seconds=timeout_seconds,
        )
        return QfitActivationResult(
            run_id=request.run_id,
            qfit=request.qfit,
            dnn=request.dnn,
            interface=request.interface,
            device=request.device,
            session_id=request.session_id,
            status="already-established",
            initial_runtime=initial,
            final_runtime=initial,
            final_radio_state=radio,
            rollback_proven=False,
            steps=(),
        )

    steps: list[MutationStepEvidence] = []
    steps.append(
        _run_mutation(
            name="link-up",
            command=commands["link-up"],
            runner=runner,
            timeout_seconds=timeout_seconds,
        )
    )
    steps.append(
        _run_mutation(
            name="radio-on",
            command=commands["radio-on"],
            runner=runner,
            timeout_seconds=timeout_seconds,
        )
    )
    radio = _wait_radio_state(
        expected=SoftwareRadioState.ON,
        command=commands["radio-query"],
        runner=runner,
        sleeper=sleeper,
        timeout_seconds=timeout_seconds,
        attempts=registration_attempts,
        poll_interval_seconds=poll_interval_seconds,
    )
    registered = _wait_runtime(
        observer=observer,
        predicate=lambda state: state.cell_acquired and state.registered,
        sleeper=sleeper,
        attempts=registration_attempts,
        poll_interval_seconds=poll_interval_seconds,
    )
    if radio is not SoftwareRadioState.ON or not registered.registered:
        clean, final_runtime, final_radio = _rollback_activation(
            commands=commands,
            runner=runner,
            observer=observer,
            sleeper=sleeper,
            timeout_seconds=timeout_seconds,
            attempts=rollback_attempts,
            poll_interval_seconds=poll_interval_seconds,
            steps=steps,
        )
        return QfitActivationResult(
            run_id=request.run_id,
            qfit=request.qfit,
            dnn=request.dnn,
            interface=request.interface,
            device=request.device,
            session_id=request.session_id,
            status="failed-clean" if clean else "failed-unresolved",
            initial_runtime=initial,
            final_runtime=final_runtime,
            final_radio_state=final_radio,
            rollback_proven=clean,
            steps=tuple(steps),
        )

    steps.append(
        _run_mutation(
            name="attach-packet-service",
            command=commands["attach"],
            runner=runner,
            timeout_seconds=timeout_seconds,
        )
    )
    attached = _wait_runtime(
        observer=observer,
        predicate=lambda state: (
            state.registered and state.packet_service is PacketServiceState.ATTACHED
        ),
        sleeper=sleeper,
        attempts=packet_attempts,
        poll_interval_seconds=poll_interval_seconds,
    )
    if attached.packet_service is not PacketServiceState.ATTACHED:
        clean, final_runtime, final_radio = _rollback_activation(
            commands=commands,
            runner=runner,
            observer=observer,
            sleeper=sleeper,
            timeout_seconds=timeout_seconds,
            attempts=rollback_attempts,
            poll_interval_seconds=poll_interval_seconds,
            steps=steps,
        )
        return QfitActivationResult(
            run_id=request.run_id,
            qfit=request.qfit,
            dnn=request.dnn,
            interface=request.interface,
            device=request.device,
            session_id=request.session_id,
            status="failed-clean" if clean else "failed-unresolved",
            initial_runtime=initial,
            final_runtime=final_runtime,
            final_radio_state=final_radio,
            rollback_proven=clean,
            steps=tuple(steps),
        )

    steps.append(
        _run_mutation(
            name="connect-session",
            command=commands["connect"],
            runner=runner,
            timeout_seconds=timeout_seconds,
        )
    )
    steps.append(
        _run_mutation(
            name="configure-ip",
            command=commands["set-ip"],
            runner=runner,
            timeout_seconds=timeout_seconds,
        )
    )
    final_runtime = _wait_runtime(
        observer=observer,
        predicate=lambda state: state.pdu_session_established,
        sleeper=sleeper,
        attempts=pdu_attempts,
        poll_interval_seconds=poll_interval_seconds,
    )
    final_radio = _query_radio_state(
        command=commands["radio-query"],
        runner=runner,
        timeout_seconds=timeout_seconds,
    )
    if final_runtime.pdu_session_established:
        return QfitActivationResult(
            run_id=request.run_id,
            qfit=request.qfit,
            dnn=request.dnn,
            interface=request.interface,
            device=request.device,
            session_id=request.session_id,
            status="pdu-established",
            initial_runtime=initial,
            final_runtime=final_runtime,
            final_radio_state=final_radio,
            rollback_proven=False,
            steps=tuple(steps),
        )

    clean, rollback_runtime, rollback_radio = _rollback_activation(
        commands=commands,
        runner=runner,
        observer=observer,
        sleeper=sleeper,
        timeout_seconds=timeout_seconds,
        attempts=rollback_attempts,
        poll_interval_seconds=poll_interval_seconds,
        steps=steps,
    )
    return QfitActivationResult(
        run_id=request.run_id,
        qfit=request.qfit,
        dnn=request.dnn,
        interface=request.interface,
        device=request.device,
        session_id=request.session_id,
        status="failed-clean" if clean else "failed-unresolved",
        initial_runtime=initial,
        final_runtime=rollback_runtime,
        final_radio_state=rollback_radio,
        rollback_proven=clean,
        steps=tuple(steps),
    )


def _qfit_gateway_command(slice_name: str, qfit: str, *remote: str) -> tuple[str, ...]:
    """Reuse strict Faraday SSH, then fail-closed SSH to one reviewed qfit."""

    from synthran.r2lab.controller import gateway_command

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
    return gateway_command(slice_name, shlex.join(inner))


def _same_authority(authority: PhysicalStartAuthority, evidence: PhysicalRunEvidence) -> bool:
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


@dataclass(frozen=True)
class AuthorizedQfitActivationOutcome:
    evidence: PhysicalRunEvidence
    activation: QfitActivationResult | None
    pre_runtime: QfitRuntimeEvidence | None


def execute_authorized_qfit_activation(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    run_root: Path,
    known_hosts: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    evidence_path: Path | None = None,
    activation_evidence_path: Path | None = None,
    sleeper: Sleeper = time.sleep,
    timeout_seconds: int = 30,
) -> AuthorizedQfitActivationOutcome:
    """Advance gNB -> qfit registration -> PDU with fresh authority at mutation time."""

    from synthran.r2lab.controller import authorize_physical_start
    from synthran.r2lab.runtime import (
        execute_qfit_management_probe,
        execute_qfit_runtime_probe,
        verify_gnb_n2,
    )

    if evidence.gnb_start is None:
        raise R2LabQfitActivationError("qfit activation requires bound gNB start evidence")
    state = evidence

    authority = authorize_physical_start(
        run_id=state.run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=timeout_seconds,
    ).validate()
    if not _same_authority(authority, state):
        raise R2LabQfitActivationError("R2Lab claim or selected-resource authority changed")

    if state.acceptance.next_stage is PhysicalAcceptanceStage.GNB_N2:
        gnb = verify_gnb_n2(
            evidence=state,
            known_hosts=known_hosts,
            runner=cluster_runner,
            timeout_seconds=min(timeout_seconds, 60),
        )
        source = (
            "qfit-preactivation-gnb:"
            f"bound-{int(gnb.deployment_bound)}:one-{int(gnb.singleton_ready)}:"
            f"n2-{gnb.n2_state.value}"
        )
        if not gnb.proven:
            state = state.fail_stage(PhysicalAcceptanceStage.GNB_N2, source=source)
            _persist(state, evidence_path)
            return AuthorizedQfitActivationOutcome(state, None, None)
        state = state.pass_stage(PhysicalAcceptanceStage.GNB_N2, source=source)
        _persist(state, evidence_path)

    if state.acceptance.next_stage is PhysicalAcceptanceStage.UE_MANAGEMENT:
        reachable = execute_qfit_management_probe(
            slice_name=slice_name,
            qfit=authority.ue,
            runner=r2lab_runner,
            timeout_seconds=min(timeout_seconds, 30),
        )
        state = (
            state.pass_stage(
                PhysicalAcceptanceStage.UE_MANAGEMENT,
                source="qfit-preactivation-management:reachable",
            )
            if reachable
            else state.fail_stage(
                PhysicalAcceptanceStage.UE_MANAGEMENT,
                source="qfit-preactivation-management:unreachable",
            )
        )
        _persist(state, evidence_path)
        if not reachable:
            return AuthorizedQfitActivationOutcome(state, None, None)

    pre_runtime: QfitRuntimeEvidence | None = None
    if state.acceptance.next_stage is PhysicalAcceptanceStage.CELL_ACQUISITION:
        pre_runtime = execute_qfit_runtime_probe(
            slice_name=slice_name,
            qfit=authority.ue,
            runner=r2lab_runner,
            timeout_seconds=min(timeout_seconds, 30),
        )
        cell_source = f"qfit-preactivation:cell-{pre_runtime.cell.value}"
        if not pre_runtime.cell_acquired:
            state = state.fail_stage(
                PhysicalAcceptanceStage.CELL_ACQUISITION,
                source=cell_source,
            )
            _persist(state, evidence_path)
            return AuthorizedQfitActivationOutcome(state, None, pre_runtime)
        state = state.pass_stage(
            PhysicalAcceptanceStage.CELL_ACQUISITION,
            source=cell_source,
        )
        registration_source = (
            f"qfit-preactivation:registration-{pre_runtime.registration.value}"
        )
        if not pre_runtime.registered:
            state = state.fail_stage(
                PhysicalAcceptanceStage.REGISTRATION,
                source=registration_source,
            )
            _persist(state, evidence_path)
            return AuthorizedQfitActivationOutcome(state, None, pre_runtime)
        state = state.pass_stage(
            PhysicalAcceptanceStage.REGISTRATION,
            source=registration_source,
        )
        _persist(state, evidence_path)

    if state.acceptance.next_stage is not PhysicalAcceptanceStage.PDU_SESSION:
        expected = state.acceptance.next_stage
        label = expected.value if expected is not None else "none"
        raise R2LabQfitActivationError(
            "qfit activation requires PDU-session as the next acceptance stage; "
            f"current next stage is {label}"
        )

    # Mutation authority is refreshed immediately before the first modem write.
    authority = authorize_physical_start(
        run_id=state.run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=timeout_seconds,
    ).validate()
    if not _same_authority(authority, state):
        raise R2LabQfitActivationError("R2Lab authority changed before qfit activation")
    current_gnb = verify_gnb_n2(
        evidence=state,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 60),
    )
    if not current_gnb.proven:
        raise R2LabQfitActivationError("current singleton gNB/N2 proof was lost before attach")
    if not execute_qfit_management_probe(
        slice_name=slice_name,
        qfit=authority.ue,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 30),
    ):
        raise R2LabQfitActivationError("selected qfit became unreachable before attach")

    def qfit_runner(command: Sequence[str], command_timeout: int) -> CommandResult:
        return r2lab_runner(
            _qfit_gateway_command(slice_name, authority.ue, *tuple(command)),
            command_timeout,
        )

    def observe() -> QfitRuntimeEvidence:
        return execute_qfit_runtime_probe(
            slice_name=slice_name,
            qfit=authority.ue,
            runner=r2lab_runner,
            timeout_seconds=min(timeout_seconds, 30),
        )

    activation = execute_qfit_activation(
        request=QfitActivationRequest(run_id=state.run_id, qfit=authority.ue),
        runner=qfit_runner,
        observer=observe,
        sleeper=sleeper,
        timeout_seconds=timeout_seconds,
    )
    if activation_evidence_path is not None:
        activation.write_json(activation_evidence_path)

    pdu_source = (
        f"qfit-activation:{activation.status}:"
        f"packet-{activation.final_runtime.packet_service.value}:"
        f"ipv4-{activation.final_runtime.ipv4.value}"
    )
    if activation.accepted:
        state = state.pass_stage(PhysicalAcceptanceStage.PDU_SESSION, source=pdu_source)
    else:
        state = state.fail_stage(PhysicalAcceptanceStage.PDU_SESSION, source=pdu_source)
    _persist(state, evidence_path)
    return AuthorizedQfitActivationOutcome(state, activation, pre_runtime)


@dataclass(frozen=True)
class AuthorizedQfitUserPlaneOutcome:
    evidence: PhysicalRunEvidence
    probe: UserPlaneProbeEvidence


def execute_authorized_qfit_user_plane(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    run_root: Path,
    known_hosts: Path,
    peer: str,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    evidence_path: Path | None = None,
    timeout_seconds: int = 30,
) -> AuthorizedQfitUserPlaneOutcome:
    """Prove user plane only after the PDU stage and fresh current-path checks."""

    from synthran.r2lab.controller import authorize_physical_start
    from synthran.r2lab.runtime import (
        execute_qfit_management_probe,
        execute_qfit_runtime_probe,
        verify_gnb_n2,
    )

    if evidence.acceptance.next_stage is not PhysicalAcceptanceStage.USER_PLANE:
        raise R2LabQfitActivationError("user-plane proof requires a passed PDU-session stage")
    authority = authorize_physical_start(
        run_id=evidence.run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=timeout_seconds,
    ).validate()
    if not _same_authority(authority, evidence):
        raise R2LabQfitActivationError("R2Lab authority changed before user-plane proof")
    gnb = verify_gnb_n2(
        evidence=evidence,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 60),
    )
    if not gnb.proven:
        raise R2LabQfitActivationError("current singleton gNB/N2 proof was lost before user plane")
    if not execute_qfit_management_probe(
        slice_name=slice_name,
        qfit=authority.ue,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 30),
    ):
        raise R2LabQfitActivationError("selected qfit is not management-reachable")
    runtime = execute_qfit_runtime_probe(
        slice_name=slice_name,
        qfit=authority.ue,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 30),
    )
    if not runtime.pdu_session_established:
        raise R2LabQfitActivationError("current qfit no longer proves the accepted PDU session")

    def qfit_runner(command: Sequence[str], command_timeout: int) -> CommandResult:
        return r2lab_runner(
            _qfit_gateway_command(slice_name, authority.ue, *tuple(command)),
            command_timeout,
        )

    probe = execute_user_plane_probe(
        peer=peer,
        runner=qfit_runner,
        command_timeout_seconds=min(timeout_seconds, 60),
    )
    state = evidence.record_user_plane(probe, source="qfit-activated-user-plane")
    _persist(state, evidence_path)
    return AuthorizedQfitUserPlaneOutcome(state, probe)


@dataclass(frozen=True)
class PhysicalWorkloadContext:
    run_id: str
    qfit: str
    interface: str
    claim_sha256: str
    package_sha256: str
    render_sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "run_id": self.run_id,
            "qfit": self.qfit,
            "interface": self.interface,
            "claim_sha256": self.claim_sha256,
            "package_sha256": self.package_sha256,
            "render_sha256": self.render_sha256,
            "backend": "r2lab",
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

    def validate(self, context: PhysicalWorkloadContext) -> "PhysicalWorkloadResult":
        if self.run_id != context.run_id:
            raise R2LabQfitActivationError("physical workload result belongs to another run")
        if not _SAFE_WORKLOAD_RE.fullmatch(self.workload_id):
            raise R2LabQfitActivationError("physical workload ID contains unsafe characters")
        if self.backend != "r2lab":
            raise R2LabQfitActivationError(
                "physical workload handoff refuses a non-R2Lab/virtual result"
            )
        if self.interface != _QFIT_INTERFACE or self.interface != context.interface:
            raise R2LabQfitActivationError("physical workload result must use wwan0")
        _validate_digest(self.evidence_sha256, "physical workload evidence digest")
        if not isinstance(self.accepted, bool) or not isinstance(self.cleanup_proven, bool):
            raise R2LabQfitActivationError("physical workload result booleans are malformed")
        return self

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-physical-workload/v1alpha1",
            "run_id": self.run_id,
            "workload_id": self.workload_id,
            "backend": self.backend,
            "interface": self.interface,
            "evidence_sha256": self.evidence_sha256,
            "accepted": self.accepted,
            "cleanup_proven": self.cleanup_proven,
        }

    def write_json(self, path: Path) -> Path:
        path = path.expanduser().resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n"
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                newline="\n",
                dir=path.parent,
                delete=False,
            ) as temporary:
                temporary.write(content)
                temporary_path = Path(temporary.name)
            temporary_path.replace(path)
        except OSError as exc:
            raise R2LabQfitActivationError("physical workload evidence could not be persisted") from exc
        return path


@dataclass(frozen=True)
class PhysicalWorkloadHandoffOutcome:
    evidence: PhysicalRunEvidence
    result: PhysicalWorkloadResult | None


def execute_physical_workload_handoff(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    run_root: Path,
    known_hosts: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    executor: WorkloadExecutor,
    evidence_path: Path | None = None,
    workload_evidence_path: Path | None = None,
    timeout_seconds: int = 30,
) -> PhysicalWorkloadHandoffOutcome:
    """Hand a proven physical path to an explicit R2Lab workload executor.

    This function never falls through to the accepted virtual experiment runtime.
    A caller must supply a physical executor, and its result must identify the
    R2Lab backend and ``wwan0`` before it can satisfy the final acceptance stage.
    """

    from synthran.r2lab.controller import authorize_physical_start
    from synthran.r2lab.runtime import (
        execute_qfit_management_probe,
        execute_qfit_runtime_probe,
        verify_gnb_n2,
    )

    if evidence.acceptance.next_stage is not PhysicalAcceptanceStage.WORKLOAD:
        raise R2LabQfitActivationError(
            "physical workload handoff requires a passed user-plane stage"
        )
    if evidence.staged is None or evidence.gnb_start is None:
        raise R2LabQfitActivationError("physical workload handoff requires staged/start evidence")

    authority = authorize_physical_start(
        run_id=evidence.run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=timeout_seconds,
    ).validate()
    if not _same_authority(authority, evidence):
        raise R2LabQfitActivationError("R2Lab authority changed before workload handoff")
    gnb = verify_gnb_n2(
        evidence=evidence,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 60),
    )
    if not gnb.proven:
        raise R2LabQfitActivationError("current singleton gNB/N2 proof was lost before workload")
    if not execute_qfit_management_probe(
        slice_name=slice_name,
        qfit=authority.ue,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 30),
    ):
        raise R2LabQfitActivationError("selected qfit is not management-reachable")
    runtime = execute_qfit_runtime_probe(
        slice_name=slice_name,
        qfit=authority.ue,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 30),
    )
    if not runtime.pdu_session_established:
        raise R2LabQfitActivationError("current qfit no longer proves a PDU session")

    context = PhysicalWorkloadContext(
        run_id=evidence.run_id,
        qfit=authority.ue,
        interface=_QFIT_INTERFACE,
        claim_sha256=authority.claim_sha256,
        package_sha256=evidence.staged.package_sha256,
        render_sha256=evidence.staged.render_sha256,
    )
    try:
        result = executor(context).validate(context)
    except R2LabQfitActivationError:
        raise
    except Exception:
        state = evidence.fail_stage(
            PhysicalAcceptanceStage.WORKLOAD,
            source="physical-workload:executor-error",
        )
        _persist(state, evidence_path)
        return PhysicalWorkloadHandoffOutcome(state, None)

    if workload_evidence_path is not None:
        result.write_json(workload_evidence_path)
    source = (
        f"physical-workload:{result.workload_id}:"
        f"accepted-{int(result.accepted)}:cleanup-{int(result.cleanup_proven)}"
    )
    passed = result.accepted and result.cleanup_proven
    state = (
        evidence.pass_stage(PhysicalAcceptanceStage.WORKLOAD, source=source)
        if passed
        else evidence.fail_stage(PhysicalAcceptanceStage.WORKLOAD, source=source)
    )
    _persist(state, evidence_path)
    return PhysicalWorkloadHandoffOutcome(state, result)
