"""Bounded, transport-aware activation for physical FR1 UEs.

The acceptance contract still requires positive NR-SA cell and registration proof.
This module makes that proof reliable: AT access is serialized on the UE, uses
only the Python standard library available in the R2Lab images, and the whole
activation owns one finite deadline instead of multiplying a large timeout by
every remote probe.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
import time
from typing import Callable, Mapping, Sequence

from synthran.live_preflight import CommandResult, subprocess_runner
from synthran.r2lab.acceptance import (
    PhysicalAcceptance,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
)
from synthran.r2lab.radio import (
    CellAcquisitionState,
    Ipv4State,
    PacketServiceState,
    RegistrationState,
    parse_ipv4_state,
    parse_packet_service,
    parse_qnwinfo,
)
from synthran.r2lab.ue import (
    AT_DEVICE,
    MBIM_DEVICE,
    UE_INTERFACE,
    PhysicalUeActivationSummary,
    PhysicalUeRuntimeEvidence,
    R2LabPhysicalUeError,
    _qmi_running,
    _record_runtime,
    _refresh_boundary,
    _start_qmi_manager,
    _stop_qmi_manager,
    _ue_read,
    _ue_required,
    _write_json,
)


Runner = Callable[[Sequence[str], int], CommandResult]
Sleeper = Callable[[float], None]
Clock = Callable[[], float]

_AT_LOCK = "/run/lock/synthran-at.lock"
_AT_TERMINAL_SECONDS = 4.0
_MAX_ACTIVATION_SECONDS = 180
_MAX_MUTATION_COMMAND_SECONDS = 30
_MAX_PROBE_COMMAND_SECONDS = 10
_POLL_INTERVAL_SECONDS = 2.0
_MBIM_REGISTER_RE = re.compile(
    r"register\s+state\s*:\s*['\"]?([a-z0-9_-]+)", re.IGNORECASE
)

# The R2Lab Quectel images already expose Python 3 and the modem TTY.  Avoid a
# pyserial dependency here: lock the port across processes, preserve its current
# speed, switch only the line discipline to raw mode, and restore it afterwards.
_AT_SCRIPT = r'''import fcntl, os, select, sys, termios, time, tty
command = sys.argv[1]
lock_fd = os.open("/run/lock/synthran-at.lock", os.O_CREAT | os.O_RDWR, 0o600)
lock_deadline = time.monotonic() + 3.0
while True:
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        break
    except BlockingIOError:
        if time.monotonic() >= lock_deadline:
            raise SystemExit(75)
        time.sleep(0.05)
fd = os.open("/dev/ttyUSB2", os.O_RDWR | os.O_NOCTTY | os.O_NONBLOCK)
original = termios.tcgetattr(fd)
try:
    tty.setraw(fd, termios.TCSANOW)
    termios.tcflush(fd, termios.TCIOFLUSH)
    os.write(fd, (command + "\r").encode("ascii"))
    deadline = time.monotonic() + 4.0
    data = bytearray()
    terminal = False
    while time.monotonic() < deadline:
        ready, _, _ = select.select([fd], [], [], min(0.25, max(0.0, deadline - time.monotonic())))
        if not ready:
            continue
        try:
            chunk = os.read(fd, 4096)
        except BlockingIOError:
            continue
        if not chunk:
            continue
        data.extend(chunk)
        normalized = bytes(data).upper()
        if b"\r\nOK\r\n" in normalized or b"\r\nERROR\r\n" in normalized:
            terminal = True
            break
    sys.stdout.write(bytes(data).decode("utf-8", "replace"))
    if not terminal:
        raise SystemExit(74)
finally:
    termios.tcsetattr(fd, termios.TCSANOW, original)
    os.close(fd)
    fcntl.flock(lock_fd, fcntl.LOCK_UN)
    os.close(lock_fd)
'''


@dataclass(frozen=True)
class ActivationObservation:
    runtime: PhysicalUeRuntimeEvidence
    transport_failures: tuple[str, ...]


class _DeadlineExpired(R2LabPhysicalUeError):
    pass


def _command_timeout(*, deadline: float, now: float, cap: int) -> int:
    """Return a bounded whole-second timeout from one total stage deadline."""

    remaining = deadline - now
    if remaining <= 0:
        raise _DeadlineExpired("physical UE activation deadline expired")
    return max(1, min(cap, int(remaining + 0.999)))


def _at(command: str) -> tuple[str, ...]:
    if command not in {"AT+QNWINFO", "AT+C5GREG?"}:
        raise R2LabPhysicalUeError("AT command is outside the sanitized UE probe allow-list")
    return ("python3", "-c", _AT_SCRIPT, command)


def parse_mbim_registration(output: str) -> RegistrationState:
    """Reduce libmbim registration output to the acceptance registration state."""

    states = {match.group(1).lower() for match in _MBIM_REGISTER_RE.finditer(output)}
    if len(states) != 1:
        return RegistrationState.UNKNOWN
    state = next(iter(states))
    if state in {"home", "roaming", "partner"}:
        return RegistrationState.REGISTERED
    if state in {"searching"}:
        return RegistrationState.SEARCHING
    if state in {"deregistered", "denied", "unknown"}:
        return RegistrationState.NOT_REGISTERED
    return RegistrationState.UNKNOWN


def _probe_timeout(deadline: float, clock: Clock) -> int:
    return _command_timeout(
        deadline=deadline,
        now=clock(),
        cap=_MAX_PROBE_COMMAND_SECONDS,
    )


def _observe_activation_runtime(
    *,
    run_id: str,
    slice_name: str,
    run_root: Path,
    runner: Runner,
    deadline: float,
    clock: Clock,
) -> ActivationObservation:
    topology = __import__(
        "synthran.r2lab.resources", fromlist=["load_topology"]
    ).load_topology(run_root=run_root, run_id=run_id).validate()
    profile = topology.ue_profile
    failures: list[str] = []

    qnwinfo = _ue_read(
        topology=topology,
        slice_name=slice_name,
        runner=runner,
        remote=_at("AT+QNWINFO"),
        timeout_seconds=_probe_timeout(deadline, clock),
    )
    if qnwinfo is None:
        failures.append("at-qnwinfo")

    if profile.mode == "mbim":
        registration_result = _ue_read(
            topology=topology,
            slice_name=slice_name,
            runner=runner,
            remote=("mbimcli", "-p", "-d", MBIM_DEVICE, "--query-registration-state"),
            timeout_seconds=_probe_timeout(deadline, clock),
        )
        registration = parse_mbim_registration(
            registration_result.stdout if registration_result is not None else ""
        )
        if registration_result is None:
            failures.append("mbim-registration")
    elif profile.mode == "qmi":
        registration_result = _ue_read(
            topology=topology,
            slice_name=slice_name,
            runner=runner,
            remote=_at("AT+C5GREG?"),
            timeout_seconds=_probe_timeout(deadline, clock),
        )
        from synthran.r2lab.radio import parse_c5greg

        registration = parse_c5greg(
            registration_result.stdout if registration_result is not None else ""
        )
        if registration_result is None:
            failures.append("at-c5greg")
    else:
        raise R2LabPhysicalUeError("selected UE mode is not supported by the canonical path")

    link = _ue_read(
        topology=topology,
        slice_name=slice_name,
        runner=runner,
        remote=("ip", "-o", "link", "show", "dev", UE_INTERFACE),
        timeout_seconds=_probe_timeout(deadline, clock),
    )
    if link is None:
        failures.append("wwan0-link")

    address = None
    if link is not None:
        address = _ue_read(
            topology=topology,
            slice_name=slice_name,
            runner=runner,
            remote=("ip", "-o", "-4", "addr", "show", "dev", UE_INTERFACE),
            timeout_seconds=_probe_timeout(deadline, clock),
        )
        if address is None:
            failures.append("wwan0-ipv4")
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
            timeout_seconds=_probe_timeout(deadline, clock),
        )
        if packet_result is None:
            failures.append("mbim-packet-service")
        packet = parse_packet_service(packet_result.stdout if packet_result is not None else "")
        manager_running = packet_result is not None
    else:
        manager_running = _qmi_running(
            topology=topology,
            slice_name=slice_name,
            run_id=run_id,
            runner=runner,
            timeout_seconds=_probe_timeout(deadline, clock),
        )
        packet = (
            PacketServiceState.ATTACHED
            if ipv4 is Ipv4State.PRESENT
            else PacketServiceState.DETACHED
        )

    runtime = PhysicalUeRuntimeEvidence(
        ue=topology.ue,
        mode=profile.mode,
        interface=UE_INTERFACE,
        cell=parse_qnwinfo(qnwinfo.stdout if qnwinfo is not None else ""),
        registration=registration,
        packet_service=packet,
        ipv4=ipv4,
        manager_running=manager_running,
        transport_error=bool(failures),
    )
    return ActivationObservation(runtime=runtime, transport_failures=tuple(failures))


def _activation_payload(
    *,
    summary: PhysicalUeActivationSummary,
    transport_failures: Sequence[str],
    deadline_exhausted: bool,
) -> dict[str, object]:
    payload = summary.to_dict()
    payload["transport_failures"] = list(transport_failures)
    payload["deadline_exhausted"] = deadline_exhausted
    return payload


def _persist_activation(
    *,
    path: Path | None,
    state: PhysicalRunEvidence,
    runtime: PhysicalUeRuntimeEvidence,
    status: str,
    failures: Sequence[str],
    deadline_exhausted: bool,
) -> PhysicalUeActivationSummary:
    summary = PhysicalUeActivationSummary(
        run_id=state.run_id,
        ue=runtime.ue,
        mode=runtime.mode,
        status=status,
        runtime=runtime,
        evidence_path=path or Path("physical-run.json"),
    )
    if path is not None:
        _write_json(
            path,
            _activation_payload(
                summary=summary,
                transport_failures=failures,
                deadline_exhausted=deadline_exhausted,
            ),
        )
    return summary


def _mbim_activate_bounded(
    *,
    topology,
    slice_name: str,
    runner: Runner,
    deadline: float,
    clock: Clock,
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
            timeout_seconds=_command_timeout(
                deadline=deadline,
                now=clock(),
                cap=_MAX_MUTATION_COMMAND_SECONDS,
            ),
            label=label,
        )


def _rollback_mbim_bounded(
    *, topology, slice_name: str, runner: Runner, deadline: float, clock: Clock
) -> None:
    for command in (
        ("mbimcli", "-p", "-d", MBIM_DEVICE, "--disconnect=session-id=0"),
        ("mbimcli", "-p", "-d", MBIM_DEVICE, "--detach-packet-service"),
        ("ip", "addr", "flush", "dev", UE_INTERFACE),
    ):
        try:
            timeout = _command_timeout(deadline=deadline, now=clock(), cap=10)
        except _DeadlineExpired:
            return
        _ue_read(
            topology=topology,
            slice_name=slice_name,
            runner=runner,
            remote=command,
            timeout_seconds=timeout,
        )


def recover_retryable_transport_failure(
    *, evidence: PhysicalRunEvidence, activation_evidence_path: Path
) -> PhysicalRunEvidence:
    """Drop only a terminal UE failure that was caused by probe transport loss.

    This does not weaken acceptance and does not mutate disk by itself.  The
    subsequent activation must refresh live authority/N2 and produce new proof
    before repaired evidence is persisted.
    """

    if evidence.acceptance.failed_stage not in {
        PhysicalAcceptanceStage.CELL_ACQUISITION,
        PhysicalAcceptanceStage.REGISTRATION,
        PhysicalAcceptanceStage.PDU_SESSION,
    }:
        return evidence
    try:
        payload = json.loads(activation_evidence_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return evidence
    if not isinstance(payload, dict) or payload.get("run_id") != evidence.run_id:
        return evidence
    runtime = payload.get("runtime")
    if not isinstance(runtime, dict) or runtime.get("transport_error") is not True:
        return evidence
    trimmed = PhysicalAcceptance(evidence=evidence.acceptance.evidence[:-1])
    return PhysicalRunEvidence(
        run_id=evidence.run_id,
        staged=evidence.staged,
        gnb_start=evidence.gnb_start,
        acceptance=trimmed,
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
    clock: Clock = time.monotonic,
    timeout_seconds: int = 30,
) -> tuple[PhysicalRunEvidence, PhysicalUeActivationSummary]:
    """Advance UE cell/registration/PDU proof under one bounded deadline."""

    if evidence.gnb_start is None:
        raise R2LabPhysicalUeError("physical UE activation requires a started gNB")
    if evidence.acceptance.next_stage not in {
        PhysicalAcceptanceStage.UE_MANAGEMENT,
        PhysicalAcceptanceStage.CELL_ACQUISITION,
        PhysicalAcceptanceStage.REGISTRATION,
        PhysicalAcceptanceStage.PDU_SESSION,
    }:
        raise R2LabPhysicalUeError("physical UE activation is not the next lifecycle boundary")

    budget_seconds = min(_MAX_ACTIVATION_SECONDS, max(30, int(timeout_seconds)))
    deadline = clock() + budget_seconds
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
        timeout_seconds=min(30, budget_seconds),
    )
    if state.acceptance.next_stage is PhysicalAcceptanceStage.UE_MANAGEMENT:
        state = state.pass_stage(
            PhysicalAcceptanceStage.UE_MANAGEMENT,
            source=f"current-management:{topology.ue}:{topology.ue_profile.mode}",
        )
        if evidence_path is not None:
            state.write_json(evidence_path)

    try:
        before = _observe_activation_runtime(
            run_id=state.run_id,
            slice_name=slice_name,
            run_root=run_root,
            runner=r2lab_runner,
            deadline=deadline,
            clock=clock,
        )
    except _DeadlineExpired as exc:
        raise R2LabPhysicalUeError(str(exc)) from exc

    if before.runtime.pdu_session_established:
        state = _record_runtime(state, before.runtime, source="preexisting-current-ue")
        if evidence_path is not None:
            state.write_json(evidence_path)
        summary = _persist_activation(
            path=activation_evidence_path,
            state=state,
            runtime=before.runtime,
            status="already-ready",
            failures=before.transport_failures,
            deadline_exhausted=False,
        )
        return state, summary

    # Do not reconnect an MBIM session that is already attached with IPv4 merely
    # because the independent cell/registration proof transport was unavailable.
    needs_transport_mutation = not (
        before.runtime.packet_service is PacketServiceState.ATTACHED
        and before.runtime.ipv4 is Ipv4State.PRESENT
    )

    _refresh_boundary(
        run_id=state.run_id,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        run_root=run_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        timeout_seconds=_command_timeout(deadline=deadline, now=clock(), cap=30),
    )

    started_qmi = False
    mutated_mbim = False
    try:
        if topology.ue_profile.mode == "mbim" and needs_transport_mutation:
            _mbim_activate_bounded(
                topology=topology,
                slice_name=slice_name,
                runner=r2lab_runner,
                deadline=deadline,
                clock=clock,
            )
            mutated_mbim = True
        elif topology.ue_profile.mode == "qmi" and needs_transport_mutation:
            _start_qmi_manager(
                topology=topology,
                slice_name=slice_name,
                run_id=state.run_id,
                runner=r2lab_runner,
                timeout_seconds=_command_timeout(
                    deadline=deadline,
                    now=clock(),
                    cap=_MAX_MUTATION_COMMAND_SECONDS,
                ),
            )
            started_qmi = True

        observation = before
        while True:
            try:
                observation = _observe_activation_runtime(
                    run_id=state.run_id,
                    slice_name=slice_name,
                    run_root=run_root,
                    runner=r2lab_runner,
                    deadline=deadline,
                    clock=clock,
                )
            except _DeadlineExpired:
                break
            if observation.runtime.pdu_session_established:
                break
            remaining = deadline - clock()
            if remaining <= 0:
                break
            sleeper(min(_POLL_INTERVAL_SECONDS, remaining))

        if observation.runtime.pdu_session_established:
            state = _record_runtime(state, observation.runtime, source="physical-ue-activation")
            if evidence_path is not None:
                state.write_json(evidence_path)
            summary = _persist_activation(
                path=activation_evidence_path,
                state=state,
                runtime=observation.runtime,
                status="activated",
                failures=observation.transport_failures,
                deadline_exhausted=False,
            )
            return state, summary

        if observation.transport_failures:
            _persist_activation(
                path=activation_evidence_path,
                state=state,
                runtime=observation.runtime,
                status="transport-failed",
                failures=observation.transport_failures,
                deadline_exhausted=True,
            )
            joined = ", ".join(observation.transport_failures)
            raise R2LabPhysicalUeError(
                f"physical UE proof transport failed before deadline: {joined}"
            )

        state = _record_runtime(state, observation.runtime, source="physical-ue-activation")
        if evidence_path is not None:
            state.write_json(evidence_path)
        summary = _persist_activation(
            path=activation_evidence_path,
            state=state,
            runtime=observation.runtime,
            status="not-proven",
            failures=(),
            deadline_exhausted=True,
        )
        return state, summary
    except Exception:
        # Roll back only transport state that this invocation actually created.
        if topology.ue_profile.mode == "mbim" and mutated_mbim:
            _rollback_mbim_bounded(
                topology=topology,
                slice_name=slice_name,
                runner=r2lab_runner,
                deadline=deadline,
                clock=clock,
            )
        elif started_qmi:
            try:
                timeout = _command_timeout(deadline=deadline, now=clock(), cap=10)
            except _DeadlineExpired:
                timeout = 1
            _stop_qmi_manager(
                topology=topology,
                slice_name=slice_name,
                run_id=state.run_id,
                runner=r2lab_runner,
                timeout_seconds=timeout,
            )
        raise
