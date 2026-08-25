"""Physical UE activation composed from pinned 5g-Ansible mechanics.

The upstream role performs modem actuation.  SynthRAN independently verifies a
functional postcondition over ``wwan0`` and records ordered acceptance.  A
convergence timeout is not a terminal acceptance failure; the run remains at the
same stage and can be resumed safely.
"""

from __future__ import annotations

import json
from pathlib import Path
import time
from typing import Callable, Sequence

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
)
from synthran.r2lab.resources import load_topology, ue_gateway_command
from synthran.r2lab.ue import (
    MBIM_DEVICE,
    UE_INTERFACE,
    PhysicalUeActivationSummary,
    PhysicalUeRuntimeEvidence,
    R2LabPhysicalUeError,
    _refresh_boundary,
    _ue_read,
    _write_json,
)
from synthran.r2lab.ue_ansible import (
    OPEN5GS_UPF_ADDRESS,
    R2LabUeAnsibleError,
    execute_selected_ue_role,
)


Runner = Callable[[Sequence[str], int], CommandResult]
Sleeper = Callable[[float], None]
Clock = Callable[[], float]
POSTCONDITION_SECONDS = 45
POLL_SECONDS = 2.0
PROBE_TIMEOUT_SECONDS = 10


def _probe_upf(
    *,
    topology,
    slice_name: str,
    runner: Runner,
    timeout_seconds: int,
) -> tuple[bool, bool]:
    """Return ``(command_completed, path_proven)`` for wwan0 -> Open5GS UPF."""

    command = ue_gateway_command(
        slice_name,
        topology.ue_profile,
        "ping",
        "-I",
        UE_INTERFACE,
        "-c",
        "1",
        "-W",
        "3",
        OPEN5GS_UPF_ADDRESS,
    )
    try:
        result = runner(command, timeout_seconds)
    except Exception:
        return False, False
    return True, result.returncode == 0


def observe_functional_ue_runtime(
    *,
    run_id: str,
    slice_name: str,
    run_root: Path = Path(".synthran/r2lab"),
    runner: Runner = subprocess_runner,
    timeout_seconds: int = PROBE_TIMEOUT_SECONDS,
) -> PhysicalUeRuntimeEvidence:
    """Observe only functional, sanitized postconditions after upstream actuation.

    Interface-bound reachability to the Open5GS UPF is stronger than a modem AT
    string for this backend: it requires the selected UE to have acquired the
    physical NR path, registered, established a PDU session and installed a
    usable ``wwan0`` route through the run-owned core.
    """

    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    profile = topology.ue_profile
    timeout_seconds = max(3, min(int(timeout_seconds), PROBE_TIMEOUT_SECONDS))

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

    packet = PacketServiceState.UNKNOWN
    manager_running = True
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
        manager_result = _ue_read(
            topology=topology,
            slice_name=slice_name,
            runner=runner,
            remote=("pgrep", "-x", "quectel-CM"),
            timeout_seconds=timeout_seconds,
        )
        manager_running = manager_result is not None
    else:
        raise R2LabPhysicalUeError("selected UE mode is unsupported by the physical path")

    probe_completed, upf_proven = _probe_upf(
        topology=topology,
        slice_name=slice_name,
        runner=runner,
        timeout_seconds=timeout_seconds,
    )
    if upf_proven:
        packet = PacketServiceState.ATTACHED

    functional_path = upf_proven and ipv4 is Ipv4State.PRESENT
    return PhysicalUeRuntimeEvidence(
        ue=topology.ue,
        mode=profile.mode,
        interface=UE_INTERFACE,
        cell=(
            CellAcquisitionState.ACQUIRED_NR_SA
            if functional_path
            else CellAcquisitionState.UNKNOWN
        ),
        registration=(
            RegistrationState.REGISTERED
            if functional_path
            else RegistrationState.UNKNOWN
        ),
        packet_service=packet,
        ipv4=ipv4,
        manager_running=manager_running,
        transport_error=(link is None or not probe_completed),
    )


def _pass_functional_path(
    state: PhysicalRunEvidence,
    runtime: PhysicalUeRuntimeEvidence,
) -> PhysicalRunEvidence:
    if not runtime.pdu_session_established:
        return state
    source = f"current-{runtime.ue}:wwan0:open5gs-upf"
    for stage, detail in (
        (PhysicalAcceptanceStage.CELL_ACQUISITION, "nr-sa-functional"),
        (PhysicalAcceptanceStage.REGISTRATION, "core-path-registered"),
        (PhysicalAcceptanceStage.PDU_SESSION, "ipv4-upf-reachable"),
    ):
        if state.acceptance.next_stage is stage:
            state = state.pass_stage(stage, source=f"{source}:{detail}")
    return state


def recover_retryable_transport_failure(
    *, evidence: PhysicalRunEvidence, activation_evidence_path: Path
) -> PhysicalRunEvidence:
    """Migrate the pre-Ansible terminal transport failure from an older run."""

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
    timeout_seconds: int = 180,
) -> tuple[PhysicalRunEvidence, PhysicalUeActivationSummary]:
    """Actuate one selected UE via pinned 5g-Ansible and prove its live path."""

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
        timeout_seconds=min(timeout_seconds, 300),
    )
    if state.acceptance.next_stage is PhysicalAcceptanceStage.UE_MANAGEMENT:
        state = state.pass_stage(
            PhysicalAcceptanceStage.UE_MANAGEMENT,
            source=f"current-management:{topology.ue}:{topology.ue_profile.mode}",
        )
    if evidence_path is not None:
        state.write_json(evidence_path)

    before = observe_functional_ue_runtime(
        run_id=state.run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
    )
    if before.pdu_session_established:
        state = _pass_functional_path(state, before)
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

    _refresh_boundary(
        run_id=state.run_id,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        run_root=run_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    try:
        execute_selected_ue_role(
            run_id=state.run_id,
            slice_name=slice_name,
            topology=topology,
            action="connect",
            run_root=run_root,
            timeout_seconds=min(timeout_seconds, 180),
        )
    except R2LabUeAnsibleError as exc:
        raise R2LabPhysicalUeError(str(exc)) from exc

    deadline = clock() + min(POSTCONDITION_SECONDS, max(10, int(timeout_seconds)))
    runtime = before
    while True:
        runtime = observe_functional_ue_runtime(
            run_id=state.run_id,
            slice_name=slice_name,
            run_root=run_root,
            runner=r2lab_runner,
        )
        if runtime.pdu_session_established or clock() >= deadline:
            break
        sleeper(POLL_SECONDS)

    state = _pass_functional_path(state, runtime)
    if evidence_path is not None:
        state.write_json(evidence_path)
    summary = PhysicalUeActivationSummary(
        run_id=state.run_id,
        ue=topology.ue,
        mode=topology.ue_profile.mode,
        status="activated" if runtime.pdu_session_established else "not-proven",
        runtime=runtime,
        evidence_path=evidence_path or Path("physical-run.json"),
    )
    if activation_evidence_path is not None:
        _write_json(activation_evidence_path, summary.to_dict())
    return state, summary
