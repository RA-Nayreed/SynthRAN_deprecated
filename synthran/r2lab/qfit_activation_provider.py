"""Provider-aligned qfit MBIM activation for the physical R2Lab path.

The stock qfit image performs packet-service attach immediately after enabling
its software radio.  Registration is therefore an observed postcondition of
that attach path, not a prerequisite for issuing the attach command.
"""

from __future__ import annotations

import time
from typing import Callable

from synthran.r2lab.radio import Ipv4State, PacketServiceState, QfitRuntimeEvidence
from synthran.r2lab.ue import (
    MutationStepEvidence,
    QfitActivationRequest,
    QfitActivationResult,
    R2LabQfitActivationError,
    Runner,
    RuntimeObserver,
    Sleeper,
    SoftwareRadioState,
    _observe_runtime,
    _query_radio_state,
    _rollback_activation,
    _run_mutation,
    _wait_radio_state,
    _wait_runtime,
    qfit_activation_commands,
)


def execute_qfit_activation_provider(
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
    """Establish a qfit MBIM session using the provider-proven ordering.

    Ordering is intentionally:

    ``link up -> radio on -> settle -> attach -> connect -> set IP``.

    Registration remains independently observed and is required by the final
    accepted runtime state, but it is not required before packet-service attach.
    """

    request.validate()
    if timeout_seconds < 5 or timeout_seconds > 120:
        raise R2LabQfitActivationError(
            "qfit activation timeout must be between 5 and 120 seconds"
        )
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
    if radio is not SoftwareRadioState.ON:
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

    # Mirror the stock qfit start.sh settle between radio enable and attach.
    sleeper(2.0)
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
        predicate=lambda state: state.packet_service is PacketServiceState.ATTACHED,
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
