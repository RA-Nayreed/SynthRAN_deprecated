"""R2Lab provider state, exact mutations, and cleanup assessment.

This module keeps provider-facing behavior together: PDU state, qfit state,
verified power transitions, and claim-release evidence. Command return codes are
diagnostic only; exact provider observations are the state truth.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
import time
from typing import Callable, Iterable, Sequence

from synthran.live_preflight import CommandResult


SUPPORTED_QFITS = frozenset(
    {"qfit07", "qfit09", "qfit18", "qfit29", "qfit32", "qfit34"}
)


class R2LabPowerStateError(RuntimeError):
    """Raised when provider PDU state output is contradictory or malformed."""


class R2LabQfitStateError(RuntimeError):
    """Raised when a qfit identifier or provider observation is malformed."""


class PowerState(str, Enum):
    ON = "on"
    OFF = "off"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class PduStatusObservation:
    resource: str
    state: PowerState
    watts: int | None = None


@dataclass(frozen=True)
class PduTransitionEvidence:
    resource: str
    requested_state: PowerState
    observed_state: PowerState
    mutation_returncode: int | None
    status_returncode: int | None
    watts: int | None = None

    @property
    def confirmed(self) -> bool:
        return self.observed_state is self.requested_state


def _validate_resource_name(resource: str) -> str:
    value = resource.strip().lower()
    if not value or len(value) > 64:
        raise R2LabPowerStateError("R2Lab resource name must contain 1-64 characters")
    if any(not (character.isalnum() or character in "._-") for character in value):
        raise R2LabPowerStateError("R2Lab resource name contains unsafe characters")
    return value


def parse_pdu_status(output: str, *, resource: str) -> PduStatusObservation:
    """Parse textual Rhubarbe PDU state for exactly one resource."""

    resource = _validate_resource_name(resource)
    pattern = re.compile(
        rf"\({re.escape(resource)}\)\s*:\s*(ON|OFF)(?:\s*\((\d+)W\))?",
        re.IGNORECASE,
    )
    matches: list[tuple[PowerState, int | None]] = []
    for line in output.splitlines():
        match = pattern.search(line)
        if match is None:
            continue
        state = PowerState.ON if match.group(1).upper() == "ON" else PowerState.OFF
        watts = int(match.group(2)) if match.group(2) is not None else None
        matches.append((state, watts))

    if not matches:
        return PduStatusObservation(resource=resource, state=PowerState.UNKNOWN)
    states = {state for state, _ in matches}
    if len(states) != 1:
        raise R2LabPowerStateError(
            f"conflicting R2Lab PDU state observations for {resource}"
        )
    state, watts = matches[-1]
    return PduStatusObservation(resource=resource, state=state, watts=watts)


def evaluate_pdu_transition(
    *,
    resource: str,
    requested_state: PowerState,
    mutation_returncode: int | None,
    status_returncode: int | None,
    status_stdout: str = "",
    status_stderr: str = "",
) -> PduTransitionEvidence:
    if requested_state is PowerState.UNKNOWN:
        raise R2LabPowerStateError("UNKNOWN cannot be requested as a PDU target state")
    combined_status = "\n".join(part for part in (status_stdout, status_stderr) if part)
    observation = parse_pdu_status(combined_status, resource=resource)
    return PduTransitionEvidence(
        resource=observation.resource,
        requested_state=requested_state,
        observed_state=observation.state,
        mutation_returncode=mutation_returncode,
        status_returncode=status_returncode,
        watts=observation.watts,
    )


RemoteRunner = Callable[[Sequence[str], int], CommandResult]
Sleeper = Callable[[float], None]


@dataclass(frozen=True)
class VerifiedPduOperation:
    evidence: PduTransitionEvidence
    mutation_transport_error: bool
    status_transport_error: bool

    @property
    def confirmed(self) -> bool:
        return self.evidence.confirmed

    @property
    def unresolved(self) -> bool:
        return not self.confirmed

    def to_dict(self) -> dict[str, object]:
        return {
            "resource": self.evidence.resource,
            "requested_state": self.evidence.requested_state.value,
            "observed_state": self.evidence.observed_state.value,
            "confirmed": self.confirmed,
            "mutation_returncode": self.evidence.mutation_returncode,
            "status_returncode": self.evidence.status_returncode,
            "mutation_transport_error": self.mutation_transport_error,
            "status_transport_error": self.status_transport_error,
            "watts": self.evidence.watts,
        }


def execute_verified_pdu_transition(
    *,
    resource: str,
    requested_state: PowerState,
    runner: RemoteRunner,
    timeout_seconds: int,
) -> VerifiedPduOperation:
    """Mutate one PDU-backed resource and verify exact textual state."""

    if requested_state is PowerState.UNKNOWN:
        raise R2LabPowerStateError("UNKNOWN cannot be requested as a PDU target state")
    action = "on" if requested_state is PowerState.ON else "off"
    mutation_returncode: int | None = None
    mutation_transport_error = False
    try:
        mutation = runner(("rhubarbe", "pdu", action, resource), timeout_seconds)
    except (RuntimeError, OSError):
        mutation_transport_error = True
    else:
        mutation_returncode = mutation.returncode

    status_returncode: int | None = None
    status_stdout = ""
    status_stderr = ""
    status_transport_error = False
    try:
        status = runner(("rhubarbe", "pdu", "status", resource), timeout_seconds)
    except (RuntimeError, OSError):
        status_transport_error = True
    else:
        status_returncode = status.returncode
        status_stdout = status.stdout
        status_stderr = status.stderr

    evidence = evaluate_pdu_transition(
        resource=resource,
        requested_state=requested_state,
        mutation_returncode=mutation_returncode,
        status_returncode=status_returncode,
        status_stdout=status_stdout,
        status_stderr=status_stderr,
    )
    return VerifiedPduOperation(
        evidence=evidence,
        mutation_transport_error=mutation_transport_error,
        status_transport_error=status_transport_error,
    )


_QFIT_PATTERN = re.compile(r"^qfit(?P<node>\d{2})$")


@dataclass(frozen=True)
class QfitStatusObservation:
    qfit: str
    node: int
    state: PowerState


def qfit_node_number(qfit: str) -> int:
    value = qfit.strip().lower()
    match = _QFIT_PATTERN.fullmatch(value)
    if match is None:
        raise R2LabQfitStateError("qfit resource must use the qfitNN form")
    node = int(match.group("node"))
    if node <= 0:
        raise R2LabQfitStateError("qfit node number must be positive")
    return node


def reviewed_qfit_node_number(qfit: str) -> int:
    """Return the node number for one reviewed qfit resource."""

    value = qfit.strip().lower()
    if value not in SUPPORTED_QFITS:
        raise R2LabQfitStateError("qfit resource is outside the reviewed inventory")
    return qfit_node_number(value)


def parse_qfit_status(output: str, *, qfit: str) -> QfitStatusObservation:
    """Parse exact ``rebootNN:on|off`` provider state for one qfit."""

    value = qfit.strip().lower()
    node = qfit_node_number(value)
    status_prefix = f"reboot{node:02d}"
    pattern = re.compile(
        rf"(?<![A-Za-z0-9_-]){re.escape(status_prefix)}\s*:\s*(on|off)(?![A-Za-z0-9_-])",
        re.IGNORECASE,
    )
    states: list[PowerState] = []
    for line in output.splitlines():
        match = pattern.search(line)
        if match is None:
            continue
        states.append(PowerState.ON if match.group(1).lower() == "on" else PowerState.OFF)
    if not states:
        return QfitStatusObservation(qfit=value, node=node, state=PowerState.UNKNOWN)
    if len(set(states)) != 1:
        raise R2LabQfitStateError(f"conflicting R2Lab qfit state observations for {value}")
    return QfitStatusObservation(qfit=value, node=node, state=states[-1])


@dataclass(frozen=True)
class VerifiedQfitOperation:
    qfit: str
    requested_state: PowerState
    observed_state: PowerState
    mutation_returncode: int | None
    status_returncode: int | None
    mutation_transport_error: bool
    status_transport_error: bool

    @property
    def confirmed(self) -> bool:
        return self.observed_state is self.requested_state

    @property
    def unresolved(self) -> bool:
        return not self.confirmed

    def to_dict(self) -> dict[str, object]:
        return {
            "qfit": self.qfit,
            "requested_state": self.requested_state.value,
            "observed_state": self.observed_state.value,
            "confirmed": self.confirmed,
            "mutation_returncode": self.mutation_returncode,
            "status_returncode": self.status_returncode,
            "mutation_transport_error": self.mutation_transport_error,
            "status_transport_error": self.status_transport_error,
        }


@dataclass(frozen=True)
class QfitUsbPowerObservation:
    qfit: str
    node: int
    state: PowerState
    status_returncode: int | None
    transport_error: bool


def parse_qfit_usb_status(output: str, *, qfit: str) -> QfitStatusObservation:
    """Parse the exact external USB power state for one reviewed qfit."""

    value = qfit.strip().lower()
    node = reviewed_qfit_node_number(value)
    states: list[PowerState] = []
    for line in output.splitlines():
        status = line.strip().lower()
        if status == "usrpon":
            states.append(PowerState.ON)
        elif status == "usrpoff":
            states.append(PowerState.OFF)
    if not states:
        return QfitStatusObservation(qfit=value, node=node, state=PowerState.UNKNOWN)
    if len(set(states)) != 1:
        raise R2LabQfitStateError(
            f"conflicting R2Lab qfit USB state observations for {value}"
        )
    return QfitStatusObservation(qfit=value, node=node, state=states[-1])


def observe_qfit_usb_power(
    *,
    qfit: str,
    runner: RemoteRunner,
    timeout_seconds: int,
) -> QfitUsbPowerObservation:
    """Observe one qfit USB rail without mutating it."""

    value = qfit.strip().lower()
    node = reviewed_qfit_node_number(value)
    status_returncode: int | None = None
    status_stdout = ""
    status_stderr = ""
    transport_error = False
    try:
        status = runner(
            ("curl", "-fsS", f"http://reboot{node:02d}/usrpstatus"),
            timeout_seconds,
        )
    except (RuntimeError, OSError):
        transport_error = True
    else:
        status_returncode = status.returncode
        status_stdout = status.stdout
        status_stderr = status.stderr

    observation = parse_qfit_usb_status(
        "\n".join(part for part in (status_stdout, status_stderr) if part),
        qfit=value,
    )
    return QfitUsbPowerObservation(
        qfit=observation.qfit,
        node=observation.node,
        state=observation.state,
        status_returncode=status_returncode,
        transport_error=transport_error,
    )


@dataclass(frozen=True)
class VerifiedQfitUsbOperation:
    qfit: str
    requested_state: PowerState
    observed_state: PowerState
    mutation_returncode: int | None
    status_returncode: int | None
    mutation_transport_error: bool
    status_transport_error: bool

    @property
    def confirmed(self) -> bool:
        return self.observed_state is self.requested_state


def execute_verified_qfit_usb_transition(
    *,
    qfit: str,
    requested_state: PowerState,
    runner: RemoteRunner,
    timeout_seconds: int,
) -> VerifiedQfitUsbOperation:
    """Mutate one external qfit USB rail and verify its exact state."""

    if requested_state is PowerState.UNKNOWN:
        raise ValueError("UNKNOWN cannot be requested as a qfit USB target state")
    value = qfit.strip().lower()
    node = reviewed_qfit_node_number(value)
    action = "usrpon" if requested_state is PowerState.ON else "usrpoff"

    mutation_returncode: int | None = None
    mutation_transport_error = False
    try:
        mutation = runner(
            ("curl", "-fsS", f"http://reboot{node:02d}/{action}"),
            timeout_seconds,
        )
    except (RuntimeError, OSError):
        mutation_transport_error = True
    else:
        mutation_returncode = mutation.returncode

    observation = observe_qfit_usb_power(
        qfit=value,
        runner=runner,
        timeout_seconds=timeout_seconds,
    )
    return VerifiedQfitUsbOperation(
        qfit=value,
        requested_state=requested_state,
        observed_state=observation.state,
        mutation_returncode=mutation_returncode,
        status_returncode=observation.status_returncode,
        mutation_transport_error=mutation_transport_error,
        status_transport_error=observation.transport_error,
    )


def execute_verified_qfit_transition(
    *,
    qfit: str,
    requested_state: PowerState,
    runner: RemoteRunner,
    timeout_seconds: int,
    sleeper: Sleeper = time.sleep,
    status_attempts: int = 60,
    status_delay_seconds: float = 2.0,
) -> VerifiedQfitOperation:
    """Mutate one qfit and verify its exact R2Lab reboot-node state.

    ``qfit off`` is asynchronous on the live provider. A successful mutation can
    therefore still be followed by a truthful ``rebootNN:on`` observation while
    the FIT host is shutting down. For OFF transitions only, keep observing that
    exact reboot node for a bounded interval. Unknown/transport-error observations
    remain unresolved instead of being converted into success.
    """

    if requested_state is PowerState.UNKNOWN:
        raise ValueError("UNKNOWN cannot be requested as a qfit target state")
    if status_attempts < 1 or status_attempts > 60:
        raise ValueError("qfit status attempts must be between 1 and 60")
    if status_delay_seconds < 0:
        raise ValueError("qfit status delay must not be negative")

    node = qfit_node_number(qfit)
    action = "on" if requested_state is PowerState.ON else "off"

    mutation_returncode: int | None = None
    mutation_transport_error = False
    try:
        mutation = runner(("qfit", action, qfit), timeout_seconds)
    except (RuntimeError, OSError):
        mutation_transport_error = True
    else:
        mutation_returncode = mutation.returncode

    status_returncode: int | None = None
    status_transport_error = False
    observation = QfitStatusObservation(qfit=qfit.strip().lower(), node=node, state=PowerState.UNKNOWN)

    for attempt in range(1, status_attempts + 1):
        status_stdout = ""
        status_stderr = ""
        try:
            status = runner(("rhubarbe", "status", str(node)), timeout_seconds)
        except (RuntimeError, OSError):
            status_transport_error = True
            break
        else:
            status_returncode = status.returncode
            status_stdout = status.stdout
            status_stderr = status.stderr

        observation = parse_qfit_status(
            "\n".join(part for part in (status_stdout, status_stderr) if part), qfit=qfit
        )
        if observation.state is requested_state:
            break

        # The live qfit shutdown path is asynchronous. Poll only while an OFF
        # request is still explicitly observed as ON; UNKNOWN remains unresolved,
        # and ON requests keep their existing one-observation semantics.
        if not (
            requested_state is PowerState.OFF
            and observation.state is PowerState.ON
            and attempt < status_attempts
        ):
            break
        sleeper(status_delay_seconds)

    return VerifiedQfitOperation(
        qfit=observation.qfit,
        requested_state=requested_state,
        observed_state=observation.state,
        mutation_returncode=mutation_returncode,
        status_returncode=status_returncode,
        mutation_transport_error=mutation_transport_error,
        status_transport_error=status_transport_error,
    )


class CleanupState(str, Enum):
    PROVEN_OFF = "proven-off"
    PROVEN_ON = "proven-on"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class CleanupEvidence:
    resource: str
    stage: str
    state: CleanupState
    source: str

    @property
    def clean(self) -> bool:
        return self.state is CleanupState.PROVEN_OFF

    def to_dict(self) -> dict[str, str | bool]:
        return {
            "resource": self.resource,
            "stage": self.stage,
            "state": self.state.value,
            "source": self.source,
            "clean": self.clean,
        }


@dataclass(frozen=True)
class ReleaseAssessment:
    evidence: tuple[CleanupEvidence, ...]

    @classmethod
    def build(cls, evidence: Iterable[CleanupEvidence]) -> "ReleaseAssessment":
        return cls(tuple(evidence))

    @property
    def claim_releasable(self) -> bool:
        return bool(self.evidence) and all(item.clean for item in self.evidence)

    @property
    def unresolved_resources(self) -> tuple[str, ...]:
        return tuple(item.resource for item in self.evidence if not item.clean)

    def to_dict(self) -> dict[str, object]:
        return {
            "claim_releasable": self.claim_releasable,
            "unresolved_resources": list(self.unresolved_resources),
            "evidence": [item.to_dict() for item in self.evidence],
        }


def release_assessment(*, ue: CleanupEvidence, radio: CleanupEvidence) -> ReleaseAssessment:
    """Release a local claim only after both exact resources are proven off."""

    return ReleaseAssessment.build((ue, radio))
