"""Cohesive physical R2Lab backend for SynthRAN.

The package is organized by subsystem rather than one file per live discovery:
provider control, radio/UE state, deployment, acceptance, runtime verification,
mutating UE/session lifecycle, and orchestration.
"""

from synthran.r2lab import controller as _controller


_UNMAPPED_GATEWAY_COMMAND = _controller.gateway_command


def _mapped_gateway_command(slice_name: str, *remote: str) -> tuple[str, ...]:
    """Map logical qfitNN resources to their physical fitNN SSH hosts.

    Every nested qfit SSH path enters through the Faraday gateway boundary.
    Keep the logical qfit identifier for provider ownership while translating
    only explicit ``root@qfitNN`` SSH destinations to ``root@fitNN``.  Force
    nested FIT SSH to use the already-trusted Faraday known-hosts file because
    the provider SSH client configuration does not select it reliably.
    """

    translated = tuple(remote)
    known_hosts = f"/home/{slice_name}/.ssh/known_hosts"
    for qfit in _controller.SUPPORTED_QFITS:
        physical = f"fit{qfit[-2:]}"
        mapped: list[str] = []
        for item in translated:
            value = item.replace(f"root@{qfit}", f"root@{physical}")
            if f"root@{physical}" in value and "UserKnownHostsFile=" not in value:
                marker = f"-- root@{physical}"
                replacement = (
                    f"-o UserKnownHostsFile={known_hosts} "
                    f"-o GlobalKnownHostsFile=/dev/null {marker}"
                )
                value = value.replace(marker, replacement)
            mapped.append(value)
        translated = tuple(mapped)
    return _UNMAPPED_GATEWAY_COMMAND(slice_name, *translated)


# Functions already defined in controller resolve gateway_command through that
# module's globals at call time, so this one binding also covers prepare/start.
# UE/runtime/workload code that imports controller.gateway_command sees the same
# mapped boundary.
_controller.gateway_command = _mapped_gateway_command

from synthran.r2lab.controller import (
    R2LabDoctorReport,
    R2LabPlan,
    R2LabResourceError,
    R2LabResult,
    R2LabSelection,
    build_plan,
    execute_physical_gnb_start,
    execute_prepare,
    execute_release,
    gateway_command,
    run_doctor,
)
from synthran.r2lab.runtime import (
    GnbN2Evidence,
    N2State,
    PhysicalRuntimeVerificationResult,
    R2LabRuntimeVerificationError,
    execute_physical_runtime_verification,
    execute_qfit_management_probe,
    execute_qfit_runtime_probe,
    parse_n2_log_state,
    verify_gnb_n2,
)
from synthran.r2lab.ue import (
    AuthorizedQfitActivationOutcome,
    AuthorizedQfitUserPlaneOutcome,
    PhysicalWorkloadContext,
    PhysicalWorkloadHandoffOutcome,
    PhysicalWorkloadResult,
    QfitActivationRequest,
    QfitActivationResult,
    R2LabQfitActivationError,
    execute_authorized_qfit_activation,
    execute_authorized_qfit_user_plane,
    execute_physical_workload_handoff,
    execute_qfit_activation,
)

__all__ = [
    "AuthorizedQfitActivationOutcome",
    "AuthorizedQfitUserPlaneOutcome",
    "GnbN2Evidence",
    "N2State",
    "PhysicalRuntimeVerificationResult",
    "PhysicalWorkloadContext",
    "PhysicalWorkloadHandoffOutcome",
    "PhysicalWorkloadResult",
    "QfitActivationRequest",
    "QfitActivationResult",
    "R2LabDoctorReport",
    "R2LabPlan",
    "R2LabQfitActivationError",
    "R2LabResourceError",
    "R2LabResult",
    "R2LabRuntimeVerificationError",
    "R2LabSelection",
    "build_plan",
    "execute_authorized_qfit_activation",
    "execute_authorized_qfit_user_plane",
    "execute_physical_gnb_start",
    "execute_physical_runtime_verification",
    "execute_physical_workload_handoff",
    "execute_prepare",
    "execute_qfit_activation",
    "execute_qfit_management_probe",
    "execute_qfit_runtime_probe",
    "execute_release",
    "gateway_command",
    "parse_n2_log_state",
    "run_doctor",
    "verify_gnb_n2",
]
