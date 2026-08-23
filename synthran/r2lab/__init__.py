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
    only explicit ``root@qfitNN`` SSH destinations to ``root@fitNN``. Force
    nested FIT SSH to use the already-trusted Faraday known-hosts file because
    the provider SSH client configuration does not select it reliably.
    """

    known_hosts = f"/home/{slice_name}/.ssh/known_hosts"
    translated = [str(item) for item in remote]

    for qfit in _controller.SUPPORTED_QFITS:
        physical = f"fit{qfit[-2:]}"
        translated = [
            item.replace(f"root@{qfit}", f"root@{physical}")
            for item in translated
        ]

        # Controller prepare uses argv tokens for nested SSH.
        if (
            translated
            and translated[0] == "ssh"
            and f"root@{physical}" in translated
            and not any("UserKnownHostsFile=" in item for item in translated)
        ):
            try:
                separator = translated.index("--")
            except ValueError:
                separator = -1
            if separator >= 0:
                translated[separator:separator] = [
                    "-o",
                    f"UserKnownHostsFile={known_hosts}",
                    "-o",
                    "GlobalKnownHostsFile=/dev/null",
                ]

        # UE/runtime/workload paths pass the nested SSH as one shell-escaped
        # command string, so inject the same trust boundary into that form too.
        for index, item in enumerate(translated):
            if f"root@{physical}" not in item or "UserKnownHostsFile=" in item:
                continue
            marker = f"-- root@{physical}"
            if marker not in item:
                continue
            replacement = (
                f"-o UserKnownHostsFile={known_hosts} "
                f"-o GlobalKnownHostsFile=/dev/null {marker}"
            )
            translated[index] = item.replace(marker, replacement)

    return _UNMAPPED_GATEWAY_COMMAND(slice_name, *tuple(translated))


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

# The live qfit image requires packet-service attach before registration becomes
# observable after its reset/init lifecycle. Bind the provider-aligned activation
# implementation into the UE module so direct and authorized callers share the
# same reviewed ordering.
from synthran.r2lab import ue as _ue
from synthran.r2lab.qfit_activation_provider import (
    execute_qfit_activation_provider as _provider_execute_qfit_activation,
)

_ue.execute_qfit_activation = _provider_execute_qfit_activation

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
