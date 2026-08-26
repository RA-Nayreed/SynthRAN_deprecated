"""Reviewed stable capability metadata for currently supported testbed resources."""

from __future__ import annotations

from synthran.r2lab.hardware import RADIOS, UES
from synthran.resources.model import ResourceDescriptor


SLICES_COMPUTE = (
    ResourceDescriptor(
        resource_id="sopnode-f1",
        provider="slices",
        kind="compute",
        capabilities=frozenset(
            {
                "compute",
                "role:core",
                "role:ran",
                "role:deployment",
                "kubernetes",
                "interface:ens2f1",
                "storage:sda1",
            }
        ),
        role_priority={"core": 20, "ran": 20, "deployment": 20},
    ),
    ResourceDescriptor(
        resource_id="sopnode-f2",
        provider="slices",
        kind="compute",
        capabilities=frozenset(
            {
                "compute",
                "role:core",
                "role:ran",
                "role:deployment",
                "kubernetes",
                "interface:ens2f1",
                "storage:sda1",
            }
        ),
        role_priority={"core": 0, "ran": 10, "deployment": 10},
    ),
    ResourceDescriptor(
        resource_id="sopnode-f3",
        provider="slices",
        kind="compute",
        capabilities=frozenset(
            {
                "compute",
                "role:core",
                "role:ran",
                "role:deployment",
                "kubernetes",
                "fhi72",
                "interface:ens15f1",
                "storage:sdb2",
            }
        ),
        role_priority={"core": 10, "ran": 0, "deployment": 10},
    ),
    ResourceDescriptor(
        resource_id="sopnode-w3",
        provider="slices",
        kind="compute",
        capabilities=frozenset(
            {
                "compute",
                "role:core",
                "role:ran",
                "role:deployment",
                "kubernetes",
                "interface:enp59s0f1np1",
                "storage:sda1",
            }
        ),
        role_priority={"core": 30, "ran": 30, "deployment": 30},
    ),
)


R2LAB_RADIOS = tuple(
    ResourceDescriptor(
        resource_id=name,
        provider="r2lab",
        kind="radio",
        capabilities=frozenset(
            {
                "radio",
                "backend:r2lab",
                f"hardware:{name}",
                "ran:srsran",
            }
        ),
        role_priority={"radio": index * 10},
    )
    for index, (name, profile) in enumerate(RADIOS.items())
    if profile.executable
)


def _ue_priority(kind: str, mode: str) -> int:
    if kind == "qhat" and mode == "mbim":
        return 10
    if kind == "qhat" and mode == "qmi":
        return 20
    if kind == "qfit" and mode == "mbim":
        return 30
    raise ValueError("executable R2Lab UE has an unsupported selection profile")


R2LAB_UES = tuple(
    ResourceDescriptor(
        resource_id=name,
        provider="r2lab",
        kind="ue",
        capabilities=frozenset({"ue", f"device:{profile.kind}", f"mode:{profile.mode}"}),
        role_priority={"ue": _ue_priority(profile.kind, profile.mode)},
    )
    for name, profile in UES.items()
    if profile.executable
)


VIRTUAL_RESOURCES = (
    ResourceDescriptor(
        resource_id="virtual:rfsim",
        provider="virtual",
        kind="virtual",
        capabilities=frozenset(
            {
                "radio",
                "radio:virtual",
                "backend:rfsim",
                "ran:oai",
                "ran:srsran",
                "ran:ueransim",
            }
        ),
        role_priority={"radio": 0},
    ),
)


def reviewed_resource_descriptors() -> tuple[ResourceDescriptor, ...]:
    """Return stable metadata only; callers must obtain live provider state separately."""

    return SLICES_COMPUTE + R2LAB_RADIOS + R2LAB_UES + VIRTUAL_RESOURCES
