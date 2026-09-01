"""Experiment-side data records for Amber traffic through a physical UE."""

from __future__ import annotations

from dataclasses import dataclass
import re


UE_INTERFACE = "wwan0"


class R2LabPhysicalUeError(ValueError):
    """Raised when physical experiment evidence is not bound to the selected UE path."""


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
        if not self.run_id or not self.ue:
            raise R2LabPhysicalUeError("physical workload context identity is incomplete")

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
