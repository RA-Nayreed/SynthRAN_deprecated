"""Backend lifecycle contract shared by virtual and physical integrations."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Protocol, runtime_checkable


class LifecycleStage(StrEnum):
    ACCESS = "access"
    RESOURCES = "resources"
    KUBERNETES = "kubernetes"
    CORE = "core"
    GNB = "gnb"
    N2 = "n2"
    UE_MANAGEMENT = "ue-management"
    CELL = "cell"
    REGISTRATION = "registration"
    PDU = "pdu"
    USER_PLANE = "user-plane"
    WORKLOAD = "workload"
    DATA = "data"
    ACCEPTANCE = "acceptance"
    CLEANUP = "cleanup"


LIFECYCLE_STAGES = tuple(LifecycleStage)
BackendName = Literal["rfsim", "r2lab"]
RadioMode = Literal["virtual", "physical"]


@dataclass(frozen=True)
class BackendContract:
    """Static implementation capability without claiming current live acceptance."""

    name: BackendName
    radio_mode: RadioMode
    implemented_stages: tuple[LifecycleStage, ...]

    def __post_init__(self) -> None:
        expected = LIFECYCLE_STAGES[: len(self.implemented_stages)]
        if self.implemented_stages != expected:
            raise ValueError(
                "backend lifecycle capability must be a contiguous prefix of the contract"
            )

    def supports(self, stage: LifecycleStage) -> bool:
        return stage in self.implemented_stages


class BackendError(RuntimeError):
    """A backend command could not be completed through its integration boundary."""


@runtime_checkable
class Backend(Protocol):
    contract: BackendContract

    def configure_parser(self, parser: argparse.ArgumentParser) -> None:
        """Add backend-owned commands to the single SynthRAN parser."""
        ...

    def dispatch(self, args: argparse.Namespace) -> int:
        """Execute one already-parsed backend command."""
        ...
