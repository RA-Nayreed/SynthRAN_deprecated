"""Current lease and allocation authority for physical R2Lab mutations."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from synthran.live_preflight import CommandResult, verify_allocations

if TYPE_CHECKING:
    from synthran.r2lab.deployment import PhysicalStartAuthority


CORE_NODE = "sopnode-f2"
RAN_NODE = "sopnode-f3"
RADIO = "n300"

Runner = Callable[[Sequence[str], int], CommandResult]
LeaseVerifier = Callable[[], "PhysicalStartAuthority"]


class R2LabPhysicalAuthorityError(RuntimeError):
    """Raised when current physical mutation authority cannot be proven."""


def verify_physical_allocation(
    *,
    runner: Runner,
    owner: str,
    allocation_id: str | None,
    timeout_seconds: int,
) -> str:
    """Prove that both physical nodes share one current owned allocation."""

    try:
        return verify_allocations(
            runner=runner,
            allocation_id=allocation_id,
            owner=owner,
            nodes={CORE_NODE, RAN_NODE},
            timeout_seconds=min(timeout_seconds, 60),
        )
    except Exception as exc:
        raise R2LabPhysicalAuthorityError(
            "current physical SLICES allocation was not proven"
        ) from exc


@dataclass(frozen=True)
class PhysicalAuthorityGuard:
    """Refresh one exact R2Lab claim and its matching SLICES allocation."""

    expected: "PhysicalStartAuthority"
    lease_verifier: LeaseVerifier
    allocation_runner: Runner
    owner: str
    allocation_id: str
    timeout_seconds: int

    @classmethod
    def open(
        cls,
        *,
        lease_verifier: LeaseVerifier,
        allocation_runner: Runner,
        owner: str,
        allocation_id: str | None,
        timeout_seconds: int,
    ) -> "PhysicalAuthorityGuard":
        expected = lease_verifier().validate()
        observed_allocation = verify_physical_allocation(
            runner=allocation_runner,
            owner=owner,
            allocation_id=allocation_id,
            timeout_seconds=timeout_seconds,
        )
        return cls(
            expected=expected,
            lease_verifier=lease_verifier,
            allocation_runner=allocation_runner,
            owner=owner,
            allocation_id=observed_allocation,
            timeout_seconds=timeout_seconds,
        )

    def verify(self) -> "PhysicalStartAuthority":
        current = self.lease_verifier().validate()
        if current != self.expected:
            raise R2LabPhysicalAuthorityError(
                "R2Lab claim or selected-resource authority changed"
            )
        verify_physical_allocation(
            runner=self.allocation_runner,
            owner=self.owner,
            allocation_id=self.allocation_id,
            timeout_seconds=self.timeout_seconds,
        )
        return current
