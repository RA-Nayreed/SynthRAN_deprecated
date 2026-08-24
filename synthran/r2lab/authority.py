"""Current lease and allocation authority for physical R2Lab mutations."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import TYPE_CHECKING

from synthran.live_preflight import (
    CommandResult,
    verify_allocations,
    verify_reservation,
)

if TYPE_CHECKING:
    from synthran.r2lab.deployment import PhysicalStartAuthority


CORE_NODE = "sopnode-f2"
RAN_NODE = "sopnode-f3"
RADIO = "n300"

Runner = Callable[[Sequence[str], int], CommandResult]
LeaseVerifier = Callable[[], "PhysicalStartAuthority"]
Clock = Callable[[], datetime]


class R2LabPhysicalAuthorityError(RuntimeError):
    """Raised when current physical mutation authority cannot be proven."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _require_reclaim_authority(
    *,
    expected: "PhysicalStartAuthority",
    lease_verifier: LeaseVerifier,
    runner: Runner,
    owner: str,
    reservation_id: str | None,
    timeout_seconds: int,
    clock: Clock,
) -> str:
    current = lease_verifier().validate()
    if current != expected:
        raise R2LabPhysicalAuthorityError(
            "R2Lab claim or selected-resource authority changed"
        )
    try:
        return verify_reservation(
            runner=runner,
            reservation_id=reservation_id,
            owner=owner,
            nodes={CORE_NODE, RAN_NODE},
            now=clock(),
            timeout_seconds=min(timeout_seconds, 60),
        )
    except Exception as exc:
        raise R2LabPhysicalAuthorityError(
            "active SLICES reservation authority was not proven"
        ) from exc


def _observe_selected_allocations(
    *, runner: Runner, timeout_seconds: int
) -> None:
    for node in (CORE_NODE, RAN_NODE):
        try:
            result = runner(
                ("pos", "allocations", "show", node),
                min(timeout_seconds, 60),
            )
        except Exception as exc:
            raise R2LabPhysicalAuthorityError(
                f"current allocation for {node} could not be observed"
            ) from exc
        if result.returncode != 0:
            raise R2LabPhysicalAuthorityError(
                f"current allocation for {node} could not be observed"
            )
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise R2LabPhysicalAuthorityError(
                f"current allocation for {node} returned malformed JSON"
            ) from exc
        if not isinstance(payload, dict):
            raise R2LabPhysicalAuthorityError(
                f"current allocation for {node} returned malformed JSON"
            )
        allocation_id = payload.get("id")
        allocation_owner = payload.get("owner")
        valid_id = (
            isinstance(allocation_id, str) and bool(allocation_id.strip())
        ) or (
            isinstance(allocation_id, int)
            and not isinstance(allocation_id, bool)
            and allocation_id >= 0
        )
        if (
            not valid_id
            or not isinstance(allocation_owner, str)
            or not allocation_owner.strip()
        ):
            raise R2LabPhysicalAuthorityError(
                f"current allocation for {node} is incomplete"
            )


def _run_allocation_mutation(
    *,
    runner: Runner,
    command: Sequence[str],
    label: str,
    timeout_seconds: int,
) -> None:
    try:
        result = runner(command, min(timeout_seconds, 60))
    except Exception as exc:
        raise R2LabPhysicalAuthorityError(f"{label} could not complete") from exc
    if result.returncode != 0:
        raise R2LabPhysicalAuthorityError(f"{label} returned nonzero")


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


def claim_physical_allocation(
    *,
    expected: "PhysicalStartAuthority",
    lease_verifier: LeaseVerifier,
    runner: Runner,
    owner: str,
    allocation_id: str | None,
    timeout_seconds: int,
    clock: Clock = _utc_now,
) -> str:
    """Reuse one owned allocation or reclaim the reserved selected nodes."""

    try:
        return verify_physical_allocation(
            runner=runner,
            owner=owner,
            allocation_id=allocation_id,
            timeout_seconds=timeout_seconds,
        )
    except R2LabPhysicalAuthorityError:
        if allocation_id is not None:
            raise

    reservation_id = _require_reclaim_authority(
        expected=expected,
        lease_verifier=lease_verifier,
        runner=runner,
        owner=owner,
        reservation_id=None,
        timeout_seconds=timeout_seconds,
        clock=clock,
    )
    _observe_selected_allocations(runner=runner, timeout_seconds=timeout_seconds)

    for node in (CORE_NODE, RAN_NODE):
        _require_reclaim_authority(
            expected=expected,
            lease_verifier=lease_verifier,
            runner=runner,
            owner=owner,
            reservation_id=reservation_id,
            timeout_seconds=timeout_seconds,
            clock=clock,
        )
        _run_allocation_mutation(
            runner=runner,
            command=("pos", "allocations", "free", "-k", node),
            label=f"forced allocation release for {node}",
            timeout_seconds=timeout_seconds,
        )

    _require_reclaim_authority(
        expected=expected,
        lease_verifier=lease_verifier,
        runner=runner,
        owner=owner,
        reservation_id=reservation_id,
        timeout_seconds=timeout_seconds,
        clock=clock,
    )
    _run_allocation_mutation(
        runner=runner,
        command=("pos", "allocations", "allocate", CORE_NODE, RAN_NODE),
        label="shared physical allocation creation",
        timeout_seconds=timeout_seconds,
    )
    _require_reclaim_authority(
        expected=expected,
        lease_verifier=lease_verifier,
        runner=runner,
        owner=owner,
        reservation_id=reservation_id,
        timeout_seconds=timeout_seconds,
        clock=clock,
    )
    return verify_physical_allocation(
        runner=runner,
        owner=owner,
        allocation_id=None,
        timeout_seconds=timeout_seconds,
    )


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
        reclaim_conflicts: bool = False,
        clock: Clock = _utc_now,
    ) -> "PhysicalAuthorityGuard":
        expected = lease_verifier().validate()
        if reclaim_conflicts:
            observed_allocation = claim_physical_allocation(
                expected=expected,
                lease_verifier=lease_verifier,
                runner=allocation_runner,
                owner=owner,
                allocation_id=allocation_id,
                timeout_seconds=timeout_seconds,
                clock=clock,
            )
        else:
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
