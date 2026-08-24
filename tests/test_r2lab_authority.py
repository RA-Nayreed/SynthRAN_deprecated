from __future__ import annotations

import json
import unittest

from synthran.live_preflight import CommandResult
from synthran.r2lab.authority import (
    PhysicalAuthorityGuard,
    R2LabPhysicalAuthorityError,
)
from synthran.r2lab.deployment import PhysicalStartAuthority


def authority(*, digest: str = "a" * 64) -> PhysicalStartAuthority:
    return PhysicalStartAuthority(
        run_id="r2lab-authority-test",
        radio="n300",
        ue="qfit07",
        ue_kind="qfit",
        claim_sha256=digest,
        lease_verified=True,
        radio_state="on",
    )


class AllocationRunner:
    def __init__(self) -> None:
        self.allocation_id = "allocation-1"
        self.commands: list[tuple[str, ...]] = []

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        if value[:3] != ("pos", "allocations", "show"):
            raise AssertionError(f"unexpected command: {value}")
        return CommandResult(
            0,
            json.dumps({"id": self.allocation_id, "owner": "test-owner"}),
            "",
        )


class PhysicalAuthorityGuardTests(unittest.TestCase):
    def test_guard_reuses_one_exact_live_boundary(self) -> None:
        runner = AllocationRunner()
        lease_calls = 0

        def verify_lease() -> PhysicalStartAuthority:
            nonlocal lease_calls
            lease_calls += 1
            return authority()

        guard = PhysicalAuthorityGuard.open(
            lease_verifier=verify_lease,
            allocation_runner=runner,
            owner="test-owner",
            allocation_id=None,
            timeout_seconds=120,
        )
        guard.verify()

        self.assertEqual("allocation-1", guard.allocation_id)
        self.assertEqual(2, lease_calls)
        self.assertEqual(4, len(runner.commands))

    def test_guard_rejects_changed_r2lab_claim(self) -> None:
        runner = AllocationRunner()
        observed = iter((authority(), authority(digest="b" * 64)))
        guard = PhysicalAuthorityGuard.open(
            lease_verifier=lambda: next(observed),
            allocation_runner=runner,
            owner="test-owner",
            allocation_id="allocation-1",
            timeout_seconds=120,
        )

        with self.assertRaisesRegex(
            R2LabPhysicalAuthorityError,
            "claim or selected-resource authority changed",
        ):
            guard.verify()

    def test_guard_rejects_changed_allocation(self) -> None:
        runner = AllocationRunner()
        guard = PhysicalAuthorityGuard.open(
            lease_verifier=authority,
            allocation_runner=runner,
            owner="test-owner",
            allocation_id=None,
            timeout_seconds=120,
        )
        runner.allocation_id = "allocation-2"

        with self.assertRaisesRegex(
            R2LabPhysicalAuthorityError,
            "allocation was not proven",
        ):
            guard.verify()


if __name__ == "__main__":
    unittest.main()
