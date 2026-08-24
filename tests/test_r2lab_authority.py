from __future__ import annotations

import json
from datetime import datetime, timezone
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


class ReclaimRunner:
    def __init__(self, *, reservation_active: bool = True) -> None:
        self.reservation_active = reservation_active
        self.allocations = {
            "sopnode-f2": ("foreign-core", "other-owner"),
            "sopnode-f3": ("foreign-ran", "other-owner"),
        }
        self.commands: list[tuple[str, ...]] = []

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        if value[:3] == ("pos", "calendar", "list"):
            records = []
            if self.reservation_active:
                records.append(
                    {
                        "id": 42,
                        "owner": "test-owner",
                        "nodes": ["sopnode-f2", "sopnode-f3"],
                        "start_date": "2026-08-24T08:00:00Z",
                        "end_date": "2026-08-24T10:00:00Z",
                    }
                )
            return CommandResult(0, json.dumps(records), "")
        if value[:3] == ("pos", "allocations", "show"):
            allocation_id, owner = self.allocations[value[3]]
            return CommandResult(
                0,
                json.dumps({"id": allocation_id, "owner": owner}),
                "",
            )
        if value[:3] == ("pos", "allocations", "free"):
            del self.allocations[value[4]]
            return CommandResult(0, "released\n", "")
        if value[:3] == ("pos", "allocations", "allocate"):
            self.allocations = {
                node: ("owned-shared", "test-owner") for node in value[3:]
            }
            return CommandResult(0, "allocated\n", "")
        raise AssertionError(f"unexpected command: {value}")


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

    def test_guard_reclaims_reserved_nodes_and_proves_shared_allocation(self) -> None:
        runner = ReclaimRunner()
        now = datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc)

        guard = PhysicalAuthorityGuard.open(
            lease_verifier=authority,
            allocation_runner=runner,
            owner="test-owner",
            allocation_id=None,
            timeout_seconds=120,
            reclaim_conflicts=True,
            clock=lambda: now,
        )
        guard.verify()

        self.assertEqual("owned-shared", guard.allocation_id)
        mutations = [
            command
            for command in runner.commands
            if command[:3]
            in {
                ("pos", "allocations", "free"),
                ("pos", "allocations", "allocate"),
            }
        ]
        self.assertEqual(
            [
                ("pos", "allocations", "free", "-k", "sopnode-f2"),
                ("pos", "allocations", "free", "-k", "sopnode-f3"),
                (
                    "pos",
                    "allocations",
                    "allocate",
                    "sopnode-f2",
                    "sopnode-f3",
                ),
            ],
            mutations,
        )
        self.assertGreaterEqual(
            sum(
                command[:3] == ("pos", "calendar", "list")
                for command in runner.commands
            ),
            4,
        )

    def test_guard_does_not_reclaim_without_active_reservation(self) -> None:
        runner = ReclaimRunner(reservation_active=False)
        now = datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc)

        with self.assertRaisesRegex(
            R2LabPhysicalAuthorityError,
            "active SLICES reservation authority was not proven",
        ):
            PhysicalAuthorityGuard.open(
                lease_verifier=authority,
                allocation_runner=runner,
                owner="test-owner",
                allocation_id=None,
                timeout_seconds=120,
                reclaim_conflicts=True,
                clock=lambda: now,
            )

        self.assertFalse(
            any(
                command[:3]
                in {
                    ("pos", "allocations", "free"),
                    ("pos", "allocations", "allocate"),
                }
                for command in runner.commands
            )
        )

    def test_guard_rechecks_reservation_before_first_release(self) -> None:
        runner = ReclaimRunner()
        observed_times = iter(
            (
                datetime(2026, 8, 24, 9, 0, tzinfo=timezone.utc),
                datetime(2026, 8, 24, 10, 0, tzinfo=timezone.utc),
            )
        )

        with self.assertRaisesRegex(
            R2LabPhysicalAuthorityError,
            "active SLICES reservation authority was not proven",
        ):
            PhysicalAuthorityGuard.open(
                lease_verifier=authority,
                allocation_runner=runner,
                owner="test-owner",
                allocation_id=None,
                timeout_seconds=120,
                reclaim_conflicts=True,
                clock=lambda: next(observed_times),
            )

        self.assertFalse(
            any(
                command[:3] == ("pos", "allocations", "free")
                for command in runner.commands
            )
        )

    def test_explicit_allocation_id_disables_reclamation(self) -> None:
        runner = ReclaimRunner()

        with self.assertRaisesRegex(
            R2LabPhysicalAuthorityError,
            "allocation was not proven",
        ):
            PhysicalAuthorityGuard.open(
                lease_verifier=authority,
                allocation_runner=runner,
                owner="test-owner",
                allocation_id="expected-allocation",
                timeout_seconds=120,
                reclaim_conflicts=True,
            )

        self.assertFalse(
            any(
                command[:3] == ("pos", "calendar", "list")
                for command in runner.commands
            )
        )


if __name__ == "__main__":
    unittest.main()
