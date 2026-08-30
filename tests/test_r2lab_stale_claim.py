from __future__ import annotations

import argparse
from contextlib import redirect_stdout
import hashlib
from io import StringIO
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from synthran.live_preflight import CommandResult
from synthran.operator import release_command
from synthran.r2lab.hardware import PhysicalTopology
from synthran.r2lab.resources import R2LabTopologyResourceError
from synthran.r2lab.stale_claim import retire_if_lease_absent


class LeaseRunner:
    def __init__(self, *, gateway_ok: bool = True, lease_ok: bool = False) -> None:
        self.gateway_ok = gateway_ok
        self.lease_ok = lease_ok
        self.commands: list[tuple[str, ...]] = []

    @staticmethod
    def remote(command: tuple[str, ...]) -> tuple[str, ...]:
        split = command.index("--")
        return command[split + 2 :]

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        remote = self.remote(value)
        if remote == ("true",):
            return CommandResult(0 if self.gateway_ok else 255, "", "")
        if remote == ("rhubarbe", "leases", "--check"):
            return CommandResult(0 if self.lease_ok else 1, "", "")
        raise AssertionError(f"unexpected provider command: {remote}")

    @property
    def remote_commands(self) -> list[tuple[str, ...]]:
        return [self.remote(command) for command in self.commands]


def topology() -> PhysicalTopology:
    return PhysicalTopology(
        core_node="sopnode-f2",
        ran_node="sopnode-f3",
        radio="n300",
        ue="qfit07",
    ).validate()


def write_claim(root: Path, run_id: str, slice_name: str) -> Path:
    selected = topology()
    run_directory = root / run_id
    run_directory.mkdir(parents=True, exist_ok=True)
    claim = root / "active.json"
    claim.write_text(
        json.dumps(
            {
                "schema": "synthran/r2lab-claim/v1alpha2",
                "run_id": run_id,
                "slice_fingerprint": hashlib.sha256(
                    slice_name.encode("utf-8")
                ).hexdigest(),
                "core_node": selected.core_node,
                "ran_node": selected.ran_node,
                "radio": selected.radio,
                "ue": selected.ue,
                "created_at_utc": "2026-08-25T20:20:06Z",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return claim


class R2LabStaleClaimTests(unittest.TestCase):
    def test_expired_lease_retires_only_local_claim_and_preserves_evidence(self) -> None:
        runner = LeaseRunner(lease_ok=False)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            run_id = "r2lab-stale-001"
            active = write_claim(root, run_id, "oulu_user")
            preserved = root / run_id / "physical-run.json"
            preserved.write_text('{"accepted": false}\n', encoding="utf-8")

            retired = retire_if_lease_absent(
                run_root=root,
                run_id=run_id,
                slice_name="oulu_user",
                topology=topology(),
                runner=runner,
            )

            self.assertIsNotNone(retired)
            self.assertFalse(active.exists())
            self.assertTrue((root / run_id / "retired-claim.json").is_file())
            retirement = json.loads(
                (root / run_id / "claim-retirement.json").read_text(encoding="utf-8")
            )
            self.assertTrue(retirement["retired"])
            self.assertFalse(retirement["hardware_mutated"])
            self.assertEqual("current-r2lab-lease-not-held", retirement["reason"])
            self.assertEqual('{"accepted": false}\n', preserved.read_text(encoding="utf-8"))

        self.assertEqual(
            [("true",), ("rhubarbe", "leases", "--check")],
            runner.remote_commands,
        )

    def test_current_lease_keeps_claim_for_exact_cleanup(self) -> None:
        runner = LeaseRunner(lease_ok=True)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            run_id = "r2lab-current-001"
            active = write_claim(root, run_id, "oulu_user")

            retired = retire_if_lease_absent(
                run_root=root,
                run_id=run_id,
                slice_name="oulu_user",
                topology=topology(),
                runner=runner,
            )

            self.assertIsNone(retired)
            self.assertTrue(active.is_file())
            self.assertFalse((root / run_id / "claim-retirement.json").exists())

    def test_gateway_failure_does_not_retire_claim(self) -> None:
        runner = LeaseRunner(gateway_ok=False)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            run_id = "r2lab-transport-001"
            active = write_claim(root, run_id, "oulu_user")

            with self.assertRaisesRegex(
                R2LabTopologyResourceError, "gateway could not be verified"
            ):
                retire_if_lease_absent(
                    run_root=root,
                    run_id=run_id,
                    slice_name="oulu_user",
                    topology=topology(),
                    runner=runner,
                )

            self.assertTrue(active.is_file())
            self.assertFalse((root / run_id / "claim-retirement.json").exists())

    def test_mismatched_claim_is_never_discarded(self) -> None:
        runner = LeaseRunner(lease_ok=False)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            run_id = "r2lab-mismatch-001"
            active = write_claim(root, run_id, "another_slice")

            with self.assertRaisesRegex(
                R2LabTopologyResourceError, "does not match"
            ):
                retire_if_lease_absent(
                    run_root=root,
                    run_id=run_id,
                    slice_name="oulu_user",
                    topology=topology(),
                    runner=runner,
                )

            self.assertTrue(active.is_file())

    def test_release_retires_expired_claim_without_cleanup_credentials(self) -> None:
        runner = LeaseRunner(lease_ok=False)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_id = "r2lab-release-stale-001"
            write_claim(root / ".synthran" / "r2lab", run_id, "oulu_user")
            args = argparse.Namespace(
                run_id=run_id,
                r2lab_slice="oulu_user",
                owner=None,
                allocation_id=None,
                known_hosts=None,
                timeout=30,
                json=True,
            )
            output = StringIO()
            previous = Path.cwd()
            os.chdir(root)
            try:
                with (
                    patch("synthran.operator.load_topology", return_value=topology()),
                    patch("synthran.operator.r2lab_runner", runner),
                    patch("synthran.operator.release_physical_resources") as release,
                    redirect_stdout(output),
                ):
                    self.assertEqual(0, release_command(args))
                release.assert_not_called()
            finally:
                os.chdir(previous)

            payload = json.loads(output.getvalue())
            self.assertTrue(payload["retired"])
            self.assertFalse(payload["released"])
            self.assertFalse(payload["hardware_mutated"])
            self.assertFalse((root / ".synthran" / "r2lab" / "active.json").exists())


if __name__ == "__main__":
    unittest.main()
