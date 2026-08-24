from __future__ import annotations

import json
from pathlib import Path
import shlex
import tempfile
import unittest

from synthran.live_preflight import CommandResult
from synthran.r2lab.handoff import (
    R2LabPhysicalHandoffError,
    execute_physical_namespace_handoff,
)


class HandoffRunner:
    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.namespace_owner = "r2lab-previous-run"
        self.deployment_owner = "r2lab-previous-run"
        self.replicas = 0
        self.pods: list[dict[str, object]] = []

    @staticmethod
    def remote(command: tuple[str, ...]) -> tuple[str, ...] | None:
        if not command or command[0] != "ssh":
            return None
        return tuple(shlex.split(command[-1]))

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)

        remote = self.remote(value)
        if remote is None:
            raise AssertionError(f"unexpected command: {value}")

        if remote[:4] == ("kubectl", "get", "namespace", "open5gs"):
            return CommandResult(0, self.namespace_owner, "")

        if remote[:3] == ("kubectl", "get", "deployment/srsran-gnb"):
            if any(item.startswith("jsonpath=") for item in remote):
                return CommandResult(0, self.deployment_owner, "")
            return CommandResult(
                0,
                json.dumps(
                    {
                        "metadata": {
                            "labels": {"synthran.run/id": self.deployment_owner}
                        },
                        "spec": {"replicas": self.replicas},
                    }
                ),
                "",
            )

        if remote[:3] == ("kubectl", "get", "pods"):
            return CommandResult(0, json.dumps({"items": self.pods}), "")

        if remote[:4] == ("kubectl", "label", "namespace", "open5gs"):
            assignment = next(
                item for item in remote if item.startswith("synthran.run/id=")
            )
            self.namespace_owner = assignment.split("=", 1)[1]
            return CommandResult(0, "namespace/open5gs labeled\n", "")

        if remote[:3] == ("kubectl", "label", "deployment/srsran-gnb"):
            assignment = next(
                item for item in remote if item.startswith("synthran.run/id=")
            )
            self.deployment_owner = assignment.split("=", 1)[1]
            return CommandResult(0, "deployment.apps/srsran-gnb labeled\n", "")

        raise AssertionError(f"unexpected remote command: {remote}")

    @property
    def remote_commands(self) -> list[tuple[str, ...]]:
        result: list[tuple[str, ...]] = []
        for command in self.commands:
            remote = self.remote(command)
            if remote is not None:
                result.append(remote)
        return result


class R2LabPhysicalNamespaceHandoffTests(unittest.TestCase):
    def run_handoff(
        self,
        runner: HandoffRunner,
        *,
        from_run_id: str = "r2lab-previous-run",
        to_run_id: str = "r2lab-current-run",
        authority_verifier=None,
    ):
        with tempfile.TemporaryDirectory() as directory:
            known_hosts = Path(directory) / "known_hosts"
            known_hosts.write_text(
                "sopnode-f2 ssh-ed25519 AAAATEST\n",
                encoding="utf-8",
            )
            return execute_physical_namespace_handoff(
                from_run_id=from_run_id,
                to_run_id=to_run_id,
                known_hosts=known_hosts,
                runner=runner,
                authority_verifier=authority_verifier or (lambda: None),
            )

    def test_handoff_rebinds_only_clean_namespace_and_reobserves_owner(self) -> None:
        runner = HandoffRunner()

        result = self.run_handoff(runner)

        self.assertTrue(result.changed)
        self.assertTrue(result.deployment_present)
        self.assertEqual(0, result.desired_replicas)
        self.assertEqual(0, result.gnb_pod_count)
        self.assertEqual("r2lab-current-run", runner.namespace_owner)
        self.assertEqual("r2lab-current-run", runner.deployment_owner)

        labels = [
            command
            for command in runner.remote_commands
            if command[:4] == ("kubectl", "label", "namespace", "open5gs")
        ]
        self.assertEqual(1, len(labels))
        self.assertIn("synthran.run/id=r2lab-current-run", labels[0])
        self.assertIn("--overwrite", labels[0])

        deployment_labels = [
            command
            for command in runner.remote_commands
            if command[:3] == ("kubectl", "label", "deployment/srsran-gnb")
        ]
        self.assertEqual(1, len(deployment_labels))
        self.assertIn("synthran.run/id=r2lab-current-run", deployment_labels[0])
        self.assertIn("--overwrite", deployment_labels[0])

        command_text = "\n".join(" ".join(command) for command in runner.commands)
        self.assertNotIn("helm", command_text)
        self.assertNotIn("--replicas=1", command_text)
        self.assertNotIn("rhubarbe", command_text)
        self.assertNotIn("qfit", command_text)
        self.assertNotIn("all-off", command_text)

        reservation_queries = [
            command
            for command in runner.commands
            if command[:3] == ("pos", "calendar", "list")
        ]
        allocation_queries = [
            command
            for command in runner.commands
            if command[:3] == ("pos", "allocations", "show")
        ]
        self.assertEqual(0, len(reservation_queries))
        self.assertEqual(0, len(allocation_queries))

    def test_external_authority_verifier_guards_both_boundaries(self) -> None:
        runner = HandoffRunner()
        verification_count = 0

        def verify_authority() -> None:
            nonlocal verification_count
            verification_count += 1

        self.run_handoff(
            runner,
            authority_verifier=verify_authority,
        )

        self.assertEqual(2, verification_count)
        self.assertFalse(
            any(
                command[:3] == ("pos", "calendar", "list")
                for command in runner.commands
            )
        )
        allocation_queries = [
            command
            for command in runner.commands
            if command[:3] == ("pos", "allocations", "show")
        ]
        self.assertEqual(0, len(allocation_queries))

    def test_retry_after_complete_handoff_performs_no_write(self) -> None:
        runner = HandoffRunner()
        first = self.run_handoff(runner)
        self.assertTrue(first.changed)

        labels_before = len(
            [
                command
                for command in runner.remote_commands
                if command[:4] == ("kubectl", "label", "namespace", "open5gs")
            ]
        )
        second = self.run_handoff(runner)
        labels_after = len(
            [
                command
                for command in runner.remote_commands
                if command[:4] == ("kubectl", "label", "namespace", "open5gs")
            ]
        )

        self.assertFalse(second.changed)
        self.assertEqual(labels_before, labels_after)
        self.assertEqual("r2lab-current-run", runner.deployment_owner)
        self.assertEqual("r2lab-current-run", runner.namespace_owner)

    def test_retry_completes_partial_deployment_handoff(self) -> None:
        runner = HandoffRunner()
        runner.namespace_owner = "r2lab-current-run"

        result = self.run_handoff(runner)

        self.assertTrue(result.changed)
        self.assertEqual("r2lab-current-run", runner.namespace_owner)
        self.assertEqual("r2lab-current-run", runner.deployment_owner)
        namespace_labels = [
            command
            for command in runner.remote_commands
            if command[:4] == ("kubectl", "label", "namespace", "open5gs")
        ]
        deployment_labels = [
            command
            for command in runner.remote_commands
            if command[:3] == ("kubectl", "label", "deployment/srsran-gnb")
        ]
        self.assertEqual([], namespace_labels)
        self.assertEqual(1, len(deployment_labels))

    def test_consecutive_handoffs_transfer_namespace_and_deployment(self) -> None:
        runner = HandoffRunner()
        first = self.run_handoff(runner)
        self.assertTrue(first.changed)

        second = self.run_handoff(
            runner,
            from_run_id="r2lab-current-run",
            to_run_id="r2lab-next-run",
        )

        self.assertTrue(second.changed)
        self.assertEqual("r2lab-next-run", runner.namespace_owner)
        self.assertEqual("r2lab-next-run", runner.deployment_owner)

    def test_handoff_rejects_unexpected_namespace_owner_before_mutation(self) -> None:
        runner = HandoffRunner()
        runner.namespace_owner = "other-run"

        with self.assertRaisesRegex(
            R2LabPhysicalHandoffError,
            "neither the expected previous run nor the new run",
        ):
            self.run_handoff(runner)

        self.assertFalse(
            any(
                command[:4] == ("kubectl", "label", "namespace", "open5gs")
                for command in runner.remote_commands
            )
        )

    def test_handoff_rejects_running_gnb_before_mutation(self) -> None:
        runner = HandoffRunner()
        runner.replicas = 1

        with self.assertRaisesRegex(R2LabPhysicalHandoffError, "replicas=0"):
            self.run_handoff(runner)

        self.assertFalse(
            any(
                command[:4] == ("kubectl", "label", "namespace", "open5gs")
                for command in runner.remote_commands
            )
        )

    def test_handoff_rejects_remaining_gnb_pod_before_mutation(self) -> None:
        runner = HandoffRunner()
        runner.pods = [{"metadata": {"name": "gnb-old"}}]

        with self.assertRaisesRegex(R2LabPhysicalHandoffError, "zero pods"):
            self.run_handoff(runner)

        self.assertFalse(
            any(
                command[:4] == ("kubectl", "label", "namespace", "open5gs")
                for command in runner.remote_commands
            )
        )

    def test_handoff_rechecks_authority_immediately_before_write(self) -> None:
        runner = HandoffRunner()
        calls = 0

        def changing_authority() -> None:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("authority changed")

        with self.assertRaisesRegex(
            R2LabPhysicalHandoffError,
            "authority changed before namespace ownership handoff",
        ):
            self.run_handoff(runner, authority_verifier=changing_authority)

        self.assertFalse(
            any(
                command[:4] == ("kubectl", "label", "namespace", "open5gs")
                for command in runner.remote_commands
            )
        )


if __name__ == "__main__":
    unittest.main()
