from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import shlex
import tempfile
import unittest
from unittest.mock import patch

from synthran.live_preflight import CommandResult
from synthran.r2lab.acceptance import (
    AcceptanceOutcome,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
)
from synthran.r2lab.deployment import PhysicalStartAuthority
from synthran.r2lab.foundation import (
    R2LabPhysicalFoundationError,
    execute_physical_foundation_acceptance,
)


RUN_ID = "r2lab-current-run"
PREVIOUS_RUN_ID = "r2lab-previous-run"


def authority() -> PhysicalStartAuthority:
    return PhysicalStartAuthority(
        run_id=RUN_ID,
        radio="n300",
        ue="qfit07",
        ue_kind="qfit",
        claim_sha256="a" * 64,
        lease_verified=True,
        radio_state="on",
    ).validate()


class FoundationRunner:
    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.namespace_owner = PREVIOUS_RUN_ID
        self.deployment_owner = PREVIOUS_RUN_ID
        self.ready_nodes = {"sopnode-f2", "sopnode-f3"}
        self.unready_nf: str | None = None
        self.reservation_ids = ["reservation-1"]

    @staticmethod
    def remote(command: tuple[str, ...]) -> tuple[str, ...] | None:
        if not command or command[0] != "ssh":
            return None
        return tuple(shlex.split(command[-1]))

    @staticmethod
    def _node(name: str, ready: bool) -> dict[str, object]:
        return {
            "metadata": {"name": name},
            "status": {
                "conditions": [
                    {"type": "Ready", "status": "True" if ready else "False"}
                ]
            },
        }

    def _core_pod(self, network_function: str) -> dict[str, object]:
        ready = network_function != self.unready_nf
        return {
            "items": [
                {
                    "metadata": {
                        "name": f"open5gs-{network_function}",
                        "labels": {"app": "open5gs", "nf": network_function},
                    },
                    "status": {
                        "containerStatuses": [
                            {"name": network_function, "ready": ready}
                        ],
                    },
                }
            ]
        }

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        if value[:3] == ("pos", "calendar", "list"):
            return CommandResult(
                0,
                json.dumps(
                    [
                        {
                            "id": reservation_id,
                            "owner": "test-owner",
                            "nodes": ["sopnode-f2", "sopnode-f3"],
                            "start_date": "2026-08-24 09:00:00",
                            "end_date": "2026-08-24 12:00:00",
                        }
                        for reservation_id in self.reservation_ids
                    ]
                ),
                "",
            )
        if value[:3] == ("pos", "allocations", "show"):
            return CommandResult(
                0,
                json.dumps(
                    {
                        "id": "allocation-1",
                        "owner": "test-owner",
                    }
                ),
                "",
            )

        remote = self.remote(value)
        if remote is None:
            raise AssertionError(f"unexpected command: {value}")
        if remote == ("kubectl", "get", "nodes", "-o", "json"):
            nodes = [
                self._node(name, name in self.ready_nodes)
                for name in ("sopnode-f2", "sopnode-f3")
            ]
            return CommandResult(0, json.dumps({"items": nodes}), "")
        if (
            remote[:5] == ("kubectl", "get", "pods", "-n", "open5gs")
            and "-l" in remote
            and "app=open5gs" in remote[remote.index("-l") + 1]
        ):
            selector = remote[remote.index("-l") + 1]
            network_function = selector.rsplit("=", 1)[1]
            return CommandResult(0, json.dumps(self._core_pod(network_function)), "")
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
                        "spec": {"replicas": 0},
                    }
                ),
                "",
            )
        if remote[:3] == ("kubectl", "get", "pods"):
            return CommandResult(0, json.dumps({"items": []}), "")
        if remote[:4] == ("kubectl", "label", "namespace", "open5gs"):
            self.namespace_owner = RUN_ID
            return CommandResult(0, "namespace/open5gs labeled\n", "")
        if remote[:3] == ("kubectl", "label", "deployment/srsran-gnb"):
            self.deployment_owner = RUN_ID
            return CommandResult(0, "deployment.apps/srsran-gnb labeled\n", "")
        raise AssertionError(f"unexpected remote command: {remote}")


class R2LabPhysicalFoundationTests(unittest.TestCase):
    NOW = datetime(2026, 8, 24, 7, 30, tzinfo=timezone.utc)

    def run_foundation(
        self,
        runner: FoundationRunner,
        directory: str,
        *,
        reservation_id: str | None = None,
        allocation_id: str | None = None,
    ):
        known_hosts = Path(directory) / "known_hosts"
        known_hosts.write_text(
            "sopnode-f2 ssh-ed25519 AAAATEST\n",
            encoding="utf-8",
        )
        with patch(
            "synthran.r2lab.foundation.authorize_physical_start",
            return_value=authority(),
        ) as authorize:
            result = execute_physical_foundation_acceptance(
                run_id=RUN_ID,
                previous_run_id=PREVIOUS_RUN_ID,
                slice_name="test-slice",
                owner="test-owner",
                reservation_id=reservation_id,
                allocation_id=allocation_id,
                known_hosts=known_hosts,
                now=self.NOW,
                run_root=Path(directory) / "runs",
                foundation_runner=runner,
            )
        self.assertGreaterEqual(authorize.call_count, 3)
        self.assertTrue(
            any(
                command[:3] == ("pos", "calendar", "list")
                for command in runner.commands
            )
        )
        self.assertTrue(
            any(
                command[:3] == ("pos", "allocations", "show")
                for command in runner.commands
            )
        )
        return result

    def test_explicit_slices_identifiers_remain_supported(self) -> None:
        runner = FoundationRunner()
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_foundation(
                runner,
                directory,
                reservation_id="reservation-1",
                allocation_id="allocation-1",
            )

        self.assertTrue(result.handoff.changed)

    def test_ambiguous_active_reservation_blocks_namespace_mutation(self) -> None:
        runner = FoundationRunner()
        runner.reservation_ids.append("reservation-2")
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(
                R2LabPhysicalFoundationError,
                "fresh SLICES authority was not proven",
            ):
                self.run_foundation(runner, directory)

        self.assertEqual(PREVIOUS_RUN_ID, runner.namespace_owner)

    def test_foundation_binds_four_ordered_stages_after_live_proof(self) -> None:
        runner = FoundationRunner()
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_foundation(runner, directory)
            evidence = PhysicalRunEvidence.read_json(result.evidence_path)

        self.assertTrue(result.handoff.changed)
        self.assertEqual(2, result.ready_node_count)
        self.assertEqual(3, result.ready_open5gs_pod_count)
        self.assertEqual(
            PhysicalAcceptanceStage.GNB_N2,
            evidence.acceptance.next_stage,
        )
        for stage in (
            PhysicalAcceptanceStage.RESOURCE_AUTHORITY,
            PhysicalAcceptanceStage.SLICES_FOUNDATION,
            PhysicalAcceptanceStage.KUBERNETES,
            PhysicalAcceptanceStage.OPEN5GS,
        ):
            self.assertEqual(AcceptanceOutcome.PASSED, evidence.acceptance.outcome_for(stage))

    def test_unready_kubernetes_node_blocks_namespace_mutation_and_evidence(self) -> None:
        runner = FoundationRunner()
        runner.ready_nodes.remove("sopnode-f3")
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(
                R2LabPhysicalFoundationError,
                "both selected SLICES nodes Ready",
            ):
                self.run_foundation(runner, directory)
            self.assertFalse((Path(directory) / "runs" / RUN_ID).exists())
        self.assertEqual(PREVIOUS_RUN_ID, runner.namespace_owner)

    def test_unready_open5gs_pod_blocks_namespace_mutation_and_evidence(self) -> None:
        runner = FoundationRunner()
        runner.unready_nf = "amf"
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(
                R2LabPhysicalFoundationError,
                "not Running and ready",
            ):
                self.run_foundation(runner, directory)
            self.assertFalse((Path(directory) / "runs" / RUN_ID).exists())
        self.assertEqual(PREVIOUS_RUN_ID, runner.namespace_owner)

    def test_retry_reverifies_live_state_without_rewriting_evidence(self) -> None:
        runner = FoundationRunner()
        with tempfile.TemporaryDirectory() as directory:
            first = self.run_foundation(runner, directory)
            before = first.evidence_path.read_bytes()
            second = self.run_foundation(runner, directory)
            after = second.evidence_path.read_bytes()

        self.assertTrue(first.handoff.changed)
        self.assertFalse(second.handoff.changed)
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
