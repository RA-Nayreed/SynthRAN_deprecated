from __future__ import annotations

import json
from pathlib import Path
import shlex
import tempfile
import unittest
from unittest.mock import ANY, patch

from synthran.live_preflight import CommandResult
from synthran.r2lab.acceptance import (
    AcceptanceOutcome,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
)
from synthran.r2lab.deployment import PhysicalStartAuthority
from synthran.r2lab.foundation import (
    Open5gsFoundationResult,
    R2LabPhysicalFoundationError,
    execute_physical_foundation_acceptance,
    reconcile_open5gs_foundation,
)


RUN_ID = "r2lab-current-run"
PREVIOUS_RUN_ID = "r2lab-previous-run"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


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
        self.missing_nf: str | None = None
        self.duplicate_nf: str | None = None

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
        if network_function == self.missing_nf:
            return {"items": []}
        ready = network_function != self.unready_nf
        item = {
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
        items = [item]
        if network_function == self.duplicate_nf:
            items.append(item)
        return {"items": items}

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        if value[:3] == ("pos", "calendar", "list"):
            return CommandResult(
                0,
                json.dumps(
                    [
                        {
                            "id": "reservation-1",
                            "owner": "test-owner",
                            "nodes": ["sopnode-f2", "sopnode-f3"],
                            "start_date": "2026-08-24 09:00:00",
                            "end_date": "2026-08-24 12:00:00",
                        }
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


class ReconciliationRunner:
    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []

    def __call__(self, command, cwd, environment, timeout_seconds):
        value = tuple(command)
        self.commands.append(value)
        if len(value) >= 8 and value[0] == "git" and value[3:5] == (
            "worktree",
            "add",
        ):
            Path(value[-2]).mkdir(parents=True)
        if value == ("git", "rev-parse", "HEAD"):
            return CommandResult(
                0,
                "a0149fc0dde39e2872945a0f3c91e804ece52d4f\n",
                "",
            )
        return CommandResult(0, "ok\n", "")


class R2LabPhysicalFoundationTests(unittest.TestCase):
    def run_foundation(
        self,
        runner: FoundationRunner,
        directory: str,
        *,
        reconciler=None,
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
            arguments = {
                "run_id": RUN_ID,
                "previous_run_id": PREVIOUS_RUN_ID,
                "slice_name": "test-slice",
                "owner": "test-owner",
                "allocation_id": "allocation-1",
                "known_hosts": known_hosts,
                "run_root": Path(directory) / "runs",
                "foundation_runner": runner,
            }
            if reconciler is not None:
                arguments["core_reconciler"] = reconciler
            result = execute_physical_foundation_acceptance(
                **arguments,
            )
        self.assertGreaterEqual(authorize.call_count, 3)
        self.assertFalse(
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

    def test_foundation_binds_four_ordered_stages_after_live_proof(self) -> None:
        runner = FoundationRunner()
        with tempfile.TemporaryDirectory() as directory:
            result = self.run_foundation(runner, directory)
            evidence = PhysicalRunEvidence.read_json(result.evidence_path)

        self.assertTrue(result.handoff.changed)
        self.assertEqual(2, result.ready_node_count)
        self.assertEqual(3, result.ready_open5gs_pod_count)
        self.assertFalse(result.open5gs_reconciled)
        self.assertFalse(result.to_dict()["legacy_gnb_stopped"])
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
            self.assertEqual(
                AcceptanceOutcome.PASSED, evidence.acceptance.outcome_for(stage)
            )

    def test_unready_kubernetes_node_blocks_namespace_mutation_and_evidence(
        self,
    ) -> None:
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

    def test_missing_open5gs_pod_is_reconciled_after_guarded_handoff(self) -> None:
        runner = FoundationRunner()
        runner.missing_nf = "smf"
        calls = []

        def reconcile(**arguments):
            calls.append(arguments)
            runner.missing_nf = None
            return Open5gsFoundationResult(
                run_id=RUN_ID,
                manifest_path=Path("manifest.json"),
                log_path=Path("open5gs-core.log"),
            )

        with tempfile.TemporaryDirectory() as directory:
            result = self.run_foundation(
                runner,
                directory,
                reconciler=reconcile,
            )

        self.assertTrue(result.open5gs_reconciled)
        self.assertEqual(1, len(calls))
        self.assertEqual(RUN_ID, runner.namespace_owner)
        self.assertEqual(RUN_ID, calls[0]["run_id"])
        self.assertEqual(Path(".deps"), calls[0]["dependency_root"])

    def test_failed_open5gs_reconciliation_creates_no_acceptance_evidence(self) -> None:
        runner = FoundationRunner()
        runner.unready_nf = "amf"

        def fail(**arguments):
            raise R2LabPhysicalFoundationError("core failed")

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(
                R2LabPhysicalFoundationError,
                "Open5GS foundation reconciliation failed",
            ):
                self.run_foundation(runner, directory, reconciler=fail)
            self.assertFalse(
                (Path(directory) / "runs" / RUN_ID / "physical-run.json").exists()
            )
        self.assertEqual(RUN_ID, runner.namespace_owner)

    def test_multiple_open5gs_pods_are_reconciled(self) -> None:
        runner = FoundationRunner()
        runner.duplicate_nf = "smf"

        def reconcile(**arguments):
            runner.duplicate_nf = None
            return Open5gsFoundationResult(
                run_id=RUN_ID,
                manifest_path=Path("manifest.json"),
                log_path=Path("open5gs-core.log"),
            )

        with tempfile.TemporaryDirectory() as directory:
            result = self.run_foundation(
                runner,
                directory,
                reconciler=reconcile,
            )

        self.assertTrue(result.open5gs_reconciled)

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

    def test_core_wrapper_uses_only_the_reviewed_physical_core_path(self) -> None:
        wrapper = (
            REPOSITORY_ROOT / "deploy" / "ansible" / "r2lab-open5gs-core.yml"
        ).read_text(encoding="utf-8")

        self.assertIn('core_node_name == "sopnode-f2"', wrapper)
        self.assertIn('ran_node_name == "sopnode-f3"', wrapper)
        self.assertIn('rru == "n300"', wrapper)
        self.assertIn('synthran_subscriber == "qfit07"', wrapper)
        self.assertIn("name: open5gs-smf2", wrapper)
        self.assertIn("name: open5gs-upf2", wrapper)
        self.assertIn("name=smf1", wrapper)
        self.assertIn("name=upf1", wrapper)
        self.assertIn("name: 5g/open5gs/config", wrapper)
        self.assertIn("name: 5g/open5gs/deploy", wrapper)
        self.assertNotIn("name: 5g/oai", wrapper)
        self.assertNotIn("name: 5g/srsRAN", wrapper)

    def test_reconciliation_executes_only_the_core_wrapper_with_qfit07(self) -> None:
        runner = ReconciliationRunner()
        authority_checks = []
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root = root / "runs"
            (run_root / RUN_ID).mkdir(parents=True)
            checkout = root / "fiveg_ansible"
            checkout.mkdir()
            known_hosts = root / "known_hosts"
            known_hosts.write_text(
                "sopnode-f2 ssh-ed25519 AAAATEST\n",
                encoding="utf-8",
            )

            with (
                patch(
                    "synthran.r2lab.foundation.validate_fiveg_checkout",
                    return_value=checkout,
                ),
                patch("synthran.r2lab.foundation.apply_network_overlay") as overlay,
            ):
                result = reconcile_open5gs_foundation(
                    run_id=RUN_ID,
                    known_hosts=known_hosts,
                    authority_verifier=lambda: authority_checks.append(True),
                    lock_path=REPOSITORY_ROOT / "dependencies.lock.yml",
                    dependency_root=root / "deps",
                    run_root=run_root,
                    repository_root=REPOSITORY_ROOT,
                    runner=runner,
                    timeout_seconds=60,
                )

            manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            inventory = (
                run_root
                / RUN_ID
                / "open5gs-foundation"
                / "hosts-physical.ini"
            ).read_text(encoding="utf-8")

        overlay.assert_called_once_with(
            ANY,
            subscriber_name="qfit07",
        )
        self.assertEqual([True, True], authority_checks)
        self.assertEqual("reconciled", manifest["status"])
        self.assertEqual("qfit07", manifest["subscriber"])
        self.assertIn('rru="n300"', inventory)
        playbooks = [
            command
            for command in runner.commands
            if command and command[0] == "ansible-playbook"
        ]
        self.assertEqual(2, len(playbooks))
        self.assertTrue(
            all(command[-1].endswith("r2lab-open5gs-core.yml") for command in playbooks)
        )


if __name__ == "__main__":
    unittest.main()
