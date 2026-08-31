from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import os
from synthran.dependencies import load_lock
from synthran.fiveg_ansible import load_inventory
from synthran.live_preflight import (
    CommandResult,
    LivePreflightError,
    load_fresh_live_evidence,
    golden_path_image_references,
    run_live_preflight,
    save_live_evidence,
    verify_allocations,
    verify_reservation,
)
from synthran.slices_controller import (
    SlicesControllerReport,
    dependency_lock_sha256,
    fingerprint,
)



REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "inventory_open5gs_srsran_rfsim.ini"
NOW = datetime(2026, 8, 12, 12, 0, tzinfo=timezone.utc)
RESERVATION_ID = "7000000001"


class FakeRunner:
    def __init__(
        self,
        *,
        reservation_owner: str = "operator",
        reservation_start: str = "2026-08-12T11:00:00Z",
        reservation_end: str = "2026-08-12T13:00:00Z",
        reservation_id: int = 7000000001,
        duplicate_reservation: bool = False,
        allocation_owner: str = "operator",
        yq_digest: str = (
            "654d2943ca1d3be2024089eb4f270f4070f491a0610481d128509b2834870049"
        ),
        helm_digest: str = (
            "f8180838c23d7c7d797b208861fecb591d9ce1690d8704ed1e4cb8e2add966c1"
        ),
        pymongo_version: str = "4.5.0",
    ) -> None:
        self.reservation_owner = reservation_owner
        self.reservation_start = reservation_start
        self.reservation_end = reservation_end
        self.reservation_id = reservation_id
        self.duplicate_reservation = duplicate_reservation
        self.allocation_owner = allocation_owner
        self.yq_digest = yq_digest
        self.helm_digest = helm_digest
        self.pymongo_version = pymongo_version
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, command: tuple[str, ...] | list[str], timeout: int) -> CommandResult:
        argv = tuple(command)
        self.calls.append(argv)
        self.assert_timeout(timeout)
        if argv == ("ansible-playbook", "--version"):
            return CommandResult(0, "ansible-playbook [core 2.20.5]\n")
        if argv == ("ansible-galaxy", "--version"):
            return CommandResult(0, "ansible-galaxy [core 2.20.5]\n")
        if argv == (
            "pos",
            "calendar",
            "list",
            "--filter",
            "owner=operator",
            "--json",
        ):
            reservation = {
                "id": self.reservation_id,
                "owner": self.reservation_owner,
                "nodes": ["lab-core", "lab-ran"],
                "start_date": self.reservation_start,
                "end_date": self.reservation_end,   
            }
            reservations = [reservation]
            if self.duplicate_reservation:
                reservations.append(dict(reservation))
            return CommandResult(
                0,
                json.dumps(reservations),
            )
        if argv[:3] == ("pos", "allocations", "show"):
            return CommandResult(
                0,
                json.dumps(
                    {
                        "id": "alloc-test",
                        "owner": self.allocation_owner,
                    }
                ),
            )
        if argv[0] == "ssh" and argv[-1] == "hostname":
            target = next(part for part in argv if "@" in part)
            return CommandResult(0, target.split("@", 1)[1] + "\n")
        if argv[0] == "ssh" and "kubectl get nodes -o json" in argv[-1]:
            return CommandResult(
                0,
                json.dumps(
                    {
                        "items": [
                            {
                                "metadata": {"name": "lab-core"},
                                "status": {
                                    "conditions": [
                                        {"type": "Ready", "status": "True"}
                                    ]
                                },
                            },
                            {
                                "metadata": {"name": "lab-ran"},
                                "status": {
                                    "conditions": [
                                        {"type": "Ready", "status": "True"}
                                    ]
                                },
                            },
                        ]
                    }
                ),
            )
        if argv[0] == "ssh" and "importlib.metadata" in argv[-1]:
            return CommandResult(
                0,
                json.dumps(
                    {
                        "dnspython": "2.3.0",
                        "kubernetes": "32.0.1",
                        "pymongo": self.pymongo_version,
                        "python-dateutil": "2.8.2",
                        "ruamel.yaml": "0.18.5",
                        "six": "1.16.0",
                    }
                ),
            )
        if argv[0] == "ssh" and "helm version --short" in argv[-1]:
            return CommandResult(0, "v3.18.4+g1234567\n")
        if argv[0] == "ssh" and "sha256sum /opt/synthran-tools/helm-" in argv[-1]:
            return CommandResult(
                0,
                f"{self.helm_digest}  /opt/synthran-tools/helm-3.18.4.tar.gz\n",
            )
        if argv[0] == "ssh" and "cmp -s /usr/local/bin/helm" in argv[-1]:
            return CommandResult(0, "helm-binary-ready\n")
        if argv[0] == "ssh" and "/usr/local/bin/yq --version" in argv[-1]:
            return CommandResult(
                0,
                "yq (https://github.com/mikefarah/yq/) version v4.45.1\n",
            )
        if argv[0] == "ssh" and "sha256sum /usr/local/bin/yq" in argv[-1]:
            return CommandResult(
                0,
                f"{self.yq_digest}  /usr/local/bin/yq\n",
            )
        if argv[0] == "ssh" and "command -v git" in argv[-1]:
            return CommandResult(0, "/usr/bin/git\n/usr/bin/kubectl\n/usr/bin/jq\n")
        if argv[0] == "ssh" and "kubectl get crd" in argv[-1]:
            return CommandResult(
                0,
                "customresourcedefinition.apiextensions.k8s.io/"
                "network-attachment-definitions.k8s.cni.cncf.io\n",
            )
        return CommandResult(2, "", "unsupported fake command")

    @staticmethod
    def assert_timeout(timeout: int) -> None:
        if timeout <= 0:
            raise AssertionError("timeout must be positive")


class LivePreflightTests(unittest.TestCase):
    def setUp(self) -> None:
        self.inventory = load_inventory(FIXTURE)
        self.lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        self.controller = SlicesControllerReport(
            dependency_lock_sha256=dependency_lock_sha256(self.lock),
            project_fingerprint=fingerprint("project-test"),
            experiment_fingerprint=fingerprint("experiment-test"),
            python_version="3.12.13",
            ansible_version="2.20.5",
            pos_version="2.5.35",
            slices_cli_version="1.0.0",
        )

    def run_ready(self, runner: FakeRunner | None = None):
        fake = runner or FakeRunner()

        with tempfile.TemporaryDirectory() as directory:
            known_hosts = Path(directory) / "known_hosts"
            known_hosts.write_text(
                "test-host ssh-ed25519 AAAATEST\n",
                encoding="utf-8",
            )

            with (
                patch.dict(
                    os.environ,
                    {"SYNTHRAN_KNOWN_HOSTS": str(known_hosts)},
                ),
                patch(
                    "synthran.live_preflight.verify_slices_controller",
                    return_value=self.controller,
                ),
            ):
                report = run_live_preflight(
                    inventory=self.inventory,
                    lock=self.lock,
                    owner="operator",
                    reservation_id=RESERVATION_ID,
                    allocation_id="alloc-test",
                    slices_project="project-test",
                    slices_experiment="experiment-test",
                    runner=fake,
                    which=lambda _name: "found",
                    image_probe=lambda _reference, _timeout: None,
                    now=NOW,
                )

        return fake, report

    def test_live_preflight_accepts_owned_current_inputs(self) -> None:
        fake, report = self.run_ready()
        self.assertTrue(report.ready, report.render())
        self.assertIn("Result: READY", report.render())
        self.assertIn(
            (
                "pos",
                "calendar",
                "list",
                "--filter",
                "owner=operator",
                "--json",
            ),
            fake.calls,
        )
        self.assertEqual(
            2,
            sum(
                call[:3] == ("pos", "allocations", "show")
                for call in fake.calls
            ),
        )

    def test_current_owner_authority_can_be_resolved_without_identifiers(self) -> None:
        runner = FakeRunner()
        reservation_id = verify_reservation(
            runner=runner,
            reservation_id=None,
            owner="operator",
            nodes={"lab-core", "lab-ran"},
            now=NOW,
            timeout_seconds=30,
        )
        allocation_id = verify_allocations(
            runner=runner,
            allocation_id=None,
            owner="operator",
            nodes={"lab-core", "lab-ran"},
            timeout_seconds=30,
        )

        self.assertEqual(RESERVATION_ID, reservation_id)
        self.assertEqual("alloc-test", allocation_id)

    def test_evidence_uses_fingerprints_not_provider_identifiers(self) -> None:
        _fake, report = self.run_ready()
        rendered = json.dumps(report.to_dict())
        self.assertNotIn(f'"{RESERVATION_ID}"', rendered)
        self.assertNotIn("alloc-test", rendered)
        self.assertNotIn('"operator"', rendered)

    def test_reservation_owner_mismatch_fails_closed(self) -> None:
        _fake, report = self.run_ready(FakeRunner(reservation_owner="someone-else"))
        self.assertFalse(report.ready)
        self.assertIn("reservation owner does not match", report.render())

    def test_expired_reservation_fails_closed(self) -> None:
        _fake, report = self.run_ready(
            FakeRunner(reservation_end="2026-08-12T11:59:59Z")
        )
        self.assertFalse(report.ready)
        self.assertIn("not active at the current UTC time", report.render())
    def test_pos_naive_local_reservation_timestamps_are_accepted(self) -> None:
        # NOW is 2026-08-12 12:00 UTC, which is 14:00 CEST.
        # POS 2.5.35 returns calendar timestamps without an explicit timezone.
        _fake, report = self.run_ready(
            FakeRunner(
                reservation_start="2026-08-12 13:00:00",
                reservation_end="2026-08-12 15:00:00",
            )
        )
        self.assertTrue(report.ready, report.render())

    def test_pos_naive_local_timestamp_is_not_treated_as_utc(self) -> None:
        # At NOW=12:00 UTC / 14:00 CEST, this local reservation has already expired.
        _fake, report = self.run_ready(
            FakeRunner(
                reservation_start="2026-08-12 12:00:00",
                reservation_end="2026-08-12 13:59:59",
            )
        )
        self.assertFalse(report.ready)
        self.assertIn("not active at the current UTC time", report.render())

    def test_missing_reservation_fails_closed(self) -> None:
        _fake, report = self.run_ready(FakeRunner(reservation_id=7000000002))
        self.assertFalse(report.ready)
        self.assertIn("was not found in the POS calendar", report.render())

    def test_duplicate_reservation_fails_closed(self) -> None:
        _fake, report = self.run_ready(FakeRunner(duplicate_reservation=True))
        self.assertFalse(report.ready)
        self.assertIn("ambiguous in the POS calendar", report.render())

    def test_allocation_owner_mismatch_fails_closed(self) -> None:
        _fake, report = self.run_ready(FakeRunner(allocation_owner="someone-else"))
        self.assertFalse(report.ready)
        self.assertIn("allocation is not owned", report.render())

    def test_remote_yq_digest_mismatch_fails_closed(self) -> None:
        _fake, report = self.run_ready(FakeRunner(yq_digest="0" * 64))
        self.assertFalse(report.ready)
        self.assertIn("remote yq digest does not match", report.render())

    def test_remote_helm_digest_mismatch_fails_closed(self) -> None:
        _fake, report = self.run_ready(FakeRunner(helm_digest="0" * 64))
        self.assertFalse(report.ready)
        self.assertIn("remote Helm archive digest does not match", report.render())

    def test_remote_python_version_mismatch_fails_closed(self) -> None:
        _fake, report = self.run_ready(FakeRunner(pymongo_version="4.6.0"))
        self.assertFalse(report.ready)
        self.assertIn("Python package versions do not match", report.render())

    def test_ssh_uses_batch_mode_and_strict_host_keys(self) -> None:
        fake, report = self.run_ready()
        self.assertTrue(report.ready)
        ssh_calls = [call for call in fake.calls if call and call[0] == "ssh"]
        self.assertTrue(ssh_calls)
        for call in ssh_calls:
            self.assertIn("BatchMode=yes", call)
            self.assertIn("StrictHostKeyChecking=yes", call)
            self.assertTrue(
                any(
                    part.startswith("UserKnownHostsFile=")
                    for part in call
                )
            )
            self.assertNotIn("sh", call[:-1])
        kubernetes_calls = [
            call for call in ssh_calls if "kubectl get crd" in call[-1]
        ]
        self.assertEqual(1, len(kubernetes_calls))
        self.assertIn("kubectl get namespace open5gs", kubernetes_calls[0][-1])
        self.assertIn(
            "test -x /opt/cni/bin/multus-shim",
            kubernetes_calls[0][-1],
        )
        self.assertNotIn(
            "test -x /opt/cni/bin/multus &&",
            kubernetes_calls[0][-1],
        )

    def test_missing_tool_skips_all_live_commands_and_fails(self) -> None:
        fake = FakeRunner()
        with patch(
            "synthran.live_preflight.verify_slices_controller",
            return_value=self.controller,
        ):
            report = run_live_preflight(
                inventory=self.inventory,
                lock=self.lock,
                owner="operator",
                reservation_id=RESERVATION_ID,
                allocation_id="alloc-test",
                slices_project="project-test",
                slices_experiment="experiment-test",
                runner=fake,
                which=lambda name: None if name == "pos" else "found",
                image_probe=lambda _reference, _timeout: None,
                now=NOW,
            )
        self.assertFalse(report.ready)
        self.assertEqual([], fake.calls)
        self.assertIn("missing required command(s): pos", report.render())

    def test_golden_path_images_are_all_digest_addressed(self) -> None:
        references = golden_path_image_references(self.lock)
        self.assertEqual(8, len(references))
        for reference in references:
            self.assertRegex(reference, r"@sha256:[0-9a-f]{64}$")

    def test_fresh_evidence_round_trip_and_stale_rejection(self) -> None:
        _fake, report = self.run_ready()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "preflight.json"
            save_live_evidence(report, path)
            payload = load_fresh_live_evidence(
                path=path,
                inventory=self.inventory,
                owner="operator",
                reservation_id=RESERVATION_ID,
                allocation_id="alloc-test",
                lock=self.lock,
                slices_project="project-test",
                slices_experiment="experiment-test",
                now=NOW + timedelta(minutes=5),
            )
            self.assertTrue(payload["ready"])
            with self.assertRaisesRegex(LivePreflightError, "stale"):
                load_fresh_live_evidence(
                    path=path,
                    inventory=self.inventory,
                    owner="operator",
                    reservation_id=RESERVATION_ID,
                    allocation_id="alloc-test",
                    lock=self.lock,
                    slices_project="project-test",
                    slices_experiment="experiment-test",
                    now=NOW + timedelta(minutes=16),
                )

    def test_evidence_cannot_authorize_different_inventory(self) -> None:
        _fake, report = self.run_ready()
        other = self.inventory.__class__(
            path=self.inventory.path,
            sha256="0" * 64,
            core_node=self.inventory.core_node,
            ran_node=self.inventory.ran_node,
            all_vars=self.inventory.all_vars,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "preflight.json"
            save_live_evidence(report, path)
            with self.assertRaisesRegex(LivePreflightError, "inventory"):
                load_fresh_live_evidence(
                    path=path,
                    inventory=other,
                    owner="operator",
                    reservation_id=RESERVATION_ID,
                    allocation_id="alloc-test",
                    lock=self.lock,
                    slices_project="project-test",
                    slices_experiment="experiment-test",
                    now=NOW,
                )


    def test_ready_boolean_cannot_replace_the_required_check_set(self) -> None:
        _fake, report = self.run_ready()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "preflight.json"
            payload = report.to_dict()
            payload["checks"] = [
                check
                for check in payload["checks"]
                if check["name"] != "reservation"
            ]
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(LivePreflightError, "check set is incomplete"):
                load_fresh_live_evidence(
                    path=path,
                    inventory=self.inventory,
                    owner="operator",
                    reservation_id=RESERVATION_ID,
                    allocation_id="alloc-test",
                    lock=self.lock,
                    slices_project="project-test",
                    slices_experiment="experiment-test",
                    now=NOW,
                )

    def test_evidence_is_bound_to_slices_context_and_lock(self) -> None:
        _fake, report = self.run_ready()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "preflight.json"
            save_live_evidence(report, path)
            with self.assertRaisesRegex(LivePreflightError, "SLICES controller context"):
                load_fresh_live_evidence(
                    path=path,
                    inventory=self.inventory,
                    owner="operator",
                    reservation_id=RESERVATION_ID,
                    allocation_id="alloc-test",
                    lock=self.lock,
                    slices_project="another-project",
                    slices_experiment="experiment-test",
                    now=NOW,
                )
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["dependency_lock_sha256"] = "0" * 64
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(LivePreflightError, "dependency lock"):
                load_fresh_live_evidence(
                    path=path,
                    inventory=self.inventory,
                    owner="operator",
                    reservation_id=RESERVATION_ID,
                    allocation_id="alloc-test",
                    lock=self.lock,
                    slices_project="project-test",
                    slices_experiment="experiment-test",
                    now=NOW,
                )

    



if __name__ == "__main__":
    unittest.main()
