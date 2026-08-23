from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shlex
import tempfile
import unittest

from synthran.dependencies import load_lock
from synthran.fiveg_ansible import load_inventory
from synthran.live_preflight import CommandResult
from synthran.r2lab.controller import (
    R2LabSelection,
    build_plan,
    execute_physical_gnb_start,
    execute_prepare,
    execute_release,
    run_doctor,
)
from synthran.r2lab.deployment import (
    DEPLOYMENT_PACKAGE_ANNOTATION,
    DEPLOYMENT_RENDER_ANNOTATION,
    DEPLOYMENT_RUN_ANNOTATION,
    DEPLOYMENT_RUN_LABEL,
    DEPLOYMENT_VALUES_ANNOTATION,
    POD_RUNTIME_STATE_KEY,
    PhysicalChartArtifact,
    PhysicalHelmRenderEvidence,
    R2LabPhysicalStagingError,
    execute_stopped_physical_staging,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RFSIM_FIXTURE = (
    REPOSITORY_ROOT / "tests" / "fixtures" / "inventory_open5gs_srsran_rfsim.ini"
)
RESERVATION_START_FIELD = "start_" + "date"
RESERVATION_END_FIELD = "end_" + "date"


class LifecycleRunner:
    """Deterministic provider double for the complete public lifecycle."""

    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.power: dict[str, str] = {}
        self.qfit_usb_power = "off"

    @staticmethod
    def remote(command: tuple[str, ...]) -> tuple[str, ...]:
        split = command.index("--")
        return command[split + 2 :]

    @staticmethod
    def qfit_node(qfit: str) -> int:
        return int(qfit.removeprefix("qfit"))

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        remote = self.remote(value)
        if remote in {
            ("true",),
            ("rhubarbe", "leases", "--check"),
        }:
            return CommandResult(0, "", "")
        if remote[:3] == ("rhubarbe", "pdu", "on") and len(remote) == 4:
            resource = remote[3]
            self.power[resource] = "on"
            return CommandResult(
                0,
                f"pdu2 chain-0@outlet-1 ({resource}): ON (28W)\n",
                "",
            )
        if remote[:3] == ("rhubarbe", "pdu", "off") and len(remote) == 4:
            resource = remote[3]
            self.power[resource] = "off"
            return CommandResult(
                1,
                f"pdu2 chain-0@outlet-1 ({resource}): OFF\n",
                "",
            )
        if remote[:3] == ("rhubarbe", "pdu", "status") and len(remote) == 4:
            resource = remote[3]
            state = self.power.get(resource)
            if state == "on":
                return CommandResult(
                    0,
                    f"pdu2 chain-0@outlet-1 ({resource}): ON (28W)\n",
                    "",
                )
            if state == "off":
                return CommandResult(
                    0,
                    f"pdu2 chain-0@outlet-1 ({resource}): OFF\n",
                    "",
                )
            return CommandResult(0, "", "")
        if remote[:2] == ("qfit", "on") and len(remote) == 3:
            qfit = remote[2]
            self.power[qfit] = "on"
            return CommandResult(0, f"reboot{self.qfit_node(qfit):02d}:ok\n", "")
        if remote[:2] == ("qfit", "off") and len(remote) == 3:
            qfit = remote[2]
            self.power[qfit] = "off"
            return CommandResult(0, f"reboot{self.qfit_node(qfit):02d}:ok\n", "")
        if remote[:2] == ("rhubarbe", "status") and len(remote) == 3:
            node = int(remote[2])
            qfit = f"qfit{node:02d}"
            state = self.power.get(qfit, "off")
            if state in {"on", "off"}:
                return CommandResult(0, f"reboot{node:02d}:{state}\n", "")
            return CommandResult(0, "", "")
        if remote == ("curl", "-fsS", "http://reboot07/usrpstatus"):
            return CommandResult(0, f"usrp{self.qfit_usb_power}\n", "")
        if remote == ("curl", "-fsS", "http://reboot07/usrpon"):
            self.qfit_usb_power = "on"
            return CommandResult(0, "ok\n", "")
        if remote == ("curl", "-fsS", "http://reboot07/usrpoff"):
            self.qfit_usb_power = "off"
            return CommandResult(0, "ok\n", "")
        if remote[:1] == ("ssh",) and (
            remote[-3:] in {
                ("test", "-c", "/dev/ttyUSB2"),
                ("test", "-c", "/dev/cdc-wdm0"),
            }
            or remote[-5:] == ("ip", "link", "show", "dev", "wwan0")
        ):
            return CommandResult(0 if self.qfit_usb_power == "on" else 1, "", "")
        if remote[:1] == ("ping",):
            return CommandResult(0, "", "")
        return CommandResult(0, "", "")

    @property
    def remote_commands(self) -> list[tuple[str, ...]]:
        return [self.remote(command) for command in self.commands]


class R2LabLifecycleGateTests(unittest.TestCase):
    def test_current_rfsim_golden_path_remains_the_regression_baseline(self) -> None:
        inventory = load_inventory(RFSIM_FIXTURE)
        self.assertEqual("open5gs", inventory.core)
        self.assertEqual("srsRAN", inventory.ran)
        self.assertEqual("rfsim", inventory.radio)

    def test_complete_r2lab_resource_lifecycle_is_exact_and_released(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user",
            radio="n300",
            ue="qhat01",
        )
        runner = LifecycleRunner()

        doctor = run_doctor(selection=selection, runner=runner)
        self.assertTrue(doctor.ready)
        self.assertEqual(
            [("true",), ("rhubarbe", "leases", "--check")],
            runner.remote_commands,
        )

        plan = build_plan(run_id="r2lab-lifecycle-001", selection=selection)
        payload = plan.to_dict()
        rendered = plan.render(as_json=True)
        self.assertFalse(payload["execution_enabled"])
        self.assertEqual("reuse-active", payload["lease_action"])
        self.assertFalse(payload["safety"]["automatic_lease_booking"])
        self.assertFalse(payload["safety"]["password_storage"])
        self.assertFalse(payload["safety"]["global_power_off"])
        self.assertFalse(payload["safety"]["mutation_returncode_is_state_truth"])
        self.assertTrue(payload["safety"]["claim_release_requires_proven_clean_state"])
        self.assertNotIn("oulu_user", rendered)
        self.assertNotIn("all-off", rendered)
        planned_commands = "\n".join(payload["commands"]).lower()
        self.assertIn("pdu status n300", planned_commands)
        self.assertNotIn("password", planned_commands)
        self.assertNotIn(" -p ", f" {planned_commands} ")

        runner.commands.clear()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            prepared = execute_prepare(
                plan=plan,
                run_root=root,
                runner=runner,
                sleeper=lambda _: None,
                reachability_attempts=1,
            )
            self.assertEqual("ready", prepared.status)
            self.assertTrue((root / "active.json").is_file())

            ready_manifest = json.loads(
                prepared.manifest_path.read_text(encoding="utf-8")
            )
            self.assertEqual("ready", ready_manifest["status"])
            self.assertEqual("held", ready_manifest["resource_claim"])
            self.assertNotIn(
                "oulu_user",
                prepared.manifest_path.read_text(encoding="utf-8"),
            )

            prepare_commands = runner.remote_commands
            self.assertEqual(
                [
                    ("rhubarbe", "leases", "--check"),
                    ("rhubarbe", "leases", "--check"),
                    ("rhubarbe", "pdu", "on", "n300"),
                    ("rhubarbe", "pdu", "status", "n300"),
                    ("rhubarbe", "leases", "--check"),
                    ("rhubarbe", "pdu", "off", "qhat01"),
                    ("rhubarbe", "pdu", "status", "qhat01"),
                    ("rhubarbe", "leases", "--check"),
                    ("rhubarbe", "pdu", "on", "qhat01"),
                    ("rhubarbe", "pdu", "status", "qhat01"),
                    ("ping", "-c", "1", "-W", "1", "qhat01"),
                    ("rhubarbe", "leases", "--check"),
                ],
                prepare_commands,
            )

            runner.commands.clear()
            released = execute_release(
                run_id=plan.run_id,
                slice_name="oulu_user",
                run_root=root,
                runner=runner,
            )
            self.assertEqual("released", released.status)
            self.assertFalse((root / "active.json").exists())

            released_manifest = json.loads(
                released.manifest_path.read_text(encoding="utf-8")
            )
            self.assertEqual("released", released_manifest["status"])
            self.assertEqual("released", released_manifest["resource_claim"])
            self.assertTrue(released_manifest["cleanup"]["claim_releasable"])

            self.assertEqual(
                [
                    ("rhubarbe", "leases", "--check"),
                    ("rhubarbe", "pdu", "off", "qhat01"),
                    ("rhubarbe", "pdu", "status", "qhat01"),
                    ("rhubarbe", "leases", "--check"),
                    ("rhubarbe", "pdu", "off", "n300"),
                    ("rhubarbe", "pdu", "status", "n300"),
                ],
                runner.remote_commands,
            )

        all_commands = "\n".join(
            " ".join(command) for command in prepare_commands + runner.remote_commands
        )
        self.assertNotIn("all-off", all_commands)
        self.assertNotIn("rhubarbe bye", all_commands)
        self.assertNotIn("password", all_commands.lower())


class StoppedStagingRunner:
    """Provider double for stopped staging and authorized singleton start."""

    def __init__(
        self,
        *,
        run_id: str,
        owner: str,
        reservation_id: str,
        allocation_id: str,
        package_sha256: str,
        values_sha256: str,
        render_sha256: str,
    ) -> None:
        self.run_id = run_id
        self.owner = owner
        self.reservation_id = reservation_id
        self.allocation_id = allocation_id
        self.package_sha256 = package_sha256
        self.values_sha256 = values_sha256
        self.render_sha256 = render_sha256
        self.commands: list[tuple[str, ...]] = []
        self.existing_replicas: int | None = None
        self.remote_digest_match = True
        self.deployment_exists = False
        self.labels: dict[str, str] = {}
        self.annotations: dict[str, str] = {}
        self.pods: list[dict[str, object]] = []

    @staticmethod
    def remote(command: tuple[str, ...]) -> tuple[str, ...] | None:
        if not command or command[0] != "ssh":
            return None
        return tuple(shlex.split(command[-1]))

    def deployment_json(self, *, replicas: int) -> str:
        return json.dumps(
            {
                "metadata": {
                    "labels": dict(self.labels),
                    "annotations": dict(self.annotations),
                },
                "spec": {"replicas": replicas},
            }
        )

    @staticmethod
    def ready_pod() -> dict[str, object]:
        return {
            "metadata": {"name": "gnb-current"},
            "status": {
                POD_RUNTIME_STATE_KEY: "Running",
                "containerStatuses": [{"name": "gnb", "ready": True}],
            },
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
                            "id": self.reservation_id,
                            "owner": self.owner,
                            "nodes": ["sopnode-f2", "sopnode-f3"],
                            RESERVATION_START_FIELD: "2026-08-22T10:00:00+00:00",
                            RESERVATION_END_FIELD: "2026-08-22T14:00:00+00:00",
                        }
                    ]
                ),
                "",
            )
        if value[:3] == ("pos", "allocations", "show"):
            return CommandResult(
                0,
                json.dumps({"id": self.allocation_id, "owner": self.owner}),
                "",
            )
        if value and value[0] == "scp":
            return CommandResult(0, "", "")

        remote = self.remote(value)
        if remote is None:
            raise AssertionError(f"unexpected command: {value}")
        if remote[:2] == ("mkdir", "-p"):
            return CommandResult(0, "", "")
        if remote and remote[0] == "sha256sum":
            if self.remote_digest_match:
                return CommandResult(
                    0,
                    f"{self.package_sha256}  {remote[1]}\n{self.values_sha256}  {remote[2]}\n",
                    "",
                )
            return CommandResult(
                0,
                f"{'0' * 64}  {remote[1]}\n{'1' * 64}  {remote[2]}\n",
                "",
            )
        if remote == ("helm", "version", "--short"):
            return CommandResult(0, "v3.18.4+g123\n", "")
        if remote[:4] == ("kubectl", "get", "namespace", "open5gs"):
            return CommandResult(0, self.run_id, "")
        if remote[:3] == ("kubectl", "get", "deployment/srsran-gnb"):
            if "--ignore-not-found" in remote:
                if not self.deployment_exists and self.existing_replicas is None:
                    return CommandResult(0, "", "")
                replicas = 0 if self.existing_replicas is None else self.existing_replicas
                return CommandResult(0, self.deployment_json(replicas=replicas), "")
            replicas = 0 if self.existing_replicas is None else self.existing_replicas
            return CommandResult(0, self.deployment_json(replicas=replicas), "")
        if remote[:3] == ("kubectl", "get", "pods"):
            return CommandResult(0, json.dumps({"items": self.pods}), "")
        if remote[:3] == ("helm", "upgrade", "--install"):
            self.deployment_exists = True
            self.existing_replicas = 0
            return CommandResult(0, "Release staged\n", "")
        if remote[:3] == ("kubectl", "label", "deployment/srsran-gnb"):
            assignment = next(
                item for item in remote if item.startswith(f"{DEPLOYMENT_RUN_LABEL}=")
            )
            key, assigned = assignment.split("=", 1)
            self.labels[key] = assigned
            return CommandResult(0, "", "")
        if remote[:3] == ("kubectl", "annotate", "deployment/srsran-gnb"):
            for item in remote:
                if item.startswith("synthran.io/") and "=" in item:
                    key, assigned = item.split("=", 1)
                    self.annotations[key] = assigned
            return CommandResult(0, "", "")
        if remote[:3] == ("kubectl", "scale", "deployment/srsran-gnb"):
            if "--replicas=0" in remote:
                self.existing_replicas = 0
                self.pods = []
                return CommandResult(0, "", "")
            if "--replicas=1" in remote:
                self.existing_replicas = 1
                self.pods = [self.ready_pod()]
                return CommandResult(0, "", "")
        raise AssertionError(f"unexpected remote command: {remote}")


class R2LabStoppedPhysicalStagingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        self.run_id = "r2lab-staging-test"
        self.owner = "rnayreed"
        self.reservation_id = "reservation-1"
        self.allocation_id = "allocation-1"
        self.now = datetime(2026, 8, 22, 12, 0, tzinfo=timezone.utc)
        self.render = PhysicalHelmRenderEvidence(
            sha256="a" * 64,
            replicas=0,
            strategy="Recreate",
            image_reference="example.invalid/gnb:v1@sha256:" + "b" * 64,
            carrier_arfcn=621_312,
            channel_bandwidth_mhz=40,
            antennas_dl=2,
            antennas_ul=2,
        )

    def make_artifact(self, root: Path) -> PhysicalChartArtifact:
        package = root / f"srsran-gnb-{self.run_id}.tgz"
        values = root / "synthran-physical-values.json"
        package.write_bytes(b"reviewed physical chart")
        values.write_text('{"replicas": 0}\n', encoding="utf-8")
        return PhysicalChartArtifact(
            run_id=self.run_id,
            package_path=package,
            values_path=values,
            package_sha256=hashlib.sha256(package.read_bytes()).hexdigest(),
            values_sha256=hashlib.sha256(values.read_bytes()).hexdigest(),
        )

    def make_runner(self, artifact: PhysicalChartArtifact) -> StoppedStagingRunner:
        return StoppedStagingRunner(
            run_id=self.run_id,
            owner=self.owner,
            reservation_id=self.reservation_id,
            allocation_id=self.allocation_id,
            package_sha256=artifact.package_sha256,
            values_sha256=artifact.values_sha256,
            render_sha256=self.render.sha256,
        )

    def stage(
        self,
        *,
        root: Path,
        artifact: PhysicalChartArtifact,
        runner: StoppedStagingRunner,
    ):
        known_hosts = root / "known_hosts"
        known_hosts.write_text("sopnode-f2 ssh-ed25519 AAAATEST\n", encoding="utf-8")
        result = execute_stopped_physical_staging(
            lock=self.lock,
            artifact=artifact,
            render_evidence=self.render,
            run_id=self.run_id,
            owner=self.owner,
            reservation_id=self.reservation_id,
            allocation_id=self.allocation_id,
            known_hosts=known_hosts,
            now=self.now,
            runner=runner,
        )
        return result, known_hosts

    def test_staging_requires_authority_transfers_exact_artifact_and_stays_stopped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = self.make_artifact(root)
            runner = self.make_runner(artifact)
            result, _ = self.stage(root=root, artifact=artifact, runner=runner)

        payload = result.to_dict()
        self.assertEqual("staged-stopped", payload["status"])
        self.assertFalse(payload["hardware_mutation"])
        self.assertTrue(payload["namespace_owned"])
        self.assertTrue(payload["deployment_bound"])
        self.assertEqual(0, payload["desired_replicas"])
        self.assertEqual(0, payload["gnb_pod_count"])
        self.assertEqual(artifact.package_sha256, payload["package_sha256"])
        self.assertEqual(artifact.values_sha256, payload["values_sha256"])
        self.assertEqual(self.render.sha256, payload["render_sha256"])
        self.assertEqual(self.run_id, runner.labels[DEPLOYMENT_RUN_LABEL])
        self.assertEqual(self.run_id, runner.annotations[DEPLOYMENT_RUN_ANNOTATION])
        self.assertEqual(
            artifact.package_sha256,
            runner.annotations[DEPLOYMENT_PACKAGE_ANNOTATION],
        )
        self.assertEqual(
            artifact.values_sha256,
            runner.annotations[DEPLOYMENT_VALUES_ANNOTATION],
        )
        self.assertEqual(
            self.render.sha256,
            runner.annotations[DEPLOYMENT_RENDER_ANNOTATION],
        )

        command_text = "\n".join(" ".join(command) for command in runner.commands)
        self.assertIn("StrictHostKeyChecking=yes", command_text)
        self.assertIn("helm upgrade --install", command_text)
        self.assertIn("kubectl label deployment/srsran-gnb", command_text)
        self.assertIn("kubectl annotate deployment/srsran-gnb", command_text)
        self.assertNotIn("--replicas=1", command_text)
        self.assertNotIn("rhubarbe", command_text)
        self.assertNotIn("qfit", command_text)
        self.assertNotIn("all-off", command_text)
        allocation_queries = [
            command
            for command in runner.commands
            if command[:3] == ("pos", "allocations", "show")
        ]
        self.assertEqual(4, len(allocation_queries))

    def test_staging_rejects_stale_render_before_any_cluster_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = self.make_artifact(root)
            known_hosts = root / "known_hosts"
            known_hosts.write_text("sopnode-f2 ssh-ed25519 AAAATEST\n", encoding="utf-8")
            runner = self.make_runner(artifact)
            stale = PhysicalHelmRenderEvidence(
                sha256="c" * 64,
                replicas=0,
                strategy="Recreate",
                image_reference=self.render.image_reference,
                carrier_arfcn=621_984,
                channel_bandwidth_mhz=60,
                antennas_dl=2,
                antennas_ul=2,
            )

            with self.assertRaisesRegex(R2LabPhysicalStagingError, "reviewed R2Lab radio reference"):
                execute_stopped_physical_staging(
                    lock=self.lock,
                    artifact=artifact,
                    render_evidence=stale,
                    run_id=self.run_id,
                    owner=self.owner,
                    reservation_id=self.reservation_id,
                    allocation_id=self.allocation_id,
                    known_hosts=known_hosts,
                    now=self.now,
                    runner=runner,
                )

        self.assertEqual([], runner.commands)

    def test_staging_refuses_changed_allocation_before_any_cluster_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = self.make_artifact(root)
            known_hosts = root / "known_hosts"
            known_hosts.write_text("sopnode-f2 ssh-ed25519 AAAATEST\n", encoding="utf-8")
            runner = self.make_runner(artifact)
            runner.allocation_id = "different-allocation"

            with self.assertRaisesRegex(R2LabPhysicalStagingError, "authority"):
                execute_stopped_physical_staging(
                    lock=self.lock,
                    artifact=artifact,
                    render_evidence=self.render,
                    run_id=self.run_id,
                    owner=self.owner,
                    reservation_id=self.reservation_id,
                    allocation_id=self.allocation_id,
                    known_hosts=known_hosts,
                    now=self.now,
                    runner=runner,
                )

        self.assertFalse(
            any(command and command[0] in {"ssh", "scp"} for command in runner.commands)
        )

    def test_staging_refuses_running_existing_gnb_before_helm_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = self.make_artifact(root)
            known_hosts = root / "known_hosts"
            known_hosts.write_text("sopnode-f2 ssh-ed25519 AAAATEST\n", encoding="utf-8")
            runner = self.make_runner(artifact)
            runner.existing_replicas = 1

            with self.assertRaisesRegex(R2LabPhysicalStagingError, "not stopped"):
                execute_stopped_physical_staging(
                    lock=self.lock,
                    artifact=artifact,
                    render_evidence=self.render,
                    run_id=self.run_id,
                    owner=self.owner,
                    reservation_id=self.reservation_id,
                    allocation_id=self.allocation_id,
                    known_hosts=known_hosts,
                    now=self.now,
                    runner=runner,
                )

        remote_commands = [runner.remote(command) for command in runner.commands]
        self.assertFalse(
            any(
                remote is not None
                and remote[:3] == ("helm", "upgrade", "--install")
                for remote in remote_commands
            )
        )

    def test_staging_refuses_remote_digest_mismatch_before_helm_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = self.make_artifact(root)
            known_hosts = root / "known_hosts"
            known_hosts.write_text("sopnode-f2 ssh-ed25519 AAAATEST\n", encoding="utf-8")
            runner = self.make_runner(artifact)
            runner.remote_digest_match = False

            with self.assertRaisesRegex(R2LabPhysicalStagingError, "digests"):
                execute_stopped_physical_staging(
                    lock=self.lock,
                    artifact=artifact,
                    render_evidence=self.render,
                    run_id=self.run_id,
                    owner=self.owner,
                    reservation_id=self.reservation_id,
                    allocation_id=self.allocation_id,
                    known_hosts=known_hosts,
                    now=self.now,
                    runner=runner,
                )

        remote_commands = [runner.remote(command) for command in runner.commands]
        self.assertFalse(
            any(
                remote is not None
                and remote[:3] == ("helm", "upgrade", "--install")
                for remote in remote_commands
            )
        )

    def test_authorized_start_rechecks_claim_and_staged_binding_before_scale_one(self) -> None:
        run_id = self.run_id
        selection = R2LabSelection.build(
            slice_name="oulu_user",
            radio="n300",
            ue="qfit07",
        )
        provider = LifecycleRunner()
        plan = build_plan(run_id=run_id, selection=selection)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root = root / "r2lab"
            execute_prepare(
                plan=plan,
                run_root=run_root,
                runner=provider,
                sleeper=lambda _: None,
                reachability_attempts=1,
            )
            artifact = self.make_artifact(root)
            cluster = self.make_runner(artifact)
            staging, known_hosts = self.stage(
                root=root,
                artifact=artifact,
                runner=cluster,
            )
            provider.commands.clear()
            cluster.commands.clear()

            started = execute_physical_gnb_start(
                run_id=run_id,
                slice_name="oulu_user",
                staging=staging,
                owner=self.owner,
                reservation_id=self.reservation_id,
                allocation_id=self.allocation_id,
                known_hosts=known_hosts,
                now=self.now,
                run_root=run_root,
                r2lab_runner=provider,
                cluster_runner=cluster,
                sleeper=lambda _: None,
                timeout_seconds=30,
            )

        payload = started.to_dict()
        self.assertEqual("gnb-started", payload["status"])
        self.assertTrue(payload["started_exactly_one"])
        self.assertTrue(payload["hardware_mutation"])
        self.assertEqual(1, payload["maximum_observed_pods"])
        self.assertEqual(staging.package_sha256, payload["package_sha256"])
        self.assertEqual(staging.values_sha256, payload["values_sha256"])
        self.assertEqual(staging.render_sha256, payload["render_sha256"])
        self.assertEqual(64, len(payload["claim_sha256"]))

        provider_remote = provider.remote_commands
        self.assertGreaterEqual(
            provider_remote.count(("rhubarbe", "leases", "--check")), 2
        )
        self.assertGreaterEqual(
            provider_remote.count(("rhubarbe", "pdu", "status", "n300")), 2
        )
        cluster_remote = [cluster.remote(command) for command in cluster.commands]
        scale_one = [
            remote
            for remote in cluster_remote
            if remote is not None and "--replicas=1" in remote
        ]
        self.assertEqual(1, len(scale_one))
        self.assertEqual(1, len(cluster.pods))

    def test_authorized_start_never_scales_up_after_claim_changes(self) -> None:
        run_id = self.run_id
        selection = R2LabSelection.build(
            slice_name="oulu_user",
            radio="n300",
            ue="qfit07",
        )
        provider = LifecycleRunner()
        plan = build_plan(run_id=run_id, selection=selection)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root = root / "r2lab"
            execute_prepare(
                plan=plan,
                run_root=run_root,
                runner=provider,
                sleeper=lambda _: None,
                reachability_attempts=1,
            )
            artifact = self.make_artifact(root)
            cluster = self.make_runner(artifact)
            staging, known_hosts = self.stage(
                root=root,
                artifact=artifact,
                runner=cluster,
            )

            calls = 0

            def changing_provider(command, timeout_seconds: int) -> CommandResult:
                nonlocal calls
                result = provider(command, timeout_seconds)
                remote = provider.remote(tuple(command))
                if remote == ("rhubarbe", "leases", "--check"):
                    calls += 1
                    if calls == 2:
                        claim = json.loads(
                            (run_root / "active.json").read_text(encoding="utf-8")
                        )
                        claim["created_at_utc"] = "2026-08-22T13:59:59Z"
                        (run_root / "active.json").write_text(
                            json.dumps(claim, indent=2, sort_keys=True) + "\n",
                            encoding="utf-8",
                        )
                return result

            cluster.commands.clear()
            with self.assertRaisesRegex(Exception, "safety boundary"):
                execute_physical_gnb_start(
                    run_id=run_id,
                    slice_name="oulu_user",
                    staging=staging,
                    owner=self.owner,
                    reservation_id=self.reservation_id,
                    allocation_id=self.allocation_id,
                    known_hosts=known_hosts,
                    now=self.now,
                    run_root=run_root,
                    r2lab_runner=changing_provider,
                    cluster_runner=cluster,
                    sleeper=lambda _: None,
                    timeout_seconds=30,
                )

        cluster_remote = [cluster.remote(command) for command in cluster.commands]
        self.assertFalse(
            any(remote is not None and "--replicas=1" in remote for remote in cluster_remote)
        )


if __name__ == "__main__":
    unittest.main()
