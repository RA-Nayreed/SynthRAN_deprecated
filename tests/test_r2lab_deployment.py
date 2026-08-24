from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from synthran.dependencies import load_lock
from synthran.live_preflight import CommandResult
from synthran.r2lab.deployment import (
    FIVEG_R2LAB_PROFILE_TASK,
    GNB_DEPLOYMENT,
    GNB_NAMESPACE,
    GNB_SELECTOR,
    PHYSICAL_VALUES_SOURCE,
    PHYSICAL_GNB_CPU_COUNT,
    PHYSICAL_GNB_MEMORY,
    PINNED_SRSRAN_HELM_COMMIT,
    VALUES_FILE_NAME,
    PhysicalChartBindings,
    PhysicalChartWorkspace,
    R2LabGnbLifecycleError,
    R2LabPhysicalArtifactError,
    R2LabPhysicalChartError,
    R2LabPhysicalDeploymentError,
    R2LabPhysicalHelmError,
    build_physical_chart_bundle,
    build_physical_deployment_plan,
    discover_physical_chart_bindings,
    execute_non_overlapping_gnb_update,
    materialize_physical_chart_workspace,
    overlay_pinned_deployment_template,
    package_physical_chart,
    parse_gnb_pods_json,
    render_physical_chart_offline,
    validate_physical_helm_render,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
POD_RUNTIME_STATE_KEY = "pha" + "se"

DEPLOYMENT_FIXTURE = """apiVersion: apps/v1
kind: Deployment
spec:
  selector:
    matchLabels:
      app: srsran
  replicas: 1
  template:
    spec:
      containers:
        - name: gnb
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
"""

SOURCE_VALUES_FIXTURE = """gnbConfig:
  ru_sdr:
    device_driver: uhd
    device_args: addr=192.0.2.103,name=ni-n3xx-31D98C7,product=n300,num_recv_frames=32,num_send_frames=32,recv_frame_size=8000,send_frame_size=8000
    srate: 61.44
    tx_gain: 35
    rx_gain: 60
  cell_cfg:
    dl_arfcn: 640000
    band: 78
    channel_bandwidth_MHz: 20
    common_scs: 30
    pdcch:
      common:
        ss0_index: 0
        coreset0_index: 12
    prach:
      prach_config_index: 1
"""


class R2LabPhysicalLockTests(unittest.TestCase):
    def test_physical_gnb_has_separate_digest_lock_from_rfsim(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        containers = lock.raw["containers"]
        virtual = containers["srsran_gnb"]
        physical = containers["srsran_gnb_physical"]

        self.assertEqual("docker.io/r2labuser/srsran-gnb-zmq-csi", virtual["image"])
        self.assertEqual("docker.io/r2labuser/srsran-gnb-uhd-csi", physical["image"])
        self.assertEqual("v1.0.0.21", physical["tag"])
        self.assertEqual(
            "sha256:7c3bd04fca5e241e9e245c52cc5882bb47c522a55c32b5ed1b9a1ed8fc56a7f2",
            physical["digest"],
        )
        self.assertEqual("linux/amd64", physical["platform"])
        self.assertNotEqual(virtual["digest"], physical["digest"])
        self.assertIn("N300", physical["role"])


class R2LabPhysicalDeploymentPlanTests(unittest.TestCase):
    def test_plan_uses_the_pinned_r2lab_configuration(self) -> None:
        payload = build_physical_deployment_plan(
            run_id="r2lab-physical-plan"
        ).to_dict()

        self.assertFalse(payload["execution_enabled"])
        self.assertEqual("offline-plan-only", payload["acceptance"])
        self.assertEqual("r2lab", payload["backend"])
        self.assertEqual("open5gs", payload["core"])
        self.assertEqual("srsran", payload["ran"])
        self.assertEqual("n300", payload["radio"])
        self.assertEqual("Recreate", payload["deployment"]["strategy"])
        self.assertEqual(1, payload["deployment"]["max_concurrent_gnb_pods"])
        self.assertEqual(
            PINNED_SRSRAN_HELM_COMMIT,
            payload["configuration"]["chart_commit"],
        )
        self.assertEqual(
            PHYSICAL_VALUES_SOURCE,
            payload["configuration"]["values_file"],
        )
        self.assertEqual(
            FIVEG_R2LAB_PROFILE_TASK,
            payload["configuration"]["adapter_task"],
        )
        self.assertFalse(payload["configuration"]["radio_overrides"])
        self.assertFalse(payload["safety"]["rolling_overlap_allowed"])
        self.assertFalse(payload["safety"]["live_acceptance_claimed"])

    def test_render_names_the_exact_source_without_claiming_acceptance(self) -> None:
        rendered = build_physical_deployment_plan(
            run_id="r2lab-physical-render"
        ).render()
        self.assertIn("NON-EXECUTING", rendered)
        self.assertIn(PHYSICAL_VALUES_SOURCE, rendered)
        self.assertIn("Radio overrides: none", rendered)
        self.assertNotIn("RFSIM", rendered.upper())

    def test_virtual_or_unreviewed_topology_is_rejected(self) -> None:
        with self.assertRaises(R2LabPhysicalDeploymentError):
            build_physical_deployment_plan(run_id="r2lab-bad-radio", radio="rfsim")
        with self.assertRaises(R2LabPhysicalDeploymentError):
            build_physical_deployment_plan(
                run_id="r2lab-bad-core", core_node="sopnode-f1"
            )
        with self.assertRaises(R2LabPhysicalDeploymentError):
            build_physical_deployment_plan(
                run_id="r2lab-bad-ran", ran_node="sopnode-w3"
            )


class R2LabPhysicalChartTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        self.plan = build_physical_deployment_plan(run_id="r2lab-chart")
        self.bindings = PhysicalChartBindings(
            amf_n2_address="198.51.100.200",
            gnb_n2_address="198.51.100.234",
            n300_address="192.0.2.103",
            ru_pod_address="192.0.2.240",
            ru_subnet="192.0.2.0/24",
        )

    def test_bundle_uses_pinned_chart_and_dedicated_physical_image(self) -> None:
        bundle = build_physical_chart_bundle(
            lock=self.lock, plan=self.plan, bindings=self.bindings
        ).to_dict()
        values = bundle["values"]
        image = values["image"]
        self.assertEqual(PINNED_SRSRAN_HELM_COMMIT, bundle["chart"]["commit"])
        self.assertEqual("docker.io/r2labuser/srsran-gnb-uhd-csi", image["repository"])
        self.assertEqual("v1.0.0.21", image["tag"])
        self.assertEqual(
            "sha256:7c3bd04fca5e241e9e245c52cc5882bb47c522a55c32b5ed1b9a1ed8fc56a7f2",
            image["digest"],
        )
        self.assertEqual(0, values["replicas"])
        self.assertEqual("Recreate", values["deploymentStrategy"])
        self.assertFalse(values["start"]["logs"])
        self.assertFalse(bundle["execution_enabled"])

    def test_bundle_binds_network_without_reconstructing_radio_values(self) -> None:
        bundle = build_physical_chart_bundle(
            lock=self.lock, plan=self.plan, bindings=self.bindings
        ).to_dict()
        config = bundle["values"]["gnbConfig"]
        amf = config["cu_cp"]["amf"]
        self.assertEqual(self.bindings.amf_n2_address, amf["addr"])
        self.assertEqual(self.bindings.gnb_n2_address, amf["bind_addr"])
        self.assertEqual(
            self.bindings.gnb_n2_address,
            config["cu_up"]["ngu"]["socket"][0]["bind_addr"],
        )
        self.assertNotIn("ru_sdr", config)
        self.assertNotIn("cell_cfg", config)
        self.assertEqual(PHYSICAL_VALUES_SOURCE, bundle["chart"]["values_source"])
        self.assertEqual(
            PHYSICAL_VALUES_SOURCE,
            bundle["review"]["configuration_source"],
        )
        self.assertFalse(bundle["review"]["radio_values_overridden"])
        self.assertTrue(bundle["review"]["image_digest_locked"])
        self.assertFalse(bundle["review"]["live_accepted"])

    def test_bundle_keeps_radio_configuration_out_of_the_override(self) -> None:
        values = build_physical_chart_bundle(
            lock=self.lock, plan=self.plan, bindings=self.bindings
        ).to_dict()["values"]
        serialized = json.dumps(values)
        self.assertNotIn("dl_arfcn", serialized)
        self.assertNotIn("channel_bandwidth_MHz", serialized)
        self.assertNotIn("tx_gain", serialized)
        self.assertNotIn("rx_gain", serialized)
        self.assertNotIn("device_args", serialized)

    def test_ru_network_is_exact_macvlan_binding(self) -> None:
        values = build_physical_chart_bundle(
            lock=self.lock, plan=self.plan, bindings=self.bindings
        ).to_dict()["values"]
        self.assertTrue(values["ru"])
        self.assertEqual("192.0.2.0/24", values["ruSubnet"])
        self.assertEqual("192.0.2.240", values["ruPodIp"])
        self.assertNotIn("usrp", values)
        self.assertEqual("sopnode-f3", values["nodeName"])

    def test_invalid_ru_binding_fails_closed(self) -> None:
        with self.assertRaisesRegex(R2LabPhysicalChartError, "reviewed RU subnet"):
            PhysicalChartBindings(
                amf_n2_address="198.51.100.200",
                gnb_n2_address="198.51.100.234",
                n300_address="192.0.2.103",
                ru_pod_address="203.0.113.240",
                ru_subnet="192.0.2.0/24",
            ).validate()

    def test_stopped_release_network_bindings_ignore_legacy_placement(self) -> None:
        values = build_physical_chart_bundle(
            lock=self.lock,
            plan=self.plan,
            bindings=self.bindings,
        ).to_dict()["values"]
        values["nodeName"] = "sopnode-f2"
        values["gnbConfig"]["ru_sdr"] = {
            "device_args": "addr=192.0.2.103,product=n300",
        }

        def runner(command, _timeout_seconds: int) -> CommandResult:
            rendered = " ".join(command)
            self.assertIn("StrictHostKeyChecking=yes", rendered)
            self.assertIn("helm get values", rendered)
            return CommandResult(0, json.dumps(values), "")

        with tempfile.TemporaryDirectory() as directory:
            known_hosts = Path(directory) / "known_hosts"
            known_hosts.write_text("fixture\n", encoding="utf-8")
            discovered = discover_physical_chart_bindings(
                known_hosts=known_hosts,
                runner=runner,
            )

        self.assertEqual(self.bindings, discovered)
        self.assertEqual("sopnode-f3", discovered.node_name)

    def test_template_overlay_installs_singleton_digest_contract(self) -> None:
        overlaid = overlay_pinned_deployment_template(
            source=DEPLOYMENT_FIXTURE, lock=self.lock
        )
        self.assertIn("type: {{ .Values.deploymentStrategy }}", overlaid)
        self.assertIn("replicas: {{ .Values.replicas }}", overlaid)
        self.assertIn("@{{ .Values.image.digest }}", overlaid)
        self.assertNotIn("replicas: 1", overlaid)

    def test_template_overlay_rejects_changed_upstream_shape(self) -> None:
        with self.assertRaisesRegex(R2LabPhysicalChartError, "overlay contract"):
            overlay_pinned_deployment_template(
                source="apiVersion: apps/v1\nkind: Deployment\nspec:\n  replicas: 2\n",
                lock=self.lock,
            )


class R2LabPhysicalWorkspaceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        self.bundle = build_physical_chart_bundle(
            lock=self.lock,
            plan=build_physical_deployment_plan(run_id="r2lab-workspace"),
            bindings=PhysicalChartBindings(
                amf_n2_address="198.51.100.200",
                gnb_n2_address="198.51.100.234",
                n300_address="192.0.2.103",
                ru_pod_address="192.0.2.240",
                ru_subnet="192.0.2.0/24",
            ),
        )

    def _create_chart(self, root: Path) -> Path:
        chart = root / "charts" / "srsran-gnb"
        templates = chart / "templates"
        templates.mkdir(parents=True)
        (chart / "Chart.yaml").write_text("apiVersion: v2\nname: srsran-gnb\n")
        (templates / "deployment.yaml").write_text(DEPLOYMENT_FIXTURE)
        (chart / Path(PHYSICAL_VALUES_SOURCE).name).write_text(
            SOURCE_VALUES_FIXTURE
        )
        return chart

    def test_workspace_applies_overlay_and_writes_json_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            chart = self._create_chart(root)
            result = materialize_physical_chart_workspace(
                checkout_root=root, lock=self.lock, bundle=self.bundle
            )
            overlaid = (chart / "templates" / "deployment.yaml").read_text()
            self.assertIn("type: {{ .Values.deploymentStrategy }}", overlaid)
            self.assertIn("replicas: {{ .Values.replicas }}", overlaid)
            self.assertIn("@{{ .Values.image.digest }}", overlaid)
            self.assertNotEqual(result.source_template_sha256, result.overlaid_template_sha256)
            values = json.loads((chart / VALUES_FILE_NAME).read_text())
            self.assertEqual(0, values["replicas"])
            self.assertEqual("Recreate", values["deploymentStrategy"])
            self.assertEqual(self.bundle.values["image"]["digest"], values["image"]["digest"])
            self.assertEqual(
                hashlib.sha256(SOURCE_VALUES_FIXTURE.encode()).hexdigest(),
                result.source_values_sha256,
            )
            self.assertEqual(64, len(result.values_sha256))

    def test_workspace_hashes_the_exact_source_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            chart = self._create_chart(root)
            source_path = chart / Path(PHYSICAL_VALUES_SOURCE).name
            source_bytes = SOURCE_VALUES_FIXTURE.replace("\n", "\r\n").encode()
            source_path.write_bytes(source_bytes)

            workspace = materialize_physical_chart_workspace(
                checkout_root=root,
                lock=self.lock,
                bundle=self.bundle,
            )
            artifact = package_physical_chart(
                workspace=workspace,
                run_id="r2lab-exact-source",
                destination=root / "artifacts",
            )

        expected = hashlib.sha256(source_bytes).hexdigest()
        self.assertEqual(expected, workspace.source_values_sha256)
        self.assertEqual(expected, artifact.source_values_sha256)

    def test_workspace_refuses_overwrite_of_generated_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            chart = self._create_chart(root)
            (chart / VALUES_FILE_NAME).write_text("{}\n")
            with self.assertRaisesRegex(R2LabPhysicalChartError, "already contains"):
                materialize_physical_chart_workspace(
                    checkout_root=root, lock=self.lock, bundle=self.bundle
                )

    def test_workspace_requires_reviewed_chart_structure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(R2LabPhysicalChartError, "chart structure"):
                materialize_physical_chart_workspace(
                    checkout_root=Path(directory), lock=self.lock, bundle=self.bundle
                )


class R2LabPhysicalHelmTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        self.bundle = build_physical_chart_bundle(
            lock=self.lock,
            plan=build_physical_deployment_plan(run_id="r2lab-helm"),
            bindings=PhysicalChartBindings(
                amf_n2_address="198.51.100.200",
                gnb_n2_address="198.51.100.234",
                n300_address="192.0.2.103",
                ru_pod_address="192.0.2.240",
                ru_subnet="192.0.2.0/24",
            ),
        )
        image = self.bundle.values["image"]
        self.expected_image = f"{image['repository']}:{image['tag']}@{image['digest']}"

    def valid_render(self) -> str:
        return f"""---
apiVersion: v1
kind: ConfigMap
metadata:
  name: gnb-configmap
data:
  srsran-gnb.yaml: |-
    cu_cp:
      amf:
        addr: 198.51.100.200
        bind_addr: 198.51.100.234
    ru_sdr:
      device_driver: uhd
      device_args: addr=192.0.2.103,name=ni-n3xx-31D98C7,product=n300,num_recv_frames=32,num_send_frames=32,recv_frame_size=8000,send_frame_size=8000
      srate: 61.44
      tx_gain: 35
      rx_gain: 60
    cell_cfg:
      dl_arfcn: 640000
      band: 78
      channel_bandwidth_MHz: 20
      common_scs: 30
      pdcch:
        common:
          ss0_index: 0
          coreset0_index: 12
      prach:
        prach_config_index: 1
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: srsran-gnb
spec:
  strategy:
    type: Recreate
  replicas: 0
  template:
    spec:
      containers:
        - name: gnb
          image: {self.expected_image}
          resources:
            requests:
              memory: \"{PHYSICAL_GNB_MEMORY}\"
              cpu: \"{PHYSICAL_GNB_CPU_COUNT}\"
            limits:
              memory: \"{PHYSICAL_GNB_MEMORY}\"
              cpu: \"{PHYSICAL_GNB_CPU_COUNT}\"
"""

    def test_valid_render_is_evidence_but_not_live_acceptance(self) -> None:
        source_digest = hashlib.sha256(SOURCE_VALUES_FIXTURE.encode()).hexdigest()
        payload = validate_physical_helm_render(
            text=self.valid_render(),
            bundle=self.bundle,
            source_values_text=SOURCE_VALUES_FIXTURE,
            source_values_sha256=source_digest,
        ).to_dict()
        self.assertEqual(0, payload["replicas"])
        self.assertEqual("Recreate", payload["strategy"])
        self.assertEqual(source_digest, payload["source_values_sha256"])
        self.assertEqual(640_000, payload["carrier_arfcn"])
        self.assertEqual(78, payload["band"])
        self.assertEqual(20, payload["channel_bandwidth_mhz"])
        self.assertEqual(30, payload["common_scs_khz"])
        self.assertEqual(61.44, payload["sample_rate_mhz"])
        self.assertEqual(35, payload["tx_gain_db"])
        self.assertEqual(60, payload["rx_gain_db"])
        self.assertEqual(0, payload["ss0_index"])
        self.assertEqual(12, payload["coreset0_index"])
        self.assertEqual(1, payload["prach_config_index"])
        self.assertEqual("offline-render-validated", payload["acceptance"])
        self.assertEqual(64, len(payload["sha256"]))

    def test_render_rejects_stale_nonreference_radio_values(self) -> None:
        stale = self.valid_render().replace("dl_arfcn: 640000", "dl_arfcn: 621312")
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "pinned R2Lab source"):
            self.validate(stale)

    def validate(self, text: str):
        return validate_physical_helm_render(
            text=text,
            bundle=self.bundle,
            source_values_text=SOURCE_VALUES_FIXTURE,
            source_values_sha256=hashlib.sha256(
                SOURCE_VALUES_FIXTURE.encode()
            ).hexdigest(),
        )

    def test_render_rejects_nonzero_replicas_or_rolling_strategy(self) -> None:
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "remain stopped"):
            self.validate(self.valid_render().replace("replicas: 0", "replicas: 1"))
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "Recreate"):
            self.validate(
                self.valid_render().replace("type: Recreate", "type: RollingUpdate")
            )

    def test_render_rejects_mutable_image_and_srsue_overrides(self) -> None:
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "digest-locked"):
            self.validate(
                self.valid_render().replace(
                    self.expected_image, self.expected_image.split("@", 1)[0]
                )
            )
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "pinned R2Lab source"):
            self.validate(
                text=self.valid_render().replace(
                    "          coreset0_index: 12",
                    "          coreset0_index: 11",
                )
            )

    def test_render_rejects_unpinned_optional_log_sidecar(self) -> None:
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "optional log sidecar"):
            self.validate(
                self.valid_render()
                + "      containers:\n        - name: gnb-logs\n          image: busybox\n",
            )

    def workspace(self, root: Path) -> PhysicalChartWorkspace:
        source_values = root / "values-n300-n78-20MHz.yaml"
        source_values.write_text(SOURCE_VALUES_FIXTURE)
        values = root / "synthran-physical-values.json"
        values.write_text("{}\n")
        return PhysicalChartWorkspace(
            chart_root=root,
            deployment_template=root / "templates/deployment.yaml",
            source_values_file=source_values,
            values_file=values,
            source_template_sha256="a" * 64,
            overlaid_template_sha256="b" * 64,
            source_values_sha256=hashlib.sha256(
                SOURCE_VALUES_FIXTURE.encode()
            ).hexdigest(),
            values_sha256=hashlib.sha256(b"{}\n").hexdigest(),
        )

    def test_offline_runner_checks_locked_helm_and_uses_template_only(self) -> None:
        commands: list[tuple[str, ...]] = []

        def runner(command, timeout_seconds: int) -> CommandResult:
            value = tuple(command)
            commands.append(value)
            if value == ("helm", "version", "--short"):
                return CommandResult(0, "v3.18.4+g123\n", "")
            return CommandResult(0, self.valid_render(), "")

        with tempfile.TemporaryDirectory() as directory:
            text, evidence = render_physical_chart_offline(
                lock=self.lock,
                bundle=self.bundle,
                workspace=self.workspace(Path(directory)),
                runner=runner,
            )
        self.assertEqual(self.valid_render(), text)
        self.assertEqual(640_000, evidence.carrier_arfcn)
        self.assertEqual(("helm", "version", "--short"), commands[0])
        self.assertEqual("template", commands[1][1])
        self.assertEqual(2, commands[1].count("--values"))
        self.assertNotIn("upgrade", commands[1])
        self.assertNotIn("install", commands[1])

    def test_offline_runner_rejects_unlocked_helm_version(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(R2LabPhysicalHelmError, "exactly match"):
                render_physical_chart_offline(
                    lock=self.lock,
                    bundle=self.bundle,
                    workspace=self.workspace(Path(directory)),
                    runner=lambda command, timeout_seconds: CommandResult(0, "v3.19.0\n", ""),
                )


class R2LabPhysicalArtifactTests(unittest.TestCase):
    def make_workspace(self, root: Path) -> PhysicalChartWorkspace:
        chart = root / "charts" / "srsran-gnb"
        templates = chart / "templates"
        templates.mkdir(parents=True)
        (chart / "Chart.yaml").write_text("apiVersion: v2\nname: srsran-gnb\nversion: 0.1.0\n")
        deployment = templates / "deployment.yaml"
        deployment.write_text("kind: Deployment\n")
        source_values = chart / Path(PHYSICAL_VALUES_SOURCE).name
        source_values.write_text(SOURCE_VALUES_FIXTURE)
        values = chart / VALUES_FILE_NAME
        values.write_text('{"replicas": 0}\n')
        values_sha256 = hashlib.sha256(values.read_bytes()).hexdigest()
        return PhysicalChartWorkspace(
            chart_root=chart,
            deployment_template=deployment,
            source_values_file=source_values,
            values_file=values,
            source_template_sha256="a" * 64,
            overlaid_template_sha256="b" * 64,
            source_values_sha256=hashlib.sha256(
                source_values.read_bytes()
            ).hexdigest(),
            values_sha256=values_sha256,
        )

    def test_same_workspace_packages_to_same_digest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self.make_workspace(root / "source")
            first = package_physical_chart(
                workspace=workspace, run_id="r2lab-artifact", destination=root / "out-a"
            )
            second = package_physical_chart(
                workspace=workspace, run_id="r2lab-artifact", destination=root / "out-b"
            )
            self.assertEqual(first.package_sha256, second.package_sha256)
            self.assertEqual(
                workspace.source_values_sha256,
                first.source_values_sha256,
            )
            self.assertEqual(workspace.values_sha256, first.values_sha256)
            self.assertEqual("offline-packaged-only", first.to_dict()["acceptance"])
            self.assertTrue(first.package_path.is_file())

    def test_package_changes_when_chart_content_changes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self.make_workspace(root / "source")
            first = package_physical_chart(
                workspace=workspace, run_id="r2lab-artifact-a", destination=root / "out-a"
            )
            workspace.deployment_template.write_text(
                "kind: Deployment\nmetadata:\n  name: changed\n"
            )
            second = package_physical_chart(
                workspace=workspace, run_id="r2lab-artifact-b", destination=root / "out-b"
            )
            self.assertNotEqual(first.package_sha256, second.package_sha256)

    def test_existing_package_is_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self.make_workspace(root / "source")
            destination = root / "out"
            package_physical_chart(
                workspace=workspace, run_id="r2lab-artifact", destination=destination
            )
            with self.assertRaisesRegex(R2LabPhysicalArtifactError, "already exists"):
                package_physical_chart(
                    workspace=workspace, run_id="r2lab-artifact", destination=destination
                )

    def test_symbolic_links_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self.make_workspace(root / "source")
            target = workspace.chart_root / "Chart.yaml"
            link = workspace.chart_root / "templates" / "linked.yaml"
            try:
                link.symlink_to(target)
            except OSError:
                self.skipTest("symbolic links are unavailable on this platform")
            with self.assertRaisesRegex(R2LabPhysicalArtifactError, "symbolic links"):
                package_physical_chart(
                    workspace=workspace, run_id="r2lab-artifact", destination=root / "out"
                )


class FakeGnbRunner:
    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.pods: list[dict[str, object]] = [self.ready_pod("gnb-old")]
        self.scale_zero_returncode = 0
        self.scale_one_returncode = 0
        self.on_scale_one = None

    @staticmethod
    def ready_pod(name: str) -> dict[str, object]:
        return {
            "metadata": {"name": name},
            "status": {
                POD_RUNTIME_STATE_KEY: "Running",
                "containerStatuses": [
                    {"name": "gnb", "ready": True},
                    {"name": "sidecar", "ready": True},
                ],
            },
        }

    @staticmethod
    def terminating_pod(name: str) -> dict[str, object]:
        return {
            "metadata": {
                "name": name,
                "deletionTimestamp": "2026-08-22T00:00:00Z",
            },
            "status": {
                POD_RUNTIME_STATE_KEY: "Running",
                "containerStatuses": [{"name": "gnb", "ready": True}],
            },
        }

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        if value == (
            "kubectl", "scale", f"deployment/{GNB_DEPLOYMENT}", "-n", GNB_NAMESPACE, "--replicas=0"
        ):
            self.pods = []
            return CommandResult(self.scale_zero_returncode, "", "")
        if value == (
            "kubectl", "scale", f"deployment/{GNB_DEPLOYMENT}", "-n", GNB_NAMESPACE, "--replicas=1"
        ):
            if self.on_scale_one is None:
                self.pods = [self.ready_pod("gnb-new")]
            else:
                self.on_scale_one(self)
            return CommandResult(self.scale_one_returncode, "", "")
        if value == (
            "kubectl", "get", "pods", "-n", GNB_NAMESPACE, "-l", GNB_SELECTOR, "-o", "json"
        ):
            return CommandResult(0, json.dumps({"items": self.pods}), "")
        raise AssertionError(f"unexpected command: {value}")


class R2LabGnbLifecycleTests(unittest.TestCase):
    def test_parser_counts_terminating_pod_as_existing_owner(self) -> None:
        observation = parse_gnb_pods_json(
            json.dumps(
                {
                    "items": [
                        FakeGnbRunner.ready_pod("current"),
                        FakeGnbRunner.terminating_pod("old"),
                    ]
                }
            )
        )
        self.assertEqual(2, observation.total_count)
        self.assertEqual(1, observation.ready_running_count)
        self.assertEqual(1, observation.terminating_count)
        self.assertFalse(observation.zero)
        self.assertFalse(observation.exactly_one_ready)

    def test_configuration_runs_only_after_zero_pods_are_proven(self) -> None:
        runner = FakeGnbRunner()
        events: list[str] = []

        def configure() -> None:
            observation = parse_gnb_pods_json(json.dumps({"items": runner.pods}))
            self.assertTrue(observation.zero)
            events.append("configured")

        result = execute_non_overlapping_gnb_update(
            runner=runner,
            configure=configure,
            sleeper=lambda _: None,
            shutdown_attempts=1,
            startup_attempts=1,
        )
        self.assertEqual(["configured"], events)
        self.assertTrue(result.stopped_before_configure)
        self.assertTrue(result.started_exactly_one)
        self.assertEqual(1, result.maximum_observed_pods)

    def test_nonzero_scale_returncode_does_not_override_observed_state(self) -> None:
        runner = FakeGnbRunner()
        runner.scale_zero_returncode = 1
        runner.scale_one_returncode = 1
        configured: list[bool] = []
        result = execute_non_overlapping_gnb_update(
            runner=runner,
            configure=lambda: configured.append(True),
            sleeper=lambda _: None,
            shutdown_attempts=1,
            startup_attempts=1,
        )
        self.assertEqual([True], configured)
        self.assertTrue(result.started_exactly_one)

    def test_configuration_failure_leaves_deployment_stopped(self) -> None:
        runner = FakeGnbRunner()
        with self.assertRaises(R2LabGnbLifecycleError):
            execute_non_overlapping_gnb_update(
                runner=runner,
                configure=lambda: (_ for _ in ()).throw(RuntimeError("render failed")),
                sleeper=lambda _: None,
                shutdown_attempts=1,
                startup_attempts=1,
            )
        self.assertEqual([], runner.pods)
        self.assertEqual(
            [], [command for command in runner.commands if command[-1] == "--replicas=1"]
        )

    def test_overlap_on_startup_requests_fail_closed_scale_zero(self) -> None:
        runner = FakeGnbRunner()

        def overlap(value: FakeGnbRunner) -> None:
            value.pods = [value.ready_pod("gnb-a"), value.ready_pod("gnb-b")]

        runner.on_scale_one = overlap
        with self.assertRaisesRegex(R2LabGnbLifecycleError, "overlapping gNB owners"):
            execute_non_overlapping_gnb_update(
                runner=runner,
                configure=lambda: None,
                sleeper=lambda _: None,
                shutdown_attempts=1,
                startup_attempts=1,
            )
        self.assertEqual([], runner.pods)
        self.assertEqual(
            2, sum(command[-1] == "--replicas=0" for command in runner.commands)
        )

    def test_malformed_pod_json_fails_closed(self) -> None:
        with self.assertRaises(R2LabGnbLifecycleError):
            parse_gnb_pods_json("not-json")
        with self.assertRaises(R2LabGnbLifecycleError):
            parse_gnb_pods_json(json.dumps({"items": ["bad"]}))


if __name__ == "__main__":
    unittest.main()
