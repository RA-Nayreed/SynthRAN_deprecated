from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from synthran.dependencies import load_lock
from synthran.live_preflight import CommandResult
from synthran.r2lab.deployment import (
    AMF_ADDRESS_PLACEHOLDER,
    GNB_BIND_ADDRESS_PLACEHOLDER,
    GNB_DEPLOYMENT,
    GNB_NAMESPACE,
    GNB_SELECTOR,
    N300_DEVICE_ARGS_PLACEHOLDER,
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
    execute_non_overlapping_gnb_update,
    materialize_physical_chart_workspace,
    overlay_pinned_deployment_template,
    package_physical_chart,
    parse_gnb_pods_json,
    render_physical_chart_offline,
    render_physical_srsran,
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
    def test_plan_is_separate_nonexecuting_r2lab_boundary(self) -> None:
        plan = build_physical_deployment_plan(run_id="r2lab-physical-plan")
        payload = plan.to_dict()

        self.assertFalse(payload["execution_enabled"])
        self.assertEqual("offline-plan-only", payload["acceptance"])
        self.assertEqual("r2lab", payload["backend"])
        self.assertEqual("open5gs", payload["core"])
        self.assertEqual("srsran", payload["ran"])
        self.assertEqual("n300", payload["radio"])
        self.assertEqual("Recreate", payload["deployment"]["strategy"])
        self.assertEqual(1, payload["deployment"]["max_concurrent_gnb_pods"])
        self.assertFalse(payload["deployment"]["srsue_specific_overrides"])
        self.assertIsNone(payload["deployment"]["coreset0_index_override"])
        self.assertIsNone(payload["deployment"]["prach_config_index_override"])
        self.assertFalse(payload["safety"]["rolling_overlap_allowed"])
        self.assertFalse(payload["safety"]["virtual_adapter_modified"])
        self.assertFalse(payload["safety"]["live_acceptance_claimed"])

    def test_reference_aligned_plan_preserves_carrier_and_ssb_semantics(self) -> None:
        payload = build_physical_deployment_plan(
            run_id="r2lab-physical-frequency"
        ).to_dict()
        intent = payload["radio_intent"]
        carrier = intent["profile"]["carrier"]
        ssb = intent["expected_ssb"]
        self.assertEqual(621_312, carrier["arfcn"])
        self.assertEqual("carrier-center", carrier["semantic"])
        self.assertEqual(621_312, ssb["arfcn"])
        self.assertEqual("ssb", ssb["semantic"])
        self.assertEqual(carrier["arfcn"], ssb["arfcn"])
        self.assertEqual(40, intent["profile"]["channel_bandwidth_mhz"])
        self.assertEqual(2, intent["profile"]["nof_antennas_dl"])
        self.assertEqual(2, intent["profile"]["nof_antennas_ul"])

    def test_render_never_claims_live_acceptance(self) -> None:
        rendered = build_physical_deployment_plan(
            run_id="r2lab-physical-render"
        ).render()
        self.assertIn("NON-EXECUTING", rendered)
        self.assertIn("not live accepted", rendered)
        self.assertIn("Recreate", rendered)
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

    def test_gain_boundary_is_fail_closed(self) -> None:
        with self.assertRaises(R2LabPhysicalDeploymentError):
            build_physical_deployment_plan(run_id="r2lab-high-tx", tx_gain_db=31)
        with self.assertRaises(R2LabPhysicalDeploymentError):
            build_physical_deployment_plan(run_id="r2lab-high-rx", rx_gain_db=41)


class R2LabPhysicalRenderTests(unittest.TestCase):
    def test_render_preserves_reference_aligned_radio_semantics(self) -> None:
        rendered = render_physical_srsran(
            build_physical_deployment_plan(run_id="r2lab-render")
        )
        payload = rendered.to_dict()
        config = payload["gnb_config"]
        cell = config["cell_cfg"]
        self.assertEqual(621_312, cell["dl_arfcn"])
        self.assertEqual(78, cell["band"])
        self.assertEqual(40, cell["channel_bandwidth_MHz"])
        self.assertEqual(30, cell["common_scs"])
        self.assertEqual(2, cell["nof_antennas_dl"])
        self.assertEqual(2, cell["nof_antennas_ul"])
        self.assertEqual([{"sst": 1}], cell["slicing"])
        review = config["synthran_review"]
        self.assertEqual(621_312, review["expected_ssb_arfcn"])
        self.assertEqual(620_040, review["reference_point_a_arfcn"])
        self.assertEqual(106, review["reference_carrier_prbs"])
        self.assertEqual(30, review["reference_scs_khz"])
        self.assertEqual(40, review["reference_nominal_bandwidth_mhz"])
        self.assertFalse(review["live_accepted"])

    def test_render_matches_pinned_cu_cp_and_remote_control_shape(self) -> None:
        config = render_physical_srsran(
            build_physical_deployment_plan(run_id="r2lab-chart-shape")
        ).to_dict()["gnb_config"]
        amf = config["cu_cp"]["amf"]
        self.assertEqual(AMF_ADDRESS_PLACEHOLDER, amf["addr"])
        self.assertEqual(GNB_BIND_ADDRESS_PLACEHOLDER, amf["bind_addr"])
        self.assertEqual(38412, amf["port"])
        self.assertEqual(8001, config["remote_control"]["port"])
        self.assertTrue(config["remote_control"]["enabled"])
        self.assertEqual(
            [{"sst": 1}],
            amf["supported_tracking_areas"][0]["plmn_list"][0][
                "tai_slice_support_list"
            ],
        )

    def test_render_keeps_runtime_network_values_as_placeholders(self) -> None:
        config = render_physical_srsran(
            build_physical_deployment_plan(run_id="r2lab-placeholders")
        ).to_dict()["gnb_config"]
        amf = config["cu_cp"]["amf"]
        self.assertEqual(AMF_ADDRESS_PLACEHOLDER, amf["addr"])
        self.assertEqual(GNB_BIND_ADDRESS_PLACEHOLDER, amf["bind_addr"])
        self.assertEqual(N300_DEVICE_ARGS_PLACEHOLDER, config["ru_sdr"]["device_args"])

    def test_render_is_uhd_recreate_and_stopped_before_lifecycle_start(self) -> None:
        payload = render_physical_srsran(
            build_physical_deployment_plan(run_id="r2lab-recreate")
        ).to_dict()
        self.assertEqual("uhd", payload["gnb_config"]["ru_sdr"]["device_driver"])
        self.assertEqual(0, payload["deployment"]["replicas"])
        self.assertEqual("Recreate", payload["deployment"]["strategy"]["type"])
        self.assertEqual(1, payload["deployment"]["desired_replicas_after_lifecycle_start"])
        self.assertFalse(payload["execution_ready"])
        self.assertEqual("offline-render-only", payload["acceptance"])

    def test_render_does_not_inherit_srsue_specific_overrides(self) -> None:
        cell = render_physical_srsran(
            build_physical_deployment_plan(run_id="r2lab-cots")
        ).to_dict()["gnb_config"]["cell_cfg"]
        self.assertNotIn("pdcch", cell)
        self.assertNotIn("prach", cell)
        self.assertNotIn("coreset0_index", str(cell))
        self.assertNotIn("prach_config_index", str(cell))

    def test_render_contains_no_rfsim_settings(self) -> None:
        text = render_physical_srsran(
            build_physical_deployment_plan(run_id="r2lab-physical-clean-render")
        ).render_json()
        self.assertNotIn("rfsim", text.lower())


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

    def test_bundle_binds_runtime_network_without_leaking_review_metadata(self) -> None:
        bundle = build_physical_chart_bundle(
            lock=self.lock, plan=self.plan, bindings=self.bindings
        ).to_dict()
        config = bundle["values"]["gnbConfig"]
        amf = config["cu_cp"]["amf"]
        self.assertEqual(self.bindings.amf_n2_address, amf["addr"])
        self.assertEqual(self.bindings.gnb_n2_address, amf["bind_addr"])
        self.assertEqual(
            f"addr={self.bindings.n300_address},type=n3xx",
            config["ru_sdr"]["device_args"],
        )
        self.assertNotIn("synthran_review", config)
        self.assertTrue(bundle["review"]["reference_aligned"])
        self.assertEqual(106, bundle["review"]["reference_carrier_prbs"])
        self.assertEqual(40, bundle["review"]["reference_nominal_bandwidth_mhz"])
        self.assertTrue(bundle["review"]["image_digest_locked"])
        self.assertFalse(bundle["review"]["live_accepted"])

    def test_bundle_preserves_40mhz_2x2_and_removes_srsue_overrides(self) -> None:
        cell = build_physical_chart_bundle(
            lock=self.lock, plan=self.plan, bindings=self.bindings
        ).to_dict()["values"]["gnbConfig"]["cell_cfg"]
        self.assertEqual(621_312, cell["dl_arfcn"])
        self.assertEqual(40, cell["channel_bandwidth_MHz"])
        self.assertEqual(2, cell["nof_antennas_dl"])
        self.assertEqual(2, cell["nof_antennas_ul"])
        self.assertNotIn("pdcch", cell)
        self.assertNotIn("prach", cell)

    def test_ru_network_is_exact_macvlan_binding(self) -> None:
        values = build_physical_chart_bundle(
            lock=self.lock, plan=self.plan, bindings=self.bindings
        ).to_dict()["values"]
        usrp = values["usrp"]
        self.assertEqual("r2lab_usrp", values["ru"])
        self.assertEqual("r2lab_usrp", usrp["master"])
        self.assertEqual("macvlan", usrp["type"])
        self.assertEqual("bridge", usrp["mode"])
        self.assertEqual(9216, usrp["mtu"])
        self.assertEqual("192.0.2.0/24", usrp["ipam"]["subnet"])
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
            self.assertEqual(64, len(result.values_sha256))

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
      device_args: addr=192.0.2.103,type=n3xx
    cell_cfg:
      dl_arfcn: 621312
      band: 78
      channel_bandwidth_MHz: 40
      common_scs: 30
      nof_antennas_dl: 2
      nof_antennas_ul: 2
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
        payload = validate_physical_helm_render(
            text=self.valid_render(), bundle=self.bundle
        ).to_dict()
        self.assertEqual(0, payload["replicas"])
        self.assertEqual("Recreate", payload["strategy"])
        self.assertEqual(621_312, payload["carrier_arfcn"])
        self.assertEqual(40, payload["channel_bandwidth_mhz"])
        self.assertEqual(2, payload["antennas_dl"])
        self.assertEqual(2, payload["antennas_ul"])
        self.assertEqual("offline-render-validated", payload["acceptance"])
        self.assertEqual(64, len(payload["sha256"]))

    def test_render_rejects_stale_smoke003_radio_values(self) -> None:
        stale = self.valid_render().replace("dl_arfcn: 621312", "dl_arfcn: 621984")
        stale = stale.replace("channel_bandwidth_MHz: 40", "channel_bandwidth_MHz: 60")
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "reviewed chart intent"):
            validate_physical_helm_render(text=stale, bundle=self.bundle)

    def test_render_rejects_nonzero_replicas_or_rolling_strategy(self) -> None:
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "remain stopped"):
            validate_physical_helm_render(
                text=self.valid_render().replace("replicas: 0", "replicas: 1"),
                bundle=self.bundle,
            )
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "Recreate"):
            validate_physical_helm_render(
                text=self.valid_render().replace("type: Recreate", "type: RollingUpdate"),
                bundle=self.bundle,
            )

    def test_render_rejects_mutable_image_and_srsue_overrides(self) -> None:
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "digest-locked"):
            validate_physical_helm_render(
                text=self.valid_render().replace(
                    self.expected_image, self.expected_image.split("@", 1)[0]
                ),
                bundle=self.bundle,
            )
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "srsUE-specific"):
            validate_physical_helm_render(
                text=self.valid_render().replace(
                    "      nof_antennas_ul: 2",
                    "      nof_antennas_ul: 2\n      coreset0_index: 12",
                ),
                bundle=self.bundle,
            )

    def test_render_rejects_unpinned_optional_log_sidecar(self) -> None:
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "optional log sidecar"):
            validate_physical_helm_render(
                text=self.valid_render()
                + "      containers:\n        - name: gnb-logs\n          image: busybox\n",
                bundle=self.bundle,
            )

    def test_offline_runner_checks_locked_helm_and_uses_template_only(self) -> None:
        workspace = PhysicalChartWorkspace(
            chart_root=Path("/tmp/chart"),
            deployment_template=Path("/tmp/chart/templates/deployment.yaml"),
            values_file=Path("/tmp/chart/synthran-physical-values.json"),
            source_template_sha256="a" * 64,
            overlaid_template_sha256="b" * 64,
            values_sha256="c" * 64,
        )
        commands: list[tuple[str, ...]] = []

        def runner(command, timeout_seconds: int) -> CommandResult:
            value = tuple(command)
            commands.append(value)
            if value == ("helm", "version", "--short"):
                return CommandResult(0, "v3.18.4+g123\n", "")
            return CommandResult(0, self.valid_render(), "")

        text, evidence = render_physical_chart_offline(
            lock=self.lock,
            bundle=self.bundle,
            workspace=workspace,
            runner=runner,
        )
        self.assertEqual(self.valid_render(), text)
        self.assertEqual(621_312, evidence.carrier_arfcn)
        self.assertEqual(("helm", "version", "--short"), commands[0])
        self.assertEqual("template", commands[1][1])
        self.assertIn("--values", commands[1])
        self.assertNotIn("upgrade", commands[1])
        self.assertNotIn("install", commands[1])

    def test_offline_runner_rejects_unlocked_helm_version(self) -> None:
        workspace = PhysicalChartWorkspace(
            chart_root=Path("/tmp/chart"),
            deployment_template=Path("/tmp/chart/templates/deployment.yaml"),
            values_file=Path("/tmp/chart/synthran-physical-values.json"),
            source_template_sha256="a" * 64,
            overlaid_template_sha256="b" * 64,
            values_sha256="c" * 64,
        )
        with self.assertRaisesRegex(R2LabPhysicalHelmError, "exactly match"):
            render_physical_chart_offline(
                lock=self.lock,
                bundle=self.bundle,
                workspace=workspace,
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
        values = chart / VALUES_FILE_NAME
        values.write_text('{"replicas": 0}\n')
        values_sha256 = hashlib.sha256(values.read_bytes()).hexdigest()
        return PhysicalChartWorkspace(
            chart_root=chart,
            deployment_template=deployment,
            values_file=values,
            source_template_sha256="a" * 64,
            overlaid_template_sha256="b" * 64,
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