from __future__ import annotations

from collections import namedtuple
from contextlib import ExitStack
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import signal
import tempfile
from typing import Sequence
import unittest
from unittest.mock import MagicMock, patch

UnameResult = namedtuple("UnameResult", ["sysname", "nodename", "release", "version", "machine"])
FAKE_UNAME = UnameResult("Linux", "duckburg", "6.5.0", "1", "x86_64")

from synthran.dependencies import load_lock
from synthran.experiment import (
    ExperimentCheck,
    ExperimentError,
    ExperimentScenario,
    TelemetryEvent,
)
from synthran.experiment_runtime import (
    CommandResult,
    ExperimentRunResult,
    ManagedProcess,
    _cleanup_live_resources,
    _cleanup_remote_run_processes,
    _reclaim_stale_experiment_runtime,
    _collect_rollout_diagnostics,
    _copy_sensor_source,
    _core_address,
    _discover_ue_deployment,
    _discover_ue_pod,
    _one_name,
    _prepare_cooja_checkout,
    _probe_experiment_host,
    _probe_ssh_forwarding,
    _remote_path_exists,
    _render_manifest,
    _ssh_reverse_tunnel_command,
    _validate_java_runtime,
    _wait_remote_tcp,
    _wait_tcp,
    execute_experiment,
)
from synthran.fiveg_ansible import InventoryHost, NetworkInventory, load_inventory
from synthran.resource_runtime import build_preparation_inventory
from synthran.rfsim_runtime import RfsimRuntimeState


class ExperimentRuntimeContractTests(unittest.TestCase):
    def test_manifest_never_claims_reservation_or_network_deployment(self) -> None:
        scenario = ExperimentScenario(
            "experiment-01",
            "network-accepted-01",
            "12.1.0.1",
        )
        manifest = _render_manifest(
            scenario,
            status="running",
            scenario_path=Path("scenario.json"),
        )
        self.assertEqual(manifest["reservation_action"], "none")
        self.assertEqual(manifest["network_deployment_action"], "none")
        self.assertEqual(manifest["network_run_id"], "network-accepted-01")
        self.assertEqual(manifest["schema"], "synthran/experiment-run/v1alpha1")

    def test_core_address_requires_literal_live_address(self) -> None:
        inventory_text = """[webshell]
localhost ansible_connection=local

[core_node]
lab-core ansible_host=192.0.2.10 ansible_user=root nic_interface=eth1 ip=192.0.2.10 storage=disk1

[ran_node]
lab-ran ansible_host=192.0.2.11 ansible_user=root nic_interface=eth1 ip=192.0.2.11 storage=disk1 boot_mode=live

[monitor_node]

[sopnodes:children]
core_node
ran_node

[k8s_workers:children]
ran_node

[all:vars]
core="open5gs"
ran="srsRAN"
core_node_name="lab-core"
ran_node_name="lab-ran"
rru="rfsim"
bridge_enabled=true
monitoring_enabled=false
"""
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "hosts.ini"
            path.write_text(inventory_text, encoding="utf-8")
            inventory = load_inventory(path)
            self.assertEqual(_core_address(inventory), "192.0.2.10")

    def test_core_address_accepts_generated_preparation_inventory(self) -> None:
        _text, inventory = build_preparation_inventory(
            core_node="sopnode-f2",
            ran_node="sopnode-f3",
            source=Path("hosts.ini"),
        )
        self.assertEqual(inventory.core_node.name, "sopnode-f2")
        self.assertEqual(inventory.core_node.variables.get("ip"), "172.28.2.77")
        self.assertEqual(_core_address(inventory), "172.28.2.77")

    def test_core_address_missing_ip_raises_experiment_error(self) -> None:
        inventory = NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost("lab-core", {}),
            ran_node=InventoryHost("lab-ran", {"ip": "192.0.2.11"}),
            all_vars={},
        )
        with self.assertRaisesRegex(
            ExperimentError,
            "^prepared inventory is missing the core node IP address$",
        ):
            _core_address(inventory)

    def test_core_address_malformed_ip_raises_experiment_error(self) -> None:
        inventory = NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost("lab-core", {"ip": "not-an-ip"}),
            ran_node=InventoryHost("lab-ran", {"ip": "192.0.2.11"}),
            all_vars={},
        )
        with self.assertRaisesRegex(
            ExperimentError,
            "^prepared inventory has an invalid core node IP address; expected a literal IPv4 or IPv6 address$",
        ):
            _core_address(inventory)


class ExperimentHostCapabilityProbeTests(unittest.TestCase):
    def _sample_inventory(self, host_name: str = "sopnode-f2") -> NetworkInventory:
        return NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost(
                host_name,
                {"ansible_host": "192.0.2.10", "ansible_user": "root", "ip": "192.0.2.10"},
            ),
            ran_node=InventoryHost(
                "sopnode-f3",
                {"ansible_host": "192.0.2.11", "ansible_user": "root", "ip": "192.0.2.11"},
            ),
            all_vars={},
        )

    def test_probe_passes_on_valid_root_environment(self) -> None:
        inventory = self._sample_inventory()
        valid_response = json.dumps(
            {
                "uid": 0,
                "tun_dev": True,
                "tun_exists": False,
                "missing_tools": [],
                "busy_ports": [],
            }
        )
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, valid_response, ""),
            ),
        ):
            _probe_experiment_host(inventory)

    def test_probe_fails_closed_when_non_root(self) -> None:
        inventory = self._sample_inventory()
        non_root_response = json.dumps(
            {
                "uid": 1000,
                "tun_dev": True,
                "tun_exists": False,
                "missing_tools": [],
                "busy_ports": [],
            }
        )
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, non_root_response, ""),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"\[FAIL\] experiment-host: remote host user is not root \(uid=1000\)",
            ):
                _probe_experiment_host(inventory)

    def test_probe_fails_closed_when_tun_dev_missing(self) -> None:
        inventory = self._sample_inventory()
        no_tun_response = json.dumps(
            {
                "uid": 0,
                "tun_dev": False,
                "tun_exists": False,
                "missing_tools": [],
                "busy_ports": [],
            }
        )
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, no_tun_response, ""),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"\[FAIL\] experiment-host: /dev/net/tun is unavailable on sopnode-f2",
            ):
                _probe_experiment_host(inventory)

    def test_probe_fails_closed_when_tools_missing(self) -> None:
        inventory = self._sample_inventory()
        missing_tools_response = json.dumps(
            {
                "uid": 0,
                "tun_dev": True,
                "tun_exists": False,
                "missing_tools": ["gcc", "make"],
                "busy_ports": [],
            }
        )
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, missing_tools_response, ""),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"\[FAIL\] experiment-host: required tools \['gcc', 'make'\] are missing on sopnode-f2",
            ):
                _probe_experiment_host(inventory)

    def test_probe_fails_closed_when_tun0_already_exists(self) -> None:
        inventory = self._sample_inventory()
        tun0_exists_response = json.dumps(
            {
                "uid": 0,
                "tun_dev": True,
                "tun_exists": True,
                "missing_tools": [],
                "busy_ports": [],
            }
        )
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, tun0_exists_response, ""),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"\[FAIL\] experiment-host: tun0 already exists on sopnode-f2; refusing to adopt or delete it",
            ):
                _probe_experiment_host(inventory)

    def test_probe_fails_closed_when_required_port_is_busy(self) -> None:
        inventory = self._sample_inventory()
        busy_port_response = json.dumps(
            {
                "uid": 0,
                "tun_dev": True,
                "tun_exists": False,
                "missing_tools": [],
                "busy_ports": [60001],
            }
        )
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, busy_port_response, ""),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"\[FAIL\] experiment-host: required ports \[60001\] are already in use on sopnode-f2",
            ):
                _probe_experiment_host(inventory)

    def test_probe_default_includes_remote_central_forward_port(self) -> None:
        inventory = self._sample_inventory()
        valid_response = json.dumps(
            {
                "uid": 0,
                "tun_dev": True,
                "tun_exists": False,
                "missing_tools": [],
                "busy_ports": [],
            }
        )
        captured: dict[str, str] = {}

        def fake_run(command, **kwargs):
            captured["command"] = " ".join(str(part) for part in command)
            return CommandResult(0, valid_response, "")

        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch("synthran.experiment_runtime._run", side_effect=fake_run),
        ):
            _probe_experiment_host(inventory)

        self.assertIn("18883", captured["command"])
        self.assertIn("18885", captured["command"])
        self.assertIn("60001", captured["command"])


class RemoteRuntimeRecoveryTests(unittest.TestCase):
    def _sample_inventory(self) -> NetworkInventory:
        return NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost("sopnode-f2", {"ip": "192.0.2.10"}),
            ran_node=InventoryHost("sopnode-f3", {"ip": "192.0.2.11"}),
            all_vars={},
        )

    def test_stale_recovery_reports_reclaimed_process_count(self) -> None:
        inventory = self._sample_inventory()
        with patch(
            "synthran.experiment_runtime._remote_process_reap",
            return_value={"killed": [10, 11], "blocked": [], "remaining": [], "workspaces": []},
        ):
            self.assertEqual(_reclaim_stale_experiment_runtime(inventory), 2)

    def test_exact_run_cleanup_uses_run_scoped_signatures(self) -> None:
        inventory = self._sample_inventory()
        with patch("synthran.experiment_runtime._remote_process_reap") as reap:
            _cleanup_remote_run_processes(
                inventory,
                remote_workspace="/tmp/synthran/iot-acceptance-test",
                ue_pod="srsran-ue-srsran-ue-abc123",
                central_deployment="synthran-exp-central-deadbeef",
            )
        kwargs = reap.call_args.kwargs
        self.assertFalse(kwargs["orphan_only"])
        joined = "\n".join(kwargs["patterns"])
        self.assertIn("iot\\-acceptance\\-test", joined)
        self.assertIn("18883:1883", joined)
        self.assertIn("18885:18884", joined)


class ReverseTunnelTests(unittest.TestCase):
    def test_reverse_tunnel_is_strictly_loopback_bound(self) -> None:
        inventory = NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost(
                "sopnode-f2",
                {"ansible_host": "192.0.2.10", "ansible_user": "root", "ip": "192.0.2.10"},
            ),
            ran_node=InventoryHost("sopnode-f3", {"ip": "192.0.2.11"}),
            all_vars={},
        )
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/tmp/known_hosts"}),
            patch("pathlib.Path.is_file", return_value=True),
        ):
            cmd = _ssh_reverse_tunnel_command(inventory, remote_port=60001, local_port=60001)

        self.assertIn("-N", cmd)
        self.assertIn("ExitOnForwardFailure=yes", cmd)
        self.assertIn("-R", cmd)
        self.assertIn("127.0.0.1:60001:127.0.0.1:60001", cmd)
        self.assertNotIn("0.0.0.0", cmd)
        self.assertNotIn("::", cmd)
        self.assertIn("root@192.0.2.10", cmd)


class CoojaCheckoutPreparationTests(unittest.TestCase):
    def test_prepare_cooja_checkout_scopes_to_tools_cooja_without_recursive(self) -> None:
        contiki = Path("/opt/contiki-ng")
        commands: list[tuple[str, ...]] = []

        def fake_checked(command: tuple[str, ...], **kwargs: object) -> str:
            commands.append(tuple(command))
            if "HEAD:tools/cooja" in command:
                return "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2\n"
            if command[-2:] == ("rev-parse", "HEAD"):
                return "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2\n"
            return ""

        with patch("synthran.experiment_runtime._checked", side_effect=fake_checked):
            target = _prepare_cooja_checkout(contiki)

        self.assertEqual(target, contiki / "tools" / "cooja")
        self.assertEqual(len(commands), 3)
        submodule_cmd = commands[0]
        self.assertEqual(
            submodule_cmd,
            (
                "git",
                "-C",
                str(contiki),
                "submodule",
                "update",
                "--init",
                "--checkout",
                "--",
                "tools/cooja",
            ),
        )
        for cmd in commands:
            self.assertNotIn("--recursive", cmd)
        self.assertEqual(
            commands[1],
            ("git", "-C", str(contiki), "rev-parse", "HEAD:tools/cooja"),
        )
        self.assertEqual(
            commands[2],
            ("git", "-C", str(contiki / "tools" / "cooja"), "rev-parse", "HEAD"),
        )


class UEDiscoveryTests(unittest.TestCase):
    def _sample_inventory(self) -> NetworkInventory:
        return NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost("lab-core", {"ip": "192.0.2.10"}),
            ran_node=InventoryHost("lab-ran", {"ip": "192.0.2.11"}),
            all_vars={},
        )

    def test_discover_ue_deployment_uses_helm_name_and_exact_run_id(self) -> None:
        inventory = self._sample_inventory()
        captured: dict[str, object] = {}

        def fake_remote_json(
            inv: NetworkInventory,
            cmd: str,
            *,
            label: str,
            timeout_seconds: int = 60,
        ) -> dict[str, object]:
            captured["inventory"] = inv
            captured["cmd"] = cmd
            captured["label"] = label
            return {"items": [{"metadata": {"name": "srsran-ue-test-deploy"}}]}

        with patch("synthran.experiment_runtime._remote_json", side_effect=fake_remote_json):
            name = _discover_ue_deployment(inventory, "net-run-12345")

        self.assertEqual(name, "srsran-ue-test-deploy")
        self.assertEqual(captured["label"], "srsUE Deployment discovery")
        cmd_str = str(captured["cmd"])
        self.assertIn("kubectl get deployments", cmd_str)
        self.assertIn("-l app.kubernetes.io/name=srsran-ue,synthran.run/id=net-run-12345", cmd_str)

    def test_discover_ue_pod_continues_to_use_component_ue_and_exact_run_id(self) -> None:
        inventory = self._sample_inventory()
        captured: dict[str, object] = {}

        def fake_remote_json(
            inv: NetworkInventory,
            cmd: str,
            *,
            label: str,
            timeout_seconds: int = 60,
        ) -> dict[str, object]:
            captured["inventory"] = inv
            captured["cmd"] = cmd
            captured["label"] = label
            return {"items": [{"metadata": {"name": "srsran-ue-pod-xyz"}}]}

        with patch("synthran.experiment_runtime._remote_json", side_effect=fake_remote_json):
            name = _discover_ue_pod(inventory, "net-run-12345")

        self.assertEqual(name, "srsran-ue-pod-xyz")
        self.assertEqual(captured["label"], "srsUE pod discovery")
        cmd_str = str(captured["cmd"])
        self.assertIn("kubectl get pods", cmd_str)
        self.assertIn("-l app=srsran,component=ue,synthran.run/id=net-run-12345", cmd_str)


class OneNameExtractionTests(unittest.TestCase):
    def test_one_name_extracts_name_successfully(self) -> None:
        payload = {"items": [{"metadata": {"name": "srsran-ue-resource"}}]}
        name = _one_name(payload, label="run-owned srsUE Deployment")
        self.assertEqual(name, "srsran-ue-resource")

    def test_one_name_fails_when_items_is_not_a_list(self) -> None:
        for malformed_payload in ({}, {"items": None}, {"items": "not-a-list"}, {"items": 123}):
            with self.assertRaisesRegex(
                ExperimentError,
                r"^run-owned srsUE Deployment discovery returned malformed data$",
            ):
                _one_name(malformed_payload, label="run-owned srsUE Deployment")

    def test_one_name_fails_when_no_resource_found(self) -> None:
        with self.assertRaisesRegex(
            ExperimentError,
            r"^no run-owned srsUE Deployment was found$",
        ):
            _one_name({"items": []}, label="run-owned srsUE Deployment")

    def test_one_name_fails_when_multiple_resources_found(self) -> None:
        payload = {
            "items": [
                {"metadata": {"name": "dep-1"}},
                {"metadata": {"name": "dep-2"}},
            ]
        }
        with self.assertRaisesRegex(
            ExperimentError,
            r"^multiple run-owned srsUE Deployment resources were found; refusing to choose one$",
        ):
            _one_name(payload, label="run-owned srsUE Deployment")


class RolloutDiagnosticsTests(unittest.TestCase):
    def _sample_inventory(self) -> NetworkInventory:
        return NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost(
                "lab-core",
                {"ansible_host": "192.0.2.10", "ansible_user": "root", "ip": "192.0.2.10"},
            ),
            ran_node=InventoryHost(
                "lab-ran",
                {"ansible_host": "192.0.2.11", "ansible_user": "root", "ip": "192.0.2.11"},
            ),
            all_vars={},
        )

    def test_collect_rollout_diagnostics_gathers_and_sanitizes(self) -> None:
        inventory = self._sample_inventory()
        executed_commands: list[Sequence[str]] = []
        subscriber_id = "00101" + "0000001121"
        subscriber_key = "fec86ba6" + "eb707ed0" + "8905757b" + "1bb44b8f"

        def fake_run(
            cmd: Sequence[str],
            *,
            timeout_seconds: int = 60,
            cwd: Path | None = None,
            input_text: str | None = None,
        ) -> CommandResult:
            executed_commands.append(cmd)
            cmd_str = " ".join(cmd)
            if "jsonpath=" in cmd_str:
                return CommandResult(0, "srsran-ue-pod-abc12", "")
            if "describe pod" in cmd_str:
                return CommandResult(0, f"Pod: srsran-ue-pod-abc12 hex {subscriber_key}", "")
            if "logs" in cmd_str:
                return CommandResult(0, f"Mosquitto starting on 192.168.1.50 id: {subscriber_id}", "")
            if "get events" in cmd_str:
                return CommandResult(0, "Event: BackOff FailedScheduling", "")
            return CommandResult(
                0,
                "NAME READY STATUS\nsrsran-ue-pod-abc12 1/2 CrashLoopBackOff",
                "",
            )

        with tempfile.TemporaryDirectory() as temporary:
            known_hosts = Path(temporary) / "known_hosts"
            known_hosts.write_text(
                "lab-core ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAI...\n",
                encoding="utf-8",
            )
            log_path = Path(temporary) / "logs" / "srsue-mqtt-rollout-diagnostics.log"
            private_path = Path(temporary) / "secret-path"
            with (
                patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": str(known_hosts)}),
                patch("synthran.experiment_runtime._run", side_effect=fake_run),
            ):
                _collect_rollout_diagnostics(
                    inventory,
                    network_run_id="net-run-12345",
                    log_path=log_path,
                    private_paths=(private_path,),
                )

            self.assertTrue(log_path.is_file())
            content = log_path.read_text(encoding="utf-8")
            self.assertIn("=== SynthRAN Rollout Diagnostics", content)
            self.assertNotIn(subscriber_key, content)
            self.assertIn("<secret>", content)


class FullRemoteExperimentRuntimeTests(unittest.TestCase):
    def _sample_inventory(self, core_name: str = "sopnode-f2") -> NetworkInventory:
        return NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost(
                core_name,
                {"ansible_host": "172.28.2.77", "ansible_user": "root", "ip": "172.28.2.77"},
            ),
            ran_node=InventoryHost(
                "sopnode-f3",
                {"ansible_host": "172.28.2.78", "ansible_user": "root", "ip": "172.28.2.78"},
            ),
            all_vars={},
        )

    def _network_artifacts(self, root: Path) -> tuple[Path, Path]:
        network_dir = root / "runs" / "net-01"
        network_dir.mkdir(parents=True)
        manifest_path = network_dir / "manifest.json"
        evidence_path = network_dir / "network-evidence.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "schema": "synthran/network-deployment/v1alpha1",
                    "run_id": "net-01",
                    "status": "path-proven",
                    "network_evidence": evidence_path.name,
                }
            ),
            encoding="utf-8",
        )
        evidence_path.write_text(
            json.dumps(
                {
                    "schema": "synthran/network-evidence/v1alpha1",
                    "run_id": "net-01",
                    "ready": True,
                    "path": {
                        "pdu_address": "12.1.0.1",
                        "pdu_network": "12.1.0.0/16",
                        "ue_interface": "tun_srsue1",
                        "slice": "slice1",
                        "sst": 1,
                        "dnn": "internet",
                    },
                    "checks": [{"name": "upf-path", "passed": True}],
                }
            ),
            encoding="utf-8",
        )
        return manifest_path, evidence_path

    def test_full_experiment_success_path(self) -> None:
        inventory = self._sample_inventory("sopnode-f2")
        progress_buffer = io.StringIO()
        commands_executed: list[tuple[str, ...]] = []

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            known_hosts = root / "known_hosts"
            known_hosts.write_text("sopnode-f2 ssh-ed25519 AAA...\n", encoding="utf-8")
            manifest_path, evidence_path = self._network_artifacts(root)
            lock = load_lock(Path("dependencies.lock.yml"))

            contiki_path = root / "contiki"
            (contiki_path / "tools" / "serial-io").mkdir(parents=True)
            (contiki_path / "tools" / "serial-io" / "Makefile").write_text("all:\n", encoding="utf-8")
            (contiki_path / "tools" / "serial-io" / "tunslip6.c").write_text("int main(){}\n", encoding="utf-8")
            java_home_path = root / "jvm_home"
            java_home_path.mkdir(parents=True)

            mock_cooja_proc = MagicMock()
            mock_cooja_proc.poll.return_value = 0

            # Mock telemetry file generation
            def mock_collect(scenario, *args, **kwargs):
                jsonl_path = kwargs.get("jsonl_path")
                if jsonl_path:
                    lines = []
                    for s_id in range(1, 11):
                        for seq in range(1, 4):
                            ev = TelemetryEvent(
                                run_id=scenario.run_id,
                                sensor_id=f"sensor-{s_id:02d}",
                                sequence=seq,
                                sensor_time_ms=1000 * seq,
                                value_milli=1000 + seq,
                            )
                            lines.append(json.dumps(ev.to_record(received_at_utc=datetime(2026, 8, 17, tzinfo=timezone.utc))))
                    jsonl_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
                res = MagicMock()
                res.completed = True
                res.records = 30
                res.sensors = 10
                return res

            with ExitStack() as stack:
                stack.enter_context(patch("sys.platform", "linux"))
                stack.enter_context(patch.object(os, "uname", return_value=FAKE_UNAME, create=True))
                stack.enter_context(patch.dict("os.environ", {"CONDA_DEFAULT_ENV": "synthran", "SYNTHRAN_KNOWN_HOSTS": str(known_hosts)}))
                stack.enter_context(patch("synthran.experiment_runtime.verify_network_path", return_value=MagicMock(ready=True)))
                stack.enter_context(patch("synthran.experiment_runtime._validate_contiki_checkout", return_value=contiki_path))
                stack.enter_context(patch("synthran.experiment_runtime._validate_java_runtime", return_value=java_home_path))
                stack.enter_context(patch("synthran.experiment_runtime._prepare_cooja_checkout"))
                mock_probe = stack.enter_context(patch("synthran.experiment_runtime._probe_experiment_host"))

                # Track remote calls
                def fake_remote(inv, *cmd, **kwargs):
                    commands_executed.append(cmd)
                    if cmd and cmd[0] == "python3" and "-c" in cmd:
                        return json.dumps({"killed": [], "blocked": [], "remaining": [], "workspaces": []})
                    return ""

                def fake_remote_json(inv, cmd, **kwargs):
                    if "ingress-snapshot" in cmd:
                        return {"accepted_connections": 10, "upstream_bytes": 4500, "downstream_bytes": 1200}
                    if "kubectl get deployments" in cmd:
                        return {"items": [{"metadata": {"name": "srsran-ue-deploy"}}]}
                    if "kubectl get pods" in cmd:
                        return {"items": [{"metadata": {"name": "srsran-ue-pod"}}]}
                    return {}

                stack.enter_context(patch("synthran.experiment_runtime._remote", side_effect=fake_remote))
                stack.enter_context(patch("synthran.experiment_runtime._remote_json", side_effect=fake_remote_json))
                stack.enter_context(patch("synthran.experiment_runtime._transfer_directory"))
                stack.enter_context(patch("synthran.experiment_runtime._transfer_file"))
                stack.enter_context(patch("synthran.experiment_runtime._kubectl_apply_object"))
                stack.enter_context(patch("synthran.experiment_runtime._kubectl_patch_deployment"))
                stack.enter_context(patch("synthran.experiment_runtime._wait_rollout"))
                stack.enter_context(
                    patch(
                        "synthran.experiment_runtime.reconcile_rfsim_runtime",
                        return_value=RfsimRuntimeState("srsran-ue-pod", "srsran-gnb-pod", "srsran-gnb", "12.1.0.2"),
                    )
                )
                stack.enter_context(patch("synthran.experiment_runtime._replace_edge_runtime_config"))
                stack.enter_context(patch("synthran.experiment_runtime._restart_edge_sidecar"))
                stack.enter_context(patch("synthran.experiment_runtime._add_ue_route"))

                counter_vals = [100, 100, 500, 200]  # tx_before, rx_before, tx_after, rx_after
                stack.enter_context(patch("synthran.experiment_runtime._interface_counter", side_effect=lambda *args: counter_vals.pop(0)))
                stack.enter_context(patch("synthran.experiment_runtime.time.sleep"))

                mock_proc = MagicMock()
                mock_proc.poll.return_value = None
                mock_stream = MagicMock()
                mock_start_proc = stack.enter_context(
                    patch(
                        "synthran.experiment_runtime._start_process",
                        return_value=ManagedProcess("test", mock_proc, root / "test.log", mock_stream),
                    )
                )
                stack.enter_context(patch("synthran.experiment_runtime._wait_tcp"))

                # Remote command runner mock
                def fake_run(cmd, *args, **kwargs):
                    cmd_str = " ".join(cmd)
                    if "sshd" in cmd_str:
                        return CommandResult(0, "allowtcpforwarding yes\n", "")
                    if "s.connect" in cmd_str:
                        return CommandResult(0, "ok\n", "")
                    if "ip -j address show dev tun0" in cmd_str or "show dev tun0" in cmd_str:
                        return CommandResult(0, '[{"addr_info":[{"local":"fd00::1"}]}]', "")
                    if "test -e" in cmd_str or "test -d" in cmd_str:
                        # Postconditions: tun0 and workspace absent (test returns 1)
                        return CommandResult(1, "", "")
                    return CommandResult(0, "", "")

                stack.enter_context(patch("synthran.experiment_runtime._run", side_effect=fake_run))
                stack.enter_context(patch("synthran.experiment_runtime.collect_mqtt", side_effect=mock_collect))
                stack.enter_context(
                    patch(
                        "synthran.experiment_runtime._cleanup_live_resources",
                        return_value=ExperimentCheck(
                            "cleanup-base-network",
                            True,
                            "experiment resources removed and accepted network path reproven",
                        ),
                    )
                )

                result = execute_experiment(
                    inventory=inventory,
                    lock=lock,
                    dependency_root=root / "deps",
                    network_manifest=manifest_path,
                    network_evidence=evidence_path,
                    run_id="iot-acceptance-test",
                    repository_root=Path(__file__).parent.parent,
                    run_root=root / "experiments",
                    progress=progress_buffer,
                )

            if not result.ready:
                print("DEBUG OUTPUT:\n", progress_buffer.getvalue())
                manifest_file = result.run_directory / "manifest.json"
                if manifest_file.is_file():
                    print("MANIFEST:\n", manifest_file.read_text(encoding="utf-8"))
            self.assertTrue(result.ready)
            output = progress_buffer.getvalue()
            self.assertIn("[synthran] experiment: iot-acceptance-test", output)
            self.assertIn("[synthran] network prerequisite: OK", output)
            self.assertIn("[synthran] experiment host: checking sopnode-f2...", output)
            self.assertIn("[synthran] experiment host: OK", output)
            self.assertIn("[synthran] Cooja dependency: OK", output)
            self.assertIn("[synthran] remote tunslip6 build: OK", output)
            self.assertIn("[synthran] accepted PDU: 12.1.0.1", output)
            self.assertIn("[synthran] runtime PDU: 12.1.0.2", output)
            self.assertIn("[synthran] serial bridge: ready on sopnode-f2", output)
            self.assertIn("[synthran] RPL border router: tun0 ready", output)
            self.assertIn("[synthran] collector: OK (30 events from 10 sensors)", output)
            self.assertIn("[synthran] [PASS] cleanup-base-network: experiment resources removed and accepted network path reproven", output)
            self.assertIn("[synthran] IOT-TO-5G PATH PROVEN", output)

            # Confirm NO sudo command was run
            for call in mock_start_proc.call_args_list:
                cmd_args = call[0][1]
                cmd_flat = " ".join(cmd_args) if isinstance(cmd_args, (list, tuple)) else str(cmd_args)
                self.assertNotIn("sudo", cmd_flat)

            # Confirm _probe_experiment_host was called with all three reserved ports
            self.assertEqual(mock_probe.call_count, 2)
            self.assertEqual(
                mock_probe.call_args_list[0].kwargs.get("required_ports"),
                (60001, 18883, 18885),
            )
            self.assertEqual(
                mock_probe.call_args_list[1].kwargs.get("required_ports"),
                (60001, 18883, 18885),
            )

    def test_early_tunslip_exit_fails_immediately_and_prints_cleanup(self) -> None:
        inventory = self._sample_inventory("sopnode-f2")
        progress_buffer = io.StringIO()

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            known_hosts = root / "known_hosts"
            known_hosts.write_text("sopnode-f2 ssh-ed25519 AAA...\n", encoding="utf-8")
            manifest_path, evidence_path = self._network_artifacts(root)
            lock = load_lock(Path("dependencies.lock.yml"))

            contiki_path = root / "contiki"
            (contiki_path / "tools" / "serial-io").mkdir(parents=True)
            (contiki_path / "tools" / "serial-io" / "Makefile").write_text("all:\n", encoding="utf-8")
            (contiki_path / "tools" / "serial-io" / "tunslip6.c").write_text("int main(){}\n", encoding="utf-8")
            java_home_path = root / "jvm_home"
            java_home_path.mkdir(parents=True)

            with ExitStack() as stack:
                stack.enter_context(patch("sys.platform", "linux"))
                stack.enter_context(patch.object(os, "uname", return_value=FAKE_UNAME, create=True))
                stack.enter_context(patch.dict("os.environ", {"CONDA_DEFAULT_ENV": "synthran", "SYNTHRAN_KNOWN_HOSTS": str(known_hosts)}))
                stack.enter_context(patch("synthran.experiment_runtime.verify_network_path", return_value=MagicMock(ready=True)))
                stack.enter_context(patch("synthran.experiment_runtime._validate_contiki_checkout", return_value=contiki_path))
                stack.enter_context(patch("synthran.experiment_runtime._validate_java_runtime", return_value=java_home_path))
                stack.enter_context(patch("synthran.experiment_runtime._prepare_cooja_checkout"))
                stack.enter_context(patch("synthran.experiment_runtime._probe_experiment_host"))
                def fake_remote_exec(inv, *cmd, **kwargs):
                    if cmd and cmd[0] == "python3" and "-c" in cmd:
                        return json.dumps({"killed": [], "blocked": [], "remaining": [], "workspaces": []})
                    return ""

                stack.enter_context(patch("synthran.experiment_runtime._remote", side_effect=fake_remote_exec))
                stack.enter_context(patch("synthran.experiment_runtime._remote_json", return_value={"items": [{"metadata": {"name": "ue-res"}}]}))
                stack.enter_context(patch("synthran.experiment_runtime._transfer_directory"))
                stack.enter_context(patch("synthran.experiment_runtime._transfer_file"))
                stack.enter_context(patch("synthran.experiment_runtime._kubectl_apply_object"))
                stack.enter_context(patch("synthran.experiment_runtime._kubectl_patch_deployment"))
                stack.enter_context(patch("synthran.experiment_runtime._wait_rollout"))
                stack.enter_context(
                    patch(
                        "synthran.experiment_runtime.reconcile_rfsim_runtime",
                        return_value=RfsimRuntimeState("srsran-ue-pod", "srsran-gnb-pod", "srsran-gnb", "12.1.0.2"),
                    )
                )
                stack.enter_context(patch("synthran.experiment_runtime._replace_edge_runtime_config"))
                stack.enter_context(patch("synthran.experiment_runtime._restart_edge_sidecar"))
                stack.enter_context(patch("synthran.experiment_runtime._add_ue_route"))
                stack.enter_context(patch("synthran.experiment_runtime._interface_counter", return_value=0))
                stack.enter_context(patch("synthran.experiment_runtime.time.sleep"))

                # Mock tunslip process that exits with code 1
                mock_tunslip_proc = MagicMock()
                mock_tunslip_proc.poll.return_value = 1
                mock_healthy_proc = MagicMock()
                mock_healthy_proc.poll.return_value = None

                def mock_start(name, *args, **kwargs):
                    if name == "tunslip6":
                        return ManagedProcess(name, mock_tunslip_proc, root / "tunslip6.log", MagicMock())
                    return ManagedProcess(name, mock_healthy_proc, root / f"{name}.log", MagicMock())

                stack.enter_context(patch("synthran.experiment_runtime._start_process", side_effect=mock_start))
                stack.enter_context(patch("synthran.experiment_runtime._wait_tcp"))
                def fake_run(cmd, *args, **kwargs):
                    cmd_str = " ".join(cmd)
                    if "sshd" in cmd_str:
                        return CommandResult(0, "allowtcpforwarding yes\n", "")
                    if "s.connect" in cmd_str:
                        return CommandResult(0, "ok\n", "")
                    if "test -e" in cmd_str or "test -d" in cmd_str:
                        # Postcondition checks: tun0 and workspace absent (test returns 1)
                        return CommandResult(1, "", "")
                    return CommandResult(1, "", "no dev")

                stack.enter_context(patch("synthran.experiment_runtime._run", side_effect=fake_run))
                stack.enter_context(
                    patch(
                        "synthran.experiment_runtime._cleanup_live_resources",
                        return_value=ExperimentCheck(
                            "cleanup-base-network",
                            True,
                            "experiment resources removed and accepted network path reproven",
                        ),
                    )
                )

                result = execute_experiment(
                    inventory=inventory,
                    lock=lock,
                    dependency_root=root / "deps",
                    network_manifest=manifest_path,
                    network_evidence=evidence_path,
                    run_id="iot-tunslip-fail",
                    repository_root=Path(__file__).parent.parent,
                    run_root=root / "experiments",
                    progress=progress_buffer,
                )

            self.assertFalse(result.ready)
            output = progress_buffer.getvalue()
            self.assertIn("[FAIL] serial bridge: remote tunslip6 exited", output)
            self.assertIn("host: sopnode-f2", output)
            self.assertIn("[PASS] cleanup-base-network: experiment resources removed and accepted network path reproven", output)
            self.assertIn("experiment path NOT PROVEN", output)


class ExperimentPrerequisitesTests(unittest.TestCase):
    def test_copy_sensor_source_copies_all_required_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "deploy" / "iot" / "sensor"
            source.mkdir(parents=True)
            (source / "Makefile").write_text("all:\n", encoding="utf-8")
            (source / "synthran-sensor.c").write_text("int main(){}\n", encoding="utf-8")
            (source / "project-conf.h").write_text("#define UIP_CONF_TCP 1\n", encoding="utf-8")

            run_dir = root / "runs" / "exp-01"
            _copy_sensor_source(root, run_dir)

            dest = run_dir / "sensor"
            self.assertTrue((dest / "Makefile").is_file())
            self.assertTrue((dest / "synthran-sensor.c").is_file())
            self.assertTrue((dest / "project-conf.h").is_file())
            self.assertEqual(
                (dest / "project-conf.h").read_text(encoding="utf-8"),
                "#define UIP_CONF_TCP 1\n",
            )

    def test_validate_java_runtime_accepts_java_21_on_stderr_and_derives_java_home(self) -> None:
        fake_result = MagicMock(returncode=0, stdout="", stderr='openjdk version "21.0.9" 2025-01-21\nOpenJDK Runtime Environment\n')
        with tempfile.TemporaryDirectory() as temporary:
            fake_bin = Path(temporary) / "env" / "bin" / "java"
            fake_bin.parent.mkdir(parents=True)
            fake_bin.write_text("#!/bin/sh\n", encoding="utf-8")
            with (
                patch("shutil.which", return_value=str(fake_bin)),
                patch("subprocess.run", return_value=fake_result),
            ):
                java_home = _validate_java_runtime()
                self.assertEqual(java_home.resolve(), (Path(temporary) / "env").resolve())

    def test_wait_tcp_fails_immediately_when_process_exits_early(self) -> None:
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 1
        managed = ManagedProcess(
            name="Cooja",
            process=mock_proc,
            log_path=Path("logs/cooja.log"),
            log_stream=MagicMock(),
        )
        with self.assertRaisesRegex(
            ExperimentError,
            r"Cooja exited with code 1 before TCP endpoint 127\.0\.0\.1:60001 became ready; see logs[/\\]cooja\.log",
        ):
            _wait_tcp("127.0.0.1", 60001, timeout_seconds=10, process=managed)

    def test_managed_process_stop_handles_running_and_stopped_processes(self) -> None:
        mock_proc = MagicMock()
        mock_proc.poll.return_value = None
        mock_proc.pid = 99999
        mock_stream = MagicMock()
        managed = ManagedProcess(
            name="Cooja",
            process=mock_proc,
            log_path=Path("logs/cooja.log"),
            log_stream=mock_stream,
        )
        with patch.object(os, "killpg", create=True) as mock_killpg:
            managed.stop()
            mock_killpg.assert_called_with(99999, signal.SIGTERM)
            mock_proc.wait.assert_called()
            mock_stream.close.assert_called()


class IfconfigPrerequisiteTests(unittest.TestCase):
    def _sample_inventory(self, host_name: str = "sopnode-f2") -> NetworkInventory:
        return NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost(
                host_name,
                {"ansible_host": "192.0.2.10", "ansible_user": "root", "ip": "192.0.2.10"},
            ),
            ran_node=InventoryHost(
                "sopnode-f3",
                {"ansible_host": "192.0.2.11", "ansible_user": "root", "ip": "192.0.2.11"},
            ),
            all_vars={},
        )

    def test_probe_passes_with_ifconfig_present(self) -> None:
        inventory = self._sample_inventory()
        valid_response = json.dumps(
            {
                "uid": 0,
                "tun_dev": True,
                "tun_exists": False,
                "missing_tools": [],
                "busy_ports": [],
            }
        )
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, valid_response, ""),
            ),
        ):
            _probe_experiment_host(inventory)

    def test_probe_fails_closed_when_ifconfig_missing(self) -> None:
        inventory = self._sample_inventory()
        missing_ifconfig_response = json.dumps(
            {
                "uid": 0,
                "tun_dev": True,
                "tun_exists": False,
                "missing_tools": ["ifconfig"],
                "busy_ports": [],
            }
        )
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, missing_ifconfig_response, ""),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"\[FAIL\] experiment-host: required tools \['ifconfig'\] are missing on sopnode-f2",
            ):
                _probe_experiment_host(inventory)


class SSHForwardingProbeTests(unittest.TestCase):
    def _sample_inventory(self, host_name: str = "sopnode-f2") -> NetworkInventory:
        return NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost(
                host_name,
                {"ansible_host": "192.0.2.10", "ansible_user": "root", "ip": "192.0.2.10"},
            ),
            ran_node=InventoryHost(
                "sopnode-f3",
                {"ansible_host": "192.0.2.11", "ansible_user": "root", "ip": "192.0.2.11"},
            ),
            all_vars={},
        )

    def test_forwarding_yes_passes(self) -> None:
        inventory = self._sample_inventory()
        sshd_output = "port 22\nallowTcpForwarding yes\npermitRootLogin yes\n"
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, sshd_output, ""),
            ),
        ):
            _probe_ssh_forwarding(inventory)

    def test_forwarding_all_passes(self) -> None:
        inventory = self._sample_inventory()
        sshd_output = "port 22\nallowtcpforwarding all\npermitRootLogin yes\n"
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, sshd_output, ""),
            ),
        ):
            _probe_ssh_forwarding(inventory)

    def test_forwarding_no_fails(self) -> None:
        inventory = self._sample_inventory()
        sshd_output = "port 22\nallowtcpforwarding no\npermitRootLogin yes\n"
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, sshd_output, ""),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"\[FAIL\] experiment-host: SSH forwarding required by the experiment is disabled",
            ):
                _probe_ssh_forwarding(inventory)

    def test_forwarding_local_only_fails(self) -> None:
        inventory = self._sample_inventory()
        sshd_output = "port 22\nallowtcpforwarding local\npermitRootLogin yes\n"
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, sshd_output, ""),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"\[FAIL\] experiment-host: SSH forwarding required by the experiment is disabled",
            ):
                _probe_ssh_forwarding(inventory)

    def test_forwarding_remote_only_fails(self) -> None:
        inventory = self._sample_inventory()
        sshd_output = "port 22\nallowtcpforwarding remote\npermitRootLogin yes\n"
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, sshd_output, ""),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"\[FAIL\] experiment-host: SSH forwarding required by the experiment is disabled",
            ):
                _probe_ssh_forwarding(inventory)

    def test_forwarding_malformed_sshd_fails(self) -> None:
        inventory = self._sample_inventory()
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(1, "", "sshd: command not found"),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"\[FAIL\] experiment-host: SSH forwarding required by the experiment is disabled",
            ):
                _probe_ssh_forwarding(inventory)

    def test_forwarding_missing_key_fails(self) -> None:
        inventory = self._sample_inventory()
        sshd_output = "port 22\npermitRootLogin yes\n"
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, sshd_output, ""),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"\[FAIL\] experiment-host: SSH forwarding required by the experiment is disabled",
            ):
                _probe_ssh_forwarding(inventory)


class TunOwnershipAndCleanupTests(unittest.TestCase):
    def _sample_inventory(self) -> NetworkInventory:
        return NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost(
                "sopnode-f2",
                {"ansible_host": "192.0.2.10", "ansible_user": "root", "ip": "192.0.2.10"},
            ),
            ran_node=InventoryHost(
                "sopnode-f3",
                {"ansible_host": "192.0.2.11", "ansible_user": "root", "ip": "192.0.2.11"},
            ),
            all_vars={},
        )

    def test_preexisting_tun0_is_rejected(self) -> None:
        inventory = self._sample_inventory()
        tun0_exists_response = json.dumps(
            {
                "uid": 0,
                "tun_dev": True,
                "tun_exists": True,
                "missing_tools": [],
                "busy_ports": [],
            }
        )
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, tun0_exists_response, ""),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"tun0 already exists.*refusing to adopt or delete it",
            ):
                _probe_experiment_host(inventory)

    def test_cleanup_failure_prevents_acceptance(self) -> None:
        inventory = self._sample_inventory()
        from synthran.dependencies import load_lock

        lock = load_lock(Path("dependencies.lock.yml"))
        scenario = ExperimentScenario("exp-01", "net-01", "12.1.0.1")
        check = _cleanup_live_resources(
            inventory=inventory,
            lock=lock,
            scenario=scenario,
            ue_deployment=None,
            remote_cleanup_errors=["remote tun0 cleanup: connection refused"],
        )
        self.assertFalse(check.passed)
        self.assertIn("remote tun0 cleanup", check.detail)

    def test_workspace_cleanup_failure_prevents_acceptance(self) -> None:
        inventory = self._sample_inventory()
        from synthran.dependencies import load_lock

        lock = load_lock(Path("dependencies.lock.yml"))
        scenario = ExperimentScenario("exp-01", "net-01", "12.1.0.1")
        check = _cleanup_live_resources(
            inventory=inventory,
            lock=lock,
            scenario=scenario,
            ue_deployment=None,
            remote_cleanup_errors=["remote workspace cleanup: permission denied"],
        )
        self.assertFalse(check.passed)
        self.assertIn("remote workspace cleanup", check.detail)

    def test_tun0_postcondition_failure_prevents_acceptance(self) -> None:
        inventory = self._sample_inventory()
        from synthran.dependencies import load_lock

        lock = load_lock(Path("dependencies.lock.yml"))
        scenario = ExperimentScenario("exp-01", "net-01", "12.1.0.1")
        check = _cleanup_live_resources(
            inventory=inventory,
            lock=lock,
            scenario=scenario,
            ue_deployment=None,
            remote_cleanup_errors=["remote tun0 cleanup postcondition: tun0 still exists"],
        )
        self.assertFalse(check.passed)
        self.assertIn("tun0 still exists", check.detail)

    def test_workspace_postcondition_failure_prevents_acceptance(self) -> None:
        inventory = self._sample_inventory()
        from synthran.dependencies import load_lock

        lock = load_lock(Path("dependencies.lock.yml"))
        scenario = ExperimentScenario("exp-01", "net-01", "12.1.0.1")
        check = _cleanup_live_resources(
            inventory=inventory,
            lock=lock,
            scenario=scenario,
            ue_deployment=None,
            remote_cleanup_errors=[
                "remote workspace cleanup postcondition: /tmp/synthran/exp-01 still exists"
            ],
        )
        self.assertFalse(check.passed)
        self.assertIn("still exists", check.detail)

    def test_successful_cleanup_with_no_errors_allows_acceptance(self) -> None:
        inventory = self._sample_inventory()
        from synthran.dependencies import load_lock

        lock = load_lock(Path("dependencies.lock.yml"))
        scenario = ExperimentScenario("exp-01", "net-01", "12.1.0.1")
        with (
            patch(
                "synthran.experiment_runtime._delete_experiment_objects",
                return_value=None,
            ),
            patch(
                "synthran.experiment_runtime.verify_network_path",
                return_value=MagicMock(ready=True),
            ),
        ):
            check = _cleanup_live_resources(
                inventory=inventory,
                lock=lock,
                scenario=scenario,
                ue_deployment=None,
                remote_cleanup_errors=[],
            )
        self.assertTrue(check.passed)
        self.assertIn("reproven", check.detail)

    def test_cleanup_accepts_cleanup_errors_keyword(self) -> None:
        inventory = self._sample_inventory()
        from synthran.dependencies import load_lock

        lock = load_lock(Path("dependencies.lock.yml"))
        scenario = ExperimentScenario("exp-01", "net-01", "12.1.0.1")
        check = _cleanup_live_resources(
            inventory=inventory,
            lock=lock,
            scenario=scenario,
            ue_deployment=None,
            cleanup_errors=["process stop (tunslip6): kill failed"],
        )
        self.assertFalse(check.passed)
        self.assertIn("process stop (tunslip6): kill failed", check.detail)

    def test_process_stop_failure_cannot_be_overridden_by_network_reproof(self) -> None:
        inventory = self._sample_inventory()
        from synthran.dependencies import load_lock

        lock = load_lock(Path("dependencies.lock.yml"))
        scenario = ExperimentScenario("exp-01", "net-01", "12.1.0.1")
        with (
            patch(
                "synthran.experiment_runtime._delete_experiment_objects",
                return_value=None,
            ),
            patch(
                "synthran.experiment_runtime.verify_network_path",
                return_value=MagicMock(ready=True),
            ),
        ):
            check = _cleanup_live_resources(
                inventory=inventory,
                lock=lock,
                scenario=scenario,
                ue_deployment=None,
                cleanup_errors=["process stop (Cooja simulator): exit timeout"],
            )
        self.assertFalse(check.passed)
        self.assertIn("process stop (Cooja simulator)", check.detail)


class RemotePathExistsTests(unittest.TestCase):
    def _sample_inventory(self) -> NetworkInventory:
        return NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost(
                "sopnode-f2",
                {"ansible_host": "192.0.2.10", "ansible_user": "root", "ip": "192.0.2.10"},
            ),
            ran_node=InventoryHost("sopnode-f3", {"ip": "192.0.2.11"}),
            all_vars={},
        )

    def test_remote_path_exists_returns_true_on_zero(self) -> None:
        inventory = self._sample_inventory()
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, "", ""),
            ),
        ):
            self.assertTrue(_remote_path_exists(inventory, "/sys/class/net/tun0"))

    def test_remote_path_absent_returns_false_on_one(self) -> None:
        inventory = self._sample_inventory()
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(1, "", ""),
            ),
        ):
            self.assertFalse(_remote_path_exists(inventory, "/sys/class/net/tun0"))

    def test_remote_path_unexpected_exit_code_raises(self) -> None:
        inventory = self._sample_inventory()
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(255, "", "SSH error"),
            ),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"remote existence probe for /sys/class/net/tun0 failed with exit code 255",
            ):
                _remote_path_exists(inventory, "/sys/class/net/tun0")


class IdempotentTunCleanupAndProcessStopExecutionTests(unittest.TestCase):
    def _sample_inventory(self) -> NetworkInventory:
        return NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost(
                "sopnode-f2",
                {"ansible_host": "192.0.2.10", "ansible_user": "root", "ip": "192.0.2.10"},
            ),
            ran_node=InventoryHost("sopnode-f3", {"ip": "192.0.2.11"}),
            all_vars={},
        )

    def _network_artifacts(self, root: Path) -> tuple[Path, Path]:
        manifest_path = root / "network-manifest.json"
        evidence_path = root / "network-evidence.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "schema": "synthran/network-deployment/v1alpha1",
                    "run_id": "net-01",
                    "status": "path-proven",
                    "network_evidence": evidence_path.name,
                }
            ),
            encoding="utf-8",
        )
        evidence_path.write_text(
            json.dumps(
                {
                    "schema": "synthran/network-evidence/v1alpha1",
                    "run_id": "net-01",
                    "ready": True,
                    "path": {
                        "pdu_address": "12.1.0.1",
                        "pdu_network": "12.1.0.0/16",
                        "ue_interface": "tun_srsue1",
                        "slice": "slice1",
                        "sst": 1,
                        "dnn": "internet",
                    },
                    "checks": [{"name": "upf-path", "passed": True}],
                }
            ),
            encoding="utf-8",
        )
        return manifest_path, evidence_path

    def _execute_test_experiment(
        self,
        *,
        tun0_initial_exists: bool = False,
        tun0_delete_error: Exception | None = None,
        tun0_postcondition_exists: bool = False,
        workspace_postcondition_exists: bool = False,
        process_stop_error: Exception | None = None,
        collector_error: Exception | None = None,
        early_tunslip_exit: bool = False,
    ) -> tuple[ExperimentRunResult, str, list[tuple[str, ...]], str | None]:
        inventory = self._sample_inventory()
        progress_buffer = io.StringIO()
        remote_calls: list[tuple[str, ...]] = []

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            known_hosts = root / "known_hosts"
            known_hosts.write_text("sopnode-f2 ssh-ed25519 AAA...\n", encoding="utf-8")
            manifest_path, evidence_path = self._network_artifacts(root)
            lock = load_lock(Path("dependencies.lock.yml"))

            contiki_path = root / "contiki"
            (contiki_path / "tools" / "serial-io").mkdir(parents=True)
            (contiki_path / "tools" / "serial-io" / "Makefile").write_text("all:\n", encoding="utf-8")
            (contiki_path / "tools" / "serial-io" / "tunslip6.c").write_text("int main(){}\n", encoding="utf-8")
            java_home_path = root / "jvm_home"
            java_home_path.mkdir(parents=True)

            def mock_collect(scenario, *args, **kwargs):
                if collector_error is not None:
                    raise collector_error
                jsonl_path = kwargs.get("jsonl_path")
                if jsonl_path:
                    lines = []
                    for s_id in range(1, 11):
                        for seq in range(1, 4):
                            ev = TelemetryEvent(
                                run_id=scenario.run_id,
                                sensor_id=f"sensor-{s_id:02d}",
                                sequence=seq,
                                sensor_time_ms=1000 * seq,
                                value_milli=1000 + seq,
                            )
                            lines.append(json.dumps(ev.to_record(received_at_utc=datetime(2026, 8, 17, tzinfo=timezone.utc))))
                    jsonl_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
                res = MagicMock()
                res.completed = True
                res.records = 30
                res.sensors = 10
                return res

            with ExitStack() as stack:
                stack.enter_context(patch("sys.platform", "linux"))
                stack.enter_context(patch.object(os, "uname", return_value=FAKE_UNAME, create=True))
                stack.enter_context(patch.dict("os.environ", {"CONDA_DEFAULT_ENV": "synthran", "SYNTHRAN_KNOWN_HOSTS": str(known_hosts)}))
                stack.enter_context(patch("synthran.experiment_runtime.verify_network_path", return_value=MagicMock(ready=True)))
                stack.enter_context(patch("synthran.experiment_runtime._validate_contiki_checkout", return_value=contiki_path))
                stack.enter_context(patch("synthran.experiment_runtime._validate_java_runtime", return_value=java_home_path))
                stack.enter_context(patch("synthran.experiment_runtime._prepare_cooja_checkout"))
                stack.enter_context(patch("synthran.experiment_runtime._probe_experiment_host"))

                def fake_remote(inv, *cmd, **kwargs):
                    remote_calls.append(cmd)
                    if "delete" in cmd and "tun0" in cmd:
                        if tun0_delete_error is not None:
                            raise tun0_delete_error
                    if cmd and cmd[0] == "python3" and "-c" in cmd:
                        return json.dumps({"killed": [], "blocked": [], "remaining": [], "workspaces": []})
                    return ""

                def fake_remote_json(inv, cmd, **kwargs):
                    if "ingress-snapshot" in cmd:
                        return {"accepted_connections": 10, "upstream_bytes": 4500, "downstream_bytes": 1200}
                    if "kubectl get deployments" in cmd:
                        return {"items": [{"metadata": {"name": "srsran-ue-deploy"}}]}
                    if "kubectl get pods" in cmd:
                        return {"items": [{"metadata": {"name": "srsran-ue-pod"}}]}
                    return {}

                stack.enter_context(patch("synthran.experiment_runtime._remote", side_effect=fake_remote))
                stack.enter_context(patch("synthran.experiment_runtime._remote_json", side_effect=fake_remote_json))
                stack.enter_context(patch("synthran.experiment_runtime._transfer_directory"))
                stack.enter_context(patch("synthran.experiment_runtime._transfer_file"))
                stack.enter_context(patch("synthran.experiment_runtime._kubectl_apply_object"))
                stack.enter_context(patch("synthran.experiment_runtime._kubectl_patch_deployment"))
                stack.enter_context(patch("synthran.experiment_runtime._wait_rollout"))
                stack.enter_context(patch("synthran.experiment_runtime._delete_experiment_objects"))
                stack.enter_context(
                    patch(
                        "synthran.experiment_runtime.reconcile_rfsim_runtime",
                        return_value=RfsimRuntimeState("srsran-ue-pod", "srsran-gnb-pod", "srsran-gnb", "12.1.0.2"),
                    )
                )
                stack.enter_context(patch("synthran.experiment_runtime._replace_edge_runtime_config"))
                stack.enter_context(patch("synthran.experiment_runtime._restart_edge_sidecar"))
                stack.enter_context(patch("synthran.experiment_runtime._add_ue_route"))

                counter_vals = [100, 100, 500, 200]
                stack.enter_context(patch("synthran.experiment_runtime._interface_counter", side_effect=lambda *args: counter_vals.pop(0)))
                stack.enter_context(patch("synthran.experiment_runtime.time.sleep"))

                mock_stream = MagicMock()

                def mock_start(name, *args, **kwargs):
                    mock_proc = MagicMock()
                    if early_tunslip_exit and name == "tunslip6":
                        mock_proc.poll.return_value = 1
                    else:
                        mock_proc.poll.return_value = None
                    managed = ManagedProcess(name, mock_proc, root / f"{name}.log", mock_stream)
                    if process_stop_error is not None and name == "tunslip6":
                        managed.stop = MagicMock(side_effect=process_stop_error)
                    return managed

                stack.enter_context(patch("synthran.experiment_runtime._start_process", side_effect=mock_start))
                stack.enter_context(patch("synthran.experiment_runtime._wait_tcp"))

                tun0_probe_count = 0

                def fake_run(cmd, *args, **kwargs):
                    nonlocal tun0_probe_count
                    cmd_str = " ".join(cmd)
                    if "sshd" in cmd_str:
                        return CommandResult(0, "allowtcpforwarding yes\n", "")
                    if "s.connect" in cmd_str:
                        return CommandResult(0, "ok\n", "")
                    if "ip -j address show dev tun0" in cmd_str or "show dev tun0" in cmd_str:
                        if early_tunslip_exit:
                            return CommandResult(1, "", "Device does not exist")
                        return CommandResult(0, '[{"addr_info":[{"local":"fd00::1"}]}]', "")
                    if "test -e /sys/class/net/tun0" in cmd_str:
                        tun0_probe_count += 1
                        if tun0_probe_count == 1:
                            return CommandResult(0 if tun0_initial_exists else 1, "", "")
                        else:
                            return CommandResult(0 if tun0_postcondition_exists else 1, "", "")
                    if "test -e" in cmd_str and "/tmp/synthran" in cmd_str:
                        return CommandResult(0 if workspace_postcondition_exists else 1, "", "")
                    return CommandResult(0, "", "")

                stack.enter_context(patch("synthran.experiment_runtime._run", side_effect=fake_run))
                stack.enter_context(patch("synthran.experiment_runtime.collect_mqtt", side_effect=mock_collect))

                result = execute_experiment(
                    inventory=inventory,
                    lock=lock,
                    dependency_root=root / "deps",
                    network_manifest=manifest_path,
                    network_evidence=evidence_path,
                    run_id="exp-idempotency-test",
                    repository_root=Path(__file__).parent.parent,
                    run_root=root / "experiments",
                    progress=progress_buffer,
                )

                manifest_file = result.run_directory / "manifest.json"
                manifest_text = manifest_file.read_text(encoding="utf-8") if manifest_file.is_file() else None

        return result, progress_buffer.getvalue(), remote_calls, manifest_text

    def test_1_tun0_automatically_disappeared_after_tunslip_stopped(self) -> None:
        result, output, remote_calls, _ = self._execute_test_experiment(
            tun0_initial_exists=False,
            tun0_postcondition_exists=False,
        )
        self.assertTrue(result.ready)
        self.assertIn("[PASS] cleanup-base-network:", output)
        self.assertIn("IOT-TO-5G PATH PROVEN", output)
        delete_calls = [c for c in remote_calls if "delete" in c and "tun0" in c]
        self.assertEqual(delete_calls, [])

    def test_2_tun0_exists_and_explicit_deletion_succeeds(self) -> None:
        result, output, remote_calls, _ = self._execute_test_experiment(
            tun0_initial_exists=True,
            tun0_postcondition_exists=False,
        )
        self.assertTrue(result.ready)
        self.assertIn("[PASS] cleanup-base-network:", output)
        self.assertIn("IOT-TO-5G PATH PROVEN", output)
        delete_calls = [c for c in remote_calls if "delete" in c and "tun0" in c]
        self.assertEqual(len(delete_calls), 1)
        self.assertEqual(delete_calls[0], ("ip", "link", "delete", "dev", "tun0"))

    def test_3_tun0_delete_fails_and_records_cleanup_error(self) -> None:
        result, output, remote_calls, _ = self._execute_test_experiment(
            tun0_initial_exists=True,
            tun0_delete_error=ExperimentError("ip link delete failed"),
            tun0_postcondition_exists=False,
        )
        self.assertFalse(result.ready)
        self.assertIn("[FAIL] cleanup-base-network: cleanup failed closed: remote tun0 cleanup: ip link delete failed", output)
        self.assertNotIn("[PASS] cleanup-base-network:", output)
        self.assertIn("experiment path NOT PROVEN", output)

    def test_4_tun0_remains_after_deletion_fails_postcondition(self) -> None:
        result, output, remote_calls, _ = self._execute_test_experiment(
            tun0_initial_exists=True,
            tun0_postcondition_exists=True,
        )
        self.assertFalse(result.ready)
        self.assertIn("[FAIL] cleanup-base-network: cleanup failed closed: remote tun0 cleanup postcondition: tun0 still exists", output)
        self.assertNotIn("[PASS] cleanup-base-network:", output)
        self.assertIn("experiment path NOT PROVEN", output)

    def test_5_creation_attempted_never_appeared_is_clean_success(self) -> None:
        result, output, remote_calls, _ = self._execute_test_experiment(
            early_tunslip_exit=True,
            tun0_initial_exists=False,
            tun0_postcondition_exists=False,
        )
        self.assertFalse(result.ready)
        self.assertIn("tunslip6 exited before tun0 became ready", output)
        delete_calls = [c for c in remote_calls if "delete" in c and "tun0" in c]
        self.assertEqual(delete_calls, [])
        self.assertIn("[PASS] cleanup-base-network:", output)

    def test_6_managed_process_stop_failure_is_part_of_cleanup_check(self) -> None:
        result, output, remote_calls, _ = self._execute_test_experiment(
            process_stop_error=RuntimeError("killpg failed"),
        )
        self.assertFalse(result.ready)
        self.assertIn("[FAIL] cleanup-base-network: cleanup failed closed: process stop (tunslip6): killpg failed", output)
        self.assertNotIn("[PASS] cleanup-base-network:", output)
        self.assertIn("experiment path NOT PROVEN", output)

    def test_7_stop_failure_cannot_be_overridden_by_network_reproof_success(self) -> None:
        result, output, remote_calls, _ = self._execute_test_experiment(
            process_stop_error=RuntimeError("killpg failed"),
            tun0_initial_exists=False,
            tun0_postcondition_exists=False,
        )
        self.assertFalse(result.ready)
        self.assertIn("[FAIL] cleanup-base-network:", output)
        self.assertNotIn("[PASS] cleanup-base-network:", output)

    def test_8_fully_successful_cleanup_passes(self) -> None:
        result, output, remote_calls, _ = self._execute_test_experiment(
            tun0_initial_exists=False,
            tun0_postcondition_exists=False,
        )
        self.assertTrue(result.ready)
        self.assertIn("[PASS] cleanup-base-network: experiment resources removed and accepted network path reproven", output)
        self.assertIn("IOT-TO-5G PATH PROVEN", output)

    def test_9_process_stop_failure_preserves_earlier_experiment_failure(self) -> None:
        result, output, remote_calls, manifest_text = self._execute_test_experiment(
            collector_error=ExperimentError("collector timeout"),
            process_stop_error=RuntimeError("killpg failed"),
        )
        self.assertFalse(result.ready)
        self.assertIn("[FAIL] cleanup-base-network: cleanup failed closed: process stop (tunslip6): killpg failed", output)
        self.assertIsNotNone(manifest_text)
        manifest_data = json.loads(manifest_text)
        self.assertEqual(manifest_data["status"], "failed")
        self.assertEqual(manifest_data["failure"], "collector timeout")


class RemoteEdgePortForwardReadinessTests(unittest.TestCase):
    def _sample_inventory(self) -> NetworkInventory:
        return NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost(
                "sopnode-f2",
                {"ansible_host": "192.0.2.10", "ansible_user": "root", "ip": "192.0.2.10"},
            ),
            ran_node=InventoryHost(
                "sopnode-f3",
                {"ansible_host": "192.0.2.11", "ansible_user": "root", "ip": "192.0.2.11"},
            ),
            all_vars={},
        )

    def test_ready_port_succeeds(self) -> None:
        inventory = self._sample_inventory()
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(0, "ok\n", ""),
            ),
            patch("synthran.experiment_runtime.time.sleep"),
        ):
            _wait_remote_tcp(
                inventory,
                host="127.0.0.1",
                port=18883,
                timeout_seconds=5,
            )

    def test_timeout_fails_clearly(self) -> None:
        inventory = self._sample_inventory()
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
            patch(
                "synthran.experiment_runtime._run",
                return_value=CommandResult(1, "", "Connection refused"),
            ),
            patch("synthran.experiment_runtime.time.sleep"),
            patch("synthran.experiment_runtime.time.monotonic", side_effect=[0.0, 0.0, 100.0, 100.0]),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"remote TCP endpoint 127\.0\.0\.1:18883 did not become ready",
            ):
                _wait_remote_tcp(
                    inventory,
                    host="127.0.0.1",
                    port=18883,
                    timeout_seconds=5,
                )

    def test_child_process_early_exit_fails_immediately(self) -> None:
        inventory = self._sample_inventory()
        mock_proc = MagicMock()
        mock_proc.poll.return_value = 1
        managed = ManagedProcess(
            name="edge MQTT port-forward",
            process=mock_proc,
            log_path=Path("logs/edge.log"),
            log_stream=MagicMock(),
        )
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/dev/null"}),
            patch("pathlib.Path.is_file", return_value=True),
        ):
            with self.assertRaisesRegex(
                ExperimentError,
                r"edge MQTT port-forward exited with code 1 before remote TCP endpoint",
            ):
                _wait_remote_tcp(
                    inventory,
                    host="127.0.0.1",
                    port=18883,
                    timeout_seconds=5,
                    process=managed,
                )


class DynamicPDURediscoveryTests(unittest.TestCase):
    def test_accepted_address_replaced_by_runtime_address(self) -> None:
        from dataclasses import replace

        scenario = ExperimentScenario("exp-01", "net-01", "12.1.0.1")
        self.assertEqual(scenario.pdu_address, "12.1.0.1")
        updated = replace(scenario, pdu_address="12.1.0.2")
        self.assertEqual(updated.pdu_address, "12.1.0.2")
        self.assertEqual(scenario.pdu_address, "12.1.0.1")

    def test_live_address_propagated_to_scenario(self) -> None:
        from dataclasses import replace

        scenario = ExperimentScenario("exp-01", "net-01", "12.1.0.1")
        runtime_address = "12.1.0.7"
        updated = replace(scenario, pdu_address=runtime_address)
        self.assertEqual(updated.pdu_address, "12.1.0.7")
        self.assertNotEqual(updated.pdu_address, scenario.pdu_address)


class ReverseTunnelStrictnessTests(unittest.TestCase):
    def test_reverse_tunnel_has_no_wildcard_binding(self) -> None:
        inventory = NetworkInventory(
            path=Path("hosts.ini"),
            sha256="0" * 64,
            core_node=InventoryHost(
                "sopnode-f2",
                {"ansible_host": "192.0.2.10", "ansible_user": "root", "ip": "192.0.2.10"},
            ),
            ran_node=InventoryHost("sopnode-f3", {"ip": "192.0.2.11"}),
            all_vars={},
        )
        with (
            patch.dict("os.environ", {"SYNTHRAN_KNOWN_HOSTS": "/tmp/known_hosts"}),
            patch("pathlib.Path.is_file", return_value=True),
        ):
            cmd = _ssh_reverse_tunnel_command(inventory, remote_port=60001, local_port=60001)

        self.assertIn("-N", cmd)
        self.assertIn("ExitOnForwardFailure=yes", cmd)
        self.assertIn("-R", cmd)
        self.assertIn("127.0.0.1:60001:127.0.0.1:60001", cmd)
        # Negative assertions: no wildcard bindings
        for part in cmd:
            self.assertNotIn("0.0.0.0", part, "reverse tunnel must not bind to 0.0.0.0")
            if part != "root@192.0.2.10":
                self.assertNotIn("::", part, "reverse tunnel must not bind to [::]")


if __name__ == "__main__":
    unittest.main()
