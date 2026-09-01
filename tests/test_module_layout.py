from __future__ import annotations

from pathlib import Path
import subprocess
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE = REPOSITORY_ROOT / "synthran"


def _tracked(path: str) -> bool:
    result = subprocess.run(
        ["git", "ls-files", "--", path],
        cwd=REPOSITORY_ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return bool(result.stdout.strip())


def _source(relative: str) -> str:
    return (SOURCE / relative).read_text(encoding="utf-8")


class ModuleLayoutTests(unittest.TestCase):
    def test_active_runtime_code_is_grouped_by_domain(self) -> None:
        required = (
            "cli.py",
            "errors.py",
            "lifecycle.py",
            "run_events.py",
            "adapters/fiveg.py",
            "experiment/__init__.py",
            "experiment/live.py",
            "experiment/physical.py",
            "experiment/resources.py",
            "network/__init__.py",
            "network/runtime.py",
            "research/__init__.py",
            "research/amber_campaign.py",
            "research/amber_runtime.py",
            "research/calibration.py",
            "research/path.py",
            "research/v2.py",
            "utils/__init__.py",
            "utils/environment.py",
            "utils/ssh.py",
        )
        for relative in required:
            self.assertTrue((SOURCE / relative).is_file(), relative)

    def test_retired_architecture_packages_do_not_return(self) -> None:
        for relative in (
            "app",
            "backends",
            "control",
            "operations",
            "resources",
            "r2lab",
            "workspace",
        ):
            self.assertFalse(_tracked(f"synthran/{relative}"), relative)

    def test_retired_provider_and_deployment_layers_do_not_return(self) -> None:
        for relative in (
            "provider.py",
            "slices_controller.py",
            "network/resources.py",
            "upstream_overlay.py",
        ):
            self.assertFalse(_tracked(f"synthran/{relative}"), relative)
        self.assertFalse(_tracked("deploy/ansible"))

    def test_lifecycle_and_cli_delegate_infrastructure_to_fiveg(self) -> None:
        lifecycle = _source("lifecycle.py")
        cli = _source("cli.py")
        combined = lifecycle + "\n" + cli
        self.assertIn("FiveGAdapter", lifecycle)
        for forbidden in (
            "ansible-playbook",
            "worktree add",
            "apply_network_overlay",
            "copytree",
            "execute_resource_preparation",
            "prepare_physical_resources",
            "stage_n3xx_gnb",
            "start_n3xx_gnb",
            "activate_physical_ue",
            "slices project use",
            "slices experiment create",
            "post5g experiment prefix",
        ):
            self.assertNotIn(forbidden, combined)
        for forbidden in ("SUPPORTED_CORE", "SUPPORTED_RAN", "SUPPORTED_RADIO"):
            self.assertNotIn(forbidden, combined)

    def test_observation_layer_cannot_deploy_or_teardown(self) -> None:
        runtime = _source("network/runtime.py")
        for forbidden in (
            "ansible-playbook",
            "worktree add",
            "apply_network_overlay",
            "copytree",
            "FiveGAdapter.up",
            "FiveGAdapter.down",
        ):
            self.assertNotIn(forbidden, runtime)

    def test_flat_duplicate_modules_do_not_return(self) -> None:
        removed = {
            "command_runtime.py",
            "entrypoint.py",
            "experiment.py",
            "experiment_cli.py",
            "experiment_resources.py",
            "experiment_runtime.py",
            "launcher.py",
            "network_runtime.py",
            "operator.py",
            "resource_runtime.py",
            "rfsim_runtime.py",
            "research.py",
            "research_collector.py",
            "research_instrumentation.py",
            "research_iperf.py",
            "research_runtime.py",
            "research_sampling.py",
            "workspace.py",
        }
        present = {path.name for path in SOURCE.iterdir() if path.is_file()}
        self.assertTrue(removed.isdisjoint(present), sorted(removed & present))
        self.assertFalse(_tracked("synthran/experiment/runtime.py"))

    def test_synthran_is_the_only_operator_entrypoint(self) -> None:
        self.assertTrue((SOURCE / "cli.py").is_file())
        for forbidden in (
            "synthran/commands",
            "synthran/__main__.py",
            "cli",
            "synthran/entrypoint.py",
            "synthran/launcher.py",
            "synthran/operator.py",
            "synthran/command_runtime.py",
            "synthran/backends",
            "synthran/experiment/commands.py",
            "synthran/network/r2lab.py",
            "synthran/r2lab",
        ):
            self.assertFalse(_tracked(forbidden), forbidden)


if __name__ == "__main__":
    unittest.main()
