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


class ModuleLayoutTests(unittest.TestCase):
    def test_active_runtime_code_is_grouped_by_domain(self) -> None:
        required = (
            "cli.py",
            "errors.py",
            "lifecycle.py",
            "provider.py",
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
            self.assertFalse((SOURCE / relative).exists(), relative)

    def test_retired_deployment_layers_do_not_return(self) -> None:
        for relative in (
            "network/resources.py",
            "upstream_overlay.py",
        ):
            self.assertFalse((SOURCE / relative).exists(), relative)
        self.assertFalse((REPOSITORY_ROOT / "deploy" / "ansible").exists())

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
        self.assertFalse((SOURCE / "experiment" / "runtime.py").exists())

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
