from __future__ import annotations

from pathlib import Path
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE = REPOSITORY_ROOT / "synthran"


class ModuleLayoutTests(unittest.TestCase):
    def test_domain_code_is_grouped_in_packages(self) -> None:
        self.assertTrue((SOURCE / "app" / "__init__.py").is_file())
        self.assertTrue((SOURCE / "app" / "model.py").is_file())
        self.assertTrue((SOURCE / "app" / "controller.py").is_file())
        self.assertTrue((SOURCE / "app" / "workflows.py").is_file())
        self.assertTrue((SOURCE / "experiment" / "__init__.py").is_file())
        self.assertTrue((SOURCE / "experiment" / "resources.py").is_file())
        self.assertTrue((SOURCE / "experiment" / "runtime.py").is_file())
        self.assertTrue((SOURCE / "network" / "__init__.py").is_file())
        self.assertTrue((SOURCE / "network" / "runtime.py").is_file())
        self.assertTrue((SOURCE / "network" / "resources.py").is_file())
        self.assertTrue((SOURCE / "network" / "rfsim.py").is_file())
        self.assertTrue((SOURCE / "operations" / "__init__.py").is_file())
        self.assertTrue((SOURCE / "operations" / "model.py").is_file())
        self.assertTrue((SOURCE / "operations" / "journal.py").is_file())
        self.assertTrue((SOURCE / "operations" / "policy.py").is_file())
        self.assertTrue((SOURCE / "operations" / "engine.py").is_file())
        self.assertTrue((SOURCE / "resources" / "__init__.py").is_file())
        self.assertTrue((SOURCE / "resources" / "model.py").is_file())
        self.assertTrue((SOURCE / "resources" / "catalog.py").is_file())
        self.assertTrue((SOURCE / "resources" / "requirements.py").is_file())
        self.assertTrue((SOURCE / "resources" / "selector.py").is_file())
        self.assertTrue((SOURCE / "resources" / "decision.py").is_file())
        self.assertTrue((SOURCE / "resources" / "transaction.py").is_file())
        self.assertTrue((SOURCE / "research" / "__init__.py").is_file())
        self.assertTrue((SOURCE / "research" / "collector.py").is_file())
        self.assertTrue((SOURCE / "research" / "instrumentation.py").is_file())
        self.assertTrue((SOURCE / "research" / "iperf.py").is_file())
        self.assertTrue((SOURCE / "research" / "runtime.py").is_file())
        self.assertTrue((SOURCE / "research" / "sampling.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "__init__.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "model.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "store.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "registry.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "records.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "access.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "status.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "session.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "initialization.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "context.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "desired.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "desired_store.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "experiment_service.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "observed.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "observed_store.py").is_file())
        self.assertTrue((SOURCE / "workspace" / "reconciliation.py").is_file())

    def test_flat_duplicate_modules_do_not_return(self) -> None:
        removed = {
            "entrypoint.py",
            "experiment.py",
            "experiment_cli.py",
            "experiment_resources.py",
            "experiment_runtime.py",
            "network_runtime.py",
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

    def test_synthran_is_the_only_operator_entrypoint(self) -> None:
        self.assertTrue((SOURCE / "launcher.py").is_file())
        self.assertTrue((SOURCE / "cli.py").is_file())
        self.assertTrue((SOURCE / "command_runtime.py").is_file())
        self.assertFalse((SOURCE / "commands").exists())
        self.assertFalse((SOURCE / "__main__.py").exists())
        self.assertFalse((SOURCE / "terminal").exists())
        self.assertFalse((REPOSITORY_ROOT / "cli").exists())
        self.assertFalse((SOURCE / "entrypoint.py").exists())
        self.assertFalse((SOURCE / "experiment" / "commands.py").exists())
        self.assertFalse((SOURCE / "network" / "r2lab.py").exists())
        self.assertFalse((SOURCE / "r2lab" / "_deployment_impl.py").exists())
        self.assertFalse((SOURCE / "r2lab" / "qfit_activation_provider.py").exists())


if __name__ == "__main__":
    unittest.main()
