from __future__ import annotations

from pathlib import Path
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE = REPOSITORY_ROOT / "synthran"


class ModuleLayoutTests(unittest.TestCase):
    def test_domain_code_is_grouped_in_packages(self) -> None:
        required = (
            "app/__init__.py",
            "app/model.py",
            "app/controller.py",
            "app/workflows.py",
            "experiment/__init__.py",
            "experiment/resources.py",
            "experiment/runtime.py",
            "network/__init__.py",
            "network/runtime.py",
            "network/resources.py",
            "network/rfsim.py",
            "operations/__init__.py",
            "operations/model.py",
            "operations/journal.py",
            "operations/policy.py",
            "operations/engine.py",
            "resources/__init__.py",
            "resources/model.py",
            "resources/catalog.py",
            "resources/requirements.py",
            "resources/selector.py",
            "resources/decision.py",
            "resources/transaction.py",
            "research/__init__.py",
            "research/collector.py",
            "research/instrumentation.py",
            "research/iperf.py",
            "research/runtime.py",
            "research/sampling.py",
            "utils/__init__.py",
            "utils/environment.py",
            "utils/ssh.py",
            "workspace/__init__.py",
            "workspace/model.py",
            "workspace/store.py",
            "workspace/registry.py",
            "workspace/records.py",
            "workspace/access.py",
            "workspace/status.py",
            "workspace/session.py",
            "workspace/initialization.py",
            "workspace/context.py",
            "workspace/desired.py",
            "workspace/desired_store.py",
            "workspace/experiment_service.py",
            "workspace/observed.py",
            "workspace/observed_store.py",
            "workspace/reconciliation.py",
        )
        for relative in required:
            self.assertTrue((SOURCE / relative).is_file(), relative)

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

    def test_superseded_r2lab_stack_does_not_return(self) -> None:
        r2lab = SOURCE / "r2lab"
        removed = {
            "authority.py",
            "cluster_ssh.py",
            "foundation.py",
            "gnb.py",
            "guards.py",
            "handoff.py",
            "readiness.py",
            "ue_overlay.py",
        }
        present = {path.name for path in r2lab.iterdir() if path.is_file()}
        self.assertTrue(removed.isdisjoint(present), sorted(removed & present))
        self.assertTrue((r2lab / "foundation_topology.py").is_file())
        self.assertTrue((r2lab / "n3xx.py").is_file())
        self.assertTrue((r2lab / "resources.py").is_file())
        self.assertTrue((r2lab / "ue_ansible.py").is_file())

    def test_synthran_is_the_only_operator_entrypoint(self) -> None:
        self.assertTrue((SOURCE / "launcher.py").is_file())
        self.assertTrue((SOURCE / "cli.py").is_file())
        self.assertTrue((SOURCE / "command_runtime.py").is_file())
        self.assertFalse((SOURCE / "commands").exists())
        self.assertFalse((SOURCE / "__main__.py").exists())
        self.assertFalse((REPOSITORY_ROOT / "cli").exists())
        self.assertFalse((SOURCE / "entrypoint.py").exists())
        self.assertFalse((SOURCE / "experiment" / "commands.py").exists())
        self.assertFalse((SOURCE / "network" / "r2lab.py").exists())
        self.assertFalse((SOURCE / "r2lab" / "_deployment_impl.py").exists())
        self.assertFalse((SOURCE / "r2lab" / "qfit_activation_provider.py").exists())


if __name__ == "__main__":
    unittest.main()
