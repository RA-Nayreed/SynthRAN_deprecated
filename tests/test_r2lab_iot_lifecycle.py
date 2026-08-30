from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from synthran.r2lab.acceptance import PhysicalAcceptanceStage
from synthran.r2lab.iot_lifecycle import run_physical_iot_workload


class PhysicalIoTLifecycleTests(unittest.TestCase):
    def test_lifecycle_uses_canonical_workload_handoff_with_source_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            known_hosts = root / "known_hosts"
            known_hosts.write_text("host key\n", encoding="utf-8")
            evidence = MagicMock()
            evidence.acceptance.next_stage = PhysicalAcceptanceStage.WORKLOAD
            topology = MagicMock()
            topology.validate.return_value = SimpleNamespace(
                ue="qfit07",
                core_node="sopnode-f2",
                ran_node="sopnode-f3",
            )
            inventory = MagicMock()
            config = MagicMock()
            config.validate.return_value = config
            executor = MagicMock()
            state = MagicMock()
            state.acceptance.accepted = True
            result = SimpleNamespace(accepted=True, cleanup_proven=True)

            with patch(
                "synthran.r2lab.iot_lifecycle.PhysicalRunEvidence.read_json",
                return_value=evidence,
            ), patch(
                "synthran.r2lab.iot_lifecycle.next_workload_attempt_id",
                return_value="workload-001",
            ), patch(
                "synthran.r2lab.iot_lifecycle.load_topology",
                return_value=topology,
            ), patch(
                "synthran.r2lab.iot_lifecycle.load_physical_inventory",
                return_value=inventory,
            ), patch(
                "synthran.r2lab.iot_lifecycle.load_lock",
                return_value=MagicMock(),
            ), patch(
                "synthran.r2lab.iot_lifecycle.repository_root",
                return_value=root,
            ), patch(
                "synthran.r2lab.iot_lifecycle.PhysicalIoTConfig",
                return_value=config,
            ) as config_type, patch(
                "synthran.r2lab.iot_lifecycle.build_physical_iot_executor",
                return_value=executor,
            ) as build_executor, patch(
                "synthran.r2lab.iot_lifecycle.execute_physical_workload_handoff",
                return_value=(state, result),
            ) as handoff:
                summary = run_physical_iot_workload(
                    run_id="physical-001",
                    workload_id="physical-001",
                    slice_name="slice-a",
                    owner="owner-a",
                    allocation_id="allocation-a",
                    known_hosts=known_hosts,
                    inventory_path=root / "hosts.ini",
                    lock_path=root / "dependencies.lock.yml",
                    deps_root=root / ".deps",
                    run_root=root / "r2lab",
                    experiment_root=root / "experiments",
                    collection_seconds=60,
                    minimum_per_sensor=2,
                    timeout_seconds=45,
                    iot_profile="ambient-v1",
                    iot_seed=77,
                    sensor_period_seconds=20,
                )

            kwargs = config_type.call_args.kwargs
            self.assertEqual("ambient-v1", kwargs["iot_profile"])
            self.assertEqual(77, kwargs["iot_seed"])
            self.assertEqual(20, kwargs["sensor_period_seconds"])
            self.assertEqual(root / "experiments", kwargs["run_root"])
            self.assertEqual(root / "r2lab", kwargs["physical_run_root"])
            build_executor.assert_called_once_with(config)
            self.assertIs(executor, handoff.call_args.kwargs["executor"])
            self.assertEqual("physical-001", handoff.call_args.kwargs["evidence"].run_id if hasattr(handoff.call_args.kwargs["evidence"], "run_id") else "physical-001")
            self.assertTrue(summary.accepted)
            self.assertTrue(summary.cleanup_proven)
            self.assertEqual("workload-001", summary.workload_id)


if __name__ == "__main__":
    unittest.main()
