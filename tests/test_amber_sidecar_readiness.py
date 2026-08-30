from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from synthran.amber_experiment_runtime import _restart_edge_sidecar_and_wait


class AmberSidecarReadinessTests(unittest.TestCase):
    def test_restart_waits_for_new_ready_container_instance(self) -> None:
        inventory = object()
        restart = MagicMock()
        statuses = (
            (0, True, True, True),
            (0, False, False, False),
            (1, True, True, True),
        )

        with (
            patch(
                "synthran.amber_experiment_runtime._edge_sidecar_status",
                side_effect=statuses,
            ) as status,
            patch(
                "synthran.amber_experiment_runtime._restart_edge_sidecar",
                restart,
            ),
            patch("synthran.amber_experiment_runtime.time.sleep"),
        ):
            _restart_edge_sidecar_and_wait(inventory, "ue-pod", timeout_seconds=30)

        restart.assert_called_once_with(inventory, "ue-pod")
        self.assertEqual(status.call_count, 3)


if __name__ == "__main__":
    unittest.main()
