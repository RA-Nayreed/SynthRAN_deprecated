from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from synthran.research.instrumentation import _restart_edge_sidecar_and_wait


def _pod_status(
    *,
    restart_count: int,
    container_ready: bool,
    pod_ready: bool,
    running: bool = True,
) -> dict[str, object]:
    return {
        "status": {
            "containerStatuses": [
                {
                    "name": "synthran-edge-mqtt",
                    "restartCount": restart_count,
                    "ready": container_ready,
                    "state": (
                        {"running": {"startedAt": "2026-08-17T09:00:00Z"}}
                        if running
                        else {"waiting": {"reason": "ContainerCreating"}}
                    ),
                }
            ],
            "conditions": [
                {
                    "type": "Ready",
                    "status": "True" if pod_ready else "False",
                }
            ],
        }
    }


class SidecarReadinessBarrierTests(unittest.TestCase):
    def test_restart_waits_for_new_ready_container_instance(self) -> None:
        inventory = object()
        restart = MagicMock()
        responses = (
            _pod_status(restart_count=0, container_ready=True, pod_ready=True),
            _pod_status(restart_count=0, container_ready=True, pod_ready=True),
            _pod_status(
                restart_count=1,
                container_ready=False,
                pod_ready=False,
                running=False,
            ),
            _pod_status(restart_count=1, container_ready=True, pod_ready=True),
        )
        with (
            patch(
                "synthran.research.instrumentation.base_runtime._remote_json",
                side_effect=responses,
            ) as remote_json,
            patch("synthran.research.instrumentation.time.sleep"),
        ):
            _restart_edge_sidecar_and_wait(
                inventory,
                "ue-pod",
                restart=restart,
                timeout_seconds=30,
            )

        restart.assert_called_once_with(inventory, "ue-pod")
        self.assertEqual(remote_json.call_count, 4)


if __name__ == "__main__":
    unittest.main()
