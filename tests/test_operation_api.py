from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

from synthran.app import ApplicationController
from synthran.control.operation_api import (
    approve_operation,
    cancel_operation,
    inspect_operation_action,
    plan_operation,
)
from synthran.workspace.desired import ExperimentDesiredState
from synthran.workspace.model import Profile, WorkspaceError, format_utc
from synthran.workspace.observed import Observation
from synthran.workspace.store import initialize_workspace, save_profile


UTC = timezone.utc
NOW = datetime(2026, 8, 18, 3, 0, tzinfo=UTC)


def observation(
    dimension: str,
    state: str = "ready",
    *,
    ownership: str = "operator",
    resource_id: str | None = None,
) -> Observation:
    return Observation(
        dimension=dimension,
        state=state,
        source="provider",
        observed_at_utc=format_utc(NOW),
        fresh_until_utc=format_utc(NOW + timedelta(minutes=20)),
        ownership=ownership,
        resource_id=resource_id,
    )


class OperationApiTests(unittest.TestCase):
    def _controller(self, base: Path) -> tuple[Path, dict[str, str], ApplicationController]:
        root = base / "repo"
        root.mkdir()
        config_home = base / "config"
        environment = {"SYNTHRAN_CONFIG_HOME": str(config_home)}
        save_profile(
            Profile(
                name="controller",
                created_at_utc=format_utc(NOW),
                updated_at_utc=format_utc(NOW),
                slices_username="operator",
            ),
            environment=environment,
        )
        initialize_workspace(
            root=root,
            profile="controller",
            project="research-project",
            now=NOW,
        )
        controller = ApplicationController(start=root, environment=environment)
        controller.create_experiment(
            desired=ExperimentDesiredState.recommended(intent="virtual-5g"),
            slices_experiment="provider-exp-01",
            now=NOW,
        )
        return root, environment, controller

    def _control_observations(self) -> dict[str, list[Observation]]:
        return {
            "controller": [observation("controller")],
            "project_access": [observation("project_access")],
            "provider_experiment": [observation("provider_experiment")],
        }

    def _network_ready_observations(self, *, path_ready: bool) -> dict[str, list[Observation]]:
        result = self._control_observations()
        for index, dimension in enumerate(
            (
                "reservation",
                "allocation",
                "preparation",
                "kubernetes",
                "core",
                "ran",
                "ue",
                "pdu",
                "upf",
                "radio",
            ),
            start=1,
        ):
            result[dimension] = [
                observation(
                    dimension,
                    ownership="synthran",
                    resource_id=f"owned-{index}",
                )
            ]
        result["path"] = [
            observation(
                "path",
                state="ready" if path_ready else "absent",
                ownership="synthran" if path_ready else "unowned",
                resource_id="path-proof" if path_ready else None,
            )
        ]
        return result

    def test_resource_action_review_is_read_only_and_blocks_unbound_targets(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, environment, controller = self._controller(Path(temporary))
            observations = self._control_observations()
            observations["reservation"] = [
                observation("reservation", state="absent", ownership="unowned")
            ]
            controller.record_observations(observations, now=NOW)

            review = inspect_operation_action(
                start=root,
                environment=environment,
                params={"action": "reserve"},
                now=NOW,
            )

            self.assertEqual(review["kind"], "reserve")
            self.assertEqual(review["risk"], "R2")
            self.assertFalse(review["can_plan"])
            self.assertIn("Fresh provider inventory", str(review["plan_block"]))
            operations = root / ".synthran" / "operations"
            operation_entries = (
                [path.name for path in operations.iterdir() if path.name.startswith("op-")]
                if operations.is_dir()
                else []
            )
            self.assertEqual(operation_entries, [])

            with self.assertRaises(WorkspaceError):
                plan_operation(
                    start=root,
                    environment=environment,
                    params={"action": "reserve"},
                    now=NOW,
                )

    def test_read_only_verification_can_be_prepared_and_cancelled_locally(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, environment, controller = self._controller(Path(temporary))
            controller.record_observations(
                self._network_ready_observations(path_ready=False),
                now=NOW,
            )

            review = inspect_operation_action(
                start=root,
                environment=environment,
                params={"action": "verify"},
                now=NOW,
            )
            self.assertEqual(review["kind"], "verify-path")
            self.assertEqual(review["risk"], "R1")
            self.assertTrue(review["can_plan"])

            planned = plan_operation(
                start=root,
                environment=environment,
                params={"action": "verify"},
                now=NOW,
            )
            self.assertEqual(planned["state"]["status"], "planned")
            self.assertFalse(planned["plan"]["approval_required"])

            cancelled = cancel_operation(
                start=root,
                environment=environment,
                params={"operation_id": planned["plan"]["operation_id"]},
                now=NOW,
            )
            self.assertEqual(cancelled["state"]["status"], "failed")
            self.assertEqual(cancelled["events"][-1]["event_type"], "operation.interrupted")

    def test_teardown_requires_destructive_approval(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root, environment, controller = self._controller(Path(temporary))
            controller.record_observations(
                self._network_ready_observations(path_ready=True),
                now=NOW,
            )

            review = inspect_operation_action(
                start=root,
                environment=environment,
                params={"action": "down"},
                now=NOW,
            )
            self.assertEqual(review["risk"], "R3")
            self.assertTrue(review["can_plan"])
            self.assertTrue(review["targets"])

            planned = plan_operation(
                start=root,
                environment=environment,
                params={"action": "down"},
                now=NOW,
            )
            operation_id = planned["plan"]["operation_id"]
            self.assertEqual(planned["plan"]["approval_mode"], "destructive")

            with self.assertRaises(WorkspaceError):
                approve_operation(
                    start=root,
                    environment=environment,
                    params={"operation_id": operation_id, "mode": "standard"},
                    now=NOW,
                )

            approved = approve_operation(
                start=root,
                environment=environment,
                params={"operation_id": operation_id, "mode": "destructive"},
                now=NOW,
            )
            self.assertEqual(approved["approval"]["mode"], "destructive")
            self.assertEqual(approved["state"]["status"], "approved")


if __name__ == "__main__":
    unittest.main()
