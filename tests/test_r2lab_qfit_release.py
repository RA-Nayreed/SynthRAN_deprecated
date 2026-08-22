from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from synthran.live_preflight import CommandResult
from synthran.network.r2lab import (
    R2LabResourceError,
    R2LabSelection,
    build_plan,
    execute_prepare,
    execute_release,
)


class QfitLifecycleRunner:
    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.radio_state = "off"
        self.qfit_state = "off"

    @staticmethod
    def remote(command: tuple[str, ...]) -> tuple[str, ...]:
        split = command.index("--")
        return command[split + 2 :]

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        remote = self.remote(value)

        if remote == ("rhubarbe", "leases", "--check"):
            return CommandResult(0, "", "")

        if remote == ("rhubarbe", "pdu", "on", "n300"):
            self.radio_state = "on"
            return CommandResult(0, "pdu2 chain-0@outlet-1 (n300): ON (28W)\n", "")
        if remote == ("rhubarbe", "pdu", "off", "n300"):
            self.radio_state = "off"
            return CommandResult(1, "pdu2 chain-0@outlet-1 (n300): OFF\n", "")
        if remote == ("rhubarbe", "pdu", "status", "n300"):
            suffix = "ON (28W)" if self.radio_state == "on" else "OFF"
            return CommandResult(0, f"pdu2 chain-0@outlet-1 (n300): {suffix}\n", "")

        if remote == ("qfit", "on", "qfit07"):
            self.qfit_state = "on"
            return CommandResult(0, "reboot07:ok\n", "")
        if remote == ("qfit", "off", "qfit07"):
            self.qfit_state = "off"
            return CommandResult(0, "reboot07:ok\n", "")
        if remote == ("rhubarbe", "status", "7"):
            return CommandResult(0, f"reboot07:{self.qfit_state}\n", "")

        if remote[:1] == ("ping",):
            return CommandResult(0, "", "")

        return CommandResult(0, "", "")

    @property
    def remote_commands(self) -> list[tuple[str, ...]]:
        return [self.remote(command) for command in self.commands]


class R2LabQfitReleaseTests(unittest.TestCase):
    def test_qfit_release_proves_ue_and_radio_off_before_dropping_claim(self) -> None:
        selection = R2LabSelection.build(
            slice_name="oulu_user",
            radio="n300",
            ue="qfit07",
        )
        plan = build_plan(run_id="r2lab-qfit-release", selection=selection)
        runner = QfitLifecycleRunner()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            execute_prepare(
                plan=plan,
                run_root=root,
                runner=runner,
                sleeper=lambda _: None,
                reachability_attempts=1,
            )
            self.assertTrue((root / "active.json").exists())

            runner.commands.clear()
            released = execute_release(
                run_id=plan.run_id,
                slice_name="oulu_user",
                run_root=root,
                runner=runner,
            )

            self.assertEqual("released", released.status)
            self.assertFalse((root / "active.json").exists())
            manifest = json.loads(released.manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(manifest["cleanup"]["claim_releasable"])
            self.assertEqual([], manifest["cleanup"]["unresolved_resources"])

        self.assertEqual(
            [
                ("rhubarbe", "leases", "--check"),
                ("qfit", "off", "qfit07"),
                ("rhubarbe", "status", "7"),
                ("rhubarbe", "leases", "--check"),
                ("rhubarbe", "pdu", "off", "n300"),
                ("rhubarbe", "pdu", "status", "n300"),
            ],
            runner.remote_commands,
        )

    def test_unresolved_qfit_status_keeps_claim_but_still_cleans_radio(self) -> None:
        class UnknownQfitStatusRunner(QfitLifecycleRunner):
            def __call__(self, command, timeout_seconds: int) -> CommandResult:
                remote = self.remote(tuple(command))
                if remote == ("rhubarbe", "status", "7"):
                    self.commands.append(tuple(command))
                    return CommandResult(0, "", "")
                return super().__call__(command, timeout_seconds)

        selection = R2LabSelection.build(
            slice_name="oulu_user",
            radio="n300",
            ue="qfit07",
        )
        plan = build_plan(run_id="r2lab-qfit-release-unknown", selection=selection)
        setup = QfitLifecycleRunner()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "r2lab"
            execute_prepare(
                plan=plan,
                run_root=root,
                runner=setup,
                sleeper=lambda _: None,
                reachability_attempts=1,
            )

            release_runner = UnknownQfitStatusRunner()
            release_runner.radio_state = "on"
            release_runner.qfit_state = "on"
            with self.assertRaises(R2LabResourceError):
                execute_release(
                    run_id=plan.run_id,
                    slice_name="oulu_user",
                    run_root=root,
                    runner=release_runner,
                )

            self.assertTrue((root / "active.json").exists())
            self.assertIn(
                ("rhubarbe", "pdu", "off", "n300"),
                release_runner.remote_commands,
            )
            manifest = json.loads(
                (root / plan.run_id / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual("release-failed", manifest["status"])
            self.assertEqual(["qfit07"], manifest["cleanup"]["unresolved_resources"])


if __name__ == "__main__":
    unittest.main()
