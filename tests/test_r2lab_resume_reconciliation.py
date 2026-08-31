from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from synthran.live_preflight import CommandResult
from synthran.r2lab.resources import (
    R2LabTopologyResourceError,
    reconcile_physical_resources,
)


RUN_ID = "physical-resume-001"
SLICE = "oulu_rnayreed"


def _write_claimed_run(root: Path) -> None:
    run_dir = root / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "topology.json").write_text(
        json.dumps(
            {
                "schema": "synthran/r2lab-topology/v1alpha1",
                "core_node": "sopnode-f2",
                "ran_node": "sopnode-f3",
                "radio": "n300",
                "ue": "qfit07",
                "dnn": "internet",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "active.json").write_text(
        json.dumps(
            {
                "schema": "synthran/r2lab-claim/v1alpha2",
                "run_id": RUN_ID,
                "slice_fingerprint": hashlib.sha256(SLICE.encode("utf-8")).hexdigest(),
                "core_node": "sopnode-f2",
                "ran_node": "sopnode-f3",
                "radio": "n300",
                "ue": "qfit07",
                "created_at_utc": "2026-08-26T00:00:00Z",
            }
        )
        + "\n",
        encoding="utf-8",
    )


class FakeR2Lab:
    def __init__(
        self,
        *,
        radio: str = "on",
        qfit: str = "on",
        usb: str = "on",
        lease: bool = True,
    ) -> None:
        self.radio = radio
        self.qfit = qfit
        self.usb = usb
        self.lease = lease
        self.commands: list[str] = []

    def __call__(self, command, _timeout) -> CommandResult:
        text = " ".join(str(part) for part in command)
        self.commands.append(text)

        if "rhubarbe leases --check" in text:
            return CommandResult(0 if self.lease else 1, "", "")

        if "rhubarbe pdu status n300" in text:
            if self.radio == "unknown":
                return CommandResult(0, "n300 status unavailable\n", "")
            state = "ON" if self.radio == "on" else "OFF"
            return CommandResult(0, f"(n300) : {state}\n", "")

        if "rhubarbe pdu on n300" in text:
            self.radio = "on"
            return CommandResult(0, "", "")

        if "rhubarbe status 7" in text:
            state = "on" if self.qfit == "on" else "off"
            return CommandResult(0, f"reboot07:{state}\n", "")

        if "rhubarbe load -i mbim-quectel-any-dnn 7" in text:
            self.qfit = "on"
            return CommandResult(0, "", "")

        if "rhubarbe wait 7" in text:
            return CommandResult(0, "", "")

        if "http://reboot07/usrpstatus" in text:
            return CommandResult(0, "usrpon\n" if self.usb == "on" else "usrpoff\n", "")

        if "http://reboot07/usrpon" in text:
            self.usb = "on"
            return CommandResult(0, "", "")

        if "root@fit07" in text:
            return CommandResult(0, "", "")

        raise AssertionError(f"unexpected command: {text}")


class R2LabResumeReconciliationTests(unittest.TestCase):
    def test_resume_restores_exact_off_radio_and_qfit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _write_claimed_run(root)
            runner = FakeR2Lab(radio="off", qfit="off", usb="off")

            authority = reconcile_physical_resources(
                run_id=RUN_ID,
                slice_name=SLICE,
                run_root=root,
                runner=runner,
                sleeper=lambda _seconds: None,
                timeout_seconds=30,
            )

            self.assertEqual("on", authority.radio_state)
            self.assertEqual("ready", authority.ue_state)
            self.assertEqual("on", runner.radio)
            self.assertEqual("on", runner.qfit)
            self.assertEqual("on", runner.usb)
            self.assertTrue(any("rhubarbe pdu on n300" in item for item in runner.commands))
            self.assertTrue(
                any("rhubarbe load -i mbim-quectel-any-dnn 7" in item for item in runner.commands)
            )
            self.assertTrue(any("http://reboot07/usrpon" in item for item in runner.commands))

    def test_resume_is_idempotent_when_resources_are_already_ready(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _write_claimed_run(root)
            runner = FakeR2Lab()

            authority = reconcile_physical_resources(
                run_id=RUN_ID,
                slice_name=SLICE,
                run_root=root,
                runner=runner,
                sleeper=lambda _seconds: None,
                timeout_seconds=30,
            )

            self.assertEqual("on", authority.radio_state)
            self.assertFalse(any("rhubarbe pdu on n300" in item for item in runner.commands))
            self.assertFalse(any("rhubarbe load -i" in item for item in runner.commands))
            self.assertFalse(any("http://reboot07/usrpon" in item for item in runner.commands))

    def test_unknown_radio_state_fails_closed_without_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _write_claimed_run(root)
            runner = FakeR2Lab(radio="unknown")

            with self.assertRaisesRegex(
                R2LabTopologyResourceError, "physical radio power state is unknown"
            ):
                reconcile_physical_resources(
                    run_id=RUN_ID,
                    slice_name=SLICE,
                    run_root=root,
                    runner=runner,
                    sleeper=lambda _seconds: None,
                    timeout_seconds=30,
                )

            self.assertFalse(any("rhubarbe pdu on n300" in item for item in runner.commands))
            self.assertFalse(any("rhubarbe load -i" in item for item in runner.commands))

    def test_missing_lease_fails_before_any_hardware_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _write_claimed_run(root)
            runner = FakeR2Lab(radio="off", qfit="off", usb="off", lease=False)

            with self.assertRaisesRegex(R2LabTopologyResourceError, "lease was not verified"):
                reconcile_physical_resources(
                    run_id=RUN_ID,
                    slice_name=SLICE,
                    run_root=root,
                    runner=runner,
                    sleeper=lambda _seconds: None,
                    timeout_seconds=30,
                )

            self.assertEqual(1, len(runner.commands))
            self.assertIn("rhubarbe leases --check", runner.commands[0])


if __name__ == "__main__":
    unittest.main()
