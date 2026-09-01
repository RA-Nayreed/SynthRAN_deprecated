from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from synthran.dependencies import load_lock
from synthran.fiveg_ansible import load_inventory
from synthran.live_preflight import CommandResult
from synthran.network.runtime import (
    NETWORK_EVIDENCE_SCHEMA,
    sanitize_deployment_text,
    verify_network_path,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "inventory_open5gs_srsran_rfsim.ini"
NOW = datetime(2026, 8, 12, 14, 0, tzinfo=timezone.utc)


def _pod(name: str) -> dict[str, object]:
    return {
        "metadata": {"name": name},
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    }


class VerificationRunner:
    def __init__(self, *, ue_items: list[dict[str, object]] | None = None) -> None:
        self.ue_items = ue_items
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, command, timeout: int) -> CommandResult:
        argv = tuple(command)
        self.calls.append(argv)
        self.assert_timeout(timeout)
        rendered = " ".join(argv)
        if "app=srsran,component=gnb" in rendered:
            return CommandResult(0, json.dumps({"items": [_pod("gnb-pod")]}))
        if "app=srsran,component=ue" in rendered:
            return CommandResult(
                0,
                json.dumps({"items": self.ue_items if self.ue_items is not None else [_pod("ue-pod")]}),
            )
        if "app=open5gs,nf=upf,name=upf1" in rendered:
            return CommandResult(0, json.dumps({"items": [_pod("upf-pod")]}))
        # ssh_command shell-quotes the grep argument, so match the invariant
        # rather than one particular quoting representation.
        if "Cell was activated" in rendered and "/var/log/gnb.log" in rendered:
            return CommandResult(0, "")
        if "ip -j address show dev tun_srsue1" in rendered:
            return CommandResult(
                0,
                json.dumps([
                    {
                        "ifname": "tun_srsue1",
                        "flags": ["POINTOPOINT", "UP", "LOWER_UP"],
                        "addr_info": [{"family": "inet", "local": "12.1.1.2", "prefixlen": 16}],
                    }
                ]),
            )
        if "ue-pod -c ue -- ip -j route show" in rendered:
            return CommandResult(0, json.dumps([{"dst": "12.1.0.0/16", "dev": "tun_srsue1"}]))
        if "upf-pod -- ip -j route show" in rendered:
            return CommandResult(0, json.dumps([{"dst": "12.1.0.0/16", "dev": "ogstun"}]))
        return CommandResult(2, "", "unsupported fake command")

    @staticmethod
    def assert_timeout(timeout: int) -> None:
        if timeout <= 0:
            raise AssertionError("timeout must be positive")


class NetworkVerificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.inventory = load_inventory(FIXTURE)
        self.lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")

    def _verify(self, runner: VerificationRunner):
        with patch.dict(os.environ, {"SYNTHRAN_KNOWN_HOSTS": str(FIXTURE.resolve())}):
            return verify_network_path(
                inventory=self.inventory,
                lock=self.lock,
                run_id="network-proof",
                runner=runner,
                now=NOW,
            )

    def test_proves_current_rfsim_pdu_path_read_only(self) -> None:
        runner = VerificationRunner()
        report = self._verify(runner)
        self.assertTrue(report.ready, report.render())
        self.assertEqual("12.1.1.2", report.pdu_address)
        payload = report.to_dict()
        self.assertEqual(NETWORK_EVIDENCE_SCHEMA, payload["schema"])
        self.assertEqual("tun_srsue1", payload["path"]["ue_interface"])
        self.assertEqual({"fiveg_ansible"}, set(payload["dependencies"]))
        self.assertTrue(runner.calls)
        for call in runner.calls:
            self.assertIn("BatchMode=yes", call)
            self.assertIn("StrictHostKeyChecking=yes", call)

    def test_fails_closed_for_ambiguous_active_ue(self) -> None:
        report = self._verify(VerificationRunner(ue_items=[_pod("ue-a"), _pod("ue-b")]))
        self.assertFalse(report.ready)
        self.assertIn("expected exactly one active srsue pod", report.render())

    def test_diagnostics_sanitization_is_not_a_deployment_executor(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            private = Path(temporary) / "secret"
            subscriber = "1234567" + "89012345"
            text = f"{private} subscriber={subscriber} address=10.10.3.5"
            sanitized = sanitize_deployment_text(text, (private,))
        self.assertNotIn(str(private), sanitized)
        self.assertNotIn(subscriber, sanitized)
        self.assertNotIn("10.10.3.5", sanitized)
        self.assertIn("<local-path>", sanitized)


if __name__ == "__main__":
    unittest.main()
