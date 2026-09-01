from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from synthran.cli import _parser
from synthran.lifecycle import _deployment_spec


class UnifiedRunTests(unittest.TestCase):
    def test_run_parser_selects_upstream_platform(self) -> None:
        physical = _parser().parse_args(
            (
                "run",
                "--radio",
                "r2lab",
                "--device",
                "n300",
                "--ue",
                "qfit07",
                "--core-node",
                "sopnode-f2",
                "--ran-node",
                "sopnode-f3",
                "--run-id",
                "physical-001",
            )
        )
        self.assertEqual("r2lab", physical.radio)
        self.assertEqual("n300", physical.device)
        self.assertEqual("qfit07", physical.ue)

        virtual = _parser().parse_args(
            (
                "run",
                "--radio",
                "rfsim",
                "--core-node",
                "sopnode-f2",
                "--ran-node",
                "sopnode-f3",
                "--run-id",
                "virtual-001",
                "--quiet",
            )
        )
        self.assertEqual("rfsim", virtual.radio)
        self.assertTrue(virtual.quiet)

    def test_rfsim_run_becomes_native_fiveg_spec(self) -> None:
        args = _parser().parse_args(
            (
                "run",
                "--radio",
                "rfsim",
                "--core-node",
                "sopnode-f2",
                "--ran-node",
                "sopnode-f3",
                "--run-id",
                "virtual-001",
            )
        )
        with tempfile.TemporaryDirectory() as temporary:
            known_hosts = Path(temporary) / "known_hosts"
            known_hosts.write_text("fixture\n", encoding="utf-8")
            spec = _deployment_spec(args, known_hosts=known_hosts)
        self.assertEqual("fiveg/deployment/v1", spec["schema"])
        self.assertEqual({"type": "rfsim", "ru": "rfsim"}, spec["platform"])
        self.assertEqual({"qhats": [], "qfits": [], "phones": []}, spec["ues"])
        self.assertEqual("none", spec["reservation"]["r2lab_mode"])

    def test_physical_run_passes_ru_ue_and_ssh_authority_upstream(self) -> None:
        args = _parser().parse_args(
            (
                "run",
                "--radio",
                "r2lab",
                "--device",
                "n300",
                "--ue",
                "qfit07",
                "--slice",
                "slice-test",
                "--core-node",
                "sopnode-f2",
                "--ran-node",
                "sopnode-f3",
                "--run-id",
                "physical-001",
            )
        )
        with tempfile.TemporaryDirectory() as temporary:
            known_hosts = Path(temporary) / "known_hosts"
            known_hosts.write_text("fixture\n", encoding="utf-8")
            spec = _deployment_spec(args, known_hosts=known_hosts)
        self.assertEqual({"type": "r2lab", "ru": "n300"}, spec["platform"])
        self.assertEqual(["qfit07"], spec["ues"]["qfits"])
        self.assertEqual(["qfit07"], spec["deployment"]["selected_ues"])
        self.assertEqual("require-existing", spec["reservation"]["r2lab_mode"])
        self.assertEqual("slice-test", spec["r2lab"]["username"])
        self.assertTrue(spec["r2lab"]["strict_host_key_checking"])


if __name__ == "__main__":
    unittest.main()
