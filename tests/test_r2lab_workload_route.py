from __future__ import annotations

import unittest
from unittest.mock import patch

from synthran.r2lab.iot_ue import (
    R2LabIoTUeError,
    _prove_ue_route,
)
from synthran.live_preflight import CommandResult
from synthran.r2lab.hardware import UES


class PhysicalWorkloadRouteTests(unittest.TestCase):
    @patch("synthran.r2lab.controller._configured_identity", return_value=None)
    @patch("synthran.r2lab.iot_ue._run")
    def test_route_probe_forces_wwan0_like_the_relay(self, run, _identity) -> None:
        run.return_value = CommandResult(
            0,
            '[{"dst":"172.28.2.77","dev":"wwan0","prefsrc":"12.1.0.2"}]',
            "",
        )

        _prove_ue_route("oulu_user", UES["qfit07"], "172.28.2.77")

        rendered = " ".join(run.call_args.args[0])
        self.assertIn("ip -j route get 172.28.2.77 oif wwan0", rendered)

    @patch("synthran.r2lab.controller._configured_identity", return_value=None)
    @patch("synthran.r2lab.iot_ue._run")
    def test_route_probe_failure_names_exact_precondition(self, run, _identity) -> None:
        run.return_value = CommandResult(2, "", "opaque remote failure")

        with self.assertRaisesRegex(
            R2LabIoTUeError,
            "physical UE interface-bound route probe returned nonzero",
        ):
            _prove_ue_route("oulu_user", UES["qfit07"], "172.28.2.77")


if __name__ == "__main__":
    unittest.main()
