from __future__ import annotations

import unittest
from unittest.mock import patch

from synthran.experiment.r2lab import (
    R2LabPhysicalExperimentError,
    _prove_ue_route,
)
from synthran.live_preflight import CommandResult
from synthran.r2lab.hardware import UES


class PhysicalWorkloadRouteTests(unittest.TestCase):
    @patch("synthran.experiment.r2lab._run")
    def test_route_probe_forces_wwan0_like_the_relay(self, run) -> None:
        run.return_value = CommandResult(
            0,
            '[{"dst":"172.28.2.77","dev":"wwan0","prefsrc":"12.1.0.2"}]',
            "",
        )

        _prove_ue_route("oulu_user", UES["qfit07"], "172.28.2.77")

        command = run.call_args.args[0]
        self.assertEqual(
            (
                "ip",
                "-j",
                "route",
                "get",
                "172.28.2.77",
                "oif",
                "wwan0",
            ),
            command[-8:],
        )

    @patch("synthran.experiment.r2lab._run")
    def test_route_probe_failure_names_exact_precondition(self, run) -> None:
        run.return_value = CommandResult(2, "", "opaque remote failure")

        with self.assertRaisesRegex(
            R2LabPhysicalExperimentError,
            "physical UE interface-bound route probe returned nonzero",
        ):
            _prove_ue_route("oulu_user", UES["qfit07"], "172.28.2.77")


if __name__ == "__main__":
    unittest.main()
