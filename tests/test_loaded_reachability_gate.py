from __future__ import annotations

import unittest
from unittest.mock import Mock

from synthran.research import LoadSpec, MeasurementSpec, ResearchExperimentSpec
from synthran.research.path import prove_pre_window_target


class LoadedReachabilityGateTests(unittest.TestCase):
    def _spec(self, *, loaded: bool) -> ResearchExperimentSpec:
        return ResearchExperimentSpec(
            campaign_id="reachability-gate",
            run_id=(
                "reachability-gate-load"
                if loaded
                else "reachability-gate-baseline"
            ),
            network_run_id="network-accepted",
            condition="load" if loaded else "baseline",
            measurement=MeasurementSpec(duration_seconds=30),
            load=(
                LoadSpec(enabled=True, target_bps=1_000_000)
                if loaded
                else LoadSpec()
            ),
            probe_target="192.0.2.10",
        )

    def test_loaded_run_uses_transport_proof_not_icmp(self) -> None:
        prove_icmp = Mock(side_effect=AssertionError("ICMP must not gate loaded runs"))
        prove_transport = Mock()

        prove_pre_window_target(
            spec=self._spec(loaded=True),
            prove_icmp=prove_icmp,
            prove_transport=prove_transport,
        )

        prove_transport.assert_called_once_with()
        prove_icmp.assert_not_called()

    def test_baseline_run_retains_bounded_icmp_proof(self) -> None:
        prove_icmp = Mock()
        prove_transport = Mock(
            side_effect=AssertionError("baseline must not start load transport")
        )

        prove_pre_window_target(
            spec=self._spec(loaded=False),
            prove_icmp=prove_icmp,
            prove_transport=prove_transport,
        )

        prove_icmp.assert_called_once_with()
        prove_transport.assert_not_called()


if __name__ == "__main__":
    unittest.main()
