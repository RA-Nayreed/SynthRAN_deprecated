from __future__ import annotations

import json
import unittest

from synthran.r2lab.deployment import R2LabGnbLifecycleError, parse_gnb_pods_json
from synthran.r2lab.runtime import N2State, parse_n2_log_state


class GnbPodObservationTests(unittest.TestCase):
    def test_exactly_one_running_ready_pod_is_proven(self) -> None:
        payload = {
            "items": [
                {
                    "metadata": {"name": "srsran-gnb-abc"},
                    "status": {
                        "phase": "Running",
                        "containerStatuses": [{"name": "gnb", "ready": True}],
                    },
                }
            ]
        }
        observation = parse_gnb_pods_json(json.dumps(payload))
        self.assertTrue(observation.exactly_one_ready)
        self.assertFalse(observation.zero)
        self.assertEqual(1, observation.total_count)

    def test_terminating_pod_is_never_ready(self) -> None:
        payload = {
            "items": [
                {
                    "metadata": {
                        "name": "srsran-gnb-abc",
                        "deletionTimestamp": "2026-08-26T20:00:00Z",
                    },
                    "status": {
                        "phase": "Running",
                        "containerStatuses": [{"name": "gnb", "ready": True}],
                    },
                }
            ]
        }
        observation = parse_gnb_pods_json(json.dumps(payload))
        self.assertFalse(observation.exactly_one_ready)
        self.assertEqual(1, observation.terminating_count)

    def test_malformed_pod_list_fails_closed(self) -> None:
        with self.assertRaises(R2LabGnbLifecycleError):
            parse_gnb_pods_json('{"items": [null]}')


class N2LogClassificationTests(unittest.TestCase):
    def test_affirmative_ngap_connection_is_established(self) -> None:
        self.assertIs(
            N2State.ESTABLISHED,
            parse_n2_log_state("NGAP connection established successfully"),
        )

    def test_error_line_does_not_count_as_established(self) -> None:
        self.assertIs(
            N2State.NOT_OBSERVED,
            parse_n2_log_state("NGAP connection failed: not established"),
        )

    def test_empty_log_is_not_observed(self) -> None:
        self.assertIs(N2State.NOT_OBSERVED, parse_n2_log_state(""))


if __name__ == "__main__":
    unittest.main()
