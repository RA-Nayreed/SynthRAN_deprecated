from __future__ import annotations

import unittest
from unittest.mock import patch

from synthran.pdu_transport_probe import (
    pdu_bound_tcp_connected,
    wait_pdu_bound_tcp_connected,
)


class PduTransportProbeTests(unittest.TestCase):
    def test_probe_binds_pdu_address_before_connecting(self) -> None:
        inventory = object()
        with patch(
            "synthran.pdu_transport_probe._remote",
            return_value="1\n",
        ) as remote:
            connected = pdu_bound_tcp_connected(
                inventory,
                "ue-pod",
                pdu_address="12.1.0.2",
                remote_address="10.10.0.2",
                remote_port=18884,
            )

        self.assertTrue(connected)
        rendered = " ".join(str(value) for value in remote.call_args.args)
        self.assertIn("s.bind((local_ip, 0))", rendered)
        self.assertIn("s.connect((remote_ip, remote_port))", rendered)
        self.assertIn("12.1.0.2", rendered)
        self.assertIn("10.10.0.2", rendered)
        self.assertIn("18884", rendered)

    def test_wait_retries_until_exact_tcp_path_connects(self) -> None:
        inventory = object()
        with (
            patch(
                "synthran.pdu_transport_probe.pdu_bound_tcp_connected",
                side_effect=(False, False, True),
            ) as connected,
            patch("synthran.pdu_transport_probe.time.sleep"),
        ):
            wait_pdu_bound_tcp_connected(
                inventory,
                "ue-pod",
                pdu_address="12.1.0.2",
                remote_address="10.10.0.2",
                remote_port=18884,
                timeout_seconds=30,
            )

        self.assertEqual(3, connected.call_count)


if __name__ == "__main__":
    unittest.main()
