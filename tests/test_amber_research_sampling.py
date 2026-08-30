from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch

from synthran.research.sampling import _ingress_snapshot


class AmberResearchIngressSamplingTests(unittest.TestCase):
    def test_sampler_prefers_amber_snapshot_and_preserves_legacy_fallback(self) -> None:
        payload = json.dumps(
            {
                "accepted_connections": 10,
                "upstream_bytes": 1234,
                "downstream_bytes": 56,
            }
        )
        with patch(
            "synthran.research.sampling.base_runtime._remote",
            return_value=payload,
        ) as remote:
            snapshot = _ingress_snapshot(MagicMock(), "research-run")

        self.assertEqual(snapshot.accepted_connections, 10)
        self.assertEqual(snapshot.upstream_bytes, 1234)
        command = " ".join(str(value) for value in remote.call_args.args)
        self.assertIn(
            "/tmp/synthran/research-run/amber-ingress-snapshot.json",
            command,
        )
        self.assertIn(
            "/tmp/synthran/research-run/ingress-snapshot.json",
            command,
        )
        self.assertLess(
            command.index("amber-ingress-snapshot.json"),
            command.index("ingress-snapshot.json"),
        )


if __name__ == "__main__":
    unittest.main()
