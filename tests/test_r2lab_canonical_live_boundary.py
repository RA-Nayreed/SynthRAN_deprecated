from __future__ import annotations

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class CanonicalLiveBoundaryTests(unittest.TestCase):
    def test_normal_physical_lifecycle_uses_canonical_live_cluster(self) -> None:
        lifecycle = (ROOT / "synthran/r2lab/lifecycle.py").read_text(encoding="utf-8")
        workload = (ROOT / "synthran/r2lab/workload_boundary.py").read_text(encoding="utf-8")

        self.assertIn("from synthran.r2lab.live_cluster import prove_user_plane", lifecycle)
        self.assertIn(
            "from synthran.r2lab.workload_boundary import execute_physical_workload_handoff",
            lifecycle,
        )
        self.assertNotIn("prove_physical_user_plane", lifecycle)
        self.assertNotIn("execute_physical_workload_handoff,", lifecycle)

        self.assertIn("from synthran.r2lab.live_cluster import R2LabLiveClusterError, verify_n2", workload)
        self.assertIn("verify_physical_authority", workload)
        for forbidden in (
            "strict_ssh_command",
            "isolated_config",
            "-F /dev/null",
            "UserKnownHostsFile",
            "StrictHostKeyChecking",
        ):
            self.assertNotIn(forbidden, workload)


if __name__ == "__main__":
    unittest.main()
