from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from synthran.dependencies import load_lock
from synthran.network.runtime import (
    DEPLOYMENT_SCHEMA,
    NetworkRuntimeError,
    load_deployment_manifest,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class NetworkManifestProvenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        self.fiveg_commit = next(
            item.commit for item in self.lock.git if item.name == "fiveg_ansible"
        )

    def _manifest(self) -> dict[str, object]:
        return {
            "schema": DEPLOYMENT_SCHEMA,
            "id": "net-run-1",
            "state": "ready",
            "fiveg_ansible_commit": self.fiveg_commit,
        }

    def _load(self, payload: dict[str, object]) -> object:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            return load_deployment_manifest(
                path=path,
                run_id="net-run-1",
                lock=self.lock,
            )

    def test_accepts_ready_upstream_manifest_at_locked_commit(self) -> None:
        manifest = self._load(self._manifest())
        self.assertEqual(self.fiveg_commit, manifest["fiveg_ansible_commit"])

    def test_rejects_upstream_commit_drift(self) -> None:
        payload = self._manifest()
        payload["fiveg_ansible_commit"] = "f" * 40
        with self.assertRaisesRegex(NetworkRuntimeError, "provenance"):
            self._load(payload)

    def test_rejects_non_ready_upstream_manifest(self) -> None:
        payload = self._manifest()
        payload["state"] = "failed"
        with self.assertRaisesRegex(NetworkRuntimeError, "not ready"):
            self._load(payload)


if __name__ == "__main__":
    unittest.main()
