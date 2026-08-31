from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from synthran.dependencies import load_lock
from synthran.fiveg_ansible import load_inventory
from synthran.network.runtime import (
    DEPLOYMENT_SCHEMA,
    NetworkRuntimeError,
    load_deployment_manifest,
)
from synthran.slices_controller import fingerprint


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = (
    REPOSITORY_ROOT
    / "tests"
    / "fixtures"
    / "inventory_open5gs_srsran_rfsim.ini"
)


class NetworkManifestProvenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        self.inventory = load_inventory(FIXTURE)
        self.project = "project-test"
        self.experiment = "experiment-test"
        self.historical_digest = "0" * 64
        self.dependencies = {
            item.name: item.commit
            for item in self.lock.git
            if item.name in {"fiveg_ansible", "open5gs_k8s", "srsran_helm"}
        }

    def _manifest(self) -> dict[str, object]:
        return {
            "schema": DEPLOYMENT_SCHEMA,
            "run_id": "net-run-1",
            "status": "path-proven",
            "inventory": {"sha256": self.inventory.sha256},
            "dependencies": dict(self.dependencies),
            "dependency_lock_sha256": self.historical_digest,
            "slices_controller": {
                "dependency_lock_sha256": self.historical_digest,
                "project_fingerprint": fingerprint(self.project),
                "experiment_fingerprint": fingerprint(self.experiment),
            },
        }

    def _load(self, payload: dict[str, object]) -> object:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            return load_deployment_manifest(
                path=path,
                run_id="net-run-1",
                inventory=self.inventory,
                lock=self.lock,
                slices_project=self.project,
                slices_experiment=self.experiment,
            )

    def test_accepts_historical_lock_when_network_pins_match(self) -> None:
        manifest = self._load(self._manifest())
        self.assertEqual(
            self.historical_digest,
            manifest["dependency_lock_sha256"],
        )

    def test_rejects_inconsistent_historical_lock_provenance(self) -> None:
        payload = self._manifest()
        payload["slices_controller"] = {
            **payload["slices_controller"],
            "dependency_lock_sha256": "1" * 64,
        }
        with self.assertRaisesRegex(
            NetworkRuntimeError,
            "SLICES context does not match",
        ):
            self._load(payload)

    def test_rejects_network_dependency_drift(self) -> None:
        payload = self._manifest()
        payload["dependencies"] = {
            **payload["dependencies"],
            "fiveg_ansible": "f" * 40,
        }
        with self.assertRaisesRegex(
            NetworkRuntimeError,
            "dependencies do not match the lock",
        ):
            self._load(payload)


if __name__ == "__main__":
    unittest.main()
