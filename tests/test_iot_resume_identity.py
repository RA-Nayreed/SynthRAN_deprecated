from __future__ import annotations

import argparse
import json
from pathlib import Path
import tempfile
import unittest

from synthran.cli import _validate_persisted_iot_identity
from synthran.errors import SynthRANError


def _write(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


class IoTResumeIdentityTests(unittest.TestCase):
    def test_rfsim_requires_amber_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _write(
                root / "runs" / "run-001" / "manifest.json",
                {
                    "run_id": "run-001",
                    "iot_source": "amber",
                    "iot_profile": "transport-v1",
                    "iot_seed": 424242,
                    "sensor_period_seconds": 10,
                },
            )
            args = argparse.Namespace(
                command="run",
                radio="rfsim",
                run_id="run-001",
                experiment_root=root / "runs",
                network_run_id=None,
                campaign=None,
                seeds=None,
                conditions=None,
                campaign_seed=None,
                iot_profile="transport-v1",
                iot_seed=424242,
                sensor_period=10,
            )
            _validate_persisted_iot_identity(args)

    def test_rfsim_requires_exact_amber_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _write(
                root / "runs" / "run-001" / "manifest.json",
                {
                    "run_id": "run-001",
                    "iot_source": "amber",
                    "iot_profile": "transport-v1",
                    "iot_seed": 17,
                    "sensor_period_seconds": 12,
                },
            )
            args = argparse.Namespace(
                command="run",
                radio="rfsim",
                run_id="run-001",
                experiment_root=root / "runs",
                network_run_id=None,
                campaign=None,
                seeds=None,
                conditions=None,
                campaign_seed=None,
                iot_profile="transport-v1",
                iot_seed=17,
                sensor_period=12,
            )
            _validate_persisted_iot_identity(args)
            args.iot_seed = 18
            with self.assertRaisesRegex(SynthRANError, "iot_seed"):
                _validate_persisted_iot_identity(args)

    def test_physical_runs_do_not_consume_legacy_workload_manifests(self) -> None:
        args = argparse.Namespace(
            command="run",
            radio="r2lab",
            run_id="run-001",
        )
        _validate_persisted_iot_identity(args)


if __name__ == "__main__":
    unittest.main()
