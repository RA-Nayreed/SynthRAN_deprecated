from __future__ import annotations

from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from synthran import cli
from synthran.research import ResearchError
from synthran.research.energy_calibration import parse_energy_scales


class AmberEnergyCalibrationCliTests(unittest.TestCase):
    def test_energy_calibration_parser_is_offline_and_profile_free(self) -> None:
        args = cli._parser().parse_args(
            [
                "research",
                "energy-calibrate",
                "--calibration-id",
                "ambient-energy-cal-01",
                "--network-run-id",
                "accepted-rfsim-network",
            ]
        )
        self.assertEqual("research", args.command)
        self.assertEqual("energy-calibrate", args.research_command)
        self.assertEqual("ambient-energy-cal-01", args.calibration_id)
        self.assertEqual("accepted-rfsim-network", args.network_run_id)
        self.assertEqual("1.0,0.75,0.5,0.33,0.25", args.scales)
        self.assertEqual(30, args.warmup_seconds)
        self.assertEqual(180, args.duration_seconds)
        self.assertFalse(hasattr(args, "inventory"))
        self.assertFalse(hasattr(args, "iot_profile"))

    def test_scale_parser_preserves_order_and_removes_duplicates(self) -> None:
        self.assertEqual((1.0, 0.5, 0.25), parse_energy_scales("1, 0.5,1.0,0.25"))
        with self.assertRaisesRegex(ResearchError, "power scale"):
            parse_energy_scales("1.0,0")

    def test_main_dispatches_energy_calibration_without_live_research(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result_path = root / "energy-calibration.json"
            result_path.write_text(
                json.dumps(
                    {
                        "schema": "synthran/amber-energy-calibration/v1alpha1",
                        "calibration_id": "ambient-energy-cal-02",
                        "recommended": {
                            "power_scale": 0.33,
                            "energy_loss_ratio": 0.25,
                            "decode_ratio": 0.60,
                            "target_band_match": True,
                            "profile_digest": "d" * 64,
                        },
                    }
                ),
                encoding="utf-8",
            )
            output = StringIO()
            with (
                patch.object(
                    cli,
                    "execute_energy_calibration",
                    return_value=result_path,
                ) as execute,
                patch.object(
                    cli.command_runtime,
                    "repository_root",
                    return_value=Path("/repo"),
                ),
                patch.object(cli, "dispatch") as legacy_dispatch,
                patch.object(cli, "execute_amber_research_experiment") as live_research,
                redirect_stdout(output),
            ):
                code = cli.main(
                    [
                        "research",
                        "energy-calibrate",
                        "--calibration-id",
                        "ambient-energy-cal-02",
                        "--network-run-id",
                        "accepted-rfsim-network",
                        "--scales",
                        "1.0,0.5,0.33",
                        "--energy-node-variation",
                        "0.1",
                        "--target-energy-loss-min",
                        "0.2",
                        "--target-energy-loss-max",
                        "0.3",
                        "--deps-root",
                        "/deps",
                        "--calibration-root",
                        "/calibrations",
                    ]
                )

        self.assertEqual(0, code)
        legacy_dispatch.assert_not_called()
        live_research.assert_not_called()
        kwargs = execute.call_args.kwargs
        self.assertEqual("ambient-energy-cal-02", kwargs["calibration_id"])
        self.assertEqual("accepted-rfsim-network", kwargs["network_run_id"])
        self.assertEqual((1.0, 0.5, 0.33), kwargs["scales"])
        self.assertEqual(0.1, kwargs["energy_node_variation"])
        self.assertEqual(0.2, kwargs["target_energy_loss_min"])
        self.assertEqual(0.3, kwargs["target_energy_loss_max"])
        self.assertEqual(Path("/repo"), kwargs["repository_root"])
        self.assertEqual(Path("/deps"), kwargs["dependency_root"])
        self.assertEqual(Path("/calibrations"), kwargs["calibration_root"])
        self.assertIn('"power_scale": 0.33', output.getvalue())


if __name__ == "__main__":
    unittest.main()
