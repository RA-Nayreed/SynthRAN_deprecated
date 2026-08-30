from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from synthran.experiment import ExperimentError, ExperimentScenario
from synthran.iot import write_run_inputs


class RetiredIoTRendererTests(unittest.TestCase):
    def test_removed_renderer_fails_closed(self) -> None:
        scenario = ExperimentScenario(
            "experiment-01",
            "network-accepted-01",
            "12.1.0.1",
        )
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(
                ExperimentError,
                "previous executable sensor renderer has been removed",
            ):
                write_run_inputs(
                    scenario,
                    run_directory=Path(temporary),
                    contiki_directory=Path("/removed/contiki"),
                )


if __name__ == "__main__":
    unittest.main()
