from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from synthran.research import LOAD_RESULT_SCHEMA, load_jsonl
from synthran.research.iperf_window import parse_measurement_load_log


class AmberResearchWindowTests(unittest.TestCase):
    def test_load_metric_excludes_warmup_intervals(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "iperf.log"
            destination = root / "load.jsonl"
            intervals = []
            for second in range(0, 40):
                # Warmup is deliberately slow; measurement is exactly 20 Mbps.
                bps = 1_000_000.0 if second < 10 else 20_000_000.0
                intervals.append(
                    {
                        "sum": {
                            "start": float(second),
                            "end": float(second + 1),
                            "bits_per_second": bps,
                        }
                    }
                )
            source.write_text(
                json.dumps({"intervals": intervals, "end": {}}),
                encoding="utf-8",
            )

            parse_measurement_load_log(
                source,
                destination,
                target_bps=20_000_000,
                protocol="udp",
                measurement_start_offset_seconds=10.0,
                measurement_duration_seconds=30.0,
            )

            records = load_jsonl(destination, schema=LOAD_RESULT_SCHEMA)
            self.assertEqual(1, len(records))
            self.assertAlmostEqual(20_000_000.0, records[0]["bits_per_second"])
            self.assertAlmostEqual(30.0, records[0]["covered_seconds"])
            self.assertEqual(30, records[0]["contributing_intervals"])


if __name__ == "__main__":
    unittest.main()
