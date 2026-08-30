from __future__ import annotations

import json
from pathlib import Path
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class UnifiedResearchSchemaTests(unittest.TestCase):
    def test_single_schema_contains_all_research_record_contracts(self) -> None:
        path = REPOSITORY_ROOT / "contracts" / "research-v1alpha1.schema.json"
        schema = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual("synthran/research/v1alpha1", schema["$id"])
        definitions = schema["$defs"]
        for name in (
            "experiment",
            "experimentV2",
            "campaign",
            "capacity",
            "measurementWindow",
            "probe",
            "networkSample",
            "loadResult",
            "summary",
            "summaryV2",
        ):
            self.assertIn(name, definitions)
        self.assertIn("measurement", definitions)
        self.assertIn("load", definitions)
        self.assertIn("identifier", definitions)
        self.assertIn("conditionName", definitions)
        self.assertEqual(
            "synthran/research-experiment/v2alpha1",
            definitions["experimentV2"]["properties"]["schema"]["const"],
        )
        self.assertEqual(
            "synthran/research-summary/v2alpha1",
            definitions["summaryV2"]["properties"]["schema"]["const"],
        )

    def test_split_research_schema_files_do_not_return(self) -> None:
        contracts = REPOSITORY_ROOT / "contracts"
        self.assertEqual(
            ["research-v1alpha1.schema.json"],
            sorted(path.name for path in contracts.glob("research*.schema.json")),
        )


if __name__ == "__main__":
    unittest.main()
