from __future__ import annotations

import io
import json
from pathlib import Path
import tempfile
import unittest

from synthran.run_events import RunEventStream, RunProgress, normalize_child_message


class ChildNormalizationTests(unittest.TestCase):
    def test_internal_doctor_and_path_proof_output_is_hidden(self) -> None:
        for line in (
            "SynthRAN doctor (offline)",
            "[PASS] inventory: open5gs + srsRAN + rfsim",
            "Result: READY",
            "SynthRAN network verification (run-001)",
            "Result: PATH PROVEN",
            "Deployment completed for run run-001; path proof is still required.",
            "Sanitized evidence: network-evidence.json",
        ):
            with self.subTest(line=line):
                self.assertIsNone(normalize_child_message(line))

    def test_ansible_and_amber_events_are_normalized(self) -> None:
        self.assertEqual(
            "  → Open5GS locked images",
            normalize_child_message("[synthran] → Open5GS locked images"),
        )
        self.assertEqual(
            "    … Open5GS locked images · 1m",
            normalize_child_message("[synthran] … Open5GS locked images · 1m"),
        )
        self.assertEqual(
            "  Amber energy treatment: external-power-scale=1, node-variation=0",
            normalize_child_message(
                "[synthran] Amber energy treatment: external-power-scale=1, node-variation=0"
            ),
        )
        self.assertEqual(
            "    180 opportunities, 144 decoded, 36 classified source loss",
            normalize_child_message(
                "[synthran] Amber source: 180 opportunities, 144 decoded, 36 classified source loss"
            ),
        )

    def test_skipped_or_unknown_child_chatter_does_not_leak(self) -> None:
        self.assertIsNone(normalize_child_message("PLAY: Deploy Open5GS CN"))
        self.assertIsNone(
            normalize_child_message("TASK: Open5GS config: Update AMF ConfigMap on disk")
        )
        self.assertIsNone(normalize_child_message("some upstream debug line"))


class RunEventStreamTests(unittest.TestCase):
    def test_terminal_and_jsonl_use_one_canonical_event(self) -> None:
        terminal = io.StringIO()
        with tempfile.TemporaryDirectory() as temporary:
            stream = RunEventStream(
                run_id="run-001",
                radio="rfsim",
                terminal=terminal,
                root=Path(temporary),
            )
            stream.emit("→ network", stage="network", event="started")
            stream.flush()
            lines = stream.path.read_text(encoding="utf-8").splitlines()

        self.assertEqual("[synthran] → network\n", terminal.getvalue())
        self.assertEqual(1, len(lines))
        event = json.loads(lines[0])
        self.assertEqual("synthran/run-event/v2", event["schema"])
        self.assertEqual("run-001", event["run_id"])
        self.assertEqual("rfsim", event["radio"])
        self.assertEqual("network", event["stage"])
        self.assertEqual("started", event["event"])
        self.assertEqual("→ network", event["message"])

    def test_child_stream_does_not_double_prefix(self) -> None:
        terminal = io.StringIO()
        with tempfile.TemporaryDirectory() as temporary:
            stream = RunEventStream(
                run_id="run-001",
                radio="rfsim",
                terminal=terminal,
                root=Path(temporary),
            )
            stream.write("[synthran] → Open5GS locked images\n")
            stream.flush()
        self.assertEqual("[synthran]   → Open5GS locked images\n", terminal.getvalue())
        self.assertNotIn("[synthran] [synthran]", terminal.getvalue())


class RunLifecycleRenderingTests(unittest.TestCase):
    def test_rfsim_path_is_nested_under_network_not_a_public_stage(self) -> None:
        terminal = io.StringIO()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            network_root = root / "runs"
            evidence_dir = network_root / "run-001"
            evidence_dir.mkdir(parents=True)
            (evidence_dir / "network-evidence.json").write_text(
                json.dumps(
                    {
                        "path": {
                            "pdu_address": "12.1.0.13",
                        }
                    }
                ),
                encoding="utf-8",
            )
            progress = RunProgress(
                run_id="run-001",
                radio="rfsim",
                terminal=terminal,
                network_root=network_root,
                experiment_root=root / "experiments",
            )
            # Keep test events out of the repository's default .synthran/events.
            progress.stream.path = root / "events.jsonl"
            progress.start("network", "deploy")
            progress.done("network", "deployed")
            progress.start("path", "prove")
            progress.done("path", "accepted")
            progress.close()

        output = terminal.getvalue()
        self.assertIn("[synthran] → network", output)
        self.assertIn("[synthran]   → verify live 5G session readiness", output)
        self.assertIn("[synthran]   ✓ PDU session · 12.1.0.13", output)
        self.assertIn("[synthran] ✓ network: READY", output)
        self.assertNotIn("→ path", output)
        self.assertNotIn("PATH PROVEN", output)

    def test_infrastructure_collapses_preflight_validator_chatter(self) -> None:
        terminal = io.StringIO()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            progress = RunProgress(
                run_id="run-001",
                radio="rfsim",
                terminal=terminal,
                network_root=root / "runs",
                experiment_root=root / "experiments",
            )
            progress.stream.path = root / "events.jsonl"
            progress.start("resources", "prepare")
            progress.done("resources", "prepared")
            progress.start("preflight", "doctor")
            progress.child_stream.write("SynthRAN doctor (offline)\n")
            progress.child_stream.write("[PASS] inventory: ready\n")
            progress.child_stream.write("Result: READY\n")
            progress.done("preflight", "verified")
            progress.close()

        output = terminal.getvalue()
        self.assertIn("[synthran] → infrastructure", output)
        self.assertIn(
            "[synthran]   ✓ authority and deployment prerequisites verified",
            output,
        )
        self.assertIn("[synthran] ✓ infrastructure", output)
        self.assertNotIn("doctor", output)
        self.assertNotIn("[PASS]", output)


if __name__ == "__main__":
    unittest.main()
