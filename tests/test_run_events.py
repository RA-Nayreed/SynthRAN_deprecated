from __future__ import annotations

import io
import json
from pathlib import Path
import tempfile
import unittest

from synthran.run_events import (
    FIVEG_EVENT_SCHEMA,
    PUBLIC_STAGES,
    RunEventStream,
    RunProgress,
    normalize_child_message,
)


class ChildNormalizationTests(unittest.TestCase):
    def test_unknown_and_deployment_chatter_is_not_interpreted(self) -> None:
        for line in (
            "SynthRAN doctor (offline)",
            "[PASS] inventory: open5gs + srsRAN + rfsim",
            "Result: READY",
            "PLAY: Deploy Open5GS CN",
            "TASK: Open5GS config: Update AMF ConfigMap on disk",
            "[synthran] → Open5GS locked images",
            "[synthran] … Open5GS locked images · 1m",
            "some upstream debug line",
        ):
            with self.subTest(line=line):
                self.assertIsNone(normalize_child_message(line))

    def test_amber_events_are_still_normalized(self) -> None:
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
            stream.write("[synthran] Amber source: 10 events ready\n")
            stream.flush()
        self.assertEqual("[synthran]     10 events ready\n", terminal.getvalue())
        self.assertNotIn("[synthran] [synthran]", terminal.getvalue())


class UpstreamRelayTests(unittest.TestCase):
    def test_fiveg_event_is_relayed_and_preserved_as_upstream_detail(self) -> None:
        terminal = io.StringIO()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            progress = RunProgress(
                run_id="run-001",
                radio="rfsim",
                terminal=terminal,
            )
            progress.stream.path = root / "events.jsonl"
            upstream = {
                "schema": FIVEG_EVENT_SCHEMA,
                "deployment_id": "run-001",
                "phase": "deployment",
                "event": "started",
                "component": "5g-stack",
                "detail": {"ignored_by_renderer": "kept-for-evidence"},
            }
            progress.relay_fiveg_event(upstream)
            progress.close()
            persisted = json.loads((root / "events.jsonl").read_text().splitlines()[0])

        self.assertEqual("[synthran]   → 5G deployment\n", terminal.getvalue())
        self.assertEqual("infrastructure", persisted["stage"])
        self.assertEqual("5g-stack", persisted["component"])
        self.assertEqual(upstream, persisted["detail"]["upstream"])

    def test_foreign_or_unknown_fiveg_events_are_ignored(self) -> None:
        terminal = io.StringIO()
        with tempfile.TemporaryDirectory() as temporary:
            progress = RunProgress(run_id="run-001", radio="rfsim", terminal=terminal)
            progress.stream.path = Path(temporary) / "events.jsonl"
            progress.relay_fiveg_event(
                {
                    "schema": FIVEG_EVENT_SCHEMA,
                    "deployment_id": "other-run",
                    "phase": "deployment",
                    "event": "started",
                }
            )
            progress.relay_fiveg_event(
                {
                    "schema": FIVEG_EVENT_SCHEMA,
                    "deployment_id": "run-001",
                    "phase": "future-phase",
                    "event": "started",
                }
            )
            progress.close()
        self.assertEqual("", terminal.getvalue())


class RunLifecycleRenderingTests(unittest.TestCase):
    def test_public_lifecycle_has_no_path_stage(self) -> None:
        self.assertEqual(
            {
                "provider",
                "infrastructure",
                "network",
                "workload",
                "acceptance",
                "cleanup",
            },
            set(PUBLIC_STAGES),
        )
        terminal = io.StringIO()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            progress = RunProgress(
                run_id="run-001",
                radio="rfsim",
                terminal=terminal,
            )
            progress.stream.path = root / "events.jsonl"
            with self.assertRaisesRegex(ValueError, "unsupported public lifecycle stage"):
                progress.start("path", "legacy stage")
            progress.start("network", "verify current session readiness")
            progress.stream.emit(
                "  ✓ PDU session · 12.1.0.13",
                stage="network",
                event="detail",
            )
            progress.done("network", "READY")
            progress.close()

        output = terminal.getvalue()
        self.assertIn("[synthran] → network: verify current session readiness", output)
        self.assertIn("[synthran]   ✓ PDU session · 12.1.0.13", output)
        self.assertIn("[synthran] ✓ network: READY", output)
        self.assertNotIn("→ path", output)
        self.assertNotIn("PATH PROVEN", output)

    def test_renderer_does_not_read_backend_chatter_to_infer_truth(self) -> None:
        terminal = io.StringIO()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            progress = RunProgress(
                run_id="run-001",
                radio="rfsim",
                terminal=terminal,
            )
            progress.stream.path = root / "events.jsonl"
            progress.start("infrastructure", "prepare")
            progress.child_stream.write("SynthRAN doctor (offline)\n")
            progress.child_stream.write("[PASS] inventory: ready\n")
            progress.child_stream.write("Result: READY\n")
            progress.done("infrastructure", "READY")
            progress.close()

        output = terminal.getvalue()
        self.assertIn("[synthran] → infrastructure: prepare", output)
        self.assertIn("[synthran] ✓ infrastructure: READY", output)
        self.assertNotIn("doctor", output)
        self.assertNotIn("[PASS]", output)


if __name__ == "__main__":
    unittest.main()
