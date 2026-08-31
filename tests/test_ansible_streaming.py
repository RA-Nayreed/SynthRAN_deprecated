from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from synthran.ansible_streaming import (
    format_duration,
    friendly_task_name,
    is_ugly_template_task,
    parse_ansible_line,
    run_streaming_ansible_command,
)


class AnsibleDurationFormattingTests(unittest.TestCase):
    def test_format_duration(self) -> None:
        self.assertEqual(format_duration(0), "0s")
        self.assertEqual(format_duration(30), "30s")
        self.assertEqual(format_duration(60), "1m")
        self.assertEqual(format_duration(90), "1m 30s")
        self.assertEqual(format_duration(3665), "61m 5s")


class AnsibleTaskMappingTests(unittest.TestCase):
    def test_friendly_task_mappings(self) -> None:
        self.assertEqual(
            friendly_task_name("Replace every reviewed mutable Open5GS image reference"),
            "Open5GS locked images",
        )
        self.assertEqual(friendly_task_name("Check job status"), "node bootstrap")
        self.assertEqual(
            friendly_task_name("setup/ovs : Ensure OVS is installed"),
            "node networking setup",
        )
        self.assertEqual(
            friendly_task_name("5g/open5gs/deploy : Wait for Open5GS Core NFs pods Ready"),
            "Open5GS: Wait for Open5GS Core NFs pods Ready",
        )
        self.assertEqual(
            friendly_task_name("5g/srsRAN/deploy : Wait for gNB cell to be activated"),
            "srsRAN: Wait for gNB cell to be activated",
        )

    def test_ugly_template_detection(self) -> None:
        self.assertTrue(
            is_ugly_template_task(
                "Attempt gNB deployment (Attempt << error 1 - 'item' is undefined >>)"
            )
        )
        self.assertFalse(is_ugly_template_task("Deploy gNB"))


class AnsibleStreamingParserTests(unittest.TestCase):
    def test_headers_are_not_execution_evidence(self) -> None:
        self.assertIsNone(parse_ansible_line("PLAY [Open5GS] ********"))
        self.assertIsNone(
            parse_ansible_line(
                "TASK [5g/open5gs/deploy : Wait for Open5GS Core NFs pods Ready] ****"
            )
        )

    def test_routine_statuses_are_suppressed(self) -> None:
        self.assertIsNone(parse_ansible_line("ok: [sopnode-f2]"))
        self.assertIsNone(parse_ansible_line("changed: [sopnode-f3]"))
        self.assertIsNone(parse_ansible_line("skipping: [sopnode-f2]"))

    def test_failure_contains_context_and_sanitized_reason(self) -> None:
        rendered = parse_ansible_line(
            'fatal: [sopnode-f2]: FAILED! => {"msg": "AMF pod did not become Ready"}',
            current_task="Open5GS: Wait for Open5GS Core NFs pods Ready",
        )
        self.assertIsNotNone(rendered)
        assert rendered is not None
        self.assertIn("✗ Open5GS: Wait for Open5GS Core NFs pods Ready", rendered)
        self.assertIn("host=sopnode-f2", rendered)
        self.assertIn("state=FATAL", rendered)
        self.assertIn("reason=AMF pod did not become Ready", rendered)
        self.assertNotIn("\n", rendered)

    def test_failure_reason_is_bounded_and_redacted(self) -> None:
        rendered = parse_ansible_line(
            'fatal: [sopnode-f2]: FAILED! => {"msg": "subscriber 123456789012345 and token aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}',
            current_task="Deploy core",
        )
        self.assertIsNotNone(rendered)
        assert rendered is not None
        self.assertNotIn("123456789012345", rendered)
        self.assertNotIn("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", rendered)
        self.assertIn("<redacted>", rendered)


class AnsibleStreamingRunnerTests(unittest.TestCase):
    def _run(self, script: str, **kwargs):
        reported: list[str] = []
        with tempfile.TemporaryDirectory() as temporary:
            result = run_streaming_ansible_command(
                [sys.executable, "-c", script],
                cwd=Path(temporary),
                environment=None,
                timeout_seconds=10,
                report=reported.append,
                **kwargs,
            )
        return result, reported

    def test_executed_visible_task_is_announced_after_host_status(self) -> None:
        script = (
            "print('TASK [Replace every reviewed mutable Open5GS image reference] *****')\n"
            "print('ok: [sopnode-f2]')\n"
        )
        result, reported = self._run(script)
        self.assertEqual(0, result.returncode)
        self.assertEqual(["→ Open5GS locked images"], reported)

    def test_skipped_visible_task_is_never_announced(self) -> None:
        script = (
            "print('TASK [Replace every reviewed mutable Open5GS image reference] *****')\n"
            "print('skipping: [sopnode-f2]')\n"
            "print('skipping: [sopnode-f3]')\n"
        )
        result, reported = self._run(script)
        self.assertEqual(0, result.returncode)
        self.assertEqual([], reported)
        self.assertIn("skipping: [sopnode-f2]", result.stdout)

    def test_unselected_routine_task_remains_hidden_when_executed(self) -> None:
        script = (
            "print('TASK [5g/open5gs/config : Update AMF ConfigMap on disk] *****')\n"
            "print('changed: [sopnode-f2]')\n"
        )
        result, reported = self._run(script)
        self.assertEqual(0, result.returncode)
        self.assertEqual([], reported)

    def test_ignored_failure_is_not_promoted(self) -> None:
        script = (
            "print('TASK [Check optional mount] *****')\n"
            "print('fatal: [sopnode-f3]: FAILED! => {\"msg\": \"optional\"}')\n"
            "print('...ignoring')\n"
            "print('TASK [Wait for AMF pod to become Ready] *****')\n"
            "print('ok: [sopnode-f2]')\n"
        )
        result, reported = self._run(script)
        self.assertEqual(0, result.returncode)
        self.assertEqual(["→ Wait for AMF pod to become Ready"], reported)

    def test_nonzero_exit_reports_failure_with_reason(self) -> None:
        script = (
            "import sys\n"
            "print('TASK [Wait for AMF pod to become Ready] *****')\n"
            "print('fatal: [sopnode-f2]: FAILED! => {\"msg\": \"readiness timeout\"}')\n"
            "sys.exit(2)\n"
        )
        result, reported = self._run(script)
        self.assertEqual(2, result.returncode)
        self.assertEqual(1, len(reported))
        self.assertIn("✗ Wait for AMF pod to become Ready", reported[0])
        self.assertIn("host=sopnode-f2", reported[0])
        self.assertIn("reason=readiness timeout", reported[0])
        self.assertNotIn("\n", reported[0])

    def test_hidden_task_failure_is_still_reported(self) -> None:
        script = (
            "import sys\n"
            "print('TASK [command argv=[\"ip\", \"link\"]] *****')\n"
            "print('fatal: [sopnode-f2]: FAILED! => {\"msg\": \"interface not found\"}')\n"
            "sys.exit(2)\n"
        )
        result, reported = self._run(script)
        self.assertEqual(2, result.returncode)
        self.assertEqual(1, len(reported))
        self.assertIn("✗ command", reported[0])
        self.assertIn("reason=interface not found", reported[0])

    def test_heartbeat_proves_long_running_visible_task(self) -> None:
        script = (
            "import sys, time\n"
            "print('TASK [Replace every reviewed mutable Open5GS image reference] *****')\n"
            "sys.stdout.flush()\n"
            "time.sleep(0.35)\n"
            "print('ok: [sopnode-f2]')\n"
        )
        result, reported = self._run(
            script,
            heartbeat_interval_seconds=0.1,
            poll_interval_seconds=0.05,
        )
        self.assertEqual(0, result.returncode)
        self.assertEqual(1, reported.count("→ Open5GS locked images"))
        self.assertGreaterEqual(
            len([item for item in reported if item.startswith("… Open5GS locked images ·")]),
            2,
        )

    def test_timeout_terminates_process(self) -> None:
        script = "import time\ntime.sleep(10)\n"
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(subprocess.TimeoutExpired):
                run_streaming_ansible_command(
                    [sys.executable, "-c", script],
                    cwd=Path(temporary),
                    environment=None,
                    timeout_seconds=1,
                    poll_interval_seconds=0.1,
                )


if __name__ == "__main__":
    unittest.main()
