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
        self.assertEqual(format_duration(59), "59s")
        self.assertEqual(format_duration(60), "1m")
        self.assertEqual(format_duration(90), "1m 30s")
        self.assertEqual(format_duration(120), "2m")
        self.assertEqual(format_duration(150), "2m 30s")
        self.assertEqual(format_duration(3665), "61m 5s")


class AnsibleTaskMappingTests(unittest.TestCase):
    def test_friendly_task_mappings(self) -> None:
        self.assertEqual(
            friendly_task_name("Deploy the locked Open5GS core into the ready cluster"),
            "Open5GS core",
        )
        self.assertEqual(
            friendly_task_name("Deploy the srsRAN gNB and srsUE into the ready cluster"),
            "srsRAN RFSIM",
        )
        self.assertEqual(
            friendly_task_name("Replace every reviewed mutable Open5GS image reference"),
            "Pinning locked Open5GS images",
        )
        self.assertEqual(
            friendly_task_name("Replace every reviewed mutable srsRAN image reference"),
            "Pinning locked srsRAN images",
        )
        self.assertEqual(
            friendly_task_name("Attach the run ID to the deployed network resources"),
            "Recording run ownership",
        )
        self.assertEqual(
            friendly_task_name("5g/open5gs/config : Configure SMF"),
            "Open5GS config: Configure SMF",
        )
        self.assertEqual(
            friendly_task_name("5g/srsRAN/deploy : Start gNB"),
            "srsRAN deploy: Start gNB",
        )

    def test_ugly_template_detection(self) -> None:
        self.assertTrue(
            is_ugly_template_task(
                "Attempt gNB deployment (Attempt << error 1 - 'item' is undefined >>)"
            )
        )
        self.assertFalse(is_ugly_template_task("Deploy gNB"))


class AnsibleStreamingParserTests(unittest.TestCase):
    def test_task_and_play_recognition(self) -> None:
        self.assertEqual(
            parse_ansible_line("PLAY [all] *********************************************************************"),
            "  PLAY: all",
        )
        self.assertIsNone(
            parse_ansible_line("TASK [gather facts] ************************************************************"),
        )
        self.assertEqual(
            parse_ansible_line("TASK [5g/open5gs/config : install packages] ************************************"),
            "  TASK: Open5GS config: install packages",
        )
        self.assertEqual(
            parse_ansible_line("TASK [Wait for AMF pod to become Ready] *****************************************"),
            "  TASK: Wait for AMF pod to become Ready",
        )
        self.assertEqual(
            parse_ansible_line("RUNNING HANDLER [restart mosquitto] *********************************************"),
            "  HANDLER: restart mosquitto",
        )

    def test_ugly_skipped_template_task_suppressed(self) -> None:
        self.assertIsNone(
            parse_ansible_line(
                "TASK [5g/srsRAN/deploy : Attempt gNB deployment (Attempt << error 1 - 'item' is undefined >>)] *"
            )
        )

    def test_task_lines_strip_argument_decorations_and_suppress_routine(self) -> None:
        self.assertEqual(
            friendly_task_name("command argv=['helm', 'status']"),
            "command",
        )
        self.assertEqual(
            friendly_task_name("file path=/etc/systemd/system/mosquitto.service"),
            "file",
        )
        self.assertEqual(
            friendly_task_name("k8s definition={'apiVersion': 'v1'}"),
            "k8s",
        )
        self.assertEqual(
            friendly_task_name("assert that=['result.rc == 0']"),
            "assert",
        )
        self.assertEqual(
            friendly_task_name("shell _raw_params=systemctl restart mosquitto"),
            "shell",
        )
        self.assertEqual(
            friendly_task_name("include_tasks apply={'tags': ['deploy']}"),
            "include_tasks",
        )

        self.assertIsNone(
            parse_ansible_line("TASK [command argv=['helm', 'status']] ****************************************"),
        )
        self.assertIsNone(
            parse_ansible_line("TASK [file path=/etc/systemd/system/mosquitto.service] *************************"),
        )
        self.assertIsNone(
            parse_ansible_line("TASK [k8s definition={'apiVersion': 'v1'}] *************************************"),
        )
        self.assertIsNone(
            parse_ansible_line("TASK [assert that=['result.rc == 0']] ******************************************"),
        )

        self.assertEqual(
            parse_ansible_line("TASK [Attach the run ID to the deployed network resources] **********************"),
            "  TASK: Recording run ownership",
        )

    def test_routine_host_statuses_are_suppressed(self) -> None:
        self.assertIsNone(parse_ansible_line("ok: [sopnode-f2]"))
        self.assertIsNone(parse_ansible_line("ok: [sopnode-f2] => (item=pkg1)"))
        self.assertIsNone(parse_ansible_line("changed: [sopnode-f3]"))
        self.assertIsNone(parse_ansible_line("changed: [sopnode-f3] => {\"changed\": true}"))
        self.assertIsNone(parse_ansible_line("skipping: [sopnode-f2]"))
        self.assertIsNone(parse_ansible_line("skipping: [sopnode-f2 -> localhost]"))

    def test_failure_statuses_remain_visible_with_context(self) -> None:
        failed_res = parse_ansible_line(
            "failed: [sopnode-f2] => {\"msg\": \"command failed\"}",
            current_task="Wait for AMF pod",
        )
        self.assertIsNotNone(failed_res)
        self.assertIn("[FAIL] Wait for AMF pod", failed_res)
        self.assertIn("host: sopnode-f2", failed_res)
        self.assertIn("state: FAILED", failed_res)

        fatal_res = parse_ansible_line(
            "fatal: [sopnode-f2]: FAILED! => {\"msg\": \"failed to start service\"}",
            current_task="Deploy core services",
        )
        self.assertIsNotNone(fatal_res)
        self.assertIn("[FAIL] Deploy core services", fatal_res)
        self.assertIn("host: sopnode-f2", fatal_res)
        self.assertIn("state: FATAL", fatal_res)

        unreach_res = parse_ansible_line(
            "unreachable: [sopnode-f3] => {\"msg\": \"host unreachable\"}",
            current_task="Ping host",
        )
        self.assertIsNotNone(unreach_res)
        self.assertIn("[FAIL] Ping host", unreach_res)
        self.assertIn("host: sopnode-f3", unreach_res)
        self.assertIn("state: UNREACHABLE", unreach_res)

    def test_suppression_of_unsafe_and_raw_lines(self) -> None:
        raw_lines = [
            "PLAY RECAP ********************************************************************",
            "sopnode-f2                  : ok=12   changed=3    unreachable=0    failed=0",
            "{\"changed\": false, \"ansible_facts\": {\"discovered_interpreter_python\": \"/usr/bin/python3\"}}",
            "    \"stdout\": \"status=active node=sopnode-f2\"",
            "    \"cmd\": [\"/opt/tool/bin\", \"--flag\", \"alpha\"]",
            "Traceback (most recent call last):",
            "  File \"<string>\", line 1, in <module>",
            "",
            "   ",
            "META: ran handlers",
            "included: deploy/ansible/tasks.yml for sopnode-f2",
        ]
        for line in raw_lines:
            self.assertIsNone(
                parse_ansible_line(line),
                f"Expected line to be suppressed, but got parsed result: {line!r}",
            )


class AnsibleStreamingRunnerTests(unittest.TestCase):
    def test_streaming_process_success_suppresses_routine_chatter_and_keeps_raw_output(self) -> None:
        script = (
            "import sys, time\n"
            "print('PLAY [Deploy the locked Open5GS core into the ready cluster] ********************')\n"
            "print('TASK [Replace every reviewed mutable Open5GS image reference] *******************')\n"
            "print('ok: [sopnode-f2]')\n"
            "print('{\"sensitive\": \"json_data_123\"}')\n"
            "print('changed: [sopnode-f3]')\n"
            "print('skipping: [sopnode-f2]')\n"
            "print('TASK [5g/srsRAN/deploy : Attempt gNB deployment (Attempt << error 1 - undefined >>)] *')\n"
            "print('skipping: [sopnode-f3]')\n"
            "sys.stdout.flush()\n"
        )
        reported: list[str] = []
        with tempfile.TemporaryDirectory() as temporary:
            cwd = Path(temporary)
            result = run_streaming_ansible_command(
                [sys.executable, "-c", script],
                cwd=cwd,
                environment=None,
                timeout_seconds=10,
                report=reported.append,
            )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(
            reported,
            [
                "  PLAY: Open5GS core",
                "  TASK: Pinning locked Open5GS images",
            ],
        )
        self.assertIn('{"sensitive": "json_data_123"}', result.stdout)
        self.assertIn("PLAY [Deploy the locked Open5GS core", result.stdout)
        self.assertIn("ok: [sopnode-f2]", result.stdout)
        self.assertIn("skipping: [sopnode-f2]", result.stdout)
        self.assertIn("Attempt << error 1 - undefined >>", result.stdout)

    def test_streaming_process_success_suppresses_ignored_failure(self) -> None:
        script = (
            "import sys\n"
            "print('TASK [Check optional mount] ****************************************************')\n"
            "print('fatal: [sopnode-f3]: FAILED! => {\"rc\": 1}')\n"
            "print('...ignoring')\n"
            "print('TASK [Wait for AMF pod to become Ready] ****************************************')\n"
            "print('ok: [sopnode-f2]')\n"
            "sys.stdout.flush()\n"
        )
        reported: list[str] = []
        with tempfile.TemporaryDirectory() as temporary:
            cwd = Path(temporary)
            result = run_streaming_ansible_command(
                [sys.executable, "-c", script],
                cwd=cwd,
                environment=None,
                timeout_seconds=10,
                report=reported.append,
            )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(reported, ["  TASK: Wait for AMF pod to become Ready"])
        self.assertFalse(any("[FAIL]" in message for message in reported))
        self.assertIn("fatal: [sopnode-f3]: FAILED!", result.stdout)
        self.assertIn("...ignoring", result.stdout)

    def test_streaming_process_nonzero_exit_shows_failure(self) -> None:
        script = (
            "import sys\n"
            "print('TASK [Wait for AMF pod to become Ready] *****************************************')\n"
            "print('fatal: [sopnode-f2]: FAILED! => {\"msg\": \"error\"}')\n"
            "sys.stdout.flush()\n"
            "sys.exit(2)\n"
        )
        reported: list[str] = []
        with tempfile.TemporaryDirectory() as temporary:
            cwd = Path(temporary)
            result = run_streaming_ansible_command(
                [sys.executable, "-c", script],
                cwd=cwd,
                environment=None,
                timeout_seconds=10,
                report=reported.append,
            )

        self.assertEqual(result.returncode, 2)
        self.assertEqual(len(reported), 2)
        self.assertEqual(reported[0], "  TASK: Wait for AMF pod to become Ready")
        self.assertIn("[FAIL] Wait for AMF pod to become Ready", reported[1])
        self.assertIn("host: sopnode-f2", reported[1])
        self.assertIn("state: FATAL", reported[1])
        self.assertIn('fatal: [sopnode-f2]: FAILED!', result.stdout)

    def test_hidden_unmapped_task_failure_still_reported_with_context(self) -> None:
        script = (
            "import sys\n"
            "print('TASK [command argv=[\"ip\", \"link\"]] ********************************************')\n"
            "print('fatal: [sopnode-f2]: FAILED! => {\"msg\": \"interface not found\"}')\n"
            "sys.stdout.flush()\n"
            "sys.exit(2)\n"
        )
        reported: list[str] = []
        with tempfile.TemporaryDirectory() as temporary:
            cwd = Path(temporary)
            result = run_streaming_ansible_command(
                [sys.executable, "-c", script],
                cwd=cwd,
                environment=None,
                timeout_seconds=10,
                report=reported.append,
            )

        self.assertEqual(result.returncode, 2)
        self.assertEqual(len(reported), 1)
        self.assertIn("[FAIL] command", reported[0])
        self.assertIn("host: sopnode-f2", reported[0])
        self.assertIn("state: FATAL", reported[0])
        self.assertIn("TASK [command argv=", result.stdout)

    def test_contextual_heartbeat_emission_with_task_name_and_duration(self) -> None:
        script = (
            "import sys, time\n"
            "print('TASK [Replace every reviewed mutable Open5GS image reference] *******************')\n"
            "sys.stdout.flush()\n"
            "time.sleep(0.35)\n"
            "print('ok: [sopnode-f2]')\n"
            "sys.stdout.flush()\n"
        )
        reported: list[str] = []
        with tempfile.TemporaryDirectory() as temporary:
            cwd = Path(temporary)
            result = run_streaming_ansible_command(
                [sys.executable, "-c", script],
                cwd=cwd,
                environment=None,
                timeout_seconds=10,
                report=reported.append,
                heartbeat_interval_seconds=0.1,
                poll_interval_seconds=0.05,
            )

        self.assertEqual(result.returncode, 0)
        self.assertIn("  TASK: Pinning locked Open5GS images", reported)
        heartbeats = [r for r in reported if "Pinning locked Open5GS images ·" in r]
        self.assertTrue(len(heartbeats) >= 2, f"Expected multiple contextual heartbeats, got: {reported}")

    def test_streaming_process_timeout_and_cleanup(self) -> None:
        script = "import time\ntime.sleep(10)\n"
        with tempfile.TemporaryDirectory() as temporary:
            cwd = Path(temporary)
            with self.assertRaises(subprocess.TimeoutExpired):
                run_streaming_ansible_command(
                    [sys.executable, "-c", script],
                    cwd=cwd,
                    environment=None,
                    timeout_seconds=1,
                    poll_interval_seconds=0.1,
                )


if __name__ == "__main__":
    unittest.main()
