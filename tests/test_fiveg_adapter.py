from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from synthran.adapters.fiveg import (
    CommandResult,
    FIVEG_EVENT_SCHEMA,
    FIVEG_SPEC_SCHEMA,
    FiveGAdapter,
    FiveGAdapterError,
    load_spec,
    write_spec,
)


class RecordingRunner:
    def __init__(self, responses: dict[str, dict[str, object]]) -> None:
        self.responses = responses
        self.commands: list[tuple[str, ...]] = []

    def __call__(self, command, cwd, environment, timeout_seconds, event_sink):
        del cwd, environment, timeout_seconds
        command = tuple(command)
        self.commands.append(command)
        verb = command[1]
        if event_sink is not None and verb == "up":
            event_sink(
                {
                    "schema": FIVEG_EVENT_SCHEMA,
                    "deployment_id": "deployment-001",
                    "phase": "deployment",
                    "event": "started",
                    "component": "5g-stack",
                }
            )
        payload = self.responses[verb]
        return CommandResult(0, json.dumps(payload), "")


class SpecTests(unittest.TestCase):
    def test_write_and_load_are_topology_agnostic(self) -> None:
        spec = {
            "schema": FIVEG_SPEC_SCHEMA,
            "id": "test-001",
            "core": {"type": "free5gc", "node": "sopnode-f2"},
            "ran": {"type": "oai", "node": "sopnode-f3"},
            "platform": {"type": "r2lab", "ru": "n300"},
            "ues": {"qfits": ["qfit07"]},
            "monitoring": {"enabled": True},
        }
        with tempfile.TemporaryDirectory() as directory:
            path = write_spec(Path(directory) / "deployment.json", spec)
            self.assertEqual(spec, load_spec(path))

    def test_write_requires_upstream_schema_and_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "deployment.json"
            with self.assertRaisesRegex(FiveGAdapterError, "schema"):
                write_spec(path, {"schema": "wrong", "id": "x"})
            with self.assertRaisesRegex(FiveGAdapterError, "non-empty id"):
                write_spec(path, {"schema": FIVEG_SPEC_SCHEMA, "id": ""})


class AdapterInvocationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        checkout = root / "fiveg"
        (checkout / "bin").mkdir(parents=True)
        (checkout / "bin" / "fiveg").write_text("#!/bin/sh\n", encoding="utf-8")
        self.spec = root / "deployment.json"
        write_spec(
            self.spec,
            {
                "schema": FIVEG_SPEC_SCHEMA,
                "id": "deployment-001",
                "core": {"type": "oai", "node": "sopnode-f2"},
                "ran": {"type": "oai", "node": "sopnode-f3"},
                "platform": {"type": "rfsim"},
            },
        )
        self.runner = RecordingRunner(
            {
                "capabilities": {"schema": "fiveg/capabilities/v1", "cores": [], "rans": []},
                "plan": {"schema": "fiveg/deployment-plan/v1", "deployment_id": "deployment-001"},
                "up": {"schema": "fiveg/deployment-manifest/v1", "deployment_id": "deployment-001"},
                "status": {"schema": "fiveg/deployment-status/v1", "deployment_id": "deployment-001"},
                "down": {"schema": "fiveg/deployment-down/v1", "deployment_id": "deployment-001"},
                "scenario": {"schema": "fiveg/scenario-result/v1", "deployment_id": "deployment-001"},
            }
        )
        self.adapter = FiveGAdapter(
            checkout=checkout,
            state_root=root / "state",
            runner=self.runner,
        )

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_exposes_only_upstream_machine_verbs(self) -> None:
        self.adapter.capabilities()
        self.adapter.plan(self.spec)
        self.adapter.up(self.spec, resume=True)
        self.adapter.status("deployment-001")
        self.adapter.down("deployment-001")
        self.adapter.scenario("deployment-001", scenario_type="ping", no_setup=True)
        verbs = [command[1] for command in self.runner.commands]
        self.assertEqual(
            ["capabilities", "plan", "up", "status", "down", "scenario"],
            verbs,
        )
        for command in self.runner.commands:
            self.assertEqual("--json", command[-1])
            self.assertNotIn("--events", command)
        self.assertIn("--resume", self.runner.commands[2])
        self.assertIn("--no-setup", self.runner.commands[5])

    def test_event_sink_requests_and_relays_versioned_upstream_events(self) -> None:
        events = []
        adapter = FiveGAdapter(
            checkout=self.adapter.checkout,
            state_root=self.adapter.state_root,
            runner=self.runner,
            event_sink=events.append,
        )
        adapter.up(self.spec)
        self.assertIn("--json", self.runner.commands[-1])
        self.assertEqual("--events", self.runner.commands[-1][-1])
        self.assertEqual(1, len(events))
        self.assertEqual(FIVEG_EVENT_SCHEMA, events[0]["schema"])
        self.assertEqual("deployment", events[0]["phase"])
        self.assertEqual("started", events[0]["event"])

    def test_rejects_wrong_machine_schema(self) -> None:
        self.runner.responses["status"] = {"schema": "unexpected"}
        with self.assertRaisesRegex(FiveGAdapterError, "unexpected schema"):
            self.adapter.status("deployment-001")

    def test_nonzero_upstream_exit_surfaces_last_stderr_line(self) -> None:
        def failed(command, cwd, environment, timeout_seconds, event_sink):
            del command, cwd, environment, timeout_seconds, event_sink
            return CommandResult(
                7,
                "",
                "Traceback omitted\nFiveGError: command failed; see deploy.log\n",
            )

        adapter = FiveGAdapter(
            checkout=self.adapter.checkout,
            state_root=self.adapter.state_root,
            runner=failed,
        )
        with self.assertRaisesRegex(
            FiveGAdapterError,
            "5g-Ansible down failed: FiveGError: command failed; see deploy.log",
        ):
            adapter.down("deployment-001")

    def test_failure_detail_ignores_structured_progress_json(self) -> None:
        event = json.dumps(
            {
                "schema": FIVEG_EVENT_SCHEMA,
                "deployment_id": "deployment-001",
                "phase": "deployment",
                "event": "failed",
            }
        )

        def failed(command, cwd, environment, timeout_seconds, event_sink):
            del command, cwd, environment, timeout_seconds, event_sink
            return CommandResult(2, "", event + "\nfiveg: deployment command failed\n")

        adapter = FiveGAdapter(
            checkout=self.adapter.checkout,
            state_root=self.adapter.state_root,
            runner=failed,
        )
        with self.assertRaisesRegex(FiveGAdapterError, "deployment command failed"):
            adapter.up(self.spec)

    def test_nonzero_upstream_exit_uses_stdout_when_stderr_is_empty(self) -> None:
        def failed(command, cwd, environment, timeout_seconds, event_sink):
            del command, cwd, environment, timeout_seconds, event_sink
            return CommandResult(2, "provider network acquisition failed\n", "")

        adapter = FiveGAdapter(
            checkout=self.adapter.checkout,
            state_root=self.adapter.state_root,
            runner=failed,
        )
        with self.assertRaisesRegex(
            FiveGAdapterError,
            "provider network acquisition failed",
        ):
            adapter.up(self.spec)


if __name__ == "__main__":
    unittest.main()
