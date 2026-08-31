from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from synthran.adapters.fiveg import (
    CommandResult,
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

    def __call__(self, command, cwd, environment, timeout_seconds):
        del cwd, environment, timeout_seconds
        command = tuple(command)
        self.commands.append(command)
        verb = command[1]
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
        self.assertIn("--resume", self.runner.commands[2])
        self.assertIn("--no-setup", self.runner.commands[5])

    def test_rejects_wrong_machine_schema(self) -> None:
        self.runner.responses["status"] = {"schema": "unexpected"}
        with self.assertRaisesRegex(FiveGAdapterError, "unexpected schema"):
            self.adapter.status("deployment-001")

    def test_rejects_nonzero_upstream_exit(self) -> None:
        def failed(command, cwd, environment, timeout_seconds):
            del command, cwd, environment, timeout_seconds
            return CommandResult(7, "", "failed")

        adapter = FiveGAdapter(
            checkout=self.adapter.checkout,
            state_root=self.adapter.state_root,
            runner=failed,
        )
        with self.assertRaisesRegex(FiveGAdapterError, "failed"):
            adapter.down("deployment-001")


if __name__ == "__main__":
    unittest.main()
