from __future__ import annotations

from pathlib import Path
import unittest

from synthran.dependencies import load_lock
from synthran.experiment.resources import (
    CENTRAL_PORT,
    RUN_LABEL,
    ROLE_LABEL,
    central_names,
    render_central_objects,
)


class ExperimentResourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lock = load_lock(Path("dependencies.lock.yml"))

    def test_central_names_are_run_scoped_and_deterministic(self) -> None:
        first = central_names("experiment-01")
        second = central_names("experiment-01")
        other = central_names("experiment-02")
        self.assertEqual(first, second)
        self.assertNotEqual(first, other)
        self.assertTrue(first["central_config"].startswith("synthran-exp-central-"))
        self.assertEqual(first["central_config"], first["central_deployment"])

    def test_central_resources_are_exact_run_owned_and_digest_locked(self) -> None:
        config, deployment = render_central_objects(
            run_id="experiment-01",
            lock=self.lock,
            core_node="lab-core",
        )
        for value in (config, deployment):
            self.assertEqual(value["metadata"]["namespace"], "open5gs")
            self.assertEqual(value["metadata"]["labels"][RUN_LABEL], "experiment-01")

        pod = deployment["spec"]["template"]
        self.assertEqual(pod["metadata"]["labels"][RUN_LABEL], "experiment-01")
        self.assertEqual(pod["metadata"]["labels"][ROLE_LABEL], "central-mqtt")
        self.assertEqual(pod["spec"]["nodeSelector"]["kubernetes.io/hostname"], "lab-core")
        self.assertTrue(pod["spec"]["hostNetwork"])
        container = pod["spec"]["containers"][0]
        self.assertIn("@sha256:", container["image"])
        self.assertEqual(container["ports"][0]["hostPort"], CENTRAL_PORT)
        self.assertIn(f"listener {CENTRAL_PORT} 0.0.0.0", config["data"]["mosquitto.conf"])

    def test_resources_never_describe_or_patch_the_upstream_ue(self) -> None:
        config, deployment = render_central_objects(
            run_id="experiment-01",
            lock=self.lock,
            core_node="lab-core",
        )
        rendered = repr((config, deployment)).lower()
        self.assertNotIn("srsran-ue", rendered)
        self.assertNotIn("sidecar", rendered)
        self.assertNotIn("strategic", rendered)
        self.assertNotIn("synthran-edge", rendered)


if __name__ == "__main__":
    unittest.main()
