from __future__ import annotations

from pathlib import Path
import unittest

from synthran.dependencies import load_lock
from synthran.experiment import ExperimentScenario
from synthran.experiment.resources import (
    DEFAULT_CONTAINER_ANNOTATION,
    EDGE_CONTAINER,
    EDGE_RUNTIME_VOLUME,
    EDGE_VOLUME,
    RUN_LABEL,
    render_edge_cleanup_patch,
    render_edge_patch,
    render_experiment_objects,
)


class ExperimentResourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scenario = ExperimentScenario(
            "experiment-01",
            "network-accepted-01",
            "12.1.0.1",
        )
        self.lock = load_lock(Path("dependencies.lock.yml"))

    def test_edge_sidecar_is_digest_locked_run_owned_and_refreshable(self) -> None:
        patch = render_edge_patch(
            self.scenario,
            lock=self.lock,
            core_address="192.0.2.10",
        )
        template = patch["spec"]["template"]
        self.assertEqual(
            template["metadata"]["labels"]["synthran.run/id"],
            "network-accepted-01",
        )
        self.assertEqual(template["metadata"]["annotations"][RUN_LABEL], "experiment-01")
        self.assertEqual(
            template["metadata"]["annotations"][DEFAULT_CONTAINER_ANNOTATION],
            "ue",
        )
        container = template["spec"]["containers"][0]
        self.assertEqual(container["name"], EDGE_CONTAINER)
        self.assertIn("@sha256:", container["image"])
        self.assertEqual(container["command"], ["/bin/sh", "-c"])
        self.assertIn("/synthran-source/mosquitto.conf", container["args"][0])
        self.assertIn("/synthran/mosquitto.conf", container["args"][0])

        volumes = {item["name"]: item for item in template["spec"]["volumes"]}
        self.assertIn(EDGE_VOLUME, volumes)
        self.assertIn(EDGE_RUNTIME_VOLUME, volumes)
        self.assertEqual(volumes[EDGE_RUNTIME_VOLUME]["emptyDir"], {})

        mounts = {item["name"]: item for item in container["volumeMounts"]}
        self.assertEqual(mounts[EDGE_VOLUME]["mountPath"], "/synthran-source")
        self.assertTrue(mounts[EDGE_VOLUME]["readOnly"])
        self.assertEqual(mounts[EDGE_RUNTIME_VOLUME]["mountPath"], "/synthran")

    def test_cleanup_deletes_only_injected_sidecar_volumes_and_annotations(self) -> None:
        patch = render_edge_cleanup_patch()
        template = patch["spec"]["template"]
        self.assertEqual(
            template["metadata"],
            {
                "annotations": {
                    RUN_LABEL: None,
                    DEFAULT_CONTAINER_ANNOTATION: None,
                }
            },
        )
        self.assertNotIn("labels", template["metadata"])
        spec = template["spec"]
        self.assertEqual(spec["containers"][0]["$patch"], "delete")
        deleted_volumes = {
            item["name"] for item in spec["volumes"] if item["$patch"] == "delete"
        }
        self.assertEqual(deleted_volumes, {EDGE_VOLUME, EDGE_RUNTIME_VOLUME})

    def test_central_resources_use_experiment_labels(self) -> None:
        objects = render_experiment_objects(
            self.scenario,
            lock=self.lock,
            core_node="lab-core",
            core_address="192.0.2.10",
        )
        self.assertEqual(len(objects), 3)
        for value in objects:
            self.assertEqual(
                value["metadata"]["labels"][RUN_LABEL],
                "experiment-01",
            )
        self.assertEqual(RUN_LABEL, "synthran.experiment/run")


if __name__ == "__main__":
    unittest.main()
