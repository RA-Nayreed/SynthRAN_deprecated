"""Run-scoped Kubernetes resources for the IoT-to-5G experiment."""

from __future__ import annotations

import hashlib
import ipaddress
import json
from typing import Any, Mapping

from synthran.dependencies import DependencyLock
from synthran.experiment import (
    ExperimentError,
    ExperimentScenario,
    render_central_mosquitto_config,
    render_edge_mosquitto_config,
)


EDGE_CONTAINER = "synthran-edge-mqtt"
EDGE_VOLUME = "synthran-experiment-edge-config"
EDGE_RUNTIME_VOLUME = "synthran-experiment-edge-runtime"
CENTRAL_PORT = 18884
RUN_LABEL = "synthran.experiment/run"
ROLE_LABEL = "synthran.experiment/role"
DEFAULT_CONTAINER_ANNOTATION = "kubectl.kubernetes.io/default-container"
MOSQUITTO_BINARY = "/usr/sbin/mosquitto"


def _mosquitto_image(lock: DependencyLock) -> str:
    containers = lock.raw.get("containers")
    entry = containers.get("mosquitto") if isinstance(containers, dict) else None
    if not isinstance(entry, dict):
        raise ExperimentError("dependency lock does not define the Mosquitto image")
    image = entry.get("image")
    digest = entry.get("digest")
    if not isinstance(image, str) or not isinstance(digest, str):
        raise ExperimentError("locked Mosquitto image is malformed")
    if not digest.startswith("sha256:") or len(digest) != 71:
        raise ExperimentError("locked Mosquitto image is not digest-addressed")
    return f"{image}@{digest}"


def _suffix(run_id: str) -> str:
    return hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:12]


def names(scenario: ExperimentScenario) -> dict[str, str]:
    suffix = _suffix(scenario.run_id)
    return {
        "edge_config": f"synthran-exp-edge-{suffix}",
        "central_config": f"synthran-exp-central-{suffix}",
        "central_deployment": f"synthran-exp-central-{suffix}",
    }


def central_names(run_id: str) -> dict[str, str]:
    suffix = _suffix(run_id)
    return {
        "central_config": f"synthran-exp-central-{suffix}",
        "central_deployment": f"synthran-exp-central-{suffix}",
    }


def render_edge_patch(
    scenario: ExperimentScenario,
    *,
    lock: DependencyLock,
    core_address: str,
) -> Mapping[str, Any]:
    try:
        ipaddress.ip_address(core_address)
    except ValueError as exc:
        raise ExperimentError("core address must be a literal IP address") from exc
    resource_names = names(scenario)
    return {
        "spec": {
            "template": {
                "metadata": {
                    "labels": {"synthran.run/id": scenario.network_run_id},
                    "annotations": {
                        RUN_LABEL: scenario.run_id,
                        DEFAULT_CONTAINER_ANNOTATION: "ue",
                    },
                },
                "spec": {
                    "volumes": [
                        {
                            "name": EDGE_VOLUME,
                            "configMap": {"name": resource_names["edge_config"]},
                        },
                        {"name": EDGE_RUNTIME_VOLUME, "emptyDir": {}},
                    ],
                    "containers": [
                        {
                            "name": EDGE_CONTAINER,
                            "image": _mosquitto_image(lock),
                            "imagePullPolicy": "IfNotPresent",
                            "command": ["/bin/sh", "-c"],
                            "args": [
                                "set -eu; "
                                "if [ ! -f /synthran/mosquitto.conf ]; then "
                                "cp /synthran-source/mosquitto.conf /synthran/mosquitto.conf; "
                                "fi; "
                                f"exec {MOSQUITTO_BINARY} -c /synthran/mosquitto.conf"
                            ],
                            "ports": [{"name": "mqtt-edge", "containerPort": 1883}],
                            "volumeMounts": [
                                {
                                    "name": EDGE_VOLUME,
                                    "mountPath": "/synthran-source",
                                    "readOnly": True,
                                },
                                {
                                    "name": EDGE_RUNTIME_VOLUME,
                                    "mountPath": "/synthran",
                                },
                            ],
                            "readinessProbe": {
                                "tcpSocket": {"port": 1883},
                                "initialDelaySeconds": 2,
                                "periodSeconds": 2,
                            },
                        }
                    ],
                },
            }
        }
    }


def render_edge_cleanup_patch() -> Mapping[str, Any]:
    return {
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        RUN_LABEL: None,
                        DEFAULT_CONTAINER_ANNOTATION: None,
                    }
                },
                "spec": {
                    "volumes": [
                        {"name": EDGE_VOLUME, "$patch": "delete"},
                        {"name": EDGE_RUNTIME_VOLUME, "$patch": "delete"},
                    ],
                    "containers": [{"name": EDGE_CONTAINER, "$patch": "delete"}],
                },
            }
        }
    }


def render_central_objects(
    *,
    run_id: str,
    lock: DependencyLock,
    core_node: str,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    """Render the run-owned host-network central MQTT resources."""

    resource_names = central_names(run_id)
    labels = {
        "app.kubernetes.io/name": "synthran-experiment",
        "app.kubernetes.io/component": "mqtt",
        RUN_LABEL: run_id,
    }
    central_config = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": resource_names["central_config"],
            "namespace": "open5gs",
            "labels": labels,
        },
        "data": {
            "mosquitto.conf": render_central_mosquitto_config().replace(
                "listener 1883 0.0.0.0", f"listener {CENTRAL_PORT} 0.0.0.0"
            )
        },
    }
    central_deployment = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": resource_names["central_deployment"],
            "namespace": "open5gs",
            "labels": labels,
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": {
                    RUN_LABEL: run_id,
                    ROLE_LABEL: "central-mqtt",
                }
            },
            "template": {
                "metadata": {
                    "labels": {
                        RUN_LABEL: run_id,
                        ROLE_LABEL: "central-mqtt",
                    }
                },
                "spec": {
                    "hostNetwork": True,
                    "dnsPolicy": "ClusterFirstWithHostNet",
                    "nodeSelector": {"kubernetes.io/hostname": core_node},
                    "containers": [
                        {
                            "name": "central-mqtt",
                            "image": _mosquitto_image(lock),
                            "imagePullPolicy": "IfNotPresent",
                            "args": [MOSQUITTO_BINARY, "-c", "/synthran/mosquitto.conf"],
                            "ports": [
                                {
                                    "name": "mqtt-central",
                                    "containerPort": CENTRAL_PORT,
                                    "hostPort": CENTRAL_PORT,
                                }
                            ],
                            "volumeMounts": [
                                {
                                    "name": "config",
                                    "mountPath": "/synthran",
                                    "readOnly": True,
                                }
                            ],
                            "readinessProbe": {
                                "tcpSocket": {"port": CENTRAL_PORT},
                                "initialDelaySeconds": 2,
                                "periodSeconds": 2,
                            },
                        }
                    ],
                    "volumes": [
                        {
                            "name": "config",
                            "configMap": {"name": resource_names["central_config"]},
                        }
                    ],
                },
            },
        },
    }
    return central_config, central_deployment


def render_experiment_objects(
    scenario: ExperimentScenario,
    *,
    lock: DependencyLock,
    core_node: str,
    core_address: str,
) -> tuple[Mapping[str, Any], ...]:
    """Render the RFSIM edge ConfigMap plus shared central MQTT resources."""

    try:
        ipaddress.ip_address(core_address)
    except ValueError as exc:
        raise ExperimentError("core address must be a literal IP address") from exc
    resource_names = names(scenario)
    labels = {
        "app.kubernetes.io/name": "synthran-experiment",
        "app.kubernetes.io/component": "mqtt",
        RUN_LABEL: scenario.run_id,
    }
    edge_config = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": resource_names["edge_config"],
            "namespace": "open5gs",
            "labels": labels,
        },
        "data": {
            "mosquitto.conf": render_edge_mosquitto_config(
                scenario,
                central_broker_address=core_address,
                central_broker_port=CENTRAL_PORT,
            )
        },
    }
    return (
        edge_config,
        *render_central_objects(
            run_id=scenario.run_id,
            lock=lock,
            core_node=core_node,
        ),
    )


def json_document(value: Mapping[str, Any]) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)
