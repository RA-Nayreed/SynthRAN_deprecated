"""Run-scoped Kubernetes resources owned by an experiment.

Only the central MQTT broker is a Kubernetes resource. 5g-Ansible-owned UE and
RAN Deployments are never patched by experiment code.
"""

from __future__ import annotations

import hashlib
from typing import Any, Mapping

from synthran.dependencies import DependencyLock
from synthran.experiment import ExperimentError, render_central_mosquitto_config


CENTRAL_PORT = 18884
RUN_LABEL = "synthran.experiment/run"
ROLE_LABEL = "synthran.experiment/role"
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


def central_names(run_id: str) -> dict[str, str]:
    suffix = _suffix(run_id)
    return {
        "central_config": f"synthran-exp-central-{suffix}",
        "central_deployment": f"synthran-exp-central-{suffix}",
    }


def render_central_objects(
    *,
    run_id: str,
    lock: DependencyLock,
    core_node: str,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    """Render the exact run-owned central MQTT ConfigMap and Deployment."""

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
