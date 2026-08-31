"""Run-owned Kubernetes resources for the physical Amber workload."""

from __future__ import annotations

import hashlib
import shlex
from typing import Any, Mapping

from synthran.dependencies import DependencyLock
from synthran.experiment import ExperimentError, validate_run_id
from synthran.experiment.live import _remote, _wait_remote_tcp
from synthran.experiment.resources import (
    CENTRAL_PORT,
    MOSQUITTO_BINARY,
    ROLE_LABEL,
    RUN_LABEL,
    _mosquitto_image,
)
from synthran.fiveg_ansible import NetworkInventory


KUBERNETES_NAMESPACE = "open5gs"


class R2LabIoTResourceError(ExperimentError):
    """Raised when physical Amber resources cannot be proven ready."""


def physical_central_name(run_id: str) -> str:
    validate_run_id(run_id)
    suffix = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:12]
    return f"synthran-r2lab-central-{suffix}"


def render_physical_central_objects(
    run_id: str,
    *,
    lock: DependencyLock,
    core_node: str,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    """Render only the run-owned host-network central broker resources."""

    name = physical_central_name(run_id)
    labels = {
        "app.kubernetes.io/name": "synthran-experiment",
        "app.kubernetes.io/component": "mqtt",
        RUN_LABEL: run_id,
    }
    config = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": KUBERNETES_NAMESPACE,
            "labels": labels,
        },
        "data": {
            "mosquitto.conf": "\n".join(
                (
                    "per_listener_settings true",
                    f"listener {CENTRAL_PORT} 0.0.0.0",
                    "allow_anonymous true",
                    "persistence false",
                    "log_type all",
                    "",
                )
            )
        },
    }
    listener_probe = (
        f"awk '$2 ~ /:{CENTRAL_PORT:04X}$/ && $4 == \"0A\" "
        "{ found=1 } END { exit !found }' /proc/net/tcp"
    )
    deployment = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": KUBERNETES_NAMESPACE,
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
                                "exec": {
                                    "command": ["/bin/sh", "-ec", listener_probe],
                                },
                                "initialDelaySeconds": 2,
                                "periodSeconds": 2,
                                "timeoutSeconds": 2,
                            },
                        }
                    ],
                    "volumes": [
                        {
                            "name": "config",
                            "configMap": {"name": name},
                        }
                    ],
                },
            },
        },
    }
    return config, deployment


def _central_rollout(inventory: NetworkInventory, deployment: str) -> None:
    try:
        _remote(
            inventory,
            "sh",
            "-c",
            "KUBECONFIG=/etc/kubernetes/admin.conf kubectl rollout status deployment/"
            f"{shlex.quote(deployment)} -n {KUBERNETES_NAMESPACE} --timeout=180s",
            label="physical central MQTT rollout",
            timeout_seconds=200,
        )
    except Exception as exc:
        diagnostics: list[str] = []
        for label, command in (
            (
                "deployment",
                "KUBECONFIG=/etc/kubernetes/admin.conf kubectl get deployment/"
                f"{shlex.quote(deployment)} -n {KUBERNETES_NAMESPACE} "
                "-o jsonpath='{.status.readyReplicas}/{.status.replicas} ready; "
                "{.status.unavailableReplicas} unavailable'",
            ),
            (
                "broker-log",
                "KUBECONFIG=/etc/kubernetes/admin.conf kubectl logs -n "
                f"{KUBERNETES_NAMESPACE} deployment/{shlex.quote(deployment)} "
                "-c central-mqtt --tail=40",
            ),
        ):
            try:
                output = _remote(
                    inventory,
                    "sh",
                    "-c",
                    command,
                    label=f"physical central MQTT {label} diagnostics",
                    timeout_seconds=20,
                ).strip()
            except Exception:
                continue
            if output:
                diagnostics.append(f"{label}: {' '.join(output.split())[:800]}")
        detail = "; ".join(diagnostics) or "no broker diagnostics were available"
        raise R2LabIoTResourceError(
            f"physical central MQTT rollout failed: {detail}"
        ) from exc

    _wait_remote_tcp(
        inventory,
        host="127.0.0.1",
        port=CENTRAL_PORT,
        timeout_seconds=15,
    )
