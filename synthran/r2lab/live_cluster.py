from __future__ import annotations

import ipaddress
import json
from pathlib import Path
import re
import shlex
from typing import Callable, Sequence

from synthran.live_preflight import CommandResult, subprocess_runner
from synthran.network.resources import SUPPORTED_NODES
from synthran.r2lab.acceptance import PhysicalAcceptanceStage, PhysicalRunEvidence
from synthran.r2lab.hardware import PhysicalTopology
from synthran.r2lab.n2 import N2State, build_amf_n2_evidence, parse_n2_log_state
from synthran.r2lab.radio import execute_user_plane_probe
from synthran.r2lab.resources import load_topology, ue_gateway_command
from synthran.r2lab.ue import PhysicalUeUserPlaneSummary, R2LabPhysicalUeError, UE_INTERFACE


Runner = Callable[[Sequence[str], int], CommandResult]
NAMESPACE = "open5gs"
RELEASE = "srsran-gnb"
GNB_SELECTOR = "app=srsran,component=gnb"
RUN_LABEL = "synthran.run/id"
RUN_ANNOTATION = "synthran.io/run-id"
REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS = ("n3network",)
_SAFE_POD = re.compile(r"^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$")


class R2LabLiveClusterError(RuntimeError):
    pass


def cluster_command(topology: PhysicalTopology, *remote: str) -> tuple[str, ...]:
    """Use the same normal OpenSSH environment that upstream 5g-Ansible relies on."""
    return ("ssh", f"root@{topology.core_node}", shlex.join(remote))


def _checked(
    runner: Runner,
    command: Sequence[str],
    timeout_seconds: int,
    label: str,
) -> CommandResult:
    try:
        result = runner(command, timeout_seconds)
    except Exception as exc:
        raise R2LabLiveClusterError(f"{label} could not complete") from exc
    if result.returncode != 0:
        raise R2LabLiveClusterError(f"{label} returned nonzero")
    return result


def _json(text: str, label: str) -> dict[str, object]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise R2LabLiveClusterError(f"{label} did not return JSON") from exc
    if not isinstance(payload, dict):
        raise R2LabLiveClusterError(f"{label} returned malformed JSON")
    return payload


def ready_nodes(
    *,
    topology: PhysicalTopology,
    runner: Runner = subprocess_runner,
    timeout_seconds: int = 60,
) -> int:
    payload = _json(
        _checked(
            runner,
            cluster_command(topology, "kubectl", "get", "nodes", "-o", "json"),
            timeout_seconds,
            "Kubernetes node readiness query",
        ).stdout,
        "Kubernetes node readiness query",
    )
    items = payload.get("items")
    if not isinstance(items, list):
        raise R2LabLiveClusterError("Kubernetes node evidence is malformed")
    ready: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        metadata = item.get("metadata")
        status = item.get("status")
        name = metadata.get("name") if isinstance(metadata, dict) else None
        conditions = status.get("conditions") if isinstance(status, dict) else None
        if isinstance(name, str) and isinstance(conditions, list) and any(
            isinstance(condition, dict)
            and condition.get("type") == "Ready"
            and condition.get("status") == "True"
            for condition in conditions
        ):
            ready.add(name.split(".", 1)[0])
    expected = set(topology.nodes)
    if not expected.issubset(ready):
        missing = ", ".join(sorted(expected - ready))
        raise R2LabLiveClusterError(f"selected Kubernetes nodes are not Ready: {missing}")
    return len(expected)


def namespace_owner(
    *,
    topology: PhysicalTopology,
    runner: Runner = subprocess_runner,
    timeout_seconds: int = 60,
) -> str | None:
    result = _checked(
        runner,
        cluster_command(
            topology,
            "kubectl",
            "get",
            "namespace",
            NAMESPACE,
            "--ignore-not-found",
            "-o",
            "json",
        ),
        timeout_seconds,
        "Open5GS namespace query",
    )
    if not result.stdout.strip():
        return None
    payload = _json(result.stdout, "Open5GS namespace query")
    metadata = payload.get("metadata")
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    owner = labels.get(RUN_LABEL) if isinstance(labels, dict) else None
    if owner is not None and not isinstance(owner, str):
        raise R2LabLiveClusterError("Open5GS namespace ownership is malformed")
    return owner


def bind_namespace_owner(
    *,
    run_id: str,
    topology: PhysicalTopology,
    runner: Runner = subprocess_runner,
    timeout_seconds: int = 60,
) -> bool:
    owner = namespace_owner(topology=topology, runner=runner, timeout_seconds=timeout_seconds)
    if owner not in {None, run_id}:
        raise R2LabLiveClusterError("current Open5GS namespace belongs to another run")
    if owner == run_id:
        return False
    existence = _checked(
        runner,
        cluster_command(
            topology,
            "kubectl",
            "get",
            "namespace",
            NAMESPACE,
            "--ignore-not-found",
            "-o",
            "name",
        ),
        timeout_seconds,
        "Open5GS namespace existence query",
    ).stdout.strip()
    if not existence:
        return False
    _checked(
        runner,
        cluster_command(
            topology,
            "kubectl",
            "label",
            "namespace",
            NAMESPACE,
            f"{RUN_LABEL}={run_id}",
            "--overwrite",
        ),
        timeout_seconds,
        "Open5GS namespace ownership bind",
    )
    return True


def open5gs_ready(
    *,
    topology: PhysicalTopology,
    runner: Runner = subprocess_runner,
    timeout_seconds: int = 60,
) -> tuple[bool, int]:
    ready = 0
    for selectors in (
        ("app=open5gs", "nf=amf"),
        ("app=open5gs", "nf=smf", "name=smf1"),
        ("app=open5gs", "nf=upf", "name=upf1"),
    ):
        payload = _json(
            _checked(
                runner,
                cluster_command(
                    topology,
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    NAMESPACE,
                    "-l",
                    ",".join(selectors),
                    "-o",
                    "json",
                ),
                timeout_seconds,
                "Open5GS readiness query",
            ).stdout,
            "Open5GS readiness query",
        )
        items = payload.get("items")
        if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict):
            continue
        pod = items[0]
        metadata = pod.get("metadata")
        status = pod.get("status")
        containers = status.get("containerStatuses") if isinstance(status, dict) else None
        if (
            isinstance(metadata, dict)
            and metadata.get("deletionTimestamp") is None
            and isinstance(containers, list)
            and containers
            and all(isinstance(container, dict) and container.get("ready") is True for container in containers)
        ):
            ready += 1
    return ready == 3, ready


def physical_networks_ready(
    *,
    topology: PhysicalTopology,
    runner: Runner = subprocess_runner,
    timeout_seconds: int = 60,
) -> tuple[bool, tuple[str, ...]]:
    payload = _json(
        _checked(
            runner,
            cluster_command(
                topology,
                "kubectl",
                "get",
                "network-attachment-definitions.k8s.cni.cncf.io",
                "-n",
                NAMESPACE,
                "-o",
                "json",
            ),
            timeout_seconds,
            "physical Multus network query",
        ).stdout,
        "physical Multus network query",
    )
    items = payload.get("items")
    if not isinstance(items, list):
        raise R2LabLiveClusterError("physical Multus network query is malformed")
    names = tuple(
        sorted(
            metadata.get("name")
            for item in items
            if isinstance(item, dict)
            and isinstance((metadata := item.get("metadata")), dict)
            and isinstance(metadata.get("name"), str)
        )
    )
    return all(name in names for name in REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS), names


def _load_expected_peer(run_root: Path, run_id: str) -> str:
    path = run_root.expanduser().resolve() / run_id / "physical" / "n3xx-artifact.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2LabLiveClusterError("N3xx artifact metadata is unavailable") from exc
    peer = payload.get("expected_gnb_peer") if isinstance(payload, dict) else None
    try:
        address = ipaddress.ip_address(peer) if isinstance(peer, str) else None
    except ValueError as exc:
        raise R2LabLiveClusterError("stored expected gNB N2 peer is malformed") from exc
    if not isinstance(address, ipaddress.IPv4Address):
        raise R2LabLiveClusterError("stored expected gNB N2 peer is missing")
    return str(address)


def verify_n2(
    *,
    run_id: str,
    run_root: Path,
    runner: Runner = subprocess_runner,
    timeout_seconds: int = 60,
) -> bool:
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    owner = namespace_owner(topology=topology, runner=runner, timeout_seconds=timeout_seconds)
    if owner != run_id:
        return False
    deployment = _json(
        _checked(
            runner,
            cluster_command(
                topology,
                "kubectl",
                "get",
                f"deployment/{RELEASE}",
                "-n",
                NAMESPACE,
                "-o",
                "json",
            ),
            timeout_seconds,
            "current physical gNB query",
        ).stdout,
        "current physical gNB query",
    )
    metadata = deployment.get("metadata")
    spec = deployment.get("spec")
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
    if (
        not isinstance(labels, dict)
        or not isinstance(annotations, dict)
        or not isinstance(spec, dict)
        or labels.get(RUN_LABEL) != run_id
        or annotations.get(RUN_ANNOTATION) != run_id
        or spec.get("replicas") != 1
    ):
        return False
    pods = _json(
        _checked(
            runner,
            cluster_command(
                topology,
                "kubectl",
                "get",
                "pods",
                "-n",
                NAMESPACE,
                "-l",
                GNB_SELECTOR,
                "-o",
                "json",
            ),
            timeout_seconds,
            "current physical gNB pod query",
        ).stdout,
        "current physical gNB pod query",
    )
    items = pods.get("items")
    if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict):
        return False
    pod = items[0]
    pod_metadata = pod.get("metadata")
    pod_status = pod.get("status")
    name = pod_metadata.get("name") if isinstance(pod_metadata, dict) else None
    statuses = pod_status.get("containerStatuses") if isinstance(pod_status, dict) else None
    gnb = next(
        (item for item in statuses if isinstance(item, dict) and item.get("name") == "gnb"),
        None,
    ) if isinstance(statuses, list) else None
    if (
        not isinstance(name, str)
        or not _SAFE_POD.fullmatch(name)
        or not isinstance(pod_status, dict)
        or pod_status.get("phase") != "Running"
        or not isinstance(gnb, dict)
        or gnb.get("ready") is not True
    ):
        return False
    logs = _checked(
        runner,
        cluster_command(
            topology,
            "kubectl",
            "logs",
            f"pod/{name}",
            "-n",
            NAMESPACE,
            "-c",
            "gnb",
            "--tail=400",
        ),
        timeout_seconds,
        "current physical gNB log query",
    )
    if parse_n2_log_state(logs.stdout) is N2State.ESTABLISHED:
        return True
    expected_peer = _load_expected_peer(run_root, run_id)
    amf_payload = _json(
        _checked(
            runner,
            cluster_command(
                topology,
                "kubectl",
                "get",
                "pods",
                "-n",
                NAMESPACE,
                "-l",
                "nf=amf",
                "-o",
                "json",
            ),
            timeout_seconds,
            "current AMF pod query",
        ).stdout,
        "current AMF pod query",
    )
    amf_items = amf_payload.get("items")
    if not isinstance(amf_items, list) or len(amf_items) != 1 or not isinstance(amf_items[0], dict):
        return False
    amf_metadata = amf_items[0].get("metadata")
    amf_name = amf_metadata.get("name") if isinstance(amf_metadata, dict) else None
    if not isinstance(amf_name, str) or not _SAFE_POD.fullmatch(amf_name):
        return False
    amf_logs = _checked(
        runner,
        cluster_command(
            topology,
            "kubectl",
            "logs",
            f"pod/{amf_name}",
            "-n",
            NAMESPACE,
            "--tail=400",
        ),
        timeout_seconds,
        "current AMF N2 log query",
    )
    return build_amf_n2_evidence(text=amf_logs.stdout, expected_peer=expected_peer).proven


def prove_user_plane(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    peer: str,
    run_root: Path,
    r2lab_runner: Runner = subprocess_runner,
    cluster_runner: Runner = subprocess_runner,
    timeout_seconds: int = 60,
) -> PhysicalUeUserPlaneSummary:
    if evidence.acceptance.next_stage is not PhysicalAcceptanceStage.USER_PLANE:
        raise R2LabPhysicalUeError("physical user-plane probe is not the next lifecycle boundary")
    topology = load_topology(run_root=run_root, run_id=evidence.run_id).validate()
    if not verify_n2(
        run_id=evidence.run_id,
        run_root=run_root,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 60),
    ):
        raise R2LabPhysicalUeError("current singleton gNB/N2 path is not proven")

    def ue_runner(command: Sequence[str], command_timeout: int) -> CommandResult:
        return r2lab_runner(
            ue_gateway_command(slice_name, topology.ue_profile, *tuple(command)),
            command_timeout,
        )

    probe = execute_user_plane_probe(
        peer=peer,
        runner=ue_runner,
        interface=UE_INTERFACE,
        command_timeout_seconds=min(timeout_seconds, 60),
    )
    state = (
        evidence.pass_stage(
            PhysicalAcceptanceStage.USER_PLANE,
            source=f"current-{topology.ue}:wwan0:{probe.received_packets}-of-{probe.requested_packets}",
        )
        if probe.proven
        else evidence.fail_stage(
            PhysicalAcceptanceStage.USER_PLANE,
            source=f"current-{topology.ue}:wwan0:probe-failed",
        )
    )
    return PhysicalUeUserPlaneSummary(evidence=state, probe=probe)
