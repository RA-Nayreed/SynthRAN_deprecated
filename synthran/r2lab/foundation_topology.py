"""Topology-driven SLICES/Kubernetes/Open5GS physical foundation.

The selected compute pair, radio, and subscriber are read from the run topology;
no stage silently falls back to the original sopnode-f2/sopnode-f3/N300/qfit07
validation topology.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
from time import monotonic
from typing import Callable, Mapping, Sequence, TextIO

from synthran.ansible_streaming import parse_ansible_line, run_streaming_ansible_command
from synthran.dependencies import load_lock
from synthran.fiveg_ansible import validate_fiveg_checkout
from synthran.live_preflight import CommandResult, Runner, subprocess_runner
from synthran.network.resources import build_preparation_inventory, locked_preparation_variables
from synthran.network.runtime import (
    RunCommand,
    atomic_json,
    golden_path_image_variables,
    run_command,
    sanitize_deployment_text,
    tree_sha256,
    validate_run_id,
)
from synthran.r2lab.acceptance import PhysicalAcceptanceStage, PhysicalRunEvidence, R2LabAcceptanceError
from synthran.r2lab.hardware import PhysicalTopology
from synthran.r2lab.resources import (
    R2LabTopologyResourceError,
    claim_selected_allocation,
    load_topology,
    verify_physical_authority,
)
from synthran.upstream_overlay import UpstreamOverlayError, apply_network_overlay


NAMESPACE = "open5gs"
RELEASE = "srsran-gnb"
GNB_SELECTOR = "app=srsran,component=gnb"
RUN_LABEL = "synthran.run/id"
REQUIRED_OPEN5GS_NFS = ("amf", "smf", "upf")
DEFAULT_TIMEOUT_SECONDS = 1800
FOUNDATION_SCHEMA = "synthran/r2lab-foundation/v1alpha2"


class R2LabTopologyFoundationError(RuntimeError):
    """Raised when the selected physical foundation cannot be proven."""


@dataclass(frozen=True)
class TopologyFoundationResult:
    run_id: str
    topology: PhysicalTopology
    allocation_id: str
    namespace_changed: bool
    open5gs_reconciled: bool
    ready_node_count: int
    ready_open5gs_pod_count: int
    evidence_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": FOUNDATION_SCHEMA,
            "run_id": self.run_id,
            "topology": {
                "core_node": self.topology.core_node,
                "ran_node": self.topology.ran_node,
                "radio": self.topology.radio,
                "ue": self.topology.ue,
            },
            "allocation_id": self.allocation_id,
            "namespace_changed": self.namespace_changed,
            "open5gs_reconciled": self.open5gs_reconciled,
            "ready_node_count": self.ready_node_count,
            "ready_open5gs_pod_count": self.ready_open5gs_pod_count,
            "next_stage": PhysicalAcceptanceStage.GNB_N2.value,
            "status": "foundation-ready",
        }


def _ssh(core_node: str, known_hosts: Path, *remote: str) -> tuple[str, ...]:
    return (
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        f"root@{core_node}",
        shlex.join(remote),
    )


def _checked(
    runner: Runner,
    command: Sequence[str],
    *,
    timeout_seconds: int,
    label: str,
    allow_not_found: bool = False,
) -> str:
    try:
        result = runner(command, timeout_seconds)
    except Exception as exc:
        raise R2LabTopologyFoundationError(f"{label} could not be observed") from exc
    if result.returncode != 0:
        if allow_not_found and result.returncode == 1:
            return ""
        raise R2LabTopologyFoundationError(f"{label} returned nonzero")
    return result.stdout


def _json_object(text: str, label: str) -> dict[str, object]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise R2LabTopologyFoundationError(f"{label} did not return JSON") from exc
    if not isinstance(payload, dict):
        raise R2LabTopologyFoundationError(f"{label} returned malformed JSON")
    return payload


def _namespace_owner(
    *, topology: PhysicalTopology, known_hosts: Path, runner: Runner, timeout_seconds: int
) -> str | None:
    text = _checked(
        runner,
        _ssh(
            topology.core_node,
            known_hosts,
            "kubectl",
            "get",
            "namespace",
            NAMESPACE,
            "--ignore-not-found",
            "-o",
            "json",
        ),
        timeout_seconds=timeout_seconds,
        label="Open5GS namespace query",
    ).strip()
    if not text:
        return None
    payload = _json_object(text, "Open5GS namespace query")
    metadata = payload.get("metadata")
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    owner = labels.get(RUN_LABEL) if isinstance(labels, dict) else None
    if owner is not None and not isinstance(owner, str):
        raise R2LabTopologyFoundationError("Open5GS namespace ownership is malformed")
    return owner


def _gnb_state(
    *, topology: PhysicalTopology, known_hosts: Path, runner: Runner, timeout_seconds: int
) -> tuple[bool, int | None, str | None, int]:
    deployment_text = _checked(
        runner,
        _ssh(
            topology.core_node,
            known_hosts,
            "kubectl",
            "get",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            "--ignore-not-found",
            "-o",
            "json",
        ),
        timeout_seconds=timeout_seconds,
        label="existing physical gNB query",
    ).strip()
    deployment_present = bool(deployment_text)
    desired: int | None = None
    owner: str | None = None
    if deployment_present:
        payload = _json_object(deployment_text, "existing physical gNB query")
        metadata = payload.get("metadata")
        spec = payload.get("spec")
        labels = metadata.get("labels") if isinstance(metadata, dict) else None
        desired = spec.get("replicas") if isinstance(spec, dict) else None
        owner = labels.get(RUN_LABEL) if isinstance(labels, dict) else None
        if not isinstance(desired, int) or isinstance(desired, bool):
            raise R2LabTopologyFoundationError("existing physical gNB replica state is malformed")
        if owner is not None and not isinstance(owner, str):
            raise R2LabTopologyFoundationError("existing physical gNB ownership is malformed")
    pods = _json_object(
        _checked(
            runner,
            _ssh(
                topology.core_node,
                known_hosts,
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
            timeout_seconds=timeout_seconds,
            label="existing physical gNB pod query",
        ),
        "existing physical gNB pod query",
    )
    items = pods.get("items")
    if not isinstance(items, list):
        raise R2LabTopologyFoundationError("existing physical gNB pod query is malformed")
    return deployment_present, desired, owner, len(items)


def _handoff_namespace(
    *,
    run_id: str,
    previous_run_id: str | None,
    slice_name: str,
    topology: PhysicalTopology,
    run_root: Path,
    known_hosts: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    timeout_seconds: int,
) -> bool:
    """Create or transfer the run-owned namespace only with a proven stopped gNB."""

    authority = lambda: verify_physical_authority(
        run_id=run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    authority()
    owner = _namespace_owner(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 60),
    )
    if owner is None:
        # Missing and unlabeled are distinguished by a direct existence query.
        namespace = _checked(
            cluster_runner,
            _ssh(
                topology.core_node,
                known_hosts,
                "kubectl",
                "get",
                "namespace",
                NAMESPACE,
                "--ignore-not-found",
                "-o",
                "name",
            ),
            timeout_seconds=min(timeout_seconds, 60),
            label="Open5GS namespace existence query",
        ).strip()
        if not namespace:
            authority()
            _checked(
                cluster_runner,
                _ssh(
                    topology.core_node,
                    known_hosts,
                    "kubectl",
                    "create",
                    "namespace",
                    NAMESPACE,
                ),
                timeout_seconds=min(timeout_seconds, 60),
                label="Open5GS namespace creation",
            )
        elif previous_run_id is None:
            # A legacy unlabeled namespace may be reclaimed only after the exact
            # gNB is stopped below.
            pass
    allowed = {run_id}
    if previous_run_id is not None:
        validate_run_id(previous_run_id)
        if previous_run_id == run_id:
            raise R2LabTopologyFoundationError("previous and current run IDs must differ")
        allowed.add(previous_run_id)
    if owner not in allowed and owner is not None:
        raise R2LabTopologyFoundationError("Open5GS namespace has an unexpected run owner")

    deployment_present, desired, deployment_owner, pod_count = _gnb_state(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 60),
    )
    if deployment_owner not in allowed and deployment_owner is not None:
        raise R2LabTopologyFoundationError("existing physical gNB has an unexpected run owner")
    if deployment_present and (desired != 0 or pod_count != 0):
        legacy_reclaim = owner is None and deployment_owner is None and pod_count <= 1
        if not legacy_reclaim:
            raise R2LabTopologyFoundationError(
                "existing physical gNB must be stopped before namespace ownership changes"
            )
        authority()
        _checked(
            cluster_runner,
            _ssh(
                topology.core_node,
                known_hosts,
                "kubectl",
                "scale",
                f"deployment/{RELEASE}",
                "-n",
                NAMESPACE,
                "--replicas=0",
            ),
            timeout_seconds=min(timeout_seconds, 60),
            label="legacy physical gNB scale-to-zero",
        )
        for _ in range(30):
            _, desired, _, pod_count = _gnb_state(
                topology=topology,
                known_hosts=known_hosts,
                runner=cluster_runner,
                timeout_seconds=min(timeout_seconds, 60),
            )
            if desired == 0 and pod_count == 0:
                break
            time.sleep(2)
        else:
            raise R2LabTopologyFoundationError("legacy physical gNB did not reach zero pods")

    changed = owner != run_id or (deployment_present and deployment_owner != run_id)
    if changed:
        authority()
        if deployment_present:
            _checked(
                cluster_runner,
                _ssh(
                    topology.core_node,
                    known_hosts,
                    "kubectl",
                    "label",
                    f"deployment/{RELEASE}",
                    "-n",
                    NAMESPACE,
                    f"{RUN_LABEL}={run_id}",
                    "--overwrite",
                ),
                timeout_seconds=min(timeout_seconds, 60),
                label="physical gNB ownership handoff",
            )
        _checked(
            cluster_runner,
            _ssh(
                topology.core_node,
                known_hosts,
                "kubectl",
                "label",
                "namespace",
                NAMESPACE,
                f"{RUN_LABEL}={run_id}",
                "--overwrite",
            ),
            timeout_seconds=min(timeout_seconds, 60),
            label="Open5GS namespace ownership handoff",
        )
    observed = _namespace_owner(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 60),
    )
    if observed != run_id:
        raise R2LabTopologyFoundationError("Open5GS namespace ownership was not proven")
    return changed


def _ready_nodes(
    *, topology: PhysicalTopology, known_hosts: Path, runner: Runner, timeout_seconds: int
) -> int:
    payload = _json_object(
        _checked(
            runner,
            _ssh(topology.core_node, known_hosts, "kubectl", "get", "nodes", "-o", "json"),
            timeout_seconds=timeout_seconds,
            label="Kubernetes node readiness query",
        ),
        "Kubernetes node readiness query",
    )
    items = payload.get("items")
    if not isinstance(items, list):
        raise R2LabTopologyFoundationError("Kubernetes node evidence is malformed")
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
        raise R2LabTopologyFoundationError(f"selected Kubernetes nodes are not Ready: {missing}")
    return len(expected)


def _open5gs_ready(
    *, topology: PhysicalTopology, known_hosts: Path, runner: Runner, timeout_seconds: int
) -> tuple[bool, int]:
    ready = 0
    for network_function in REQUIRED_OPEN5GS_NFS:
        payload = _json_object(
            _checked(
                runner,
                _ssh(
                    topology.core_node,
                    known_hosts,
                    "kubectl",
                    "get",
                    "pods",
                    "-n",
                    NAMESPACE,
                    "-l",
                    f"app=open5gs,nf={network_function}",
                    "-o",
                    "json",
                ),
                timeout_seconds=timeout_seconds,
                label=f"Open5GS {network_function.upper()} readiness query",
            ),
            f"Open5GS {network_function.upper()} readiness query",
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
    return ready == len(REQUIRED_OPEN5GS_NFS), ready


def _locked_git_commit(lock, name: str) -> str:
    dependency = next((item for item in lock.git if item.name == name), None)
    if dependency is None:
        raise R2LabTopologyFoundationError(f"dependency lock is missing {name}")
    return dependency.commit


def reconcile_open5gs_topology(
    *,
    run_id: str,
    slice_name: str,
    topology: PhysicalTopology,
    known_hosts: Path,
    authority_verifier: Callable[[], object],
    lock_path: Path,
    dependency_root: Path,
    run_root: Path,
    repository_root: Path,
    runner: RunCommand = run_command,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    progress: TextIO | None = None,
) -> None:
    """Reconcile the pinned Open5GS core for the selected topology."""

    lock = load_lock(lock_path)
    checkout = validate_fiveg_checkout(lock, dependency_root)
    preparation_variables = locked_preparation_variables(lock)
    fiveg_commit = _locked_git_commit(lock, "fiveg_ansible")
    open5gs_commit = _locked_git_commit(lock, "open5gs_k8s")
    run_directory = run_root.expanduser().resolve() / run_id
    output_directory = run_directory / "open5gs-foundation"
    output_directory.mkdir(parents=True, exist_ok=True)
    overlay_source = repository_root.expanduser().resolve() / "deploy" / "ansible"
    inventory_path = output_directory / "hosts-physical.ini"
    inventory_text, _ = build_preparation_inventory(
        core_node=topology.core_node,
        ran_node=topology.ran_node,
        source=inventory_path,
    )
    inventory_text = inventory_text.replace('rru="rfsim"', f'rru="{topology.radio}"', 1)
    inventory_path.write_text(inventory_text, encoding="utf-8", newline="\n")
    variables_path = output_directory / "locked-open5gs-images.json"
    atomic_json(
        variables_path,
        {"synthran_images": golden_path_image_variables(lock), **preparation_variables},
    )
    log_path = output_directory / "open5gs-core.log"
    log_parts: list[str] = []

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] {message}", file=progress, flush=True)

    def stage(name: str, command: Sequence[str], cwd: Path, environment: Mapping[str, str] | None = None, *, streaming: bool = False) -> CommandResult:
        started = monotonic()
        report(f"{name}: running...")
        log_parts.append(f"=== {name} ===")
        try:
            if streaming and runner is run_command:
                result = run_streaming_ansible_command(
                    command,
                    cwd,
                    environment,
                    timeout_seconds,
                    report=report,
                )
            else:
                result = runner(command, cwd, environment, timeout_seconds)
                if streaming and progress is not None:
                    for line in result.stdout.splitlines():
                        message = parse_ansible_line(line)
                        if message is not None:
                            report(message)
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
            raise R2LabTopologyFoundationError(f"Open5GS stage {name} could not complete") from exc
        log_parts.extend((result.stdout, result.stderr))
        if result.returncode != 0:
            raise R2LabTopologyFoundationError(f"Open5GS stage {name} failed; see sanitized log")
        report(f"{name}: OK ({monotonic() - started:.1f}s)")
        return result

    environment = dict(os.environ)
    collections = output_directory / "collections"
    environment.update(
        {
            "ANSIBLE_COLLECTIONS_PATH": str(collections),
            "ANSIBLE_HOST_KEY_CHECKING": "True",
            "ANSIBLE_STDOUT_CALLBACK": "ansible.builtin.default",
            "ANSIBLE_SSH_ARGS": (
                "-o ControlMaster=auto -o ControlPersist=60s "
                "-o StrictHostKeyChecking=yes "
                f"-o UserKnownHostsFile={shlex.quote(str(known_hosts))} "
                "-o GlobalKnownHostsFile=/dev/null"
            ),
            "ANSIBLE_NOCOLOR": "True",
            "ANSIBLE_RETRY_FILES_ENABLED": "False",
        }
    )
    worktree_parent = Path(tempfile.mkdtemp(prefix="worktree-", dir=output_directory))
    worktree = worktree_parent / "fiveg_ansible"
    added = False
    try:
        stage(
            "open5gs-worktree",
            ("git", "-C", str(checkout), "worktree", "add", "--detach", str(worktree), fiveg_commit),
            repository_root,
        )
        added = True
        proof = stage("open5gs-worktree-proof", ("git", "rev-parse", "HEAD"), worktree)
        if proof.stdout.strip() != fiveg_commit:
            raise R2LabTopologyFoundationError("isolated Open5GS worktree does not match lock")
        overlay_directory = worktree / ".synthran"
        shutil.copytree(overlay_source, overlay_directory)
        apply_network_overlay(worktree, subscriber_name=topology.ue)
        environment["ANSIBLE_ROLES_PATH"] = str(worktree / "roles")
        stage(
            "open5gs-collections",
            (
                "ansible-galaxy",
                "collection",
                "install",
                "-r",
                str(overlay_directory / "requirements.yml"),
                "-p",
                str(collections),
            ),
            worktree,
            environment,
        )
        runtime_playbook = (
            "ansible-playbook",
            "-i",
            str(inventory_path),
            "--limit",
            "core_node",
            "-e",
            f"@{variables_path}",
            str(overlay_directory / "prepare-python-runtime.yml"),
        )
        playbook = (
            "ansible-playbook",
            "-i",
            str(inventory_path),
            "-e",
            "fiveg_profile=default",
            "-e",
            f"repo_branch={open5gs_commit}",
            "-e",
            f"synthran_run_id={run_id}",
            "-e",
            f"synthran_subscriber={topology.ue}",
            "-e",
            "deployment_option=open5gs",
            "-e",
            f"@{variables_path}",
            str(overlay_directory / "r2lab-open5gs-core.yml"),
        )
        stage("open5gs-runtime-syntax", (*runtime_playbook[:-1], "--syntax-check", runtime_playbook[-1]), worktree, environment)
        stage("open5gs-syntax", (*playbook[:-1], "--syntax-check", playbook[-1]), worktree, environment)
        authority_verifier()
        stage("open5gs-runtime", runtime_playbook, worktree, environment, streaming=True)
        authority_verifier()
        stage("open5gs-reconcile", playbook, worktree, environment, streaming=True)
        authority_verifier()
    except UpstreamOverlayError as exc:
        raise R2LabTopologyFoundationError(str(exc)) from exc
    finally:
        log_path.write_text(
            sanitize_deployment_text(
                "\n".join(log_parts),
                (known_hosts, lock_path, dependency_root, repository_root, run_directory),
            ),
            encoding="utf-8",
            newline="\n",
        )
        if added:
            runner(
                ("git", "-C", str(checkout), "worktree", "remove", "--force", str(worktree)),
                repository_root,
                None,
                timeout_seconds,
            )
        shutil.rmtree(worktree_parent, ignore_errors=True)


def accept_topology_foundation(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    previous_run_id: str | None = None,
    run_root: Path = Path(".synthran/r2lab"),
    lock_path: Path = Path("dependencies.lock.yml"),
    dependency_root: Path = Path(".deps"),
    repository_root: Path = Path("."),
    r2lab_runner: Runner = subprocess_runner,
    cluster_runner: Runner = subprocess_runner,
    reconciliation_runner: RunCommand = run_command,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    progress: TextIO | None = None,
) -> TopologyFoundationResult:
    """Accept the selected compute/core foundation and persist ordered evidence."""

    validate_run_id(run_id)
    topology = load_topology(run_root=run_root, run_id=run_id)
    if timeout_seconds < 60 or timeout_seconds > 3600:
        raise R2LabTopologyFoundationError("foundation timeout must be between 60 and 3600 seconds")
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabTopologyFoundationError("strict SLICES known-hosts file is missing")

    def authority() -> object:
        return verify_physical_authority(
            run_id=run_id,
            slice_name=slice_name,
            run_root=run_root,
            runner=r2lab_runner,
            timeout_seconds=min(timeout_seconds, 300),
        )

    try:
        allocation = claim_selected_allocation(
            run_id=run_id,
            slice_name=slice_name,
            topology=topology,
            r2lab_runner=r2lab_runner,
            allocation_runner=cluster_runner,
            owner=owner,
            allocation_id=allocation_id,
            timeout_seconds=min(timeout_seconds, 300),
        )
        authority()
    except R2LabTopologyResourceError as exc:
        raise R2LabTopologyFoundationError(str(exc)) from exc

    ready_nodes = _ready_nodes(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    namespace_changed = _handoff_namespace(
        run_id=run_id,
        previous_run_id=previous_run_id,
        slice_name=slice_name,
        topology=topology,
        run_root=run_root,
        known_hosts=known_hosts,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    healthy, ready_pods = _open5gs_ready(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    reconciled = not healthy
    if reconciled:
        reconcile_open5gs_topology(
            run_id=run_id,
            slice_name=slice_name,
            topology=topology,
            known_hosts=known_hosts,
            authority_verifier=authority,
            lock_path=lock_path,
            dependency_root=dependency_root,
            run_root=run_root,
            repository_root=repository_root,
            runner=reconciliation_runner,
            timeout_seconds=timeout_seconds,
            progress=progress,
        )
        healthy, ready_pods = _open5gs_ready(
            topology=topology,
            known_hosts=known_hosts,
            runner=cluster_runner,
            timeout_seconds=min(timeout_seconds, 300),
        )
    if not healthy:
        raise R2LabTopologyFoundationError("Open5GS did not reach one ready AMF/SMF/UPF set")
    authority()
    if _namespace_owner(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 60),
    ) != run_id:
        raise R2LabTopologyFoundationError("Open5GS namespace ownership changed")

    evidence = PhysicalRunEvidence(run_id=run_id)
    for stage, source in (
        (
            PhysicalAcceptanceStage.RESOURCE_AUTHORITY,
            f"current-r2lab:{topology.radio}:{topology.ue}",
        ),
        (
            PhysicalAcceptanceStage.SLICES_FOUNDATION,
            f"current-slices:{topology.core_node}:{topology.ran_node}",
        ),
        (PhysicalAcceptanceStage.KUBERNETES, "selected-compute-nodes-ready"),
        (PhysicalAcceptanceStage.OPEN5GS, "owned-open5gs-core-ready"),
    ):
        evidence = evidence.pass_stage(stage, source=source)
    evidence_path = run_root.expanduser().resolve() / run_id / "physical-run.json"
    try:
        if evidence_path.exists():
            existing = PhysicalRunEvidence.read_json(evidence_path)
            if existing != evidence:
                raise R2LabTopologyFoundationError("physical run evidence already contains different state")
        else:
            evidence.write_json(evidence_path)
    except R2LabAcceptanceError as exc:
        raise R2LabTopologyFoundationError(str(exc)) from exc

    return TopologyFoundationResult(
        run_id=run_id,
        topology=topology,
        allocation_id=allocation,
        namespace_changed=namespace_changed,
        open5gs_reconciled=reconciled,
        ready_node_count=ready_nodes,
        ready_open5gs_pod_count=ready_pods,
        evidence_path=evidence_path,
    )
