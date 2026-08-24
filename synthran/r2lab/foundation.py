"""Evidence-backed acceptance of the reused SLICES and Open5GS foundation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
from time import monotonic
from typing import Mapping, Sequence, TextIO

from synthran.ansible_streaming import parse_ansible_line, run_streaming_ansible_command
from synthran.dependencies import load_lock
from synthran.fiveg_ansible import validate_fiveg_checkout
from synthran.live_preflight import CommandResult, Runner, subprocess_runner
from synthran.network.resources import (
    build_preparation_inventory,
    locked_preparation_variables,
)
from synthran.network.runtime import (
    RunCommand,
    atomic_json,
    golden_path_image_variables,
    run_command,
    sanitize_deployment_text,
    tree_sha256,
    validate_run_id,
)
from synthran.r2lab.acceptance import (
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
    R2LabAcceptanceError,
)
from synthran.r2lab.controller import authorize_physical_start
from synthran.r2lab.authority import PhysicalAuthorityGuard
from synthran.r2lab.deployment import (
    CORE_NODE,
    NAMESPACE,
    PhysicalStartAuthority,
    RAN_NODE,
)
from synthran.r2lab.handoff import (
    PhysicalNamespaceHandoffResult,
    R2LabPhysicalHandoffError,
    execute_physical_namespace_handoff,
)
from synthran.upstream_overlay import UpstreamOverlayError, apply_network_overlay


REQUIRED_OPEN5GS_NFS = frozenset({"amf", "smf", "upf"})
DEFAULT_FOUNDATION_TIMEOUT_SECONDS = 1800
OPEN5GS_FOUNDATION_SCHEMA = "synthran/r2lab-open5gs-foundation/v1alpha1"
PHYSICAL_SUBSCRIBER = "qfit07"

AuthorityVerifier = Callable[[], object]


class R2LabPhysicalFoundationError(RuntimeError):
    """Raised when the reused physical foundation cannot be accepted safely."""


@dataclass(frozen=True)
class Open5gsFoundationResult:
    run_id: str
    manifest_path: Path
    log_path: Path


@dataclass(frozen=True)
class PhysicalFoundationResult:
    run_id: str
    previous_run_id: str
    handoff: PhysicalNamespaceHandoffResult
    ready_node_count: int
    ready_open5gs_pod_count: int
    open5gs_reconciled: bool
    evidence_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "previous_run_id": self.previous_run_id,
            "namespace_changed": self.handoff.changed,
            "legacy_gnb_stopped": self.handoff.legacy_gnb_stopped,
            "ready_node_count": self.ready_node_count,
            "ready_open5gs_pod_count": self.ready_open5gs_pod_count,
            "open5gs_reconciled": self.open5gs_reconciled,
            "next_stage": PhysicalAcceptanceStage.GNB_N2.value,
            "status": "foundation-ready",
            "hardware_mutation": False,
        }


def _ssh(known_hosts: Path, *remote: str) -> tuple[str, ...]:
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
        f"root@{CORE_NODE}",
        shlex.join(remote),
    )


def _checked(
    runner: Runner,
    command: Sequence[str],
    *,
    timeout_seconds: int,
    label: str,
) -> str:
    try:
        result = runner(command, timeout_seconds)
    except Exception as exc:
        raise R2LabPhysicalFoundationError(f"{label} could not be observed") from exc
    if result.returncode != 0:
        raise R2LabPhysicalFoundationError(f"{label} returned nonzero")
    return result.stdout


def _json_object(text: str, label: str) -> Mapping[str, object]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise R2LabPhysicalFoundationError(f"{label} did not return JSON") from exc
    if not isinstance(payload, dict):
        raise R2LabPhysicalFoundationError(f"{label} returned malformed JSON")
    return payload


def _locked_git_commit(lock, name: str) -> str:
    dependency = next((item for item in lock.git if item.name == name), None)
    if dependency is None:
        raise R2LabPhysicalFoundationError(
            f"dependency lock is missing {name}"
        )
    return dependency.commit


def _write_exact(path: Path, content: str, label: str) -> None:
    try:
        if path.exists():
            if path.read_text(encoding="utf-8") != content:
                raise R2LabPhysicalFoundationError(
                    f"existing {label} does not match the reviewed content"
                )
            return
        path.write_text(content, encoding="utf-8", newline="\n")
    except OSError as exc:
        raise R2LabPhysicalFoundationError(f"unable to write {label}") from exc


def reconcile_open5gs_foundation(
    *,
    run_id: str,
    known_hosts: Path,
    authority_verifier: AuthorityVerifier,
    lock_path: Path = Path("dependencies.lock.yml"),
    dependency_root: Path = Path(".deps"),
    run_root: Path = Path(".synthran/r2lab"),
    repository_root: Path = Path("."),
    runner: RunCommand = run_command,
    timeout_seconds: int = DEFAULT_FOUNDATION_TIMEOUT_SECONDS,
    progress: TextIO | None = None,
) -> Open5gsFoundationResult:
    """Reconcile only the pinned qfit07 Open5GS core on the owned cluster."""

    validate_run_id(run_id)
    if timeout_seconds < 60 or timeout_seconds > 3600:
        raise R2LabPhysicalFoundationError(
            "foundation timeout must be between 60 and 3600 seconds"
        )
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabPhysicalFoundationError("strict SLICES known-hosts file is missing")

    repository_root = repository_root.resolve()
    run_directory = run_root.resolve() / run_id
    if not run_directory.is_dir():
        raise R2LabPhysicalFoundationError("prepared R2Lab run directory is missing")
    output_directory = run_directory / "open5gs-foundation"
    output_directory.mkdir(parents=True, exist_ok=True)

    try:
        lock = load_lock(lock_path)
        checkout = validate_fiveg_checkout(lock, dependency_root)
        preparation_variables = locked_preparation_variables(lock)
    except Exception as exc:
        raise R2LabPhysicalFoundationError(
            "locked Open5GS reconciliation inputs are not ready"
        ) from exc

    overlay_source = repository_root / "deploy" / "ansible"
    wrapper_source = overlay_source / "r2lab-open5gs-core.yml"
    runtime_source = overlay_source / "prepare-python-runtime.yml"
    runtime_tasks_source = overlay_source / "tasks" / "prepare-python-runtime.yml"
    if any(
        not source.is_file()
        for source in (wrapper_source, runtime_source, runtime_tasks_source)
    ):
        raise R2LabPhysicalFoundationError(
            "physical Open5GS reconciliation overlay is incomplete"
        )

    inventory_path = output_directory / "hosts-physical.ini"
    inventory_text, _ = build_preparation_inventory(
        core_node=CORE_NODE,
        ran_node=RAN_NODE,
        source=inventory_path,
    )
    inventory_text = inventory_text.replace('rru="rfsim"', 'rru="n300"', 1)
    _write_exact(inventory_path, inventory_text, "physical inventory")

    variables_path = output_directory / "locked-open5gs-images.json"
    manifest_path = output_directory / "manifest.json"
    log_path = output_directory / "open5gs-core.log"
    fiveg_commit = _locked_git_commit(lock, "fiveg_ansible")
    open5gs_commit = _locked_git_commit(lock, "open5gs_k8s")
    overlay_sha256 = tree_sha256(overlay_source)
    atomic_json(
        variables_path,
        {
            "synthran_images": golden_path_image_variables(lock),
            **preparation_variables,
        },
    )

    log_parts: list[str] = []

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] {message}", file=progress, flush=True)

    def persist(status: str, failure_stage: str | None = None) -> None:
        payload: dict[str, object] = {
            "schema": OPEN5GS_FOUNDATION_SCHEMA,
            "run_id": run_id,
            "status": status,
            "core_node": CORE_NODE,
            "ran_node": RAN_NODE,
            "subscriber": PHYSICAL_SUBSCRIBER,
            "dependencies": {
                "fiveg_ansible": fiveg_commit,
                "open5gs_k8s": open5gs_commit,
                "remote_python": preparation_variables[
                    "synthran_remote_python_expected"
                ],
            },
            "overlay_sha256": overlay_sha256,
            "hardware_mutation": False,
        }
        if failure_stage is not None:
            payload["failure_stage"] = failure_stage
        atomic_json(manifest_path, payload)
        log_path.write_text(
            sanitize_deployment_text(
                "\n".join(log_parts),
                (
                    known_hosts,
                    lock_path,
                    dependency_root,
                    repository_root,
                    run_directory,
                ),
            ),
            encoding="utf-8",
            newline="\n",
        )

    def stage(
        name: str,
        command: Sequence[str],
        cwd: Path,
        environment: Mapping[str, str] | None = None,
        *,
        streaming: bool = False,
    ) -> CommandResult:
        report(f"{name}: running...")
        started = monotonic()
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
            persist("failed", name)
            report(f"{name}: FAILED")
            raise R2LabPhysicalFoundationError(
                f"Open5GS reconciliation stage {name} could not complete"
            ) from exc
        log_parts.extend((result.stdout, result.stderr))
        if result.returncode != 0:
            persist("failed", name)
            report(f"{name}: FAILED")
            raise R2LabPhysicalFoundationError(
                f"Open5GS reconciliation stage {name} failed; see the sanitized log"
            )
        report(f"{name}: OK ({monotonic() - started:.1f}s)")
        return result

    def verify_authority(name: str) -> None:
        report(f"{name}: running...")
        log_parts.append(f"=== {name} ===")
        try:
            authority_verifier()
        except Exception as exc:
            persist("failed", name)
            report(f"{name}: FAILED")
            raise R2LabPhysicalFoundationError(
                f"Open5GS reconciliation stage {name} could not complete"
            ) from exc
        log_parts.append("authority verified")
        report(f"{name}: OK")

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

    persist("running")
    with tempfile.TemporaryDirectory(
        prefix="worktree-", dir=output_directory
    ) as temporary:
        worktree = Path(temporary) / "fiveg_ansible"
        worktree_added = False
        try:
            stage(
                "open5gs-worktree",
                (
                    "git",
                    "-C",
                    str(checkout),
                    "worktree",
                    "add",
                    "--detach",
                    str(worktree),
                    fiveg_commit,
                ),
                repository_root,
            )
            worktree_added = True
            proof = stage(
                "open5gs-worktree-proof",
                ("git", "rev-parse", "HEAD"),
                worktree,
            )
            if proof.stdout.strip() != fiveg_commit:
                persist("failed", "open5gs-worktree-proof")
                raise R2LabPhysicalFoundationError(
                    "isolated Open5GS worktree does not match the lock"
                )

            overlay_directory = worktree / ".synthran"
            shutil.copytree(overlay_source, overlay_directory)
            apply_network_overlay(
                worktree,
                subscriber_name=PHYSICAL_SUBSCRIBER,
            )
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
                f"synthran_subscriber={PHYSICAL_SUBSCRIBER}",
                "-e",
                "deployment_option=open5gs",
                "-e",
                f"@{variables_path}",
                str(overlay_directory / "r2lab-open5gs-core.yml"),
            )
            stage(
                "open5gs-runtime-syntax",
                (*runtime_playbook[:-1], "--syntax-check", runtime_playbook[-1]),
                worktree,
                environment,
            )
            stage(
                "open5gs-syntax",
                (*playbook[:-1], "--syntax-check", playbook[-1]),
                worktree,
                environment,
            )
            verify_authority("open5gs-runtime-authority-before")
            stage(
                "open5gs-runtime",
                runtime_playbook,
                worktree,
                environment,
                streaming=True,
            )
            verify_authority("open5gs-runtime-authority-after")
            stage(
                "open5gs-reconcile",
                playbook,
                worktree,
                environment,
                streaming=True,
            )
            verify_authority("open5gs-reconcile-authority-after")
        except UpstreamOverlayError as exc:
            persist("failed", "open5gs-overlay")
            raise R2LabPhysicalFoundationError(str(exc)) from exc
        finally:
            if worktree_added:
                cleanup = runner(
                    (
                        "git",
                        "-C",
                        str(checkout),
                        "worktree",
                        "remove",
                        "--force",
                        str(worktree),
                    ),
                    repository_root,
                    None,
                    timeout_seconds,
                )
                if cleanup.returncode != 0:
                    log_parts.extend(
                        ("=== open5gs-worktree-cleanup ===", cleanup.stdout)
                    )

    persist("reconciled")
    return Open5gsFoundationResult(
        run_id=run_id,
        manifest_path=manifest_path,
        log_path=log_path,
    )


def _ready_node_count(payload: Mapping[str, object]) -> int:
    items = payload.get("items")
    if not isinstance(items, list):
        raise R2LabPhysicalFoundationError("Kubernetes node evidence is malformed")
    ready_nodes: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            raise R2LabPhysicalFoundationError("Kubernetes node evidence is malformed")
        metadata = item.get("metadata")
        status = item.get("status")
        if not isinstance(metadata, dict) or not isinstance(status, dict):
            raise R2LabPhysicalFoundationError("Kubernetes node evidence is incomplete")
        name = metadata.get("name")
        conditions = status.get("conditions")
        if not isinstance(name, str) or not isinstance(conditions, list):
            raise R2LabPhysicalFoundationError(
                "Kubernetes node readiness is unavailable"
            )
        if any(
            isinstance(condition, dict)
            and condition.get("type") == "Ready"
            and condition.get("status") == "True"
            for condition in conditions
        ):
            ready_nodes.add(name.split(".", 1)[0])
    expected = {CORE_NODE, RAN_NODE}
    if not expected.issubset(ready_nodes):
        raise R2LabPhysicalFoundationError(
            "Kubernetes does not report both selected SLICES nodes Ready"
        )
    return len(expected)


def _open5gs_pod_ready(
    payload: Mapping[str, object], network_function: str
) -> bool:
    items = payload.get("items")
    if not isinstance(items, list):
        raise R2LabPhysicalFoundationError("Open5GS pod evidence is malformed")
    if len(items) != 1:
        return False
    item = items[0]
    if not isinstance(item, dict):
        raise R2LabPhysicalFoundationError("Open5GS pod evidence is malformed")
    metadata = item.get("metadata")
    status = item.get("status")
    if not isinstance(metadata, dict) or not isinstance(status, dict):
        raise R2LabPhysicalFoundationError("Open5GS pod evidence is incomplete")
    labels = metadata.get("labels")
    if (
        not isinstance(labels, dict)
        or labels.get("app") != "open5gs"
        or labels.get("nf") != network_function
    ):
        raise R2LabPhysicalFoundationError("Open5GS pod identity is inconsistent")
    containers = status.get("containerStatuses")
    return not (
        metadata.get("deletionTimestamp") is not None
        or not isinstance(containers, list)
        or not containers
        or not all(
            isinstance(container, dict) and container.get("ready") is True
            for container in containers
        )
    )


def _require_ready_open5gs_pod(
    payload: Mapping[str, object], network_function: str
) -> None:
    if _open5gs_pod_ready(payload, network_function):
        return
    items = payload.get("items")
    if isinstance(items, list) and len(items) == 1:
        raise R2LabPhysicalFoundationError(
            f"Open5GS {network_function.upper()} pod is not Running and ready"
        )
    raise R2LabPhysicalFoundationError(
        f"Open5GS {network_function.upper()} does not have exactly one pod"
    )


def _open5gs_health(
    *, runner: Runner, known_hosts: Path, timeout_seconds: int
) -> tuple[str, ...]:
    unhealthy: list[str] = []
    for network_function in sorted(REQUIRED_OPEN5GS_NFS):
        label = f"Open5GS {network_function.upper()} readiness query"
        payload = _json_object(
            _checked(
                runner,
                _ssh(
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
                label=label,
            ),
            label,
        )
        if not _open5gs_pod_ready(payload, network_function):
            unhealthy.append(network_function)
    return tuple(unhealthy)


def _foundation_evidence(run_id: str) -> PhysicalRunEvidence:
    evidence = PhysicalRunEvidence(run_id=run_id)
    for stage, source in (
        (PhysicalAcceptanceStage.RESOURCE_AUTHORITY, "current-r2lab-claim-lease-n300"),
        (PhysicalAcceptanceStage.SLICES_FOUNDATION, "current-slices-f2-f3-authority"),
        (PhysicalAcceptanceStage.KUBERNETES, "selected-slices-nodes-ready"),
        (PhysicalAcceptanceStage.OPEN5GS, "owned-open5gs-core-ready"),
    ):
        evidence = evidence.pass_stage(stage, source=source)
    return evidence


def execute_physical_foundation_acceptance(
    *,
    run_id: str,
    previous_run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    run_root: Path = Path(".synthran/r2lab"),
    lock_path: Path = Path("dependencies.lock.yml"),
    dependency_root: Path = Path(".deps"),
    repository_root: Path = Path("."),
    r2lab_runner: Runner = subprocess_runner,
    foundation_runner: Runner = subprocess_runner,
    reconciliation_runner: RunCommand = run_command,
    core_reconciler: Callable[..., Open5gsFoundationResult] = (
        reconcile_open5gs_foundation
    ),
    timeout_seconds: int = DEFAULT_FOUNDATION_TIMEOUT_SECONDS,
    progress: TextIO | None = None,
) -> PhysicalFoundationResult:
    """Reconcile and accept one current, healthy, stopped physical foundation."""

    if timeout_seconds < 60 or timeout_seconds > 3600:
        raise R2LabPhysicalFoundationError(
            "foundation timeout must be between 60 and 3600 seconds"
        )
    known_hosts = known_hosts.expanduser().resolve()
    probe_timeout_seconds = min(timeout_seconds, 300)

    def verify_r2lab_authority() -> PhysicalStartAuthority:
        return authorize_physical_start(
            run_id=run_id,
            slice_name=slice_name,
            run_root=run_root,
            runner=r2lab_runner,
            timeout_seconds=probe_timeout_seconds,
        )

    try:
        authority = PhysicalAuthorityGuard.open(
            lease_verifier=verify_r2lab_authority,
            allocation_runner=foundation_runner,
            owner=owner,
            allocation_id=allocation_id,
            timeout_seconds=probe_timeout_seconds,
            reclaim_conflicts=True,
        )
    except RuntimeError as exc:
        raise R2LabPhysicalFoundationError(
            "current physical authority was not proven"
        ) from exc

    ready_nodes = _ready_node_count(
        _json_object(
            _checked(
                foundation_runner,
                _ssh(known_hosts, "kubectl", "get", "nodes", "-o", "json"),
                timeout_seconds=probe_timeout_seconds,
                label="Kubernetes node readiness query",
            ),
            "Kubernetes node readiness query",
        )
    )

    try:
        handoff = execute_physical_namespace_handoff(
            from_run_id=previous_run_id,
            to_run_id=run_id,
            known_hosts=known_hosts,
            runner=foundation_runner,
            authority_verifier=authority.verify,
            reclaim_unowned=True,
            timeout_seconds=probe_timeout_seconds,
        )
    except R2LabPhysicalHandoffError as exc:
        raise R2LabPhysicalFoundationError(
            f"physical namespace ownership was not proven: {exc}"
        ) from exc

    unhealthy_network_functions = _open5gs_health(
        runner=foundation_runner,
        known_hosts=known_hosts,
        timeout_seconds=probe_timeout_seconds,
    )
    open5gs_reconciled = bool(unhealthy_network_functions)
    if open5gs_reconciled:
        try:
            authority.verify()
            core_reconciler(
                run_id=run_id,
                known_hosts=known_hosts,
                authority_verifier=authority.verify,
                lock_path=lock_path,
                dependency_root=dependency_root,
                run_root=run_root,
                repository_root=repository_root,
                runner=reconciliation_runner,
                timeout_seconds=timeout_seconds,
                progress=progress,
            )
            authority.verify()
        except RuntimeError as exc:
            raise R2LabPhysicalFoundationError(
                "Open5GS foundation reconciliation failed"
            ) from exc

        remaining_unhealthy = _open5gs_health(
            runner=foundation_runner,
            known_hosts=known_hosts,
            timeout_seconds=probe_timeout_seconds,
        )
        if remaining_unhealthy:
            names = ", ".join(name.upper() for name in remaining_unhealthy)
            raise R2LabPhysicalFoundationError(
                f"Open5GS reconciliation left unhealthy network functions: {names}"
            )
    ready_open5gs_pods = len(REQUIRED_OPEN5GS_NFS)

    try:
        authority.verify()
    except RuntimeError as exc:
        raise R2LabPhysicalFoundationError(
            "physical authority changed during foundation verification"
        ) from exc
    observed_owner = _checked(
        foundation_runner,
        _ssh(
            known_hosts,
            "kubectl",
            "get",
            "namespace",
            NAMESPACE,
            "-o",
            "jsonpath={.metadata.labels.synthran\\.run/id}",
        ),
        timeout_seconds=probe_timeout_seconds,
        label="Open5GS namespace ownership verification",
    ).strip()
    if observed_owner != run_id:
        raise R2LabPhysicalFoundationError(
            "Open5GS namespace ownership changed during foundation verification"
        )

    try:
        authority.verify()
    except RuntimeError as exc:
        raise R2LabPhysicalFoundationError(
            "physical authority changed during foundation verification"
        ) from exc

    evidence_path = run_root.resolve() / run_id / "physical-run.json"
    try:
        evidence = _foundation_evidence(run_id)
        if evidence_path.exists():
            existing = PhysicalRunEvidence.read_json(evidence_path)
            if existing != evidence:
                raise R2LabPhysicalFoundationError(
                    "physical run evidence already contains different state"
                )
        else:
            evidence.write_json(evidence_path)
    except R2LabAcceptanceError as exc:
        raise R2LabPhysicalFoundationError(
            "physical foundation evidence could not be persisted safely"
        ) from exc

    return PhysicalFoundationResult(
        run_id=run_id,
        previous_run_id=previous_run_id,
        handoff=handoff,
        ready_node_count=ready_nodes,
        ready_open5gs_pod_count=ready_open5gs_pods,
        open5gs_reconciled=open5gs_reconciled,
        evidence_path=evidence_path,
    )
