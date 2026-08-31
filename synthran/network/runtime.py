"""Operator-triggered golden-path deployment and network-evidence collection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
import os
import shlex
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from time import monotonic
from typing import Any, Callable, Mapping, Sequence, TextIO

from synthran.ansible_streaming import parse_ansible_line, run_streaming_ansible_command
from synthran.dependencies import DependencyLock
from synthran.fiveg_ansible import (
    NetworkDeploymentPlan,
    NetworkInventory,
    validate_fiveg_checkout,
)
from synthran.live_preflight import (
    CommandResult,
    LivePreflightError,
    Runner,
    load_fresh_live_evidence,
    ssh_command,
    subprocess_runner,
)
from synthran.slices_controller import (
    SlicesControllerError,
    dependency_lock_sha256 as dependency_lock_sha256,
    fingerprint as context_fingerprint,
    verify_slices_controller,
)
from synthran.upstream_overlay import UpstreamOverlayError, apply_network_overlay


DEPLOYMENT_SCHEMA = "synthran/network-deployment/v1alpha1"
NETWORK_EVIDENCE_SCHEMA = "synthran/network-evidence/v1alpha1"
RUN_ID_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
HEX_SECRET_RE = re.compile(r"(?i)\b[0-9a-f]{32}\b")
SUBSCRIBER_RE = re.compile(r"\b[0-9]{14,16}\b")
PRIVATE_IPV4_RE = re.compile(
    r"(?<![0-9])(?:10(?:\.[0-9]{1,3}){3}|"
    r"192\.168(?:\.[0-9]{1,3}){2}|"
    r"172\.(?:1[6-9]|2[0-9]|3[01])(?:\.[0-9]{1,3}){2})(?![0-9])"
)
DEFAULT_DEPLOY_TIMEOUT_SECONDS = 3600
GOLDEN_PATH_NAMESPACE = "open5gs"
GOLDEN_PATH_INTERFACE = "tun_srsue1"
GOLDEN_PATH_PDU_NETWORK = ipaddress.ip_network("12.1.0.0/16")
KUBERNETES_NAME_RE = re.compile(
    r"^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$"
)


class NetworkRuntimeError(RuntimeError):
    """Raised when a live deployment or its proof fails closed."""


RunCommand = Callable[
    [Sequence[str], Path, Mapping[str, str] | None, int],
    CommandResult,
]


def run_command(
    command: Sequence[str],
    cwd: Path,
    environment: Mapping[str, str] | None,
    timeout_seconds: int,
) -> CommandResult:
    """Run one local deployment command without a shell."""

    try:
        completed = subprocess.run(
            list(command),
            cwd=cwd,
            env=dict(environment) if environment is not None else None,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout_seconds,
            check=False,
        )
    except FileNotFoundError as exc:
        raise NetworkRuntimeError("a required deployment command was not found") from exc
    except subprocess.TimeoutExpired as exc:
        raise NetworkRuntimeError("a deployment stage exceeded its timeout") from exc
    return CommandResult(completed.returncode, completed.stdout or "")


def validate_run_id(value: str) -> str:
    if not RUN_ID_RE.fullmatch(value):
        raise NetworkRuntimeError(
            "run ID must be 1-63 lowercase letters, numbers, or internal hyphens"
        )
    return value


def atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        delete=False,
    ) as temporary:
        temporary.write(content)
        temporary_path = Path(temporary.name)
    temporary_path.replace(path)


def _container_reference(lock: DependencyLock, name: str) -> str:
    containers = lock.raw.get("containers")
    if not isinstance(containers, dict):
        raise NetworkRuntimeError("dependency lock container mapping is unavailable")
    entry = containers.get(name)
    if not isinstance(entry, dict):
        raise NetworkRuntimeError(f"dependency lock is missing container {name}")
    image = entry.get("image")
    digest = entry.get("digest")
    if (
        not isinstance(image, str)
        or not image
        or not isinstance(digest, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
    ):
        raise NetworkRuntimeError(f"container {name} is not digest-addressed")
    return f"{image}@{digest}"


def golden_path_image_variables(lock: DependencyLock) -> dict[str, str]:
    """Return only the locked image variables consumed by the wrapper playbook."""

    return {
        name: _container_reference(lock, name)
        for name in (
            "open5gs",
            "open5gs_smf",
            "open5gs_mongodb",
            "open5gs_amf",
            "srsran_gnb",
            "srsran_ue",
            "busybox_1_32",
            "busybox_1_36",
        )
    }


def tree_sha256(root: Path) -> str:
    digest = hashlib.sha256()
    try:
        files = sorted(path for path in root.rglob("*") if path.is_file())
        for path in files:
            relative = path.relative_to(root).as_posix().encode("utf-8")
            digest.update(relative)
            digest.update(b"\0")
            digest.update(path.read_bytes())
    except OSError as exc:
        raise NetworkRuntimeError("unable to hash the golden-path deployment overlay") from exc
    if not files:
        raise NetworkRuntimeError("golden-path deployment overlay is empty")
    return digest.hexdigest()


def sanitize_deployment_text(text: str, private_paths: Sequence[Path]) -> str:
    """Remove credentials, subscriber identifiers, private IPs, and local paths."""

    sanitized = text
    for path in sorted(
        {str(item.resolve(strict=False)) for item in private_paths},
        key=len,
        reverse=True,
    ):
        if path:
            sanitized = sanitized.replace(path, "<local-path>")
            sanitized = sanitized.replace(path.replace("\\", "/"), "<local-path>")
    sanitized = HEX_SECRET_RE.sub("<secret>", sanitized)
    sanitized = SUBSCRIBER_RE.sub("<subscriber-id>", sanitized)
    sanitized = PRIVATE_IPV4_RE.sub("<private-ip>", sanitized)
    return sanitized


def _deployment_manifest(
    *,
    run_id: str,
    status: str,
    plan: NetworkDeploymentPlan,
    preflight: Mapping[str, Any],
    overlay_sha256: str,
    failure_stage: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": DEPLOYMENT_SCHEMA,
        "run_id": run_id,
        "status": status,
        "updated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "inventory": plan.inventory.redacted_summary(),
        "profile": plan.profile,
        "dependencies": {
            "fiveg_ansible": plan.fiveg_ansible_commit,
            "open5gs_k8s": plan.open5gs_k8s_commit,
            "srsran_helm": plan.srsran_helm_commit,
        },
        "dependency_lock_sha256": preflight["dependency_lock_sha256"],
        "slices_controller": preflight["slices_controller"],
        "overlays": {"ansible_overlay_sha256": overlay_sha256},
        "authority": {
            key: preflight[key]
            for key in (
                "owner_fingerprint",
                "reservation_fingerprint",
                "allocation_fingerprint",
            )
        },
        "reservation_action": "none",
        "worktree": "worktree",
        "deployment_log": "deployment.log",
    }
    if failure_stage is not None:
        payload["failure_stage"] = failure_stage
    return payload


@dataclass(frozen=True)
class DeploymentResult:
    run_id: str
    run_directory: Path
    manifest_path: Path
    log_path: Path


def execute_network_deployment(
    *,
    plan: NetworkDeploymentPlan,
    lock: DependencyLock,
    dependency_root: Path,
    live_evidence_path: Path,
    owner: str,
    reservation_id: str,
    allocation_id: str,
    slices_project: str,
    slices_experiment: str,
    run_id: str,
    run_root: Path = Path(".synthran/runs"),
    repository_root: Path = Path("."),
    runner: RunCommand = run_command,
    timeout_seconds: int = DEFAULT_DEPLOY_TIMEOUT_SECONDS,
    progress: TextIO | None = None,
) -> DeploymentResult:
    """Execute the narrow operator-authorized deployment in a detached worktree."""

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] {message}", file=progress, flush=True)

    report(f"network deployment started: run={run_id}")

    run_id = validate_run_id(run_id)
    if plan.profile != "default":
        raise NetworkRuntimeError("live golden-path deployment supports only profile=default")
    if plan.inventory.core_node.name == plan.inventory.ran_node.name:
        raise NetworkRuntimeError(
            "live golden-path deployment requires separate core and RAN nodes"
        )
    if timeout_seconds < 60 or timeout_seconds > 14400:
        raise NetworkRuntimeError("deployment timeout must be between 60 and 14400 seconds")
    known_hosts_value = os.environ.get("SYNTHRAN_KNOWN_HOSTS")
    if not known_hosts_value:
        raise NetworkRuntimeError(
            "live deployment requires SYNTHRAN_KNOWN_HOSTS from preparation authority"
        )

    known_hosts_path = Path(known_hosts_value).expanduser().resolve()
    if not known_hosts_path.is_file():
        raise NetworkRuntimeError(
            "SYNTHRAN_KNOWN_HOSTS does not name an existing file"
        )
    try:
        active_controller = verify_slices_controller(
            lock=lock,
            project=slices_project,
            experiment=slices_experiment,
            timeout_seconds=min(timeout_seconds, 300),
        )
    except SlicesControllerError as exc:
        raise NetworkRuntimeError(str(exc)) from exc

    preflight = load_fresh_live_evidence(
        path=live_evidence_path,
        inventory=plan.inventory,
        owner=owner,
        reservation_id=reservation_id,
        allocation_id=allocation_id,
        lock=lock,
        slices_project=slices_project,
        slices_experiment=slices_experiment,
    )
    if preflight.get("slices_controller") != active_controller.to_dict():
        raise NetworkRuntimeError(
            "live preflight evidence controller versions do not match the active shell"
        )
    checkout = validate_fiveg_checkout(lock, dependency_root)
    overlay_source = repository_root.resolve() / "deploy" / "ansible"
    if not (overlay_source / "golden-path-deploy.yml").is_file():
        raise NetworkRuntimeError("SynthRAN golden-path wrapper playbook is missing")
    overlay_sha256 = tree_sha256(overlay_source)

    run_root = run_root.resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    run_directory = run_root / run_id
    try:
        run_directory.mkdir()
    except FileExistsError as exc:
        raise NetworkRuntimeError("run directory already exists; choose a new run ID") from exc
    worktree = run_directory / "worktree"
    manifest_path = run_directory / "manifest.json"
    log_path = run_directory / "deployment.log"
    variables_path = run_directory / "locked-images.json"
    log_parts: list[str] = []

    def write_manifest(status: str, stage: str | None = None) -> None:
        atomic_json(
            manifest_path,
            _deployment_manifest(
                run_id=run_id,
                status=status,
                plan=plan,
                preflight=preflight,
                overlay_sha256=overlay_sha256,
                failure_stage=stage,
            ),
        )

    def finish_log() -> None:
        log_path.write_text(
            sanitize_deployment_text(
                "\n".join(log_parts),
                (
                    plan.inventory.path,
                    repository_root,
                    dependency_root,
                    run_directory,
                    live_evidence_path,
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
        log_parts.append(f"=== {name} ===")
        report(f"{name}: running...")
        started = monotonic()
        try:
            if streaming:
                if runner is run_command:
                    result = run_streaming_ansible_command(
                        command,
                        cwd,
                        environment,
                        timeout_seconds,
                        report=report,
                    )
                else:
                    result = runner(command, cwd, environment, timeout_seconds)
                    if progress is not None:
                        for line in result.stdout.splitlines():
                            parsed = parse_ansible_line(line)
                            if parsed is not None:
                                report(parsed)
            else:
                result = runner(command, cwd, environment, timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            elapsed = monotonic() - started
            report(f"{name}: FAILED ({elapsed:.1f}s)")
            write_manifest("failed", name)
            finish_log()
            raise NetworkRuntimeError(
                f"deployment stage {name} exceeded its timeout"
            ) from exc
        except (NetworkRuntimeError, OSError):
            elapsed = monotonic() - started
            report(f"{name}: FAILED ({elapsed:.1f}s)")
            write_manifest("failed", name)
            finish_log()
            raise

        elapsed = monotonic() - started
        log_parts.append(result.stdout)
        log_parts.append(result.stderr)
        if result.returncode != 0:
            report(f"{name}: FAILED ({elapsed:.1f}s)")
            write_manifest("failed", name)
            finish_log()
            raise NetworkRuntimeError(
                f"deployment stage {name} failed; see the sanitized run log"
            )
        report(f"{name}: OK ({elapsed:.1f}s)")
        return result

    write_manifest("running")
    stage(
        "isolated-worktree",
        (
            "git",
            "-C",
            str(checkout),
            "worktree",
            "add",
            "--detach",
            str(worktree),
            plan.fiveg_ansible_commit,
        ),
        repository_root,
    )
    head = stage(
        "verify-worktree",
        ("git", "rev-parse", "HEAD"),
        worktree,
    ).stdout.strip()
    if head != plan.fiveg_ansible_commit:
        write_manifest("failed", "verify-worktree")
        finish_log()
        raise NetworkRuntimeError("isolated worktree commit does not match the lock")

    overlay_directory = worktree / ".synthran"
    try:
        shutil.copytree(overlay_source, overlay_directory)
        wrapper = overlay_directory / "golden-path-deploy.yml"
        atomic_json(
            variables_path,
            {"synthran_images": golden_path_image_variables(lock)},
        )
    except OSError as exc:
        write_manifest("failed", "prepare-overlay")
        finish_log()
        raise NetworkRuntimeError("unable to prepare the isolated deployment overlay") from exc

    report("upstream-overlay: running...")
    try:
        apply_network_overlay(worktree)
    except UpstreamOverlayError as exc:
        write_manifest("failed", "upstream-overlay")
        log_parts.append(f"=== upstream-overlay ===\n{exc}")
        finish_log()
        report("upstream-overlay: FAILED")
        raise NetworkRuntimeError(str(exc)) from exc
    log_parts.append("=== upstream-overlay ===\nexact pinned-source transformations applied")
    report("upstream-overlay: OK")

    collections = run_directory / "collections"
    environment = dict(os.environ)
    environment.update(
        {
            "ANSIBLE_COLLECTIONS_PATH": str(collections),
            "ANSIBLE_HOST_KEY_CHECKING": "True",
            "ANSIBLE_STDOUT_CALLBACK": "ansible.builtin.default",
            "ANSIBLE_SSH_ARGS": (
                "-o ControlMaster=auto -o ControlPersist=60s "
                "-o StrictHostKeyChecking=yes "
                "-o UserKnownHostsFile="
                f"{shlex.quote(str(known_hosts_path))}"
            ),
            "ANSIBLE_NOCOLOR": "True",
            "ANSIBLE_RETRY_FILES_ENABLED": "False",
            "ANSIBLE_ROLES_PATH": str(worktree / "roles"),
        }
    )
    stage(
        "ansible-collections",
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
    playbook_command = (
        "ansible-playbook",
        "-i",
        str(plan.inventory.path.resolve()),
        "-e",
        f"fiveg_profile={plan.profile}",
        "-e",
        f"repo_branch={plan.open5gs_k8s_commit}",
        "-e",
        f"version={plan.srsran_helm_commit}",
        "-e",
        f"synthran_run_id={run_id}",
        "-e",
        "ue_count=1",
        "-e",
        "deployment_option=open5gs",
        "-e",
        f"@{variables_path}",
        str(wrapper),
    )
    stage(
        "ansible-syntax",
        (*playbook_command[:-1], "--syntax-check", playbook_command[-1]),
        worktree,
        environment,
    )
    stage(
        "ansible-deployment",
        playbook_command,
        worktree,
        environment,
        streaming=True,
    )
    write_manifest("deployed-unverified")
    finish_log()
    report("network deployment: DEPLOYED, verification still required")
    return DeploymentResult(run_id, run_directory, manifest_path, log_path)


@dataclass(frozen=True)
class VerificationCheck:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class NetworkVerificationReport:
    run_id: str
    generated_at_utc: datetime
    inventory_sha256: str
    dependencies: Mapping[str, str]
    checks: tuple[VerificationCheck, ...]
    pdu_address: str | None = None

    @property
    def ready(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": NETWORK_EVIDENCE_SCHEMA,
            "run_id": self.run_id,
            "generated_at_utc": self.generated_at_utc.isoformat().replace("+00:00", "Z"),
            "inventory_sha256": self.inventory_sha256,
            "dependencies": dict(self.dependencies),
            "path": {
                "core": "open5gs",
                "ran": "srsRAN",
                "radio": "rfsim",
                "slice": "slice1",
                "sst": 1,
                "dnn": "internet",
                "ue_interface": GOLDEN_PATH_INTERFACE,
                "pdu_address": self.pdu_address,
                "pdu_network": str(GOLDEN_PATH_PDU_NETWORK),
            },
            "checks": [
                {"name": check.name, "passed": check.passed, "detail": check.detail}
                for check in self.checks
            ],
            "ready": self.ready,
        }

    def render(self) -> str:
        lines = [f"SynthRAN network verification ({self.run_id})"]
        for check in self.checks:
            lines.append(
                f"[{'PASS' if check.passed else 'FAIL'}] {check.name}: {check.detail}"
            )
        lines.append(f"Result: {'PATH PROVEN' if self.ready else 'NOT PROVEN'}")
        return "\n".join(lines)


def _remote_json(
    runner: Runner,
    inventory: NetworkInventory,
    command: str,
    timeout_seconds: int,
    label: str,
) -> Any:
    result = runner(
        ssh_command(inventory.core_node, "sh", "-c", command),
        timeout_seconds,
    )
    if result.returncode != 0:
        raise NetworkRuntimeError(f"{label} failed")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise NetworkRuntimeError(f"{label} did not return JSON") from exc


def _one_ready_pod(payload: Any, label: str, run_id: str) -> Mapping[str, Any]:
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise NetworkRuntimeError(f"{label} pod evidence is malformed")
    raw_items = payload["items"]
    if not all(isinstance(item, dict) for item in raw_items):
        raise NetworkRuntimeError(f"{label} pod evidence is malformed")
    items = [
        item
        for item in raw_items
        if not (
            isinstance(item.get("metadata"), dict)
            and item["metadata"].get("deletionTimestamp") is not None
        )
    ]
    if len(items) != 1:
        raise NetworkRuntimeError(f"expected exactly one {label} pod")
    pod = items[0]
    metadata = pod.get("metadata")
    status = pod.get("status")
    if not isinstance(metadata, dict) or not isinstance(status, dict):
        raise NetworkRuntimeError(f"{label} pod evidence is incomplete")
    labels = metadata.get("labels")
    if not isinstance(labels, dict) or labels.get("synthran.run/id") != run_id:
        raise NetworkRuntimeError(f"{label} pod is not owned by this run ID")
    conditions = status.get("conditions")
    ready = isinstance(conditions, list) and any(
        isinstance(item, dict)
        and item.get("type") == "Ready"
        and item.get("status") == "True"
        for item in conditions
    )
    if not ready:
        raise NetworkRuntimeError(f"{label} pod is not Ready")
    name = metadata.get("name")
    if not isinstance(name, str) or not KUBERNETES_NAME_RE.fullmatch(name):
        raise NetworkRuntimeError(f"{label} pod name is unsafe")
    return pod


def _require_image_digest(
    pod: Mapping[str, Any],
    *,
    container_name: str,
    expected_reference: str,
    label: str,
) -> None:
    status = pod.get("status")
    statuses = status.get("containerStatuses") if isinstance(status, dict) else None
    if not isinstance(statuses, list):
        raise NetworkRuntimeError(f"{label} image evidence is unavailable")
    expected_digest = expected_reference.rsplit("@", 1)[1]
    for item in statuses:
        if isinstance(item, dict) and item.get("name") == container_name:
            image_id = item.get("imageID")
            state = item.get("state")
            if (
                isinstance(image_id, str)
                and expected_digest in image_id
                and item.get("ready") is True
                and isinstance(state, dict)
                and isinstance(state.get("running"), dict)
            ):
                return
            raise NetworkRuntimeError(
                f"{label} container is not healthy on the locked digest"
            )
    raise NetworkRuntimeError(f"{label} container was not found")


def verify_network_path(
    *,
    inventory: NetworkInventory,
    lock: DependencyLock,
    run_id: str,
    runner: Runner = subprocess_runner,
    timeout_seconds: int = 30,
    now: datetime | None = None,
) -> NetworkVerificationReport:
    """Prove gNB, srsUE, UE PDU tunnel, and slice-one UPF routing."""

    run_id = validate_run_id(run_id)
    images = golden_path_image_variables(lock)
    checks: list[VerificationCheck] = []
    pdu_address: str | None = None

    def record(name: str, operation: Callable[[], str]) -> bool:
        try:
            detail = operation()
        except (NetworkRuntimeError, LivePreflightError) as exc:
            checks.append(VerificationCheck(name, False, str(exc)))
            return False
        checks.append(VerificationCheck(name, True, detail))
        return True

    pod_specs = (
        ("gnb", "app=srsran,component=gnb", "gnb", images["srsran_gnb"]),
        ("srsue", "app=srsran,component=ue", "ue", images["srsran_ue"]),
        ("slice1-upf", "app=open5gs,nf=upf,name=upf1", "upf", images["open5gs"]),
    )
    pods: dict[str, Mapping[str, Any]] = {}
    for label, selector, container, image in pod_specs:
        def discover(
            label: str = label,
            selector: str = selector,
            container: str = container,
            image: str = image,
        ) -> str:
            payload = _remote_json(
                runner,
                inventory,
                "KUBECONFIG=/etc/kubernetes/admin.conf "
                f"kubectl get pods -n {GOLDEN_PATH_NAMESPACE} -l {selector} -o json",
                timeout_seconds,
                f"{label} discovery",
            )
            pod = _one_ready_pod(payload, label, run_id)
            _require_image_digest(
                pod,
                container_name=container,
                expected_reference=image,
                label=label,
            )
            if label == "gnb":
                _require_image_digest(
                    pod,
                    container_name="gnb-logs",
                    expected_reference=images["busybox_1_36"],
                    label="gnb log helper",
                )
            pods[label] = pod
            return "one run-owned pod has healthy digest-locked containers"

        record(label, discover)

    gnb_pod = pods.get("gnb")
    if gnb_pod is None:
        checks.append(
            VerificationCheck(
                "gnb-cell",
                False,
                "not probed because the gNB pod check failed",
            )
        )
    else:
        gnb_name = str(gnb_pod["metadata"]["name"])

        def verify_gnb_cell() -> str:
            result = runner(
                ssh_command(
                    inventory.core_node,
                    "sh",
                    "-c",
                    "KUBECONFIG=/etc/kubernetes/admin.conf "
                    f"kubectl exec -n {GOLDEN_PATH_NAMESPACE} {gnb_name} "
                    "-c gnb-logs -- grep -q 'Cell was activated' "
                    "/var/log/gnb.log",
                ),
                timeout_seconds,
            )
            if result.returncode != 0:
                raise NetworkRuntimeError("gNB cell activation was not proven")
            return "the locked gNB log reports an activated cell"

        record("gnb-cell", verify_gnb_cell)

    ue_pod = pods.get("srsue")
    if ue_pod is None:
        checks.append(
            VerificationCheck(
                "ue-tunnel",
                False,
                "not probed because the srsUE pod check failed",
            )
        )
    else:
        ue_name = str(ue_pod["metadata"]["name"])

        def verify_ue_tunnel() -> str:
            nonlocal pdu_address
            addresses = _remote_json(
                runner,
                inventory,
                "KUBECONFIG=/etc/kubernetes/admin.conf "
                f"kubectl exec -n {GOLDEN_PATH_NAMESPACE} {ue_name} -c ue -- "
                f"ip -j address show dev {GOLDEN_PATH_INTERFACE}",
                timeout_seconds,
                "UE tunnel probe",
            )
            if not isinstance(addresses, list) or len(addresses) != 1:
                raise NetworkRuntimeError("UE tunnel interface evidence is malformed")
            interface = addresses[0]
            if not isinstance(interface, dict) or "UP" not in interface.get("flags", []):
                raise NetworkRuntimeError(f"{GOLDEN_PATH_INTERFACE} is not UP")
            address_info = interface.get("addr_info")
            candidates: list[ipaddress.IPv4Address] = []
            if isinstance(address_info, list):
                for item in address_info:
                    if isinstance(item, dict) and item.get("family") == "inet":
                        try:
                            candidates.append(ipaddress.ip_address(item.get("local")))
                        except ValueError:
                            continue
            selected = next(
                (
                    address
                    for address in candidates
                    if isinstance(address, ipaddress.IPv4Address)
                    and address in GOLDEN_PATH_PDU_NETWORK
                ),
                None,
            )
            if selected is None:
                raise NetworkRuntimeError("UE has no slice-one PDU address")
            routes = _remote_json(
                runner,
                inventory,
                "KUBECONFIG=/etc/kubernetes/admin.conf "
                f"kubectl exec -n {GOLDEN_PATH_NAMESPACE} {ue_name} -c ue -- ip -j route show",
                timeout_seconds,
                "UE route probe",
            )
            if not isinstance(routes, list) or not any(
                isinstance(route, dict)
                and route.get("dst") == str(GOLDEN_PATH_PDU_NETWORK)
                and route.get("dev") == GOLDEN_PATH_INTERFACE
                for route in routes
            ):
                raise NetworkRuntimeError("UE slice-one route is missing")
            pdu_address = str(selected)
            return f"{GOLDEN_PATH_INTERFACE} is UP with the expected PDU address and route"

        record("ue-tunnel", verify_ue_tunnel)

    upf_pod = pods.get("slice1-upf")
    if upf_pod is None:
        checks.append(
            VerificationCheck(
                "upf-route",
                False,
                "not probed because the slice-one UPF pod check failed",
            )
        )
    else:
        upf_name = str(upf_pod["metadata"]["name"])

        def verify_upf_route() -> str:
            routes = _remote_json(
                runner,
                inventory,
                "KUBECONFIG=/etc/kubernetes/admin.conf "
                f"kubectl exec -n {GOLDEN_PATH_NAMESPACE} {upf_name} -- ip -j route show",
                timeout_seconds,
                "UPF route probe",
            )
            if not isinstance(routes, list) or not any(
                isinstance(route, dict)
                and route.get("dst") == str(GOLDEN_PATH_PDU_NETWORK)
                and route.get("dev") == "ogstun"
                for route in routes
            ):
                raise NetworkRuntimeError("slice-one UPF route through ogstun is missing")
            return "slice-one PDU network is selected through ogstun"

        record("upf-route", verify_upf_route)

    dependencies = {
        item.name: item.commit
        for item in lock.git
        if item.name in {"fiveg_ansible", "open5gs_k8s", "srsran_helm"}
    }
    return NetworkVerificationReport(
        run_id=run_id,
        generated_at_utc=(now or datetime.now(timezone.utc)).astimezone(timezone.utc),
        inventory_sha256=inventory.sha256,
        dependencies=dependencies,
        checks=tuple(checks),
        pdu_address=pdu_address,
    )


def load_deployment_manifest(
    *,
    path: Path,
    run_id: str,
    inventory: NetworkInventory,
    lock: DependencyLock,
    slices_project: str,
    slices_experiment: str,
) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise NetworkRuntimeError("deployment manifest was not found") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NetworkRuntimeError("deployment manifest must be readable JSON") from exc
    if not isinstance(payload, dict) or payload.get("schema") != DEPLOYMENT_SCHEMA:
        raise NetworkRuntimeError("deployment manifest schema is unsupported")
    if payload.get("run_id") != validate_run_id(run_id):
        raise NetworkRuntimeError("deployment manifest run ID does not match")
    status = payload.get("status")
    if status not in ("deployed-unverified", "path-proven"):
        raise NetworkRuntimeError(
            f"deployment manifest is not awaiting verification (status={status})"
        )
    inventory_data = payload.get("inventory")
    if not isinstance(inventory_data, dict) or inventory_data.get("sha256") != inventory.sha256:
        raise NetworkRuntimeError("deployment manifest inventory does not match")
    expected = {
        item.name: item.commit
        for item in lock.git
        if item.name in {"fiveg_ansible", "open5gs_k8s", "srsran_helm"}
    }
    if payload.get("dependencies") != expected:
        raise NetworkRuntimeError("deployment manifest dependencies do not match the lock")

    manifest_lock_digest = payload.get("dependency_lock_sha256")
    if (
        not isinstance(manifest_lock_digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", manifest_lock_digest) is None
    ):
        raise NetworkRuntimeError(
            "deployment manifest dependency lock provenance is invalid"
        )

    controller = payload.get("slices_controller")
    if (
        not isinstance(controller, dict)
        or controller.get("dependency_lock_sha256") != manifest_lock_digest
        or controller.get("project_fingerprint") != context_fingerprint(slices_project)
        or controller.get("experiment_fingerprint") != context_fingerprint(slices_experiment)
    ):
        raise NetworkRuntimeError("deployment manifest SLICES context does not match")
    return payload


def save_network_evidence(
    report: NetworkVerificationReport,
    destination: Path,
    manifest_path: Path | None = None,
) -> None:
    atomic_json(destination, report.to_dict())
    if manifest_path is not None and report.ready:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if payload.get("status") == "deployed-unverified":
            payload["status"] = "path-proven"
        payload["updated_at_utc"] = (
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )
        payload["network_evidence"] = destination.name
        atomic_json(manifest_path, payload)
