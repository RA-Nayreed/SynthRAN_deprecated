"""Explicit operator-run preparation of SLICES resources for the golden path."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile
from time import monotonic
from typing import Any, Mapping, Sequence, TextIO
from zoneinfo import ZoneInfo

from synthran.ansible_streaming import parse_ansible_line, run_streaming_ansible_command
from synthran.dependencies import DependencyLock
from synthran.fiveg_ansible import NetworkInventory, parse_inventory, validate_fiveg_checkout
from synthran.live_preflight import (
    SAFE_IDENTIFIER_RE,
    CommandResult,
    LivePreflightError,
    verify_reservation,
)
from synthran.network.runtime import (
    RunCommand,
    atomic_json,
    run_command,
    sanitize_deployment_text,
    tree_sha256,
    validate_run_id,
)
from synthran.slices_controller import SlicesControllerError, verify_slices_controller
from synthran.upstream_overlay import UpstreamOverlayError, apply_preparation_overlay


PREPARATION_SCHEMA = "synthran/resource-preparation/v1alpha1"
DEFAULT_DURATION_MINUTES = 120
DEFAULT_PREPARATION_TIMEOUT_SECONDS = 3600
RESERVATION_ID_RE = re.compile(r"^[0-9]+$")
POS_TIMEZONE = ZoneInfo("Europe/Berlin")


class ResourcePreparationError(RuntimeError):
    """Raised when resource preparation cannot continue safely."""


@dataclass(frozen=True)
class NodeSpec:
    name: str
    nic_interface: str
    ip: str
    storage: str


SUPPORTED_NODES: Mapping[str, NodeSpec] = {
    "sopnode-f1": NodeSpec("sopnode-f1", "ens2f1", "172.28.2.76", "sda1"),
    "sopnode-f2": NodeSpec("sopnode-f2", "ens2f1", "172.28.2.77", "sda1"),
    "sopnode-f3": NodeSpec("sopnode-f3", "ens15f1", "172.28.2.95", "sdb2"),
    "sopnode-w3": NodeSpec("sopnode-w3", "enp59s0f1np1", "172.28.2.71", "sda1"),
}


def _node(value: str, role: str) -> NodeSpec:
    try:
        return SUPPORTED_NODES[value]
    except KeyError as exc:
        supported = ", ".join(sorted(SUPPORTED_NODES))
        raise ResourcePreparationError(
            f"unsupported {role} node; choose one of: {supported}"
        ) from exc


def build_preparation_inventory(
    *, core_node: str, ran_node: str, source: Path
) -> tuple[str, NetworkInventory]:
    """Build the exact ignored inventory for one supported Duckburg node pair."""

    core = _node(core_node, "core")
    ran = _node(ran_node, "RAN")
    if core.name == ran.name:
        raise ResourcePreparationError(
            "resource preparation requires separate core and RAN nodes"
        )
    text = f'''[webshell]
localhost ansible_connection=local

[core_node]
{core.name} ansible_user=root nic_interface={core.nic_interface} ip={core.ip} storage={core.storage}

[ran_node]
{ran.name} ansible_user=root nic_interface={ran.nic_interface} ip={ran.ip} storage={ran.storage} boot_mode=live

[monitor_node]

[sopnodes:children]
core_node
ran_node

[k8s_workers:children]
ran_node

[all:vars]
core="open5gs"
ran="srsRAN"
core_node_name="{core.name}"
ran_node_name="{ran.name}"
rru="rfsim"
fhi72=false
aw2s=false
f3_ran={'true' if ran.name == 'sopnode-f3' else 'false'}
bridge_enabled=true
monitoring_enabled=false
'''
    return text, parse_inventory(text, source=source)


def _validate_duration(value: int) -> int:
    if value < 30 or value > 720:
        raise ResourcePreparationError(
            "reservation duration must be between 30 and 720 minutes"
        )
    return value


def _validate_authority(value: str, label: str) -> str:
    if not SAFE_IDENTIFIER_RE.fullmatch(value):
        raise ResourcePreparationError(
            f"{label} must contain only letters, numbers, '.', '_', ':', or '-'"
        )
    return value


def _fiveg_commit(lock: DependencyLock) -> str:
    for dependency in lock.git:
        if dependency.name == "fiveg_ansible":
            return dependency.commit
    raise ResourcePreparationError("dependency lock is missing fiveg_ansible")


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _json_records(text: str, label: str) -> tuple[Mapping[str, Any], ...]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ResourcePreparationError(f"{label} did not return JSON") from exc
    if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
        raise ResourcePreparationError(f"{label} must return an array of objects")
    return tuple(payload)


def _json_object(text: str, label: str) -> Mapping[str, Any]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ResourcePreparationError(f"{label} did not return JSON") from exc
    if not isinstance(payload, dict):
        raise ResourcePreparationError(f"{label} must return one object")
    return payload


def _record_nodes(value: object, label: str) -> set[str]:
    if not isinstance(value, list):
        raise ResourcePreparationError(f"{label} has no node array")
    nodes: set[str] = set()
    for item in value:
        if isinstance(item, str) and item.strip():
            nodes.add(item.strip())
            continue
        if isinstance(item, dict):
            candidate = next(
                (
                    item.get(key)
                    for key in ("id", "name", "node")
                    if isinstance(item.get(key), str) and item.get(key).strip()
                ),
                None,
            )
            if candidate is not None:
                nodes.add(candidate.strip())
                continue
        raise ResourcePreparationError(f"{label} contains an invalid node")
    return nodes


def _record_identifier(record: Mapping[str, Any], label: str) -> str:
    value = record.get("id")
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return str(value)
    if isinstance(value, str) and value.strip():
        return _validate_authority(value.strip(), label)
    raise ResourcePreparationError(f"{label} is missing from POS output")


def _record_owner(record: Mapping[str, Any], label: str) -> str:
    value = record.get("owner")
    if not isinstance(value, str) or not value.strip():
        raise ResourcePreparationError(f"{label} is missing from POS output")
    return value.strip()


def _record_time(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ResourcePreparationError(f"{label} is missing from POS output")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ResourcePreparationError(f"{label} is not ISO-8601") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=POS_TIMEZONE)
    return parsed.astimezone(timezone.utc)


def _reservation_ids(text: str) -> set[str]:
    identifiers: set[str] = set()
    for record in _json_records(text, "POS reservation query"):
        identifier = _record_identifier(record, "reservation ID")
        if not RESERVATION_ID_RE.fullmatch(identifier):
            raise ResourcePreparationError(
                "POS reservation query returned a non-numeric identifier"
            )
        identifiers.add(identifier)
    return identifiers


def _discover_active_reservation(
    text: str,
    *,
    owner: str,
    nodes: set[str],
    now: datetime,
) -> str | None:
    """Choose the active owned reservation covering the nodes with most runway."""

    candidates: list[tuple[datetime, datetime, int, str]] = []
    for record in _json_records(text, "POS reservation query"):
        identifier = _record_identifier(record, "reservation ID")
        if not RESERVATION_ID_RE.fullmatch(identifier):
            raise ResourcePreparationError(
                "POS reservation query returned a non-numeric identifier"
            )
        if _record_owner(record, "reservation owner") != owner:
            continue
        record_nodes = _record_nodes(record.get("nodes"), "POS reservation")
        if not nodes.issubset(record_nodes):
            continue
        starts_at = _record_time(record.get("start_date"), "reservation start")
        ends_at = _record_time(record.get("end_date"), "reservation end")
        if starts_at <= now < ends_at:
            candidates.append((ends_at, starts_at, int(identifier), identifier))
    if not candidates:
        return None
    return max(candidates)[3]


@dataclass(frozen=True)
class ResourcePreparationPlan:
    run_id: str
    core_node: NodeSpec
    ran_node: NodeSpec
    duration_minutes: int
    fiveg_ansible_commit: str
    reservation_action: str
    bootstrap_status: str
    bootstrap_reason: str

    def to_dict(self) -> dict[str, Any]:
        reservation_commands = (
            ["pos calendar list --filter owner='<operator>' --json"]
            if self.reservation_action == "reuse"
            else [
                "pos calendar list --filter owner='<operator>' --json",
                "pos calendar create -d "
                f"{self.duration_minutes} -s now {self.core_node.name} {self.ran_node.name} "
                "(only when no active owned reservation covers both nodes)",
            ]
        )
        return {
            "schema": PREPARATION_SCHEMA,
            "execution_enabled": False,
            "run_id": self.run_id,
            "nodes": {"core": self.core_node.name, "ran": self.ran_node.name},
            "duration_minutes": self.duration_minutes,
            "reservation_action": self.reservation_action,
            "dependencies": {"fiveg_ansible": self.fiveg_ansible_commit},
            "resource_bootstrap": {
                "status": self.bootstrap_status,
                "reason": self.bootstrap_reason,
            },
            "commands": [
                "git -C '<locked-fiveg-checkout>' worktree add --detach "
                f"'<isolated-worktree>' {self.fiveg_ansible_commit}",
                "apply SynthRAN upstream preparation overlay",
                "ansible-playbook --syntax-check '<upstream-deploy.yml>'",
                *reservation_commands,
                f"pos allocations free -k {self.core_node.name} (only if stale/conflicting)",
                f"pos allocations free -k {self.ran_node.name} (only if stale/conflicting)",
                f"pos allocations allocate {self.core_node.name} {self.ran_node.name}",
                "ansible-playbook '<upstream-deploy.yml>' -e synthran_prepare_only=true",
                "ansible-playbook '<prepare-network.yml>'",
                "ansible-playbook '<prepare-tools.yml>'",
            ],
        }

    def render(self, *, as_json: bool = False) -> str:
        payload = self.to_dict()
        if as_json:
            return json.dumps(payload, indent=2, sort_keys=True)
        lines = [
            "SynthRAN SLICES resource preparation plan (NON-EXECUTING)",
            f"Run ID: {self.run_id}",
            f"Nodes: core={self.core_node.name} ran={self.ran_node.name}",
            f"Reservation: {self.reservation_action}",
            f"Bootstrap: {self.bootstrap_status.upper()} - {self.bootstrap_reason}",
            "Planned commands:",
        ]
        lines.extend(f"  {command}" for command in payload["commands"])
        lines.extend(
            (
                "Live execution images and resets both nodes, rebuilds Kubernetes, "
                "reconciles the keyed GRE foundation, and installs locked deployment tools.",
                "It stops before Open5GS or srsRAN deployment.",
            )
        )
        return "\n".join(lines)


def build_resource_preparation_plan(
    *,
    lock: DependencyLock,
    core_node: str,
    ran_node: str,
    duration_minutes: int,
    run_id: str,
    reservation_id: str | None = None,
) -> ResourcePreparationPlan:
    """Build a redacted plan without contacting POS or writing local state."""

    try:
        validated_run_id = validate_run_id(run_id)
    except Exception as exc:
        raise ResourcePreparationError(str(exc)) from exc
    core = _node(core_node, "core")
    ran = _node(ran_node, "RAN")
    if core.name == ran.name:
        raise ResourcePreparationError(
            "resource preparation requires separate core and RAN nodes"
        )
    if reservation_id is not None and not RESERVATION_ID_RE.fullmatch(reservation_id):
        raise ResourcePreparationError("reservation ID must be numeric")
    bootstrap = lock.raw.get("resource_bootstrap")
    if not isinstance(bootstrap, dict):
        raise ResourcePreparationError("resource bootstrap lock state is unavailable")
    bootstrap_status = bootstrap.get("status")
    bootstrap_reason = bootstrap.get("reason")
    if not isinstance(bootstrap_status, str) or not isinstance(bootstrap_reason, str):
        raise ResourcePreparationError("resource bootstrap lock state is malformed")
    return ResourcePreparationPlan(
        run_id=validated_run_id,
        core_node=core,
        ran_node=ran,
        duration_minutes=_validate_duration(duration_minutes),
        fiveg_ansible_commit=_fiveg_commit(lock),
        reservation_action="reuse" if reservation_id is not None else "discover-or-create",
        bootstrap_status=bootstrap_status,
        bootstrap_reason=bootstrap_reason,
    )


def locked_preparation_variables(lock: DependencyLock) -> dict[str, Any]:
    tools = lock.raw.get("tools")
    remote_python = lock.raw.get("remote_python")
    packages = remote_python.get("packages") if isinstance(remote_python, dict) else None
    if not isinstance(tools, dict) or not isinstance(packages, dict):
        raise ResourcePreparationError("locked preparation tools are unavailable")
    yq = tools.get("yq_linux_amd64")
    helm = tools.get("helm_linux_amd64")
    if not isinstance(yq, dict) or not isinstance(helm, dict):
        raise ResourcePreparationError("locked yq and Helm tools are required")
    required_fields = ("version", "sha256", "url", "path")
    if any(
        not isinstance(tool.get(field), str) or not tool[field]
        for tool in (yq, helm)
        for field in required_fields
    ):
        raise ResourcePreparationError("locked preparation tool metadata is malformed")
    if "kubernetes" not in packages:
        raise ResourcePreparationError(
            "remote Python package lock must include kubernetes"
        )
    return {
        "synthran_remote_python_packages": [
            f"{name}=={version}" for name, version in sorted(packages.items())
        ],
        "synthran_remote_python_expected": dict(sorted(packages.items())),
        "synthran_yq": {field: yq[field] for field in required_fields},
        "synthran_helm": {field: helm[field] for field in required_fields},
    }


def _allocation_state_before_mutation(
    text: str, *, owner: str, nodes: set[str]
) -> tuple[str, str | None, set[str]]:
    """Classify selected-node allocation state after reservation authority is proven."""

    touched: list[tuple[Mapping[str, Any], set[str]]] = []
    reclaim_nodes: set[str] = set()
    for record in _json_records(text, "POS allocation list"):
        record_nodes = _record_nodes(record.get("nodes"), "POS allocation")
        intersection = nodes.intersection(record_nodes)
        if intersection:
            touched.append((record, record_nodes))
            reclaim_nodes.update(intersection)
    if not touched:
        return "create", None, set()
    if len(touched) == 1:
        record, record_nodes = touched[0]
        if (
            nodes.issubset(record_nodes)
            and _record_owner(record, "allocation owner") == owner
        ):
            return "reuse", _record_identifier(record, "allocation ID"), set()
    return "reclaim-and-create", None, reclaim_nodes


def _allocation_after_mutation(
    outputs: Mapping[str, str], *, owner: str
) -> str:
    identifiers: set[str] = set()
    for node, text in outputs.items():
        record = _json_object(text, f"POS allocation query for {node}")
        if _record_owner(record, "allocation owner") != owner:
            raise ResourcePreparationError(
                "a selected node allocation is not owned by the expected operator"
            )
        identifiers.add(_record_identifier(record, "allocation ID"))
    if len(identifiers) != 1:
        raise ResourcePreparationError(
            "selected nodes are not in one shared allocation"
        )
    return next(iter(identifiers))


def _preparation_manifest(
    *,
    plan: ResourcePreparationPlan,
    inventory: NetworkInventory,
    status: str,
    overlay_sha256: str,
    owner: str,
    reservation_id: str | None,
    allocation_id: str | None,
    slices_controller: Mapping[str, Any],
    reservation_action: str,
    allocation_action: str,
    failure_stage: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": PREPARATION_SCHEMA,
        "run_id": plan.run_id,
        "status": status,
        "updated_at_utc": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "inventory": inventory.redacted_summary(),
        "dependencies": {"fiveg_ansible": plan.fiveg_ansible_commit},
        "dependency_lock_sha256": slices_controller["dependency_lock_sha256"],
        "slices_controller": dict(slices_controller),
        "overlays": {"ansible_overlay_sha256": overlay_sha256},
        "authority": {
            "owner_fingerprint": _fingerprint(owner),
            "reservation_fingerprint": (
                _fingerprint(reservation_id) if reservation_id is not None else None
            ),
            "allocation_fingerprint": (
                _fingerprint(allocation_id) if allocation_id is not None else None
            ),
        },
        "reservation_action": reservation_action,
        "allocation_action": allocation_action,
        "inventory_file": "hosts.ini",
        "authority_file": "authority.env",
        "preparation_log": "preparation.log",
        "worktree": "worktree",
    }
    if failure_stage is not None:
        payload["failure_stage"] = failure_stage
    return payload


@dataclass(frozen=True)
class ResourcePreparationResult:
    run_id: str
    run_directory: Path
    inventory_path: Path
    authority_path: Path
    manifest_path: Path
    log_path: Path


def _write_authority(
    path: Path,
    *,
    owner: str,
    slices_project: str,
    slices_experiment: str,
    reservation_id: str | None,
    allocation_id: str | None,
) -> None:
    lines = [
        f"export SYNTHRAN_OWNER={owner}",
        f"export SYNTHRAN_SLICES_PROJECT={slices_project}",
        f"export SYNTHRAN_SLICES_EXPERIMENT={slices_experiment}",
        "export SYNTHRAN_KNOWN_HOSTS="
        f"{shlex.quote(str(path.parent / 'known_hosts'))}",
    ]
    if reservation_id is not None:
        lines.append(f"export SYNTHRAN_RESERVATION_ID={reservation_id}")
    if allocation_id is not None:
        lines.append(f"export SYNTHRAN_ALLOCATION_ID={allocation_id}")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.chmod(temporary, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            descriptor = -1
            handle.write("\n".join(lines) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        os.chmod(path, 0o600)
    except OSError:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise


def execute_resource_preparation(
    *,
    plan: ResourcePreparationPlan,
    lock: DependencyLock,
    dependency_root: Path,
    owner: str,
    slices_project: str,
    slices_experiment: str,
    reservation_id: str | None = None,
    run_root: Path = Path(".synthran/preparations"),
    repository_root: Path = Path("."),
    runner: RunCommand = run_command,
    timeout_seconds: int = DEFAULT_PREPARATION_TIMEOUT_SECONDS,
    now: datetime | None = None,
    progress: TextIO | None = None,
) -> ResourcePreparationResult:
    """Prepare an operator-authorized node pair and stop before 5G deployment."""

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] {message}", file=progress, flush=True)

    report(
        f"preparation started: run={plan.run_id} "
        f"core={plan.core_node.name} ran={plan.ran_node.name}"
    )
    owner = _validate_authority(owner, "owner")
    if plan.reservation_action == "reuse":
        if reservation_id is None or not RESERVATION_ID_RE.fullmatch(reservation_id):
            raise ResourcePreparationError("a numeric reservation ID is required")
    elif plan.reservation_action == "discover-or-create":
        if reservation_id is not None:
            raise ResourcePreparationError(
                "automatic reservation discovery cannot also receive a reservation ID"
            )
    else:
        raise ResourcePreparationError("unsupported reservation action")
    if timeout_seconds < 60 or timeout_seconds > 14400:
        raise ResourcePreparationError(
            "preparation timeout must be between 60 and 14400 seconds"
        )
    if plan.bootstrap_status != "ready":
        raise ResourcePreparationError(
            "live resource preparation is blocked by the dependency lock: "
            + plan.bootstrap_reason
        )

    report("controller-preflight: running...")
    try:
        controller_report = verify_slices_controller(
            lock=lock,
            project=slices_project,
            experiment=slices_experiment,
            timeout_seconds=min(timeout_seconds, 300),
        )
    except SlicesControllerError as exc:
        report("controller-preflight: FAILED")
        raise ResourcePreparationError(str(exc)) from exc
    report("controller-preflight: OK")

    missing_tools = [
        name
        for name in (
            "slices",
            "post5g",
            "pos",
            "git",
            "ansible-galaxy",
            "ansible-playbook",
        )
        if shutil.which(name) is None
    ]
    if missing_tools:
        raise ResourcePreparationError(
            "missing required command(s): " + ", ".join(missing_tools)
        )

    checkout = validate_fiveg_checkout(lock, dependency_root)
    overlay_source = repository_root.resolve() / "deploy" / "ansible"
    preparation_playbook = overlay_source / "prepare-tools.yml"
    network_foundation_playbook = overlay_source / "prepare-network.yml"
    preparation_requirements = overlay_source / "preparation-requirements.yml"
    if (
        not preparation_playbook.is_file()
        or not network_foundation_playbook.is_file()
        or not preparation_requirements.is_file()
    ):
        raise ResourcePreparationError(
            "SynthRAN resource preparation overlay is incomplete"
        )
    overlay_sha256 = tree_sha256(overlay_source)
    variables = locked_preparation_variables(lock)

    run_root = run_root.resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    run_directory = run_root / plan.run_id
    try:
        run_directory.mkdir()
    except FileExistsError as exc:
        raise ResourcePreparationError(
            "preparation directory already exists; choose a new run ID"
        ) from exc

    worktree = run_directory / "worktree"
    inventory_path = run_directory / "hosts.ini"
    variables_path = run_directory / "locked-tools.json"
    authority_path = run_directory / "authority.env"
    manifest_path = run_directory / "manifest.json"
    log_path = run_directory / "preparation.log"
    inventory_text, inventory = build_preparation_inventory(
        core_node=plan.core_node.name,
        ran_node=plan.ran_node.name,
        source=inventory_path,
    )
    inventory_path.write_text(inventory_text, encoding="utf-8", newline="\n")
    atomic_json(variables_path, variables)

    current_reservation = reservation_id
    current_allocation: str | None = None
    reservation_action = "reuse-explicit" if reservation_id is not None else "pending"
    allocation_action = "pending"
    log_parts: list[str] = []

    def write_manifest(status: str, stage_name: str | None = None) -> None:
        atomic_json(
            manifest_path,
            _preparation_manifest(
                plan=plan,
                inventory=inventory,
                status=status,
                overlay_sha256=overlay_sha256,
                owner=owner,
                reservation_id=current_reservation,
                allocation_id=current_allocation,
                slices_controller=controller_report.to_dict(),
                reservation_action=reservation_action,
                allocation_action=allocation_action,
                failure_stage=stage_name,
            ),
        )

    def finish_log() -> None:
        text = "\n".join(log_parts)
        for private_value in (owner, current_reservation, current_allocation):
            if private_value:
                text = text.replace(private_value, "<authority>")
        log_path.write_text(
            sanitize_deployment_text(
                text,
                (
                    repository_root,
                    dependency_root,
                    checkout,
                    run_directory,
                    inventory_path,
                ),
            ),
            encoding="utf-8",
            newline="\n",
        )

    def fail(stage_name: str, message: str) -> None:
        log_parts.append(message)
        write_manifest("failed", stage_name)
        finish_log()

    def persist_authority() -> None:
        try:
            _write_authority(
                authority_path,
                owner=owner,
                slices_project=slices_project,
                slices_experiment=slices_experiment,
                reservation_id=current_reservation,
                allocation_id=current_allocation,
            )
        except OSError as exc:
            fail("authority-file", "unable to write the private authority file")
            raise ResourcePreparationError(
                "unable to write the private preparation authority file"
            ) from exc

    def reservation_ids(text: str, stage_name: str) -> set[str]:
        try:
            return _reservation_ids(text)
        except ResourcePreparationError:
            fail(
                stage_name,
                "POS reservation output did not contain a safe numeric identifier set",
            )
            raise

    def stage(
        name: str,
        command: Sequence[str],
        cwd: Path,
        environment: Mapping[str, str] | None = None,
        *,
        retain_output: bool = True,
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
            fail(name, f"{name} exceeded its timeout")
            raise ResourcePreparationError(
                f"preparation stage {name} exceeded its timeout"
            ) from exc
        except (OSError, RuntimeError) as exc:
            elapsed = monotonic() - started
            report(f"{name}: FAILED ({elapsed:.1f}s)")
            fail(name, f"{name} could not be completed")
            raise ResourcePreparationError(
                f"preparation stage {name} could not be completed"
            ) from exc

        elapsed = monotonic() - started
        if retain_output:
            log_parts.extend((result.stdout, result.stderr))
        else:
            log_parts.append("provider output was intentionally not retained")
        if result.returncode != 0:
            report(f"{name}: FAILED ({elapsed:.1f}s)")
            fail(name, f"{name} returned nonzero")
            raise ResourcePreparationError(
                f"preparation stage {name} failed; see the sanitized preparation log"
            )
        report(f"{name}: OK ({elapsed:.1f}s)")
        return result

    write_manifest("running")
    persist_authority()
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
        fail("verify-worktree", "isolated worktree commit did not match the lock")
        raise ResourcePreparationError(
            "isolated worktree commit does not match the lock"
        )

    overlay_directory = worktree / ".synthran"
    try:
        shutil.copytree(overlay_source, overlay_directory)
    except OSError as exc:
        fail("prepare-overlay", "unable to copy the preparation overlay")
        raise ResourcePreparationError(
            "unable to prepare the isolated resource overlay"
        ) from exc

    report("upstream-overlay: running...")
    try:
        apply_preparation_overlay(worktree)
    except UpstreamOverlayError as exc:
        fail("upstream-overlay", str(exc))
        report("upstream-overlay: FAILED")
        raise ResourcePreparationError(str(exc)) from exc
    log_parts.append(
        "=== upstream-overlay ===\nexact pinned-source transformations applied"
    )
    report("upstream-overlay: OK")

    collections = run_directory / "collections"
    environment = dict(os.environ)
    environment.update(
        {
            "ANSIBLE_COLLECTIONS_PATH": str(collections),
            "ANSIBLE_HOST_KEY_CHECKING": "True",
            "ANSIBLE_NOCOLOR": "True",
            "ANSIBLE_RETRY_FILES_ENABLED": "False",
            "ANSIBLE_ROLES_PATH": str(worktree / "roles"),
            "ANSIBLE_SSH_ARGS": (
                "-o ControlMaster=auto -o ControlPersist=60s "
                "-o StrictHostKeyChecking=accept-new "
                "-o UserKnownHostsFile="
                f"{shlex.quote(str(run_directory / 'known_hosts'))}"
            ),
        }
    )
    stage(
        "ansible-collections",
        (
            "ansible-galaxy",
            "collection",
            "install",
            "-r",
            str(overlay_directory / "preparation-requirements.yml"),
            "-p",
            str(collections),
        ),
        worktree,
        environment,
    )
    upstream_command = (
        "ansible-playbook",
        "-i",
        str(inventory_path),
        "-e",
        "fiveg_profile=default",
        "-e",
        "synthran_prepare_only=true",
        "-e",
        "no_boot=false",
        str(worktree / "playbooks" / "deploy.yml"),
    )
    foundation_command = (
        "ansible-playbook",
        "-i",
        str(inventory_path),
        str(overlay_directory / "prepare-network.yml"),
    )
    tools_command = (
        "ansible-playbook",
        "-i",
        str(inventory_path),
        "-e",
        f"@{variables_path}",
        str(overlay_directory / "prepare-tools.yml"),
    )
    stage(
        "upstream-syntax",
        (*upstream_command[:-1], "--syntax-check", upstream_command[-1]),
        worktree,
        environment,
    )
    stage(
        "network-foundation-syntax",
        (*foundation_command[:-1], "--syntax-check", foundation_command[-1]),
        worktree,
        environment,
    )
    stage(
        "tool-preparation-syntax",
        (*tools_command[:-1], "--syntax-check", tools_command[-1]),
        worktree,
        environment,
    )

    reservation_query = (
        "pos",
        "calendar",
        "list",
        "--filter",
        f"owner={owner}",
        "--json",
    )
    provider_now = now or datetime.now(timezone.utc)
    if current_reservation is None:
        reservation_snapshot = stage(
            "reservation-inspection",
            reservation_query,
            repository_root,
            retain_output=False,
        ).stdout
        before_ids = reservation_ids(reservation_snapshot, "reservation-inspection")
        try:
            discovered = _discover_active_reservation(
                reservation_snapshot,
                owner=owner,
                nodes={plan.core_node.name, plan.ran_node.name},
                now=provider_now,
            )
        except ResourcePreparationError:
            fail("reservation-inspection", "active reservation discovery failed closed")
            raise
        if discovered is not None:
            current_reservation = discovered
            reservation_action = "reuse-discovered"
            report("reservation-discovery: active owned reservation selected")
        else:
            stage(
                "reservation-create",
                (
                    "pos",
                    "calendar",
                    "create",
                    "-d",
                    str(plan.duration_minutes),
                    "-s",
                    "now",
                    plan.core_node.name,
                    plan.ran_node.name,
                ),
                repository_root,
                retain_output=False,
            )
            after_snapshot = stage(
                "reservation-discovery",
                reservation_query,
                repository_root,
                retain_output=False,
            ).stdout
            after_ids = reservation_ids(after_snapshot, "reservation-discovery")
            new_ids = after_ids - before_ids
            if len(new_ids) != 1:
                fail(
                    "reservation-discovery",
                    "POS did not expose exactly one new numeric reservation identifier",
                )
                raise ResourcePreparationError(
                    "created reservation could not be identified unambiguously"
                )
            current_reservation = next(iter(new_ids))
            reservation_action = "create"
        persist_authority()
        write_manifest("running")

    def provider_runner(
        command: Sequence[str], probe_timeout: int
    ) -> CommandResult:
        return runner(command, repository_root, None, probe_timeout)

    try:
        verify_reservation(
            runner=provider_runner,
            reservation_id=current_reservation,
            owner=owner,
            nodes={plan.core_node.name, plan.ran_node.name},
            now=provider_now,
            timeout_seconds=timeout_seconds,
        )
    except (LivePreflightError, OSError, RuntimeError) as exc:
        fail("reservation-verification", "reservation verification failed closed")
        raise ResourcePreparationError(
            "created, discovered, or supplied reservation could not be verified"
        ) from exc
    log_parts.extend(
        (
            "=== reservation-verification ===",
            "reservation ownership, node coverage, and active time verified",
        )
    )

    allocation_list = stage(
        "allocation-inspection",
        ("pos", "allocations", "list", "--json"),
        repository_root,
        retain_output=False,
    ).stdout
    try:
        allocation_action, current_allocation, reclaim_nodes = (
            _allocation_state_before_mutation(
                allocation_list,
                owner=owner,
                nodes={plan.core_node.name, plan.ran_node.name},
            )
        )
    except ResourcePreparationError:
        fail("allocation-inspection", "selected node allocation state is malformed")
        raise

    if allocation_action == "reclaim-and-create":
        for role, node in (
            ("core", plan.core_node.name),
            ("ran", plan.ran_node.name),
        ):
            if node not in reclaim_nodes:
                continue
            stage(
                f"allocation-reclaim-{role}",
                ("pos", "allocations", "free", "-k", node),
                repository_root,
                retain_output=False,
            )
        current_allocation = None

    if current_allocation is None:
        stage(
            "allocation-create",
            (
                "pos",
                "allocations",
                "allocate",
                plan.core_node.name,
                plan.ran_node.name,
            ),
            repository_root,
            retain_output=False,
        )
        persist_authority()
        write_manifest("running")

    allocation_outputs = {
        node: stage(
            f"allocation-verification-{role}",
            ("pos", "allocations", "show", node),
            repository_root,
            retain_output=False,
        ).stdout
        for role, node in (
            ("core", plan.core_node.name),
            ("ran", plan.ran_node.name),
        )
    }
    try:
        observed_allocation = _allocation_after_mutation(
            allocation_outputs, owner=owner
        )
    except ResourcePreparationError:
        fail("allocation-verification", "shared allocation verification failed")
        raise
    if current_allocation is not None and observed_allocation != current_allocation:
        fail("allocation-verification", "allocation identity changed unexpectedly")
        raise ResourcePreparationError(
            "selected node allocation identity changed unexpectedly"
        )
    current_allocation = observed_allocation
    persist_authority()
    write_manifest("running")

    stage(
        "upstream-resource-preparation",
        upstream_command,
        worktree,
        environment,
        streaming=True,
    )
    stage(
        "network-foundation-reconciliation",
        foundation_command,
        worktree,
        environment,
        streaming=True,
    )
    stage(
        "locked-tool-preparation",
        tools_command,
        worktree,
        environment,
        streaming=True,
    )
    write_manifest("prepared")
    finish_log()
    report("resource preparation: COMPLETE")
    return ResourcePreparationResult(
        plan.run_id,
        run_directory,
        inventory_path,
        authority_path,
        manifest_path,
        log_path,
    )
