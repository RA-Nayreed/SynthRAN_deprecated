"""Pinned 5g-Ansible actuation boundary for one selected R2Lab UE.

SynthRAN owns authority, exact resource selection, postconditions, and public
evidence. Modem mechanics stay in the locked upstream Ansible roles and use the
same sanitized streaming path as every other SynthRAN Ansible operation.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Callable, Literal, Mapping, Sequence, TextIO

from synthran.ansible_streaming import parse_ansible_line, run_streaming_ansible_command
from synthran.dependencies import DependencyLock, load_lock
from synthran.fiveg_ansible import FiveGAnsibleError, validate_fiveg_checkout
from synthran.live_preflight import CommandResult
from synthran.network.runtime import run_command
from synthran.r2lab.controller import _configured_identity
from synthran.r2lab.hardware import PhysicalTopology


UeRoleAction = Literal["setup", "connect", "stop"]
RunCommand = Callable[[Sequence[str], Path, Mapping[str, str] | None, int], CommandResult]
CheckoutValidator = Callable[[DependencyLock, Path], Path]

FIVEG_PROFILE = "synthran"
UPSTREAM_PROFILE = "group_vars/all/5g_profile_default.yaml"
SETUP_ROLE = "roles/r2lab/ue/setup/tasks/main.yml"
CONNECT_ROLE = "roles/r2lab/ue/connect/tasks/main.yml"
STOP_ROLE = "roles/r2lab/ue/stop/tasks/main.yml"
OPEN5GS_SLICE_NAME = "slice1"
OPEN5GS_DNN = "internet"
OPEN5GS_UE_PREFIX = "12.1.1"
OPEN5GS_UPF_ADDRESS = f"{OPEN5GS_UE_PREFIX}.1"

_ROLE_TIMEOUT_FLOORS: Mapping[UeRoleAction, int] = {
    "setup": 600,
    "connect": 300,
    "stop": 120,
}
ROLE_TIMEOUT_SECONDS = 900


class R2LabUeAnsibleError(RuntimeError):
    """Raised when selected-UE actuation through pinned 5g-Ansible fails."""


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, object]) -> Path:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(dict(payload), indent=2, sort_keys=True) + "\n"
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=path.parent,
            delete=False,
        ) as stream:
            stream.write(content)
            temporary = Path(stream.name)
        temporary.replace(path)
    except OSError as exc:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise R2LabUeAnsibleError("physical UE role evidence could not be persisted") from exc
    return path


def _dependency_commit(lock: DependencyLock) -> str:
    dependency = next((item for item in lock.git if item.name == "fiveg_ansible"), None)
    if dependency is None:
        raise R2LabUeAnsibleError("dependency lock does not define fiveg_ansible")
    return dependency.commit


def _validate_pinned_profile(checkout: Path, topology: PhysicalTopology) -> None:
    required = (UPSTREAM_PROFILE, SETUP_ROLE, CONNECT_ROLE, STOP_ROLE)
    if any(not (checkout / relative).is_file() for relative in required):
        raise R2LabUeAnsibleError("pinned 5g-Ansible checkout is missing R2Lab UE roles")
    try:
        profile = (checkout / UPSTREAM_PROFILE).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise R2LabUeAnsibleError("pinned 5g-Ansible profile is unavailable") from exc
    contract = (
        f"  - name: {OPEN5GS_SLICE_NAME}\n",
        f"    dnn: {OPEN5GS_DNN}\n",
        f"    ip_prefix: \"{OPEN5GS_UE_PREFIX}\"\n",
        f"  {topology.ue}:\n",
    )
    if any(item not in profile for item in contract):
        raise R2LabUeAnsibleError(
            "pinned 5g-Ansible UE profile no longer matches the SynthRAN Open5GS contract"
        )


def _selected_host(topology: PhysicalTopology) -> str:
    host = topology.ue_profile.host
    if not host or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789-." for character in host):
        raise R2LabUeAnsibleError("selected UE runtime host is unsafe for Ansible inventory")
    return host


def _safe_slice(slice_name: str) -> str:
    if not slice_name or any(
        not (character.isalnum() or character in "._-") for character in slice_name
    ):
        raise R2LabUeAnsibleError("R2Lab slice name is unsafe for Ansible inventory")
    return slice_name


def _role_ue(topology: PhysicalTopology, action: UeRoleAction) -> str:
    if action == "setup":
        if topology.ue_profile.kind != "qhat":
            raise R2LabUeAnsibleError(
                "the pinned setup role cannot safely map qfit resource names to FIT runtime hosts"
            )
        return topology.ue
    return _selected_host(topology)


def _inventory(*, slice_name: str, topology: PhysicalTopology, action: UeRoleAction) -> str:
    slice_name = _safe_slice(slice_name)
    role_ue = _role_ue(topology, action)
    mode = topology.ue_profile.mode
    if mode not in {"mbim", "qmi"}:
        raise R2LabUeAnsibleError("selected UE mode is unsupported by the pinned UE roles")

    faraday = (
        "[faraday]\n"
        f"faraday.inria.fr ansible_user={slice_name} "
        "ansible_ssh_common_args='-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes'\n\n"
    )
    if action == "setup":
        return faraday + "[qhats]\n" + f"{role_ue} mode={mode}\n"
    return faraday + "[selected_ue]\n" + f"{role_ue} mode={mode}\n"


def _profile(topology: PhysicalTopology, action: UeRoleAction) -> str:
    role_ue = _role_ue(topology, action)
    return (
        "slices:\n"
        f"  - name: {OPEN5GS_SLICE_NAME}\n"
        f"    dnn: {OPEN5GS_DNN}\n"
        "    sst: \"1\"\n"
        "    sd: \"EMPTY\"\n"
        f"    ip_prefix: \"{OPEN5GS_UE_PREFIX}\"\n"
        "ues:\n"
        f"  {role_ue}:\n"
        f"    slice: {OPEN5GS_SLICE_NAME}\n"
    )


def _playbook(*, action: UeRoleAction, topology: PhysicalTopology) -> str:
    role_ue = _role_ue(topology, action)
    vars_: list[str] = []
    if action == "setup":
        vars_.extend((f"        fiveg_profile: {FIVEG_PROFILE}", f"        rru: {topology.radio}"))
    else:
        vars_.append(f"        ue_item: {role_ue}")
        if action == "connect":
            vars_.extend((f"        fiveg_profile: {FIVEG_PROFILE}", "        ignore_task_errors: false"))
    rendered_vars = "\n".join(vars_) + "\n"
    return (
        "---\n"
        f"- name: SynthRAN selected R2Lab UE {action}\n"
        "  hosts: faraday\n"
        "  gather_facts: false\n"
        "  roles:\n"
        f"    - role: r2lab/ue/{action}\n"
        "      vars:\n"
        f"{rendered_vars}"
    )


def _effective_timeout(action: UeRoleAction, requested: int) -> int:
    floor = _ROLE_TIMEOUT_FLOORS[action]
    return min(max(int(requested), floor), ROLE_TIMEOUT_SECONDS)


@dataclass(frozen=True)
class UeRoleResult:
    run_id: str
    ue: str
    host: str
    mode: str
    action: UeRoleAction
    fiveg_ansible_commit: str
    inventory_sha256: str
    profile_sha256: str
    playbook_sha256: str
    status: str
    evidence_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-ue-role/v1alpha1",
            "run_id": self.run_id,
            "ue": self.ue,
            "host": self.host,
            "mode": self.mode,
            "action": self.action,
            "fiveg_ansible_commit": self.fiveg_ansible_commit,
            "inventory_sha256": self.inventory_sha256,
            "profile_sha256": self.profile_sha256,
            "playbook_sha256": self.playbook_sha256,
            "status": self.status,
            "evidence_path": str(self.evidence_path),
        }


def execute_selected_ue_role(
    *,
    run_id: str,
    slice_name: str,
    topology: PhysicalTopology,
    action: UeRoleAction,
    lock_path: Path = Path("dependencies.lock.yml"),
    deps_root: Path = Path(".deps"),
    run_root: Path = Path(".synthran/r2lab"),
    timeout_seconds: int = ROLE_TIMEOUT_SECONDS,
    runner: RunCommand = run_command,
    checkout_validator: CheckoutValidator = validate_fiveg_checkout,
    progress: TextIO | None = None,
) -> UeRoleResult:
    """Run one locked upstream UE role for the exact selected physical UE."""

    topology = topology.validate()
    if action not in {"setup", "connect", "stop"}:
        raise R2LabUeAnsibleError("physical UE role action must be setup, connect, or stop")
    timeout_seconds = _effective_timeout(action, timeout_seconds)
    try:
        lock = load_lock(lock_path)
        checkout = checkout_validator(lock, deps_root.expanduser().resolve())
    except (FiveGAnsibleError, OSError, ValueError) as exc:
        raise R2LabUeAnsibleError(str(exc)) from exc
    _validate_pinned_profile(checkout, topology)

    inventory_text = _inventory(slice_name=slice_name, topology=topology, action=action)
    profile_text = _profile(topology, action)
    playbook_text = _playbook(action=action, topology=topology)
    physical = run_root.expanduser().resolve() / run_id / "physical"
    evidence_path = physical / f"physical-ue-role-{action}.json"
    physical.mkdir(parents=True, exist_ok=True)

    environment = dict(os.environ)
    environment.update(
        {
            "ANSIBLE_HOST_KEY_CHECKING": "True",
            "ANSIBLE_NOCOLOR": "1",
            "ANSIBLE_RETRY_FILES_ENABLED": "False",
            "ANSIBLE_ROLES_PATH": str(checkout / "roles"),
        }
    )
    identity = _configured_identity(slice_name)
    if identity is not None:
        environment["ANSIBLE_PRIVATE_KEY_FILE"] = str(identity)

    payload = UeRoleResult(
        run_id=run_id,
        ue=topology.ue,
        host=_selected_host(topology),
        mode=topology.ue_profile.mode,
        action=action,
        fiveg_ansible_commit=_dependency_commit(lock),
        inventory_sha256=_sha256(inventory_text),
        profile_sha256=_sha256(profile_text),
        playbook_sha256=_sha256(playbook_text),
        status="running",
        evidence_path=evidence_path,
    )
    _atomic_json(evidence_path, payload.to_dict())

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] ue-{action}: {message}", file=progress, flush=True)

    report("running...")
    try:
        with tempfile.TemporaryDirectory(prefix="ue-ansible-", dir=physical) as directory:
            root = Path(directory)
            playbook_dir = root / "playbooks"
            profile_dir = root / "group_vars" / "all"
            playbook_dir.mkdir(parents=True)
            profile_dir.mkdir(parents=True)
            inventory_path = root / "hosts.ini"
            profile_path = profile_dir / f"5g_profile_{FIVEG_PROFILE}.yaml"
            playbook_path = playbook_dir / f"selected-ue-{action}.yml"
            inventory_path.write_text(inventory_text, encoding="utf-8", newline="\n")
            profile_path.write_text(profile_text, encoding="utf-8", newline="\n")
            playbook_path.write_text(playbook_text, encoding="utf-8", newline="\n")
            command = ("ansible-playbook", "-i", str(inventory_path), str(playbook_path))
            if runner is run_command:
                result = run_streaming_ansible_command(
                    command,
                    root,
                    environment,
                    timeout_seconds,
                    report=report,
                )
            else:
                result = runner(command, root, environment, timeout_seconds)
                if progress is not None:
                    for line in result.stdout.splitlines():
                        rendered = parse_ansible_line(line)
                        if rendered is not None:
                            report(rendered)
    except Exception as exc:
        failed = UeRoleResult(**{**payload.__dict__, "status": "failed"})
        _atomic_json(evidence_path, failed.to_dict())
        report("FAILED")
        raise R2LabUeAnsibleError(f"pinned 5g-Ansible UE {action} role could not complete") from exc

    if result.returncode != 0:
        failed = UeRoleResult(**{**payload.__dict__, "status": "failed"})
        _atomic_json(evidence_path, failed.to_dict())
        report("FAILED")
        raise R2LabUeAnsibleError(f"pinned 5g-Ansible UE {action} role returned nonzero")

    completed = UeRoleResult(**{**payload.__dict__, "status": "completed"})
    _atomic_json(evidence_path, completed.to_dict())
    report("OK")
    return completed
