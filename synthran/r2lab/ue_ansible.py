"""Pinned 5g-Ansible actuation boundary for one selected R2Lab UE."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Callable, Literal, Mapping, Sequence, TextIO

from synthran.ansible_streaming import parse_ansible_line, run_streaming_ansible_command
from synthran.dependencies import DependencyLock, load_lock
from synthran.fiveg_ansible import FiveGAnsibleError, validate_fiveg_checkout
from synthran.live_preflight import CommandResult
from synthran.network.runtime import run_command
from synthran.r2lab.controller import _configured_identity
from synthran.r2lab.hardware import PhysicalTopology
from synthran.utils.ssh import ansible_ssh_common_args


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
ROLE_TIMEOUT_SECONDS = 900
_VALID_ACTIONS = frozenset({"setup", "connect", "stop"})
_ROLE_TIMEOUT_FLOORS: Mapping[UeRoleAction, int] = {
    "setup": 600,
    "connect": 300,
    "stop": 120,
}
_FORBIDDEN_SSH = (
    "StrictHostKeyChecking=no",
    "UserKnownHostsFile=/dev/null",
    "GlobalKnownHostsFile=/dev/null",
)

_UPSTREAM_MBIM_BLOCK = '''        - name: "MBIM: stop.sh + start.sh on {{ ue_item }} if wwan0 not reachable"
          shell: >
            ssh -o ConnectTimeout=5 -o BatchMode=yes -o StrictHostKeyChecking=no
            root@{{ ue_item }}
            'stop.sh; start.sh -F {{ current_dnn }}'
          when:
            - ue_mode == 'mbim'
            - current_dnn is defined
            - not wwan0_up
          ignore_errors: "{{ ignore_task_errors | default(true) }}"
'''

_STABLE_MBIM_BLOCK = '''        - name: "MBIM: stop {{ ue_item }} once before reconnect"
          shell: >
            ssh
            root@{{ ue_item }}
            'stop.sh'
          when:
            - ue_mode == 'mbim'
            - current_dnn is defined
            - not wwan0_up
          ignore_errors: "{{ ignore_task_errors | default(true) }}"

        - name: "MBIM: start {{ ue_item }} and wait for modem readiness"
          shell: >
            ssh
            root@{{ ue_item }}
            'start.sh -F {{ current_dnn }} -q'
          register: mbim_start
          until: mbim_start.rc == 0
          retries: 10
          delay: 3
          when:
            - ue_mode == 'mbim'
            - current_dnn is defined
            - not wwan0_up
          ignore_errors: "{{ ignore_task_errors | default(true) }}"
'''


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
    faraday_args = ansible_ssh_common_args(isolated_config=True, connect_timeout=10)
    faraday = (
        "[faraday]\n"
        f"faraday.inria.fr ansible_user={slice_name} "
        f"ansible_ssh_common_args='{faraday_args}'\n\n"
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
    return (
        "---\n"
        f"- name: SynthRAN selected R2Lab UE {action}\n"
        "  hosts: faraday\n"
        "  gather_facts: false\n"
        "  roles:\n"
        f"    - role: r2lab/ue/{action}\n"
        "      vars:\n"
        + "\n".join(vars_)
        + "\n"
    )


def _effective_timeout(action: UeRoleAction, requested: int) -> int:
    return min(max(int(requested), _ROLE_TIMEOUT_FLOORS[action]), ROLE_TIMEOUT_SECONDS)


def _apply_connect_convergence(path: Path) -> None:
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise R2LabUeAnsibleError("pinned UE connect role is unavailable") from exc
    if source.count(_UPSTREAM_MBIM_BLOCK) != 1:
        raise R2LabUeAnsibleError(
            "pinned UE connect role drifted from the reviewed MBIM bring-up contract"
        )
    rendered = source.replace(_UPSTREAM_MBIM_BLOCK, _STABLE_MBIM_BLOCK, 1)
    if "stop.sh; start.sh" in rendered or "until: mbim_start.rc == 0" not in rendered:
        raise R2LabUeAnsibleError("MBIM convergence transformation was not applied exactly")
    path.write_text(rendered, encoding="utf-8", newline="\n")


def _is_ssh_command_line(line: str) -> bool:
    stripped = line.lstrip()
    return stripped == "ssh" or stripped.startswith("ssh ") or "shell: ssh " in line


def _harden_ssh_line(line: str, strict_prefix: str) -> str:
    if not _is_ssh_command_line(line):
        return line
    stripped = line.lstrip()
    if stripped == "ssh":
        return line[: len(line) - len(stripped)] + strict_prefix
    marker = "ssh "
    index = line.find(marker)
    if index < 0:
        raise R2LabUeAnsibleError("pinned UE role contains an unsupported SSH command form")
    before = line[:index]
    after = line[index + len("ssh") :]
    for pattern in (
        r"\s+-F\s+\S+",
        r"\s+-o\s+BatchMode=\S+",
        r"\s+-o\s+ConnectTimeout=\S+",
        r"\s+-o\s+StrictHostKeyChecking=\S+",
        r"\s+-o\s+UserKnownHostsFile=\S+",
        r"\s+-o\s+GlobalKnownHostsFile=\S+",
    ):
        after = re.sub(pattern, "", after)
    return before + strict_prefix + " " + after.lstrip()


def _harden_role_tree(role_root: Path, *, slice_name: str) -> str:
    strict_prefix = "ssh " + ansible_ssh_common_args(
        known_hosts=f"/home/{_safe_slice(slice_name)}/.ssh/known_hosts",
        isolated_config=True,
        connect_timeout=5,
    )
    task_files = sorted(
        path for path in role_root.rglob("*") if path.is_file() and path.suffix in {".yml", ".yaml"}
    )
    if not task_files:
        raise R2LabUeAnsibleError("pinned UE role contains no YAML task files")
    digest = hashlib.sha256()
    for path in task_files:
        try:
            source = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise R2LabUeAnsibleError("pinned UE role task file is unreadable") from exc
        rendered = "\n".join(_harden_ssh_line(line, strict_prefix) for line in source.splitlines())
        if source.endswith("\n"):
            rendered += "\n"
        if any(token in rendered for token in _FORBIDDEN_SSH):
            raise R2LabUeAnsibleError("pinned UE role still contains an insecure SSH option")
        for line in rendered.splitlines():
            if _is_ssh_command_line(line) and "StrictHostKeyChecking=yes" not in line:
                raise R2LabUeAnsibleError("pinned UE role contains an unguarded SSH command")
        path.write_text(rendered, encoding="utf-8", newline="\n")
        digest.update(path.relative_to(role_root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(rendered.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def _prepare_role_copy(
    *, checkout: Path, root: Path, action: UeRoleAction, slice_name: str
) -> tuple[Path, str]:
    source_role = checkout / "roles" / "r2lab" / "ue" / action
    target_role = root / "roles" / "r2lab" / "ue" / action
    if not source_role.is_dir():
        raise R2LabUeAnsibleError("pinned 5g-Ansible UE role directory is missing")
    target_role.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_role, target_role)
    if action == "connect":
        _apply_connect_convergence(target_role / "tasks" / "main.yml")
    return root / "roles", _harden_role_tree(target_role, slice_name=slice_name)


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
    role_sha256: str
    status: str
    evidence_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-ue-role/v1alpha2",
            "run_id": self.run_id,
            "ue": self.ue,
            "host": self.host,
            "mode": self.mode,
            "action": self.action,
            "fiveg_ansible_commit": self.fiveg_ansible_commit,
            "inventory_sha256": self.inventory_sha256,
            "profile_sha256": self.profile_sha256,
            "playbook_sha256": self.playbook_sha256,
            "role_sha256": self.role_sha256,
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
    """Run one isolated, strict-SSH copy of the locked upstream UE role."""

    topology = topology.validate()
    if action not in _VALID_ACTIONS:
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
        }
    )
    identity = _configured_identity(slice_name)
    if identity is not None:
        environment["ANSIBLE_PRIVATE_KEY_FILE"] = str(identity)

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] ue-{action}: {message}", file=progress, flush=True)

    running: UeRoleResult | None = None
    result: CommandResult | None = None
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

            overlay_roles, role_sha256 = _prepare_role_copy(
                checkout=checkout,
                root=root,
                action=action,
                slice_name=slice_name,
            )
            environment["ANSIBLE_ROLES_PATH"] = os.pathsep.join(
                (str(overlay_roles), str(checkout / "roles"))
            )
            running = UeRoleResult(
                run_id=run_id,
                ue=topology.ue,
                host=_selected_host(topology),
                mode=topology.ue_profile.mode,
                action=action,
                fiveg_ansible_commit=_dependency_commit(lock),
                inventory_sha256=_sha256(inventory_text),
                profile_sha256=_sha256(profile_text),
                playbook_sha256=_sha256(playbook_text),
                role_sha256=role_sha256,
                status="running",
                evidence_path=evidence_path,
            )
            _atomic_json(evidence_path, running.to_dict())

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
        if running is not None:
            _atomic_json(
                evidence_path,
                UeRoleResult(**{**running.__dict__, "status": "failed"}).to_dict(),
            )
        report("FAILED")
        raise R2LabUeAnsibleError(f"pinned 5g-Ansible UE {action} role could not complete") from exc

    assert running is not None and result is not None
    if result.returncode != 0:
        failed = UeRoleResult(**{**running.__dict__, "status": "failed"})
        _atomic_json(evidence_path, failed.to_dict())
        report("FAILED")
        raise R2LabUeAnsibleError(f"pinned 5g-Ansible UE {action} role returned nonzero")

    completed = UeRoleResult(**{**running.__dict__, "status": "completed"})
    _atomic_json(evidence_path, completed.to_dict())
    report("OK")
    return completed
