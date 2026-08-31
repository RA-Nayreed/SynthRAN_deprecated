from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shlex
import shutil
import tempfile
import time
from time import monotonic
from typing import Mapping, Sequence, TextIO

from synthran.ansible_streaming import parse_ansible_line, run_streaming_ansible_command
from synthran.dependencies import load_lock
from synthran.fiveg_ansible import validate_fiveg_checkout
from synthran.live_preflight import CommandResult, subprocess_runner
from synthran.network.resources import build_preparation_inventory, locked_preparation_variables
from synthran.network.runtime import (
    RunCommand,
    atomic_json,
    golden_path_image_variables,
    run_command,
    sanitize_deployment_text,
)
from synthran.r2lab.resources import load_topology
from synthran.upstream_overlay import UpstreamOverlayError, apply_network_overlay


class R2LabUpstreamRoleError(RuntimeError):
    pass


def _git_commit(lock, name: str) -> str:
    dependency = next((item for item in lock.git if item.name == name), None)
    if dependency is None:
        raise R2LabUpstreamRoleError(f"dependency lock is missing {name}")
    return dependency.commit


def _container_reference(lock, name: str) -> str:
    containers = lock.raw.get("containers")
    entry = containers.get(name) if isinstance(containers, dict) else None
    image = entry.get("image") if isinstance(entry, dict) else None
    digest = entry.get("digest") if isinstance(entry, dict) else None
    if (
        not isinstance(image, str)
        or not image
        or not isinstance(digest, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None
    ):
        raise R2LabUpstreamRoleError(f"container lock {name} is not digest-addressed")
    return f"{image}@{digest}"


def _physical_variables(lock) -> dict[str, object]:
    images = golden_path_image_variables(lock)
    images.update(
        {
            "srsran_gnb_physical": _container_reference(lock, "srsran_gnb_physical"),
            "srsran_gnb_physical_n320": _container_reference(
                lock, "srsran_gnb_physical_n320"
            ),
        }
    )
    return {
        "synthran_images": images,
        **locked_preparation_variables(lock),
    }


def _run_stage(
    *,
    name: str,
    command: Sequence[str],
    cwd: Path,
    environment: Mapping[str, str] | None,
    timeout_seconds: int,
    log_parts: list[str],
    progress: TextIO | None,
    streaming: bool = False,
    runner: RunCommand = run_command,
) -> CommandResult:
    started = monotonic()
    if progress is not None:
        print(f"[synthran] {name}: running...", file=progress, flush=True)
    try:
        if streaming and runner is run_command:
            result = run_streaming_ansible_command(
                command,
                cwd,
                environment,
                timeout_seconds,
                report=(
                    (
                        lambda message: print(
                            f"[synthran] {message}", file=progress, flush=True
                        )
                    )
                    if progress is not None
                    else None
                ),
            )
        else:
            result = runner(command, cwd, environment, timeout_seconds)
            if streaming and progress is not None:
                for line in result.stdout.splitlines():
                    message = parse_ansible_line(line)
                    if message is not None:
                        print(f"[synthran] {message}", file=progress, flush=True)
    except Exception as exc:
        raise R2LabUpstreamRoleError(f"{name} could not complete") from exc
    log_parts.extend((f"=== {name} ===", result.stdout, result.stderr))
    if result.returncode != 0:
        raise R2LabUpstreamRoleError(f"{name} failed; see sanitized upstream log")
    if progress is not None:
        print(
            f"[synthran] {name}: OK ({monotonic() - started:.1f}s)",
            file=progress,
            flush=True,
        )
    return result


def _ansible_environment(*, collections: Path, roles: Path) -> dict[str, str]:
    """Keep only non-SSH execution settings; upstream ansible.cfg owns SSH behavior."""
    environment = dict(os.environ)
    environment.update(
        {
            "ANSIBLE_COLLECTIONS_PATH": str(collections),
            "ANSIBLE_NOCOLOR": "True",
            "ANSIBLE_RETRY_FILES_ENABLED": "False",
            "ANSIBLE_ROLES_PATH": str(roles),
        }
    )
    return environment


def _syntax_command(command: Sequence[str]) -> tuple[str, ...]:
    return (*command[:-1], "--syntax-check", command[-1])


def _prepare_worktree(
    *,
    checkout: Path,
    commit: str,
    output_directory: Path,
    prefix: str,
    repository_root: Path,
    timeout_seconds: int,
    log_parts: list[str],
    progress: TextIO | None,
    runner: RunCommand,
) -> tuple[Path, Path]:
    worktree_parent = Path(tempfile.mkdtemp(prefix=prefix, dir=output_directory))
    worktree = worktree_parent / "fiveg_ansible"
    _run_stage(
        name=f"{prefix.rstrip('-')}-worktree",
        command=(
            "git",
            "-C",
            str(checkout),
            "worktree",
            "add",
            "--detach",
            str(worktree),
            commit,
        ),
        cwd=repository_root,
        environment=None,
        timeout_seconds=min(timeout_seconds, 300),
        log_parts=log_parts,
        progress=progress,
        runner=runner,
    )
    return worktree_parent, worktree


def _remove_worktree(
    *,
    checkout: Path,
    worktree_parent: Path,
    worktree: Path,
    repository_root: Path,
    timeout_seconds: int,
    runner: RunCommand,
) -> None:
    runner(
        ("git", "-C", str(checkout), "worktree", "remove", "--force", str(worktree)),
        repository_root,
        None,
        min(timeout_seconds, 300),
    )
    shutil.rmtree(worktree_parent, ignore_errors=True)


def _run_foundation(
    *,
    topology,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    timeout_seconds: int,
    progress: TextIO | None,
    runner: RunCommand,
) -> None:
    del slice_name, owner, allocation_id, known_hosts
    lock = load_lock(lock_path)
    checkout = validate_fiveg_checkout(lock, deps_root)
    fiveg_commit = _git_commit(lock, "fiveg_ansible")
    repository_root = Path(".").resolve()
    overlay_source = repository_root / "deploy" / "ansible"
    output_directory = run_root.expanduser().resolve() / run_id / "physical" / "upstream"
    output_directory.mkdir(parents=True, exist_ok=True)
    inventory_path = output_directory / "foundation-hosts.ini"
    inventory_text, _ = build_preparation_inventory(
        core_node=topology.core_node,
        ran_node=topology.ran_node,
        source=inventory_path,
    )
    inventory_text = inventory_text.replace('rru="rfsim"', f'rru="{topology.radio}"', 1)
    inventory_path.write_text(inventory_text, encoding="utf-8", newline="\n")
    variables_path = output_directory / "foundation-variables.json"
    atomic_json(variables_path, _physical_variables(lock))
    log_path = output_directory / "foundation.log"
    log_parts: list[str] = []

    worktree_parent: Path | None = None
    worktree: Path | None = None
    try:
        worktree_parent, worktree = _prepare_worktree(
            checkout=checkout,
            commit=fiveg_commit,
            output_directory=output_directory,
            prefix="foundation-",
            repository_root=repository_root,
            timeout_seconds=timeout_seconds,
            log_parts=log_parts,
            progress=progress,
            runner=runner,
        )
        overlay_directory = worktree / ".synthran"
        shutil.copytree(overlay_source, overlay_directory)

        collections = output_directory / "collections"
        environment = _ansible_environment(
            collections=collections,
            roles=worktree / "roles",
        )
        _run_stage(
            name="foundation-collections",
            command=(
                "ansible-galaxy",
                "collection",
                "install",
                "-r",
                str(overlay_directory / "preparation-requirements.yml"),
                "-p",
                str(collections),
            ),
            cwd=worktree,
            environment=environment,
            timeout_seconds=min(timeout_seconds, 600),
            log_parts=log_parts,
            progress=progress,
            runner=runner,
        )

        pos_command = (
            "ansible-playbook",
            "-i",
            str(inventory_path),
            "-e",
            f"node={topology.core_node}",
            "-e",
            "no_boot=false",
            "-c",
            "local",
            str(worktree / "playbooks" / "run_pos.yml"),
        )
        cluster_command = (
            "ansible-playbook",
            "-i",
            str(inventory_path),
            str(overlay_directory / "r2lab-foundation.yml"),
        )
        network_command = (
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

        for name, command in (
            ("foundation-pos-syntax", pos_command),
            ("foundation-cluster-syntax", cluster_command),
            ("foundation-network-syntax", network_command),
            ("foundation-tools-syntax", tools_command),
        ):
            _run_stage(
                name=name,
                command=_syntax_command(command),
                cwd=worktree,
                environment=environment,
                timeout_seconds=min(timeout_seconds, 600),
                log_parts=log_parts,
                progress=progress,
                runner=runner,
            )

        for role_name, node in (
            ("foundation-pos-core", topology.core_node),
            ("foundation-pos-ran", topology.ran_node),
        ):
            command = (
                "ansible-playbook",
                "-i",
                str(inventory_path),
                "-e",
                f"node={node}",
                "-e",
                "no_boot=false",
                "-c",
                "local",
                str(worktree / "playbooks" / "run_pos.yml"),
            )
            _run_stage(
                name=role_name,
                command=command,
                cwd=worktree,
                environment=environment,
                timeout_seconds=timeout_seconds,
                log_parts=log_parts,
                progress=progress,
                streaming=True,
                runner=runner,
            )

        for stage_name, command in (
            ("foundation-cluster", cluster_command),
            ("foundation-network", network_command),
            ("foundation-tools", tools_command),
        ):
            _run_stage(
                name=stage_name,
                command=command,
                cwd=worktree,
                environment=environment,
                timeout_seconds=timeout_seconds,
                log_parts=log_parts,
                progress=progress,
                streaming=True,
                runner=runner,
            )
    except UpstreamOverlayError as exc:
        raise R2LabUpstreamRoleError(str(exc)) from exc
    finally:
        log_path.write_text(
            sanitize_deployment_text(
                "\n".join(log_parts),
                (lock_path, deps_root, repository_root, output_directory),
            ),
            encoding="utf-8",
            newline="\n",
        )
        if worktree_parent is not None and worktree is not None:
            _remove_worktree(
                checkout=checkout,
                worktree_parent=worktree_parent,
                worktree=worktree,
                repository_root=repository_root,
                timeout_seconds=timeout_seconds,
                runner=runner,
            )


def _run_gnb(
    *,
    topology,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    timeout_seconds: int,
    progress: TextIO | None,
    runner: RunCommand,
) -> None:
    del slice_name, owner, allocation_id, known_hosts
    lock = load_lock(lock_path)
    checkout = validate_fiveg_checkout(lock, deps_root)
    fiveg_commit = _git_commit(lock, "fiveg_ansible")
    srsran_commit = _git_commit(lock, "srsran_helm")
    repository_root = Path(".").resolve()
    overlay_source = repository_root / "deploy" / "ansible"
    output_directory = run_root.expanduser().resolve() / run_id / "physical" / "upstream"
    output_directory.mkdir(parents=True, exist_ok=True)
    inventory_path = output_directory / "gnb-hosts.ini"
    inventory_text, _ = build_preparation_inventory(
        core_node=topology.core_node,
        ran_node=topology.ran_node,
        source=inventory_path,
    )
    inventory_text = inventory_text.replace('rru="rfsim"', f'rru="{topology.radio}"', 1)
    inventory_path.write_text(inventory_text, encoding="utf-8", newline="\n")
    variables_path = output_directory / "gnb-variables.json"
    atomic_json(variables_path, _physical_variables(lock))
    log_path = output_directory / "gnb.log"
    log_parts: list[str] = []

    worktree_parent: Path | None = None
    worktree: Path | None = None
    try:
        worktree_parent, worktree = _prepare_worktree(
            checkout=checkout,
            commit=fiveg_commit,
            output_directory=output_directory,
            prefix="gnb-",
            repository_root=repository_root,
            timeout_seconds=timeout_seconds,
            log_parts=log_parts,
            progress=progress,
            runner=runner,
        )
        overlay_directory = worktree / ".synthran"
        shutil.copytree(overlay_source, overlay_directory)
        apply_network_overlay(worktree, subscriber_name=topology.ue)

        collections = output_directory / "collections"
        environment = _ansible_environment(
            collections=collections,
            roles=worktree / "roles",
        )
        _run_stage(
            name="gnb-collections",
            command=(
                "ansible-galaxy",
                "collection",
                "install",
                "-r",
                str(overlay_directory / "preparation-requirements.yml"),
                "-p",
                str(collections),
            ),
            cwd=worktree,
            environment=environment,
            timeout_seconds=min(timeout_seconds, 600),
            log_parts=log_parts,
            progress=progress,
            runner=runner,
        )

        command = (
            "ansible-playbook",
            "-i",
            str(inventory_path),
            "-e",
            "fiveg_profile=default",
            "-e",
            f"version={srsran_commit}",
            "-e",
            f"synthran_run_id={run_id}",
            "-e",
            f"synthran_fiveg_ansible_commit={fiveg_commit}",
            "-e",
            f"@{variables_path}",
            str(overlay_directory / "r2lab-srsran-gnb.yml"),
        )
        _run_stage(
            name="physical-gnb-role-syntax",
            command=_syntax_command(command),
            cwd=worktree,
            environment=environment,
            timeout_seconds=min(timeout_seconds, 600),
            log_parts=log_parts,
            progress=progress,
            runner=runner,
        )
        _run_stage(
            name="physical-gnb-role",
            command=command,
            cwd=worktree,
            environment=environment,
            timeout_seconds=timeout_seconds,
            log_parts=log_parts,
            progress=progress,
            streaming=True,
            runner=runner,
        )
    except UpstreamOverlayError as exc:
        raise R2LabUpstreamRoleError(str(exc)) from exc
    finally:
        log_path.write_text(
            sanitize_deployment_text(
                "\n".join(log_parts),
                (lock_path, deps_root, repository_root, output_directory),
            ),
            encoding="utf-8",
            newline="\n",
        )
        if worktree_parent is not None and worktree is not None:
            _remove_worktree(
                checkout=checkout,
                worktree_parent=worktree_parent,
                worktree=worktree,
                repository_root=repository_root,
                timeout_seconds=timeout_seconds,
                runner=runner,
            )


def _run_upstream(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    mode: str,
    timeout_seconds: int,
    progress: TextIO | None,
    runner: RunCommand = run_command,
) -> None:
    if mode not in {"foundation", "gnb"}:
        raise R2LabUpstreamRoleError("unsupported physical upstream convergence mode")
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    target = _run_foundation if mode == "foundation" else _run_gnb
    target(
        topology=topology,
        run_id=run_id,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        lock_path=lock_path,
        deps_root=deps_root,
        run_root=run_root,
        timeout_seconds=timeout_seconds,
        progress=progress,
        runner=runner,
    )


def _cluster_command(topology, *remote: str) -> tuple[str, ...]:
    return ("ssh", f"root@{topology.core_node}", shlex.join(remote))


def stop_role_managed_gnb(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    run_root: Path,
    timeout_seconds: int,
) -> dict[str, object]:
    del slice_name, owner, allocation_id, known_hosts
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    fiveg_commit = _git_commit(load_lock(lock_path), "fiveg_ansible")
    result = subprocess_runner(
        _cluster_command(
            topology,
            "kubectl",
            "get",
            "deployment/srsran-gnb",
            "-n",
            "open5gs",
            "-o",
            "json",
        ),
        min(timeout_seconds, 60),
    )
    if result.returncode != 0:
        raise R2LabUpstreamRoleError("role-managed gNB ownership query returned nonzero")
    try:
        deployment = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise R2LabUpstreamRoleError(
            "role-managed gNB ownership query returned malformed JSON"
        ) from exc
    metadata = deployment.get("metadata") if isinstance(deployment, dict) else None
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
    if (
        not isinstance(labels, dict)
        or not isinstance(annotations, dict)
        or labels.get("synthran.run/id") != run_id
        or annotations.get("synthran.io/run-id") != run_id
        or annotations.get("synthran.io/deployment-authority")
        != f"fiveg_ansible:{fiveg_commit}"
    ):
        raise R2LabUpstreamRoleError(
            "role-managed gNB cleanup refuses foreign or unbound state"
        )

    scaled = subprocess_runner(
        _cluster_command(
            topology,
            "kubectl",
            "scale",
            "deployment/srsran-gnb",
            "-n",
            "open5gs",
            "--replicas=0",
        ),
        min(timeout_seconds, 60),
    )
    if scaled.returncode != 0:
        raise R2LabUpstreamRoleError("role-managed gNB scale-to-zero returned nonzero")

    for attempt in range(30):
        pods = subprocess_runner(
            _cluster_command(
                topology,
                "kubectl",
                "get",
                "pods",
                "-n",
                "open5gs",
                "-l",
                "app=srsran,component=gnb",
                "-o",
                "json",
            ),
            min(timeout_seconds, 60),
        )
        if pods.returncode != 0:
            raise R2LabUpstreamRoleError("role-managed gNB zero-pod query returned nonzero")
        try:
            payload = json.loads(pods.stdout)
        except json.JSONDecodeError as exc:
            raise R2LabUpstreamRoleError(
                "role-managed gNB zero-pod query returned malformed JSON"
            ) from exc
        items = payload.get("items") if isinstance(payload, dict) else None
        if isinstance(items, list) and not items:
            return {
                "status": "stopped",
                "run_id": run_id,
                "deployment_authority": f"fiveg_ansible:{fiveg_commit}",
                "gnb_pod_count": 0,
            }
        if attempt < 29:
            time.sleep(2)
    raise R2LabUpstreamRoleError("role-managed gNB did not reach zero pods")


def converge_kubernetes_foundation(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    timeout_seconds: int,
    progress: TextIO | None = None,
) -> None:
    _run_upstream(
        run_id=run_id,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        lock_path=lock_path,
        deps_root=deps_root,
        run_root=run_root,
        mode="foundation",
        timeout_seconds=timeout_seconds,
        progress=progress,
    )


def converge_physical_gnb(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    timeout_seconds: int,
    progress: TextIO | None = None,
) -> None:
    _run_upstream(
        run_id=run_id,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        lock_path=lock_path,
        deps_root=deps_root,
        run_root=run_root,
        mode="gnb",
        timeout_seconds=timeout_seconds,
        progress=progress,
    )
