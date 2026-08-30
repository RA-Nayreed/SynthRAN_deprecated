from __future__ import annotations

from pathlib import Path
import shutil
from typing import TextIO

from synthran.dependencies import load_lock
from synthran.fiveg_ansible import validate_fiveg_checkout
from synthran.network.resources import build_preparation_inventory
from synthran.network.runtime import RunCommand, atomic_json, run_command, sanitize_deployment_text
from synthran.r2lab.resources import load_topology
from synthran.r2lab.upstream_roles import (
    R2LabUpstreamRoleError,
    _ansible_environment,
    _git_commit,
    _physical_variables,
    _prepare_worktree,
    _remove_worktree,
    _run_stage,
    _syntax_command,
)
from synthran.upstream_overlay import apply_network_overlay


def _report(progress: TextIO | None, message: str) -> None:
    if progress is not None:
        print(f"[synthran] {message}", file=progress, flush=True)


def _inventory(*, topology, path: Path) -> Path:
    text, _ = build_preparation_inventory(
        core_node=topology.core_node,
        ran_node=topology.ran_node,
        source=path,
    )
    text = text.replace('rru="rfsim"', f'rru="{topology.radio}"', 1)
    path.write_text(text, encoding="utf-8", newline="\n")
    return path


def _ansible_reachable(
    *,
    node: str,
    inventory_path: Path,
    worktree: Path,
    environment: dict[str, str],
    timeout_seconds: int,
    runner: RunCommand,
    log_parts: list[str],
) -> bool:
    command = (
        "ansible",
        "-i",
        str(inventory_path),
        node,
        "-m",
        "ansible.builtin.ping",
    )
    try:
        result = runner(command, worktree, environment, min(timeout_seconds, 60))
    except Exception as exc:
        log_parts.extend((f"=== foundation-reachability:{node} ===", "", str(exc)))
        return False
    log_parts.extend((f"=== foundation-reachability:{node} ===", result.stdout, result.stderr))
    return result.returncode == 0


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
    runner: RunCommand = run_command,
) -> None:
    """Converge Kubernetes; POS only nodes upstream Ansible cannot currently reach."""
    del slice_name, owner, allocation_id, known_hosts
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    lock = load_lock(lock_path)
    checkout = validate_fiveg_checkout(lock, deps_root)
    fiveg_commit = _git_commit(lock, "fiveg_ansible")
    repository_root = Path(".").resolve()
    overlay_source = repository_root / "deploy" / "ansible"
    output_directory = run_root.expanduser().resolve() / run_id / "physical" / "upstream"
    output_directory.mkdir(parents=True, exist_ok=True)
    inventory_path = _inventory(
        topology=topology,
        path=output_directory / "foundation-hosts.ini",
    )
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
        pos_syntax = (
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
        for name, command in (
            ("foundation-pos-syntax", pos_syntax),
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

        unreachable = [
            node
            for node in (topology.core_node, topology.ran_node)
            if not _ansible_reachable(
                node=node,
                inventory_path=inventory_path,
                worktree=worktree,
                environment=environment,
                timeout_seconds=timeout_seconds,
                runner=runner,
                log_parts=log_parts,
            )
        ]
        if not unreachable:
            _report(
                progress,
                "foundation-pos: skipped; selected sopnodes already reachable through upstream Ansible",
            )
        for node in unreachable:
            role_name = "foundation-pos-core" if node == topology.core_node else "foundation-pos-ran"
            _report(
                progress,
                f"foundation-pos: {node} unreachable through upstream Ansible; POS required",
            )
            _run_stage(
                name=role_name,
                command=(
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
                ),
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


def converge_open5gs(
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
    runner: RunCommand = run_command,
) -> None:
    """Run the pinned upstream Open5GS roles with their normal Ansible SSH behavior."""
    del slice_name, owner, allocation_id, known_hosts
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    lock = load_lock(lock_path)
    checkout = validate_fiveg_checkout(lock, deps_root)
    fiveg_commit = _git_commit(lock, "fiveg_ansible")
    repository_root = Path(".").resolve()
    overlay_source = repository_root / "deploy" / "ansible"
    output_directory = run_root.expanduser().resolve() / run_id / "physical" / "upstream"
    output_directory.mkdir(parents=True, exist_ok=True)
    inventory_path = _inventory(
        topology=topology,
        path=output_directory / "open5gs-hosts.ini",
    )
    variables_path = output_directory / "open5gs-variables.json"
    atomic_json(variables_path, _physical_variables(lock))
    log_path = output_directory / "open5gs.log"
    log_parts: list[str] = []
    worktree_parent: Path | None = None
    worktree: Path | None = None

    try:
        worktree_parent, worktree = _prepare_worktree(
            checkout=checkout,
            commit=fiveg_commit,
            output_directory=output_directory,
            prefix="open5gs-",
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
            name="open5gs-collections",
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
            f"synthran_run_id={run_id}",
            "-e",
            f"synthran_subscriber={topology.ue}",
            "-e",
            f"@{variables_path}",
            str(overlay_directory / "r2lab-open5gs-core.yml"),
        )
        _run_stage(
            name="open5gs-role-syntax",
            command=_syntax_command(command),
            cwd=worktree,
            environment=environment,
            timeout_seconds=min(timeout_seconds, 600),
            log_parts=log_parts,
            progress=progress,
            runner=runner,
        )
        _run_stage(
            name="open5gs-role",
            command=command,
            cwd=worktree,
            environment=environment,
            timeout_seconds=timeout_seconds,
            log_parts=log_parts,
            progress=progress,
            streaming=True,
            runner=runner,
        )
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
