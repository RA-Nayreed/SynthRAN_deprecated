from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
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
from synthran.utils.ssh import ansible_ssh_common_args, strict_ssh_command


class R2LabUpstreamRoleError(RuntimeError):
    pass


POST_POS_SSH_ATTEMPTS = 36
POST_POS_SSH_INTERVAL_SECONDS = 5.0


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


def _short_hostname(value: str) -> str:
    return value.strip().lower().split(".", 1)[0]


def _keyscan_exact_host(
    *,
    host: str,
    cwd: Path,
    runner: RunCommand,
    log_parts: list[str],
) -> str:
    result = runner(
        ("ssh-keyscan", "-T", "5", "-t", "ed25519", host),
        cwd,
        None,
        15,
    )
    log_parts.extend(
        (
            f"=== post-pos-keyscan:{host} ===",
            result.stdout,
            result.stderr,
        )
    )
    if result.returncode != 0:
        raise R2LabUpstreamRoleError(
            f"post-POS SSH host-key scan failed for {host}"
        )
    candidates = []
    for raw in result.stdout.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        if len(fields) != 3:
            continue
        names, algorithm, key = fields
        if algorithm != "ssh-ed25519":
            continue
        if host not in names.split(","):
            continue
        if not key:
            continue
        candidates.append(line)
    unique = tuple(dict.fromkeys(candidates))
    if len(unique) != 1:
        raise R2LabUpstreamRoleError(
            f"post-POS SSH host-key scan for {host} was not unambiguous"
        )
    return unique[0]


def _strict_post_pos_ready(
    *,
    host: str,
    known_hosts: Path,
    cwd: Path,
    runner: RunCommand,
    log_parts: list[str],
    timeout_seconds: int,
) -> None:
    last_error = ""
    for attempt in range(1, POST_POS_SSH_ATTEMPTS + 1):
        command = strict_ssh_command(
            f"root@{host}",
            "hostname",
            known_hosts=known_hosts,
            isolated_config=True,
            connect_timeout=10,
        )
        result = runner(command, cwd, None, min(timeout_seconds, 30))
        if result.returncode == 0:
            observed = result.stdout.splitlines()[0] if result.stdout.splitlines() else ""
            if _short_hostname(observed) == _short_hostname(host):
                log_parts.extend(
                    (
                        f"=== post-pos-strict-ssh:{host} ===",
                        f"ready on attempt {attempt}",
                        "",
                    )
                )
                return
            last_error = "remote hostname did not match the selected node"
        else:
            last_error = (
                result.stderr.strip().splitlines()[-1]
                if result.stderr.strip()
                else "SSH returned nonzero"
            )
        if attempt < POST_POS_SSH_ATTEMPTS:
            time.sleep(POST_POS_SSH_INTERVAL_SECONDS)
    log_parts.extend(
        (
            f"=== post-pos-strict-ssh:{host} ===",
            "",
            last_error,
        )
    )
    raise R2LabUpstreamRoleError(
        f"post-POS strict SSH did not become ready for {host}"
    )


def _establish_post_pos_ssh(
    *,
    topology,
    known_hosts: Path,
    output_directory: Path,
    cwd: Path,
    runner: RunCommand,
    log_parts: list[str],
    progress: TextIO | None,
    timeout_seconds: int,
) -> None:
    hosts = (topology.core_node, topology.ran_node)
    if progress is not None:
        print(
            "[synthran] foundation-ssh: establishing post-POS strict SSH identities",
            file=progress,
            flush=True,
        )

    lines = tuple(
        _keyscan_exact_host(
            host=host,
            cwd=cwd,
            runner=runner,
            log_parts=log_parts,
        )
        for host in hosts
    )

    previous_text = known_hosts.read_text(encoding="utf-8")
    preserved: list[str] = []
    for raw in previous_text.splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            preserved.append(raw)
            continue
        names = stripped.split(maxsplit=1)[0].split(",")
        if any(host in names for host in hosts):
            continue
        preserved.append(raw)

    backup = output_directory / "pre-pos-known-hosts"
    backup.write_text(previous_text, encoding="utf-8", newline="\n")
    os.chmod(backup, 0o600)

    known_hosts.parent.mkdir(parents=True, exist_ok=True)
    temporary = known_hosts.with_name(f".{known_hosts.name}.post-pos")
    merged = [*preserved, *lines]
    temporary.write_text("\n".join(merged) + "\n", encoding="utf-8", newline="\n")
    os.chmod(temporary, 0o600)
    temporary.replace(known_hosts)
    os.chmod(known_hosts, 0o600)

    evidence = {
        "schema": "synthran/r2lab-post-pos-ssh/v1alpha1",
        "hosts": {
            host: {
                "algorithm": "ssh-ed25519",
                "key_line_sha256": hashlib.sha256(line.encode("utf-8")).hexdigest(),
            }
            for host, line in zip(hosts, lines, strict=True)
        },
    }
    atomic_json(output_directory / "post-pos-ssh.json", evidence)

    for host in hosts:
        _strict_post_pos_ready(
            host=host,
            known_hosts=known_hosts,
            cwd=cwd,
            runner=runner,
            log_parts=log_parts,
            timeout_seconds=timeout_seconds,
        )

    if progress is not None:
        print(
            "[synthran] foundation-ssh: strict host identity and root SSH proven",
            file=progress,
            flush=True,
        )


def _ansible_environment(
    *,
    collections: Path,
    roles: Path,
    known_hosts: Path,
) -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "ANSIBLE_COLLECTIONS_PATH": str(collections),
            "ANSIBLE_HOST_KEY_CHECKING": "True",
            "ANSIBLE_NOCOLOR": "True",
            "ANSIBLE_RETRY_FILES_ENABLED": "False",
            "ANSIBLE_ROLES_PATH": str(roles),
            "ANSIBLE_SSH_ARGS": (
                f"{ansible_ssh_common_args(known_hosts=known_hosts, isolated_config=True)} "
                "-o ControlMaster=auto -o ControlPersist=60s"
            ),
        }
    )
    return environment


def _syntax_command(command: Sequence[str]) -> tuple[str, ...]:
    return (*command[:-1], "--syntax-check", command[-1])


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

    worktree_parent = Path(tempfile.mkdtemp(prefix="foundation-", dir=output_directory))
    worktree = worktree_parent / "fiveg_ansible"
    added = False
    try:
        _run_stage(
            name="foundation-worktree",
            command=(
                "git",
                "-C",
                str(checkout),
                "worktree",
                "add",
                "--detach",
                str(worktree),
                fiveg_commit,
            ),
            cwd=repository_root,
            environment=None,
            timeout_seconds=min(timeout_seconds, 300),
            log_parts=log_parts,
            progress=progress,
            runner=runner,
        )
        added = True
        overlay_directory = worktree / ".synthran"
        shutil.copytree(overlay_source, overlay_directory)

        collections = output_directory / "collections"
        environment = _ansible_environment(
            collections=collections,
            roles=worktree / "roles",
            known_hosts=known_hosts,
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

        _establish_post_pos_ssh(
            topology=topology,
            known_hosts=known_hosts,
            output_directory=output_directory,
            cwd=worktree,
            runner=runner,
            log_parts=log_parts,
            progress=progress,
            timeout_seconds=timeout_seconds,
        )
        environment = _ansible_environment(
            collections=collections,
            roles=worktree / "roles",
            known_hosts=known_hosts,
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
                (known_hosts, lock_path, deps_root, repository_root, output_directory),
            ),
            encoding="utf-8",
            newline="\n",
        )
        if added:
            runner(
                ("git", "-C", str(checkout), "worktree", "remove", "--force", str(worktree)),
                repository_root,
                None,
                min(timeout_seconds, 300),
            )
        shutil.rmtree(worktree_parent, ignore_errors=True)


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

    worktree_parent = Path(tempfile.mkdtemp(prefix="gnb-", dir=output_directory))
    worktree = worktree_parent / "fiveg_ansible"
    added = False
    try:
        _run_stage(
            name="gnb-worktree",
            command=(
                "git",
                "-C",
                str(checkout),
                "worktree",
                "add",
                "--detach",
                str(worktree),
                fiveg_commit,
            ),
            cwd=repository_root,
            environment=None,
            timeout_seconds=min(timeout_seconds, 300),
            log_parts=log_parts,
            progress=progress,
            runner=runner,
        )
        added = True
        overlay_directory = worktree / ".synthran"
        shutil.copytree(overlay_source, overlay_directory)
        apply_network_overlay(worktree, subscriber_name=topology.ue)

        collections = output_directory / "collections"
        environment = _ansible_environment(
            collections=collections,
            roles=worktree / "roles",
            known_hosts=known_hosts,
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
                (known_hosts, lock_path, deps_root, repository_root, output_directory),
            ),
            encoding="utf-8",
            newline="\n",
        )
        if added:
            runner(
                ("git", "-C", str(checkout), "worktree", "remove", "--force", str(worktree)),
                repository_root,
                None,
                min(timeout_seconds, 300),
            )
        shutil.rmtree(worktree_parent, ignore_errors=True)


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
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabUpstreamRoleError("strict SLICES known-hosts file is missing")
    if mode == "foundation":
        _run_foundation(
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
    else:
        _run_gnb(
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


def _cluster_command(topology, known_hosts: Path, *remote: str) -> tuple[str, ...]:
    try:
        return strict_ssh_command(
            f"root@{topology.core_node}",
            *remote,
            known_hosts=known_hosts,
            isolated_config=True,
            quote_remote=True,
        )
    except ValueError as exc:
        raise R2LabUpstreamRoleError(str(exc)) from exc


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
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    known_hosts = known_hosts.expanduser().resolve()
    fiveg_commit = _git_commit(load_lock(lock_path), "fiveg_ansible")
    result = subprocess_runner(
        _cluster_command(
            topology,
            known_hosts,
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
        raise R2LabUpstreamRoleError("role-managed gNB ownership query returned malformed JSON") from exc
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
        raise R2LabUpstreamRoleError("role-managed gNB cleanup refuses foreign or unbound state")
    scaled = subprocess_runner(
        _cluster_command(
            topology,
            known_hosts,
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
                known_hosts,
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
            raise R2LabUpstreamRoleError("role-managed gNB zero-pod query returned malformed JSON") from exc
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
