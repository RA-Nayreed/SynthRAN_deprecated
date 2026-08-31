"""Temporary compatibility facade for legacy physical lifecycle callers.

5g-Ansible is the sole authority for POS, Kubernetes, core, RAN, RU and UE
deployment.  The functions in this module preserve old SynthRAN call signatures
only while the legacy staged R2Lab lifecycle is being removed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, TextIO

from synthran.adapters.fiveg import FiveGAdapter, FiveGAdapterError, write_spec
from synthran.dependencies import load_lock
from synthran.r2lab.resources import load_topology


class R2LabUpstreamRoleError(RuntimeError):
    """Raised when the upstream 5g-Ansible physical deployment cannot converge."""


def _report(progress: TextIO | None, message: str) -> None:
    if progress is not None:
        print(f"[synthran] {message}", file=progress, flush=True)


def _ue_selection(name: str) -> dict[str, list[str]]:
    """Translate a persisted legacy UE name into the upstream spec shape."""

    result = {"qhats": [], "qfits": [], "phones": []}
    if name.startswith("qhat"):
        result["qhats"].append(name)
    elif name.startswith("qfit"):
        result["qfits"].append(name)
    elif name.startswith("phone"):
        result["phones"].append(name)
    else:
        raise R2LabUpstreamRoleError(
            "legacy physical topology cannot be translated to a 5g-Ansible UE group"
        )
    return result


def _state_root(run_root: Path, run_id: str) -> Path:
    return run_root.expanduser().resolve() / run_id / "physical" / "fiveg-state"


def _spec_path(run_root: Path, run_id: str) -> Path:
    return run_root.expanduser().resolve() / run_id / "physical" / "fiveg-deployment.json"


def _legacy_physical_spec(
    *,
    run_id: str,
    slice_name: str,
    known_hosts: Path,
    run_root: Path,
) -> Mapping[str, Any]:
    """Translate an already-persisted legacy physical run into one upstream spec.

    This does not define SynthRAN's supported topology matrix.  It only preserves
    the meaning of old run state while callers migrate to supplying a native
    ``fiveg/deployment/v1`` document directly.
    """

    topology = load_topology(run_root=run_root, run_id=run_id)
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabUpstreamRoleError("strict R2Lab known-hosts file is missing")
    return {
        "schema": "fiveg/deployment/v1",
        "id": run_id,
        "core": {"type": "open5gs", "node": topology.core_node},
        "ran": {"type": "srsRAN", "node": topology.ran_node},
        "platform": {"type": "r2lab", "ru": topology.radio},
        "ues": _ue_selection(topology.ue),
        "monitoring": {"enabled": False},
        "profile": "default",
        # Legacy resume already owns its SLICES reservation.  5g-Ansible still
        # verifies the existing R2Lab lease and owns all deployment mechanics.
        "reservation": {
            "enabled": False,
            "duration_minutes": 120,
            "r2lab_mode": "require-existing",
        },
        "deployment": {
            "selected_slices": ["slice1"],
            "selected_ues": [topology.ue],
            "open5gs_webui_enabled": False,
            "open5gs_admin_account_enabled": False,
            "pos_manage_allocation": True,
            "cleanup_namespaces": [],
            "extra_vars": {},
        },
        "scenario": {"type": "none"},
        "r2lab": {
            "username": slice_name,
            "known_hosts_file": str(known_hosts),
            "strict_host_key_checking": True,
        },
    }


def _adapter(
    *,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    run_id: str,
    timeout_seconds: int,
) -> FiveGAdapter:
    try:
        lock = load_lock(lock_path)
        return FiveGAdapter.from_lock(
            lock=lock,
            dependency_root=deps_root,
            state_root=_state_root(run_root, run_id),
            timeout_seconds=timeout_seconds,
        )
    except (FiveGAdapterError, OSError) as exc:
        raise R2LabUpstreamRoleError(str(exc)) from exc


def converge_physical_deployment(
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
) -> Mapping[str, Any]:
    """Converge the complete persisted physical topology through 5g-Ansible."""

    del owner, allocation_id
    spec = _legacy_physical_spec(
        run_id=run_id,
        slice_name=slice_name,
        known_hosts=known_hosts,
        run_root=run_root,
    )
    spec_path = write_spec(_spec_path(run_root, run_id), spec)
    adapter = _adapter(
        lock_path=lock_path,
        deps_root=deps_root,
        run_root=run_root,
        run_id=run_id,
        timeout_seconds=timeout_seconds,
    )
    deployment_state = adapter.state_root / run_id / "state.json"
    resume = deployment_state.is_file()
    _report(
        progress,
        "physical deployment: delegating full convergence to pinned 5g-Ansible"
        + (" (resume)" if resume else ""),
    )
    try:
        adapter.plan(spec_path)
        manifest = adapter.up(spec_path, resume=resume)
    except FiveGAdapterError as exc:
        raise R2LabUpstreamRoleError(str(exc)) from exc
    if manifest.get("state") != "ready":
        raise R2LabUpstreamRoleError("5g-Ansible physical deployment did not reach ready")
    _report(progress, "physical deployment: upstream state is ready")
    return manifest


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
    """Legacy alias: one upstream deployment now owns the whole foundation."""

    converge_physical_deployment(
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
    """Legacy alias: gNB convergence is part of the full upstream deployment."""

    converge_physical_deployment(
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
    )


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
    deps_root: Path = Path(".deps"),
) -> dict[str, object]:
    """Legacy stop alias; upstream now owns cleanup of the deployment as a unit."""

    del slice_name, owner, allocation_id, known_hosts
    adapter = _adapter(
        lock_path=lock_path,
        deps_root=deps_root,
        run_root=run_root,
        run_id=run_id,
        timeout_seconds=timeout_seconds,
    )
    try:
        result = adapter.down(run_id)
    except FiveGAdapterError as exc:
        raise R2LabUpstreamRoleError(str(exc)) from exc
    return {
        "status": "stopped",
        "run_id": run_id,
        "deployment_authority": "fiveg-ansible",
        "upstream": dict(result),
    }
