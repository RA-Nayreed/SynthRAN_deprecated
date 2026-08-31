"""Legacy physical convergence aliases during the 5g-Ansible cutover.

The old module independently prepared POS, Kubernetes and Open5GS.  Those are
now one 5g-Ansible deployment lifecycle.  These names remain only until the
staged R2Lab resume code is simplified in the same purge PR.
"""

from __future__ import annotations

from pathlib import Path
from typing import TextIO

from synthran.r2lab.upstream_roles import (
    R2LabUpstreamRoleError,
    converge_physical_deployment,
)


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
    runner=None,
) -> None:
    """Compatibility alias for one complete upstream physical deployment."""

    del runner
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
    runner=None,
) -> None:
    """Compatibility alias; core convergence is owned by 5g-Ansible."""

    del runner
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


__all__ = [
    "R2LabUpstreamRoleError",
    "converge_kubernetes_foundation",
    "converge_open5gs",
]
