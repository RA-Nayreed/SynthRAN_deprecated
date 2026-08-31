"""Read-only compatibility checks for the pinned 5g-Ansible deployment interface.

Historically this module rewrote files inside an isolated 5g-Ansible worktree.
That ownership boundary is gone: deployment policy now lives in 5g-Ansible itself.
The legacy function names remain temporarily so older call sites fail closed without
mutating the dependency checkout while the wider Great Purge removes those calls.
"""

from __future__ import annotations

from pathlib import Path


class UpstreamOverlayError(RuntimeError):
    """Raised when the pinned 5g-Ansible checkout lacks its declared interface."""


_REQUIRED_FILES = (
    "bin/fiveg",
    "group_vars/all/all.yml",
    "playbooks/deploy.yml",
    "playbooks/deploy_r2lab.yml",
    "playbooks/down.yml",
    "roles/r2lab/cleanup/tasks/main.yml",
)

_REQUIRED_POLICY_NAMES = (
    "fiveg_prepare_only",
    "fiveg_allow_live_installs",
    "fiveg_manage_os_dependencies",
    "fiveg_manage_python_dependencies",
    "fiveg_disruptive_cluster_ops_enabled",
    "fiveg_k8s_env_enabled",
    "fiveg_python_interpreter",
    "fiveg_selected_slices",
    "fiveg_selected_ues",
    "fiveg_cleanup_namespaces",
    "open5gs_webui_enabled",
    "open5gs_admin_account_enabled",
    "pos_manage_allocation",
    "r2lab_strict_host_key_checking",
)


def _read(root: Path, relative: str) -> str:
    path = root / relative
    try:
        return path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise UpstreamOverlayError(
            f"pinned 5g-Ansible interface is unreadable or missing: {relative}"
        ) from exc


def validate_upstream_interface(worktree: Path) -> None:
    """Validate the machine/policy contract without changing any upstream file."""

    missing = [relative for relative in _REQUIRED_FILES if not (worktree / relative).is_file()]
    if missing:
        raise UpstreamOverlayError(
            "pinned 5g-Ansible checkout is missing required interfaces: "
            + ", ".join(missing)
        )

    defaults = _read(worktree, "group_vars/all/all.yml")
    absent_policy = [name for name in _REQUIRED_POLICY_NAMES if name not in defaults]
    if absent_policy:
        raise UpstreamOverlayError(
            "pinned 5g-Ansible policy contract is incomplete: "
            + ", ".join(absent_policy)
        )

    cleanup = _read(worktree, "roles/r2lab/cleanup/tasks/main.yml")
    if "all-off" in cleanup:
        raise UpstreamOverlayError(
            "pinned 5g-Ansible contains forbidden global R2Lab cleanup"
        )
    if "rhubarbe pdu off" not in cleanup or "r2lab_selected_ues" not in cleanup:
        raise UpstreamOverlayError(
            "pinned 5g-Ansible does not expose selected-resource R2Lab cleanup"
        )


def apply_network_overlay(
    worktree: Path, *, subscriber_name: str = "uesim01"
) -> None:
    """Compatibility shim: validate upstream policy; never rewrite the checkout."""

    del subscriber_name
    validate_upstream_interface(worktree)


def apply_preparation_overlay(worktree: Path) -> None:
    """Compatibility shim: validate upstream policy; never rewrite the checkout."""

    validate_upstream_interface(worktree)
