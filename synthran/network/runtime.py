"""5G deployment boundary and experiment-path evidence helpers.

5g-Ansible is the deployment authority.  This module contains only a migration
facade from the existing SynthRAN lifecycle into the upstream machine API plus
re-exports of the current experiment path verifier.  The legacy executor is
kept in a private module only until its remaining observation helpers are
extracted; it is never called by ``execute_network_deployment`` below.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Mapping, TextIO

from synthran.adapters.fiveg import (
    FIVEG_SPEC_SCHEMA,
    FiveGAdapter,
    FiveGAdapterError,
    write_spec,
)
from synthran.dependencies import DependencyLock
from synthran.fiveg_ansible import NetworkDeploymentPlan, NetworkInventory
from synthran.network import _legacy_runtime as _legacy


DEPLOYMENT_SCHEMA = "fiveg/deployment-manifest/v1"
NETWORK_EVIDENCE_SCHEMA = _legacy.NETWORK_EVIDENCE_SCHEMA
DEFAULT_DEPLOY_TIMEOUT_SECONDS = 3600

NetworkRuntimeError = _legacy.NetworkRuntimeError
RunCommand = _legacy.RunCommand
VerificationCheck = _legacy.VerificationCheck
NetworkVerificationReport = _legacy.NetworkVerificationReport
run_command = _legacy.run_command
atomic_json = _legacy.atomic_json
golden_path_image_variables = _legacy.golden_path_image_variables
sanitize_deployment_text = _legacy.sanitize_deployment_text
validate_run_id = _legacy.validate_run_id
verify_network_path = _legacy.verify_network_path

# Temporary migration-only helpers still consumed by the old resource and
# provenance layers.  They stay private to this package boundary and disappear
# when ``network.resources`` and the legacy manifest contract are removed.
tree_sha256 = _legacy.tree_sha256
context_fingerprint = _legacy.context_fingerprint


@dataclass(frozen=True)
class DeploymentResult:
    run_id: str
    run_directory: Path
    manifest_path: Path
    log_path: Path
    spec_path: Path


def _locked_commit(lock: DependencyLock, name: str) -> str:
    dependency = next((item for item in lock.git if item.name == name), None)
    if dependency is None:
        raise NetworkRuntimeError(f"dependency lock does not define {name}")
    return dependency.commit


def _migration_spec(*, plan: NetworkDeploymentPlan, lock: DependencyLock, run_id: str) -> dict[str, Any]:
    """Translate the old RFSIM lifecycle inputs without making support decisions.

    This translator exists only so the lifecycle can move to the upstream
    executor before its CLI/config surface is converted to native 5g-Ansible
    specs.  5g-Ansible remains responsible for validating the resulting
    topology.
    """

    radio = plan.inventory.radio
    platform_type = "rfsim" if radio == "rfsim" else "r2lab"
    deployment: dict[str, Any] = {
        "selected_slices": ["slice1"] if plan.inventory.core == "open5gs" else [],
        "selected_ues": ["uesim01"] if platform_type == "rfsim" and plan.inventory.ran == "srsRAN" else [],
        "open5gs_webui_enabled": False,
        "open5gs_admin_account_enabled": False,
        "extra_vars": {
            # These are still locked by SynthRAN today.  They move into the
            # upstream dependency lock in the next ownership batch.
            "repo_branch": _locked_commit(lock, "open5gs_k8s"),
            "version": _locked_commit(lock, "srsran_helm"),
        },
    }
    return {
        "schema": FIVEG_SPEC_SCHEMA,
        "id": run_id,
        "core": {
            "type": plan.inventory.core,
            "node": plan.inventory.core_node.name,
        },
        "ran": {
            "type": plan.inventory.ran,
            "node": plan.inventory.ran_node.name,
        },
        "platform": {"type": platform_type, "ru": radio},
        "ues": {"qhats": [], "qfits": [], "phones": []},
        "monitoring": {
            "enabled": plan.inventory.monitoring_enabled,
            "node": plan.inventory.all_vars.get("monitor_node_name", "sopnode-f1"),
        },
        "profile": plan.profile,
        # The old SynthRAN lifecycle has already acquired the reservation at
        # this migration point.  This flag disappears when lifecycle ownership
        # itself moves upstream.
        "reservation": {"enabled": False, "r2lab_mode": "none"},
        "deployment": deployment,
        "scenario": {"type": "none"},
    }


def execute_network_deployment(
    *,
    plan: NetworkDeploymentPlan,
    lock: DependencyLock,
    dependency_root: Path,
    run_id: str,
    run_root: Path = Path(".synthran/runs"),
    timeout_seconds: int = DEFAULT_DEPLOY_TIMEOUT_SECONDS,
    progress: TextIO | None = None,
    resume: bool = False,
    **_: Any,
) -> DeploymentResult:
    """Deploy through the pinned 5g-Ansible machine API only."""

    run_id = validate_run_id(run_id)
    state_root = run_root.expanduser().resolve()
    state_root.mkdir(parents=True, exist_ok=True)
    spec_path = state_root / f"{run_id}.fiveg.json"
    write_spec(spec_path, _migration_spec(plan=plan, lock=lock, run_id=run_id))

    try:
        adapter = FiveGAdapter.from_lock(
            lock=lock,
            dependency_root=dependency_root,
            state_root=state_root,
            timeout_seconds=timeout_seconds,
        )
        if progress is not None:
            print(f"[synthran] 5g-Ansible plan: {run_id}", file=progress, flush=True)
        adapter.plan(spec_path)
        if progress is not None:
            print(f"[synthran] 5g-Ansible up: {run_id}", file=progress, flush=True)
        manifest = adapter.up(spec_path, resume=resume)
    except FiveGAdapterError as exc:
        raise NetworkRuntimeError(str(exc)) from exc

    if manifest.get("id") != run_id or manifest.get("state") != "ready":
        raise NetworkRuntimeError("5g-Ansible did not return a ready deployment manifest")
    expected_commit = _locked_commit(lock, "fiveg_ansible")
    if manifest.get("fiveg_ansible_commit") != expected_commit:
        raise NetworkRuntimeError("5g-Ansible manifest commit does not match the lock")

    directory_value = manifest.get("state_directory")
    if not isinstance(directory_value, str) or not directory_value:
        raise NetworkRuntimeError("5g-Ansible manifest did not report its state directory")
    run_directory = Path(directory_value).expanduser().resolve()
    manifest_path = run_directory / "manifest.json"
    log_path = run_directory / "deploy.log"
    if not manifest_path.is_file():
        raise NetworkRuntimeError("5g-Ansible deployment manifest was not persisted")
    return DeploymentResult(
        run_id=run_id,
        run_directory=run_directory,
        manifest_path=manifest_path,
        log_path=log_path,
        spec_path=spec_path,
    )


def load_deployment_manifest(
    *,
    path: Path,
    run_id: str,
    lock: DependencyLock,
    **_: Any,
) -> Mapping[str, Any]:
    """Validate upstream deployment identity without reconstructing its truth."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise NetworkRuntimeError("5g-Ansible deployment manifest was not found") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NetworkRuntimeError("5g-Ansible deployment manifest must be readable JSON") from exc
    if not isinstance(payload, dict) or payload.get("schema") != DEPLOYMENT_SCHEMA:
        raise NetworkRuntimeError("5g-Ansible deployment manifest schema is unsupported")
    if payload.get("id") != validate_run_id(run_id):
        raise NetworkRuntimeError("5g-Ansible deployment manifest ID does not match")
    if payload.get("state") != "ready":
        raise NetworkRuntimeError("5g-Ansible deployment is not ready")
    if payload.get("fiveg_ansible_commit") != _locked_commit(lock, "fiveg_ansible"):
        raise NetworkRuntimeError("5g-Ansible deployment provenance does not match the lock")
    return payload


def save_network_evidence(
    report: NetworkVerificationReport,
    destination: Path,
    manifest_path: Path | None = None,
) -> None:
    """Persist experiment evidence without mutating the upstream manifest."""

    del manifest_path
    atomic_json(destination, report.to_dict())
