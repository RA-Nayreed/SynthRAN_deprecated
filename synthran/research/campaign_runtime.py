"""Stable RFSIM runtime ownership for sequential Amber campaign runs.

A campaign is one experimental epoch. The srsUE sidecar and PDU identity are
established once, then each scheduled run updates only run-scoped MQTT routing
and measurement state. Per-run cleanup removes run-owned central objects while
final campaign cleanup restores the accepted base network exactly once.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Mapping

import synthran.amber_experiment_runtime as amber_runtime
import synthran.experiment.runtime as base_runtime
from synthran.dependencies import load_lock
from synthran.experiment import ExperimentError
from synthran.experiment.resources import (
    EDGE_VOLUME,
    RUN_LABEL,
    names,
    render_edge_cleanup_patch as canonical_edge_cleanup_patch,
)
from synthran.fiveg_ansible import NetworkInventory, load_inventory
from synthran.network.rfsim import RfsimRuntimeState
from synthran.research import ResearchError, atomic_json


CAMPAIGN_RUNTIME_SCHEMA = "synthran/research-campaign-runtime/v2alpha1"


@dataclass(frozen=True)
class CampaignIdentity:
    campaign_id: str
    network_run_id: str
    expected_run_ids: tuple[str, ...]


@dataclass(frozen=True)
class StableRuntime:
    ue_pod: str
    gnb_pod: str
    gnb_deployment: str
    pdu_address: str

    @classmethod
    def from_state(cls, value: RfsimRuntimeState) -> "StableRuntime":
        return cls(
            ue_pod=value.ue_pod,
            gnb_pod=value.gnb_pod,
            gnb_deployment=value.gnb_deployment,
            pdu_address=value.pdu_address,
        )

    def to_state(self) -> RfsimRuntimeState:
        return RfsimRuntimeState(
            ue_pod=self.ue_pod,
            gnb_pod=self.gnb_pod,
            gnb_deployment=self.gnb_deployment,
            pdu_address=self.pdu_address,
        )


def _load_campaign(path: Path) -> CampaignIdentity:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResearchError("campaign runtime requires a readable campaign file") from exc
    if not isinstance(value, dict) or value.get("schema") != "synthran/research-campaign/v1alpha1":
        raise ResearchError("campaign runtime requires a persisted campaign specification")
    campaign_id = value.get("campaign_id")
    network_run_id = value.get("network_run_id")
    raw_runs = value.get("runs")
    if not isinstance(campaign_id, str) or not campaign_id:
        raise ResearchError("campaign runtime specification has no campaign ID")
    if not isinstance(network_run_id, str) or not network_run_id:
        raise ResearchError("campaign runtime specification has no network run ID")
    if not isinstance(raw_runs, list) or not raw_runs:
        raise ResearchError("campaign runtime specification has no scheduled runs")
    run_ids: list[str] = []
    for item in raw_runs:
        run_id = item.get("run_id") if isinstance(item, dict) else None
        if not isinstance(run_id, str) or not run_id:
            raise ResearchError("campaign runtime specification contains a malformed run")
        run_ids.append(run_id)
    if len(run_ids) != len(set(run_ids)):
        raise ResearchError("campaign runtime specification contains duplicate run IDs")
    return CampaignIdentity(campaign_id, network_run_id, tuple(run_ids))


class CampaignRuntimeSession:
    """Keep one srsUE/RFSIM/PDU epoch stable for an Amber campaign."""

    def __init__(
        self,
        *,
        campaign_path: Path,
        inventory_path: Path,
        lock_path: Path = Path("dependencies.lock.yml"),
        run_root: Path = Path(".synthran/experiments"),
        target: str | None = None,
    ) -> None:
        self.campaign_path = campaign_path.expanduser().resolve()
        self.inventory_path = inventory_path.expanduser().resolve()
        self.lock_path = lock_path.expanduser().resolve()
        self.run_root = run_root.expanduser().resolve()
        self.target = target

        self.identity: CampaignIdentity | None = None
        self.inventory: NetworkInventory | None = None
        self.lock = None
        self.stable: StableRuntime | None = None
        self.current_run_id: str | None = None
        self.observed_run_ids: list[str] = []
        self.reloads: list[dict[str, Any]] = []
        self.cleanup_errors: list[str] = []
        self.cleanup_valid: bool | None = None
        self.base_network_reproved: bool | None = None
        self._entered = False

        self._original_cleanup_patch = None
        self._original_base_reconcile = None
        self._original_amber_reconcile = None
        self._original_amber_edge_patch = None
        self._original_amber_objects = None
        self._original_amber_config = None
        self._original_amber_restart = None

    @property
    def campaign_id(self) -> str:
        if self.identity is None:
            raise ResearchError("campaign runtime identity is unavailable")
        return self.identity.campaign_id

    @property
    def network_run_id(self) -> str:
        if self.identity is None:
            raise ResearchError("campaign runtime identity is unavailable")
        return self.identity.network_run_id

    @property
    def expected_run_ids(self) -> tuple[str, ...]:
        if self.identity is None:
            raise ResearchError("campaign runtime identity is unavailable")
        return self.identity.expected_run_ids

    @property
    def evidence_path(self) -> Path:
        return self.run_root / f"{self.campaign_id}-campaign-runtime.json"

    @property
    def campaign_edge_config_name(self) -> str:
        import hashlib

        suffix = hashlib.sha256(self.campaign_id.encode("utf-8")).hexdigest()[:12]
        return f"synthran-exp-edge-{suffix}"

    def __enter__(self) -> "CampaignRuntimeSession":
        if self._entered:
            raise ResearchError("campaign runtime session cannot be entered twice")
        self.identity = _load_campaign(self.campaign_path)
        self.inventory = load_inventory(self.inventory_path)
        self.lock = load_lock(self.lock_path)

        self._original_cleanup_patch = base_runtime.render_edge_cleanup_patch
        self._original_base_reconcile = base_runtime.reconcile_rfsim_runtime
        self._original_amber_reconcile = amber_runtime.reconcile_rfsim_runtime
        self._original_amber_edge_patch = amber_runtime.render_edge_patch
        self._original_amber_objects = amber_runtime.render_experiment_objects
        self._original_amber_config = amber_runtime.render_edge_mosquitto_config
        self._original_amber_restart = amber_runtime._restart_edge_sidecar

        # Per-run cleanup must leave the campaign-owned sidecar in place. The
        # base cleanup function resolves these globals at call time.
        base_runtime.render_edge_cleanup_patch = lambda: {}
        base_runtime.reconcile_rfsim_runtime = self._reconcile_runtime

        # Amber imports its runtime hooks directly, so bind the same campaign
        # contract at the actual Amber execution boundary as well.
        amber_runtime.render_edge_patch = self._render_edge_patch
        amber_runtime.render_experiment_objects = self._render_experiment_objects
        amber_runtime.render_edge_mosquitto_config = self._render_edge_config
        amber_runtime.reconcile_rfsim_runtime = self._reconcile_runtime
        amber_runtime._restart_edge_sidecar = self._reload_edge_sidecar

        self._entered = True
        self._write_evidence(final=False)
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if not self._entered:
            return False
        assert self._original_cleanup_patch is not None
        assert self._original_base_reconcile is not None
        assert self._original_amber_reconcile is not None
        assert self._original_amber_edge_patch is not None
        assert self._original_amber_objects is not None
        assert self._original_amber_config is not None
        assert self._original_amber_restart is not None

        base_runtime.render_edge_cleanup_patch = self._original_cleanup_patch
        base_runtime.reconcile_rfsim_runtime = self._original_base_reconcile
        amber_runtime.reconcile_rfsim_runtime = self._original_amber_reconcile
        amber_runtime.render_edge_patch = self._original_amber_edge_patch
        amber_runtime.render_experiment_objects = self._original_amber_objects
        amber_runtime.render_edge_mosquitto_config = self._original_amber_config
        amber_runtime._restart_edge_sidecar = self._original_amber_restart

        try:
            self._restore_base_runtime()
        except Exception as cleanup_exc:
            self.cleanup_errors.append(str(cleanup_exc))
            self.cleanup_valid = False
            self._write_evidence(final=True)
            if exc_type is None:
                raise
        else:
            self.cleanup_valid = True
            self._write_evidence(final=True)
        finally:
            self._entered = False
        return False

    def _require_run(self, run_id: str) -> None:
        if run_id not in self.expected_run_ids:
            raise ResearchError(
                f"campaign runtime refuses run {run_id!r}; it is not in the persisted schedule"
            )
        self.current_run_id = run_id
        if run_id not in self.observed_run_ids:
            self.observed_run_ids.append(run_id)

    def _render_edge_patch(self, scenario, *, lock, core_address):
        self._require_run(scenario.run_id)
        patch = self._original_amber_edge_patch(
            scenario,
            lock=lock,
            core_address=core_address,
        )
        result = json.loads(json.dumps(patch))
        template = result["spec"]["template"]
        template.setdefault("metadata", {}).setdefault("annotations", {})[
            RUN_LABEL
        ] = self.campaign_id
        volumes = template["spec"]["volumes"]
        matches = [item for item in volumes if item.get("name") == EDGE_VOLUME]
        if len(matches) != 1:
            raise ResearchError("campaign edge patch has ambiguous MQTT config volume")
        matches[0]["configMap"]["name"] = self.campaign_edge_config_name
        return result

    def _render_experiment_objects(
        self,
        scenario,
        *,
        lock,
        core_node,
        core_address,
    ):
        self._require_run(scenario.run_id)
        objects = list(
            self._original_amber_objects(
                scenario,
                lock=lock,
                core_node=core_node,
                core_address=core_address,
            )
        )
        expected_edge_name = names(scenario)["edge_config"]
        edge_matches = [
            item
            for item in objects
            if item.get("kind") == "ConfigMap"
            and isinstance(item.get("metadata"), Mapping)
            and item["metadata"].get("name") == expected_edge_name
        ]
        if len(edge_matches) != 1:
            raise ResearchError("campaign could not identify exactly one edge MQTT ConfigMap")
        edge = edge_matches[0]
        metadata = edge["metadata"]
        metadata["name"] = self.campaign_edge_config_name
        labels = metadata.setdefault("labels", {})
        labels[RUN_LABEL] = self.campaign_id
        return tuple(objects)

    def _render_edge_config(
        self,
        scenario,
        *,
        central_broker_address,
        central_broker_port,
    ) -> str:
        self._require_run(scenario.run_id)
        if self.stable is not None and scenario.pdu_address != self.stable.pdu_address:
            raise ResearchError(
                "campaign refused a per-run PDU change before MQTT configuration"
            )
        return self._original_amber_config(
            scenario,
            central_broker_address=central_broker_address,
            central_broker_port=central_broker_port,
        )

    def _reconcile_runtime(
        self,
        inventory: NetworkInventory,
        *,
        network_run_id: str,
    ) -> RfsimRuntimeState:
        if network_run_id != self.network_run_id:
            raise ResearchError("campaign runtime was asked to reconcile a different network")
        if self.stable is None:
            state = self._original_amber_reconcile(
                inventory,
                network_run_id=network_run_id,
            )
            self.stable = StableRuntime.from_state(state)
            self._write_evidence(final=False)
            return state

        ue_pod = base_runtime._discover_ue_deployment(inventory, network_run_id)
        # _discover_ue_deployment returns the deployment, so verify the actual
        # run-owned UE pod separately through the canonical RFSIM discovery.
        del ue_pod
        current = self._original_amber_reconcile
        # Do not invoke full reconciliation after the first run: that restarts
        # the radio epoch. Prove the existing pod/PDU directly instead.
        from synthran.network.rfsim import _current_pdu_address, _discover_pod

        current_ue_pod = _discover_pod(
            inventory,
            component="ue",
            network_run_id=network_run_id,
        )
        if current_ue_pod != self.stable.ue_pod:
            raise ResearchError("campaign UE pod changed between scheduled runs")
        current_pdu = _current_pdu_address(inventory, current_ue_pod)
        if current_pdu != self.stable.pdu_address:
            raise ResearchError("campaign PDU address changed between scheduled runs")
        return self.stable.to_state()

    def _reload_edge_sidecar(self, inventory: NetworkInventory, pod: str) -> None:
        if self.stable is not None and pod != self.stable.ue_pod:
            raise ResearchError("campaign MQTT reload targeted a different UE pod")
        before = base_runtime._container_restart_count(
            inventory,
            pod,
            amber_runtime.EDGE_CONTAINER,
        )
        self._original_amber_restart(inventory, pod)
        after = base_runtime._wait_container_restart(
            inventory,
            pod,
            amber_runtime.EDGE_CONTAINER,
            before,
        )
        self.reloads.append(
            {
                "run_id": self.current_run_id,
                "ue_pod": pod,
                "before_restart_count": before,
                "after_restart_count": after,
            }
        )
        self._write_evidence(final=False)

    def _restore_base_runtime(self) -> None:
        if self.inventory is None or self.lock is None:
            raise ResearchError("campaign cleanup has no inventory or dependency lock")
        if self.stable is None:
            self.cleanup_valid = True
            self.base_network_reproved = None
            return

        deployment = base_runtime._discover_ue_deployment(
            self.inventory,
            self.network_run_id,
        )
        base_runtime._kubectl_patch_deployment(
            self.inventory,
            deployment,
            canonical_edge_cleanup_patch(),
            label="campaign srsUE sidecar cleanup",
        )
        base_runtime._wait_rollout(
            self.inventory,
            deployment,
            label="campaign srsUE cleanup rollout",
        )
        self._original_base_reconcile(
            self.inventory,
            network_run_id=self.network_run_id,
        )
        base_runtime._delete_experiment_objects(self.inventory, self.campaign_id)
        restored = base_runtime.verify_network_path(
            inventory=self.inventory,
            lock=self.lock,
            run_id=self.network_run_id,
            timeout_seconds=120,
        )
        self.base_network_reproved = restored.ready
        if not restored.ready:
            raise ResearchError("campaign cleanup did not reprove the accepted base network")

    def _write_evidence(self, *, final: bool) -> None:
        if self.identity is None:
            return
        atomic_json(
            self.evidence_path,
            {
                "schema": CAMPAIGN_RUNTIME_SCHEMA,
                "campaign_id": self.campaign_id,
                "network_run_id": self.network_run_id,
                "target": self.target,
                "expected_run_ids": list(self.expected_run_ids),
                "observed_run_ids": list(self.observed_run_ids),
                "stable_runtime": (
                    {
                        "ue_pod": self.stable.ue_pod,
                        "gnb_pod": self.stable.gnb_pod,
                        "gnb_deployment": self.stable.gnb_deployment,
                        "pdu_address": self.stable.pdu_address,
                    }
                    if self.stable is not None
                    else None
                ),
                "mqtt_reloads": list(self.reloads),
                "cleanup_valid": self.cleanup_valid if final else None,
                "base_network_reproved": self.base_network_reproved if final else None,
                "cleanup_errors": list(self.cleanup_errors),
            },
        )
