"""Stable RFSIM ownership for sequential Amber campaign runs.

A campaign is one experimental epoch. The srsUE pod, gNB pod, and PDU address
are established once. Scheduled runs may replace run-scoped central resources
and reload the MQTT bridge, but they must not roll the UE or restart the RFSIM
radio epoch. Final cleanup restores the accepted base network exactly once.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
import shlex
import time
from typing import Any, Mapping

import synthran.amber_experiment_runtime as amber_runtime
import synthran.experiment.runtime as base_runtime
from synthran.dependencies import DependencyLock
from synthran.experiment.resources import (
    EDGE_VOLUME,
    RUN_LABEL,
    names,
    render_edge_cleanup_patch as canonical_edge_cleanup_patch,
)
from synthran.fiveg_ansible import NetworkInventory
from synthran.network.rfsim import (
    RfsimRuntimeState,
    _current_pdu_address,
    _deployment_owner_for_pod,
    _discover_pod,
)
from synthran.research import ResearchCampaign, ResearchError, atomic_json


CAMPAIGN_RUNTIME_SCHEMA = "synthran/research-campaign-runtime/v2alpha1"


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


class CampaignRuntimeSession:
    """Keep one UE/gNB/PDU epoch stable for one Amber campaign execution."""

    def __init__(
        self,
        *,
        campaign: ResearchCampaign,
        inventory: NetworkInventory,
        lock: DependencyLock,
        run_root: Path,
        target: str | None = None,
    ) -> None:
        self.campaign = campaign
        self.inventory = inventory
        self.lock = lock
        self.run_root = run_root.expanduser().resolve()
        self.target = target

        self.stable: StableRuntime | None = None
        self.current_run_id: str | None = None
        self.observed_run_ids: list[str] = []
        self.reloads: list[dict[str, Any]] = []
        self.cleanup_errors: list[str] = []
        self.cleanup_valid: bool | None = None
        self.base_network_reproved: bool | None = None
        self.sidecar_patch_requested = False
        self.campaign_resources_requested = False
        self.command_failed = False
        self._entered = False

        self._original_cleanup_patch = None
        self._original_base_reconcile = None
        self._original_amber_reconcile = None
        self._original_amber_edge_patch = None
        self._original_amber_objects = None
        self._original_amber_config = None
        self._original_amber_restart_wait = None

    @property
    def campaign_id(self) -> str:
        return self.campaign.campaign_id

    @property
    def network_run_id(self) -> str:
        return self.campaign.network_run_id

    @property
    def expected_run_ids(self) -> tuple[str, ...]:
        return tuple(item.run_id for item in self.campaign.runs)

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

        self._original_cleanup_patch = base_runtime.render_edge_cleanup_patch
        self._original_base_reconcile = base_runtime.reconcile_rfsim_runtime
        self._original_amber_reconcile = amber_runtime.reconcile_rfsim_runtime
        self._original_amber_edge_patch = amber_runtime.render_edge_patch
        self._original_amber_objects = amber_runtime.render_experiment_objects
        self._original_amber_config = amber_runtime.render_edge_mosquitto_config
        self._original_amber_restart_wait = amber_runtime._restart_edge_sidecar_and_wait

        # Per-run cleanup must preserve the campaign-owned sidecar and radio
        # epoch. The final session cleanup restores both exactly once.
        base_runtime.render_edge_cleanup_patch = lambda: {}
        base_runtime.reconcile_rfsim_runtime = self._reconcile_runtime

        # Amber imported these hooks directly, so bind the campaign contract at
        # the actual live execution boundary too.
        amber_runtime.render_edge_patch = self._render_edge_patch
        amber_runtime.render_experiment_objects = self._render_experiment_objects
        amber_runtime.render_edge_mosquitto_config = self._render_edge_config
        amber_runtime.reconcile_rfsim_runtime = self._reconcile_runtime
        amber_runtime._restart_edge_sidecar_and_wait = self._reload_edge_sidecar_and_wait

        self._entered = True
        self._write_evidence(final=False)
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if not self._entered:
            return False
        self.command_failed = exc is not None

        assert self._original_cleanup_patch is not None
        assert self._original_base_reconcile is not None
        assert self._original_amber_reconcile is not None
        assert self._original_amber_edge_patch is not None
        assert self._original_amber_objects is not None
        assert self._original_amber_config is not None
        assert self._original_amber_restart_wait is not None

        # Restore canonical functions before final cleanup so cleanup itself
        # cannot accidentally use the campaign no-op hooks.
        base_runtime.render_edge_cleanup_patch = self._original_cleanup_patch
        base_runtime.reconcile_rfsim_runtime = self._original_base_reconcile
        amber_runtime.reconcile_rfsim_runtime = self._original_amber_reconcile
        amber_runtime.render_edge_patch = self._original_amber_edge_patch
        amber_runtime.render_experiment_objects = self._original_amber_objects
        amber_runtime.render_edge_mosquitto_config = self._original_amber_config
        amber_runtime._restart_edge_sidecar_and_wait = self._original_amber_restart_wait

        try:
            self._restore_base_runtime()
        except Exception as cleanup_exc:
            self.cleanup_errors.append(str(cleanup_exc))
            self.cleanup_valid = False
            self._write_evidence(final=True)
            if exc_type is None:
                raise ResearchError(
                    f"campaign cleanup failed: {cleanup_exc}"
                ) from cleanup_exc
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
            self._write_evidence(final=False)

    def _render_edge_patch(self, scenario, *, lock, core_address):
        self._require_run(scenario.run_id)
        patch = deepcopy(
            self._original_amber_edge_patch(
                scenario,
                lock=lock,
                core_address=core_address,
            )
        )
        template = patch["spec"]["template"]
        template.setdefault("metadata", {}).setdefault("annotations", {})[
            RUN_LABEL
        ] = self.campaign_id
        volumes = template["spec"]["volumes"]
        matches = [item for item in volumes if item.get("name") == EDGE_VOLUME]
        if len(matches) != 1:
            raise ResearchError("campaign edge patch has ambiguous MQTT config volume")
        matches[0]["configMap"]["name"] = self.campaign_edge_config_name
        self.sidecar_patch_requested = True
        return patch

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
            deepcopy(
                self._original_amber_objects(
                    scenario,
                    lock=lock,
                    core_node=core_node,
                    core_address=core_address,
                )
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
        metadata.setdefault("labels", {})[RUN_LABEL] = self.campaign_id
        self.campaign_resources_requested = True
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
            raise ResearchError("campaign refused a per-run PDU change before MQTT reload")
        config = self._original_amber_config(
            scenario,
            central_broker_address=central_broker_address,
            central_broker_port=central_broker_port,
        )
        if "bridge_reload_type immediate" in config:
            return config
        marker = "restart_timeout 5"
        if marker not in config:
            raise ResearchError("campaign MQTT config has no bridge restart marker")
        return config.replace(
            marker,
            "bridge_reload_type immediate\n" + marker,
            1,
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

        current_ue = _discover_pod(
            inventory,
            component="ue",
            network_run_id=network_run_id,
        )
        current_gnb = _discover_pod(
            inventory,
            component="gnb",
            network_run_id=network_run_id,
        )
        current_gnb_deployment = _deployment_owner_for_pod(inventory, current_gnb)
        current_pdu = _current_pdu_address(inventory, current_ue)
        observed = StableRuntime(
            ue_pod=current_ue,
            gnb_pod=current_gnb,
            gnb_deployment=current_gnb_deployment,
            pdu_address=current_pdu,
        )
        if observed != self.stable:
            raise ResearchError(
                "campaign RFSIM identity drift: "
                f"expected ue={self.stable.ue_pod}, gnb={self.stable.gnb_pod}, "
                f"gnb-deployment={self.stable.gnb_deployment}, pdu={self.stable.pdu_address}; "
                f"observed ue={observed.ue_pod}, gnb={observed.gnb_pod}, "
                f"gnb-deployment={observed.gnb_deployment}, pdu={observed.pdu_address}"
            )
        return self.stable.to_state()

    def _reload_edge_sidecar_and_wait(
        self,
        inventory: NetworkInventory,
        pod: str,
        *,
        timeout_seconds: int = 60,
    ) -> None:
        if self.stable is not None and pod != self.stable.ue_pod:
            raise ResearchError("campaign MQTT reload targeted a different UE pod")
        before, container_ready, pod_ready, running = amber_runtime._edge_sidecar_status(
            inventory,
            pod,
        )
        if not (container_ready and pod_ready and running):
            raise ResearchError("campaign MQTT sidecar is not Ready before config reload")

        base_runtime._remote(
            inventory,
            "sh",
            "-c",
            "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
            f"-n {amber_runtime.KUBERNETES_NAMESPACE} {shlex.quote(pod)} "
            f"-c {amber_runtime.EDGE_CONTAINER} -- sh -c 'kill -HUP 1'",
            label="campaign edge MQTT config reload",
        )

        deadline = time.monotonic() + timeout_seconds
        latest = "reload readiness not yet observed"
        while time.monotonic() < deadline:
            try:
                count, container_ready, pod_ready, running = amber_runtime._edge_sidecar_status(
                    inventory,
                    pod,
                )
            except Exception as status_exc:
                latest = str(status_exc)
            else:
                latest = (
                    f"restartCount={count}, containerReady={container_ready}, "
                    f"podReady={pod_ready}, running={running}"
                )
                if count != before:
                    raise ResearchError(
                        "campaign MQTT sidecar restarted during in-place config reload "
                        f"(restartCount {before} -> {count})"
                    )
                if container_ready and pod_ready and running:
                    self.reloads.append(
                        {
                            "run_id": self.current_run_id,
                            "ue_pod": pod,
                            "method": "SIGHUP",
                            "restart_count": count,
                        }
                    )
                    self._write_evidence(final=False)
                    return
            time.sleep(0.5)
        raise ResearchError(
            "campaign MQTT sidecar did not remain Ready after in-place config reload "
            f"within {timeout_seconds}s ({latest})"
        )

    def _restore_base_runtime(self) -> None:
        if self.sidecar_patch_requested:
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

        if self.campaign_resources_requested:
            base_runtime._delete_experiment_objects(self.inventory, self.campaign_id)

        restored = base_runtime.verify_network_path(
            inventory=self.inventory,
            lock=self.lock,
            run_id=self.network_run_id,
            timeout_seconds=120,
        )
        self.base_network_reproved = restored.ready
        if not restored.ready:
            failing = "; ".join(
                f"{check.name}: {check.detail}"
                for check in restored.checks
                if not check.passed
            )
            raise ResearchError(
                "campaign cleanup did not reprove the accepted base network"
                + (f" ({failing})" if failing else "")
            )

    def _write_evidence(self, *, final: bool) -> None:
        expected = set(self.expected_run_ids)
        observed = set(self.observed_run_ids)
        if final:
            if self.cleanup_valid is False:
                status = "cleanup-failed"
            elif self.command_failed:
                status = "aborted"
            elif expected and observed == expected:
                status = "complete"
            else:
                status = "incomplete"
        elif self.stable is not None:
            status = "active"
        else:
            status = "initializing"

        atomic_json(
            self.evidence_path,
            {
                "schema": CAMPAIGN_RUNTIME_SCHEMA,
                "campaign_id": self.campaign_id,
                "network_run_id": self.network_run_id,
                "target": self.target,
                "status": status,
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
                "sidecar_patch_requested": self.sidecar_patch_requested,
                "campaign_resources_requested": self.campaign_resources_requested,
                "cleanup_valid": self.cleanup_valid if final else None,
                "base_network_reproved": self.base_network_reproved if final else None,
                "cleanup_errors": list(self.cleanup_errors),
            },
        )
