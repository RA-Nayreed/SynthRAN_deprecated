"""Campaign-scoped runtime lifecycle for controlled research campaigns.

A research campaign compares load conditions, not UE registration churn. The
single-run experiment runtime patches the srsUE Deployment with an MQTT sidecar
and restores it afterwards. Repeating that lifecycle for every campaign
treatment rolls the UE and forces a fresh RFSIM/PDU session each time.

This module scopes that instrumentation to the whole ``campaign-run`` command:

* install the MQTT sidecar once and keep its pod template stable;
* reconcile RFSIM once, then require the exact UE/gNB/PDU identity to remain
  unchanged for every later treatment;
* reload the campaign MQTT bridge configuration in place between treatments,
  without terminating the sidecar container or accumulating restart backoff;
* preserve the established readiness contract: loaded treatments prove the
  real iperf3 TCP control transport, while baseline treatments use ICMP;
* keep per-run central MQTT/Cooja/measurement resources run-scoped;
* restore the original srsUE Deployment and reprove the accepted network once
  when the campaign command exits, including invalid/aborted campaigns.

All runtime substitutions are restored in ``__exit__`` so single-run research
behavior is unchanged.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shlex
import time
from typing import Any, Mapping, Sequence

from synthran.dependencies import DependencyLock, load_lock
from synthran.experiment.resources import (
    EDGE_VOLUME,
    RUN_LABEL,
    render_edge_cleanup_patch as canonical_edge_cleanup_patch,
)
from synthran.fiveg_ansible import NetworkInventory, load_inventory
from synthran.network.runtime import verify_network_path
from synthran.network.rfsim import (
    RfsimRuntimeState,
    _current_pdu_address,
    _discover_pod,
)
from synthran.research import RESEARCH_CAMPAIGN_SCHEMA, ResearchError, atomic_json
import synthran.experiment.runtime as base_runtime
import synthran.research.instrumentation as research_instrumentation
import synthran.research.runtime as research_runtime


CAMPAIGN_RUNTIME_SCHEMA = "synthran/research-campaign-runtime/v1alpha1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _option_value(arguments: Sequence[str], name: str) -> str | None:
    for index, item in enumerate(arguments):
        if item == name and index + 1 < len(arguments):
            return arguments[index + 1]
        prefix = name + "="
        if item.startswith(prefix):
            return item[len(prefix) :]
    return None


def is_campaign_run(arguments: Sequence[str]) -> bool:
    return tuple(arguments[:2]) == ("research", "campaign-run")


def _campaign_edge_config_name(campaign_id: str) -> str:
    suffix = hashlib.sha256(campaign_id.encode("utf-8")).hexdigest()[:12]
    return f"synthran-campaign-edge-{suffix}"


@dataclass
class CampaignRuntimeSession:
    """Hold one UE/RFSIM/PDU epoch for one CLI campaign execution."""

    arguments: tuple[str, ...]
    campaign_path: Path | None = None
    inventory_path: Path | None = None
    lock_path: Path = Path("dependencies.lock.yml")
    target: str | None = None
    campaign_id: str | None = None
    network_run_id: str | None = None
    expected_run_ids: tuple[str, ...] = ()
    evidence_path: Path | None = None
    stable_state: RfsimRuntimeState | None = None
    final_base_state: RfsimRuntimeState | None = None
    observed_run_ids: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    sidecar_patch_requested: bool = False
    command_exit_code: int | None = None
    cleanup_error: str | None = None
    started_at_utc: str = field(default_factory=_utc_now)
    ended_at_utc: str | None = None

    _inventory: NetworkInventory | None = field(default=None, init=False, repr=False)
    _lock: DependencyLock | None = field(default=None, init=False, repr=False)
    _original_edge_patch: Any = field(default=None, init=False, repr=False)
    _original_edge_cleanup_patch: Any = field(default=None, init=False, repr=False)
    _original_experiment_objects: Any = field(default=None, init=False, repr=False)
    _original_reconcile: Any = field(default=None, init=False, repr=False)
    _original_pre_window_target: Any = field(default=None, init=False, repr=False)
    _original_restart_edge_sidecar: Any = field(default=None, init=False, repr=False)
    _original_sidecar_barrier: Any = field(default=None, init=False, repr=False)
    _original_render_edge_config: Any = field(default=None, init=False, repr=False)

    @classmethod
    def from_cli_arguments(cls, arguments: Sequence[str]) -> "CampaignRuntimeSession":
        campaign = _option_value(arguments, "--campaign")
        inventory = _option_value(arguments, "--inventory")
        lock = _option_value(arguments, "--lock")
        target = _option_value(arguments, "--target")
        return cls(
            arguments=tuple(arguments),
            campaign_path=Path(campaign) if campaign is not None else None,
            inventory_path=Path(inventory) if inventory is not None else None,
            lock_path=Path(lock) if lock is not None else Path("dependencies.lock.yml"),
            target=target,
        )

    def __enter__(self) -> "CampaignRuntimeSession":
        self._original_edge_patch = base_runtime.render_edge_patch
        self._original_edge_cleanup_patch = base_runtime.render_edge_cleanup_patch
        self._original_experiment_objects = base_runtime.render_experiment_objects
        self._original_reconcile = base_runtime.reconcile_rfsim_runtime
        self._original_pre_window_target = research_runtime._prove_pre_window_target
        self._original_restart_edge_sidecar = base_runtime._restart_edge_sidecar
        self._original_sidecar_barrier = (
            research_instrumentation._restart_edge_sidecar_and_wait
        )
        self._original_render_edge_config = base_runtime.render_edge_mosquitto_config

        base_runtime.render_edge_patch = self._render_edge_patch
        base_runtime.render_edge_cleanup_patch = self._render_edge_cleanup_patch
        base_runtime.render_experiment_objects = self._render_experiment_objects
        base_runtime.reconcile_rfsim_runtime = self._reconcile_runtime
        base_runtime._restart_edge_sidecar = self._reload_edge_sidecar
        base_runtime.render_edge_mosquitto_config = self._render_edge_mosquitto_config
        research_instrumentation._restart_edge_sidecar_and_wait = (
            self._reload_edge_sidecar_and_wait
        )
        research_runtime._prove_pre_window_target = self._prove_pre_window_target
        return self

    def __exit__(self, _exc_type: Any, exc: BaseException | None, _tb: Any) -> bool:
        if exc is not None:
            self.errors.append(f"campaign command exception: {exc}")
        try:
            self._restore_base_runtime()
        except Exception as cleanup_exc:
            self.cleanup_error = str(cleanup_exc)
            self.errors.append(f"campaign cleanup: {cleanup_exc}")
        finally:
            self.ended_at_utc = _utc_now()
            self._write_evidence()
            base_runtime.render_edge_patch = self._original_edge_patch
            base_runtime.render_edge_cleanup_patch = self._original_edge_cleanup_patch
            base_runtime.render_experiment_objects = self._original_experiment_objects
            base_runtime.reconcile_rfsim_runtime = self._original_reconcile
            base_runtime._restart_edge_sidecar = self._original_restart_edge_sidecar
            base_runtime.render_edge_mosquitto_config = self._original_render_edge_config
            research_instrumentation._restart_edge_sidecar_and_wait = (
                self._original_sidecar_barrier
            )
            research_runtime._prove_pre_window_target = self._original_pre_window_target
        return False

    def _ensure_campaign(self) -> None:
        if self.campaign_id is not None:
            return
        if self.campaign_path is None:
            raise ResearchError("campaign runtime requires --campaign")
        try:
            payload = json.loads(self.campaign_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ResearchError("campaign runtime could not read the persisted campaign") from exc
        if not isinstance(payload, Mapping) or payload.get("schema") != RESEARCH_CAMPAIGN_SCHEMA:
            raise ResearchError("campaign runtime requires a supported persisted campaign")
        campaign_id = payload.get("campaign_id")
        network_run_id = payload.get("network_run_id")
        raw_runs = payload.get("runs")
        if not isinstance(campaign_id, str) or not isinstance(network_run_id, str):
            raise ResearchError("campaign runtime campaign identity is malformed")
        if not isinstance(raw_runs, list):
            raise ResearchError("campaign runtime run schedule is malformed")
        run_ids: list[str] = []
        for item in raw_runs:
            if not isinstance(item, Mapping) or not isinstance(item.get("run_id"), str):
                raise ResearchError("campaign runtime contains a malformed scheduled run")
            run_ids.append(str(item["run_id"]))
        self.campaign_id = campaign_id
        self.network_run_id = network_run_id
        self.expected_run_ids = tuple(run_ids)
        self.evidence_path = self.campaign_path.with_name(f"{campaign_id}-runtime.json")
        self._write_evidence()

    def _ensure_inventory(self) -> NetworkInventory:
        if self._inventory is None:
            if self.inventory_path is None:
                raise ResearchError("campaign runtime requires --inventory")
            self._inventory = load_inventory(self.inventory_path)
        return self._inventory

    def _ensure_lock(self) -> DependencyLock:
        if self._lock is None:
            self._lock = load_lock(self.lock_path)
        return self._lock

    def _record_run(self, run_id: str) -> None:
        if run_id not in self.observed_run_ids:
            self.observed_run_ids.append(run_id)
            self._write_evidence()

    @property
    def edge_config_name(self) -> str:
        self._ensure_campaign()
        assert self.campaign_id is not None
        return _campaign_edge_config_name(self.campaign_id)

    def _render_edge_patch(
        self,
        scenario: Any,
        *,
        lock: Any,
        core_address: str,
    ) -> Mapping[str, Any]:
        self._ensure_campaign()
        assert self.campaign_id is not None
        assert self.network_run_id is not None
        if scenario.network_run_id != self.network_run_id:
            raise ResearchError("campaign runtime network run changed")
        self._record_run(scenario.run_id)
        patch = deepcopy(
            self._original_edge_patch(
                scenario,
                lock=lock,
                core_address=core_address,
            )
        )
        try:
            template = patch["spec"]["template"]
            annotations = template["metadata"]["annotations"]
            annotations[RUN_LABEL] = self.campaign_id
            volumes = template["spec"]["volumes"]
            edge_volume = next(item for item in volumes if item.get("name") == EDGE_VOLUME)
            edge_volume["configMap"]["name"] = self.edge_config_name
        except (KeyError, StopIteration, TypeError) as exc:
            raise ResearchError("campaign runtime could not stabilize the MQTT sidecar patch") from exc
        self.sidecar_patch_requested = True
        return patch

    def _render_edge_cleanup_patch(self) -> Mapping[str, Any]:
        return {}

    def _render_experiment_objects(
        self,
        scenario: Any,
        *,
        lock: Any,
        core_node: str,
        core_address: str,
    ) -> tuple[Mapping[str, Any], ...]:
        self._ensure_campaign()
        assert self.campaign_id is not None
        assert self.network_run_id is not None
        if scenario.network_run_id != self.network_run_id:
            raise ResearchError("campaign runtime network run changed")
        self._record_run(scenario.run_id)
        objects = list(
            self._original_experiment_objects(
                scenario,
                lock=lock,
                core_node=core_node,
                core_address=core_address,
            )
        )
        if len(objects) < 1:
            raise ResearchError("campaign runtime did not receive the edge MQTT ConfigMap")
        edge = deepcopy(objects[0])
        try:
            metadata = edge["metadata"]
            metadata["name"] = self.edge_config_name
            labels = metadata.setdefault("labels", {})
            labels[RUN_LABEL] = self.campaign_id
        except (KeyError, TypeError) as exc:
            raise ResearchError("campaign runtime edge ConfigMap is malformed") from exc
        objects[0] = edge
        return tuple(objects)

    def _render_edge_mosquitto_config(
        self,
        scenario: Any,
        *,
        central_broker_address: str,
        central_broker_port: int = 1883,
    ) -> str:
        config = self._original_render_edge_config(
            scenario,
            central_broker_address=central_broker_address,
            central_broker_port=central_broker_port,
        )
        if "bridge_reload_type immediate" in config:
            return config
        marker = "restart_timeout 5"
        if marker not in config:
            raise ResearchError("campaign MQTT config is missing the bridge restart marker")
        return config.replace(
            marker,
            "bridge_reload_type immediate\n" + marker,
            1,
        )

    @staticmethod
    def _reload_edge_sidecar(inventory: NetworkInventory, pod: str) -> None:
        base_runtime._remote(
            inventory,
            "sh",
            "-c",
            "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
            f"-n {base_runtime.KUBERNETES_NAMESPACE} {shlex.quote(pod)} "
            f"-c {base_runtime.EDGE_CONTAINER} -- sh -c 'kill -HUP 1'",
            label="edge MQTT sidecar reload",
        )

    @staticmethod
    def _reload_edge_sidecar_and_wait(
        inventory: NetworkInventory,
        pod: str,
        *,
        restart: Any,
        timeout_seconds: int = 60,
    ) -> None:
        before, container_ready, pod_ready, running = (
            research_instrumentation._edge_sidecar_status(inventory, pod)
        )
        if not (container_ready and pod_ready and running):
            raise ResearchError("edge MQTT sidecar is not Ready before in-place config reload")
        restart(inventory, pod)
        time.sleep(1)
        deadline = time.monotonic() + timeout_seconds
        latest = "reload readiness not yet observed"
        while time.monotonic() < deadline:
            try:
                count, container_ready, pod_ready, running = (
                    research_instrumentation._edge_sidecar_status(inventory, pod)
                )
            except Exception as exc:
                latest = str(exc)
            else:
                latest = (
                    f"restartCount={count}, containerReady={container_ready}, "
                    f"podReady={pod_ready}, running={running}"
                )
                if count != before:
                    raise ResearchError(
                        "edge MQTT sidecar restarted during in-place config reload "
                        f"(restartCount {before} -> {count})"
                    )
                if container_ready and pod_ready and running:
                    return
            time.sleep(1)
        raise ResearchError(
            "edge MQTT sidecar reload did not return Ready without a container "
            f"restart within {timeout_seconds}s ({latest})"
        )

    def _reconcile_runtime(
        self,
        inventory: NetworkInventory,
        *,
        network_run_id: str,
    ) -> RfsimRuntimeState:
        self._ensure_campaign()
        assert self.network_run_id is not None
        if network_run_id != self.network_run_id:
            raise ResearchError("campaign runtime reconcile requested a different network run")
        self._inventory = inventory

        if self.stable_state is None:
            state = self._original_reconcile(
                inventory,
                network_run_id=network_run_id,
            )
            self.stable_state = state
            self._write_evidence()
            return state

        expected = self.stable_state
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
        current_pdu = _current_pdu_address(inventory, current_ue)
        if (
            current_ue != expected.ue_pod
            or current_gnb != expected.gnb_pod
            or current_pdu != expected.pdu_address
        ):
            detail = (
                "campaign UE/RFSIM/PDU identity drift: "
                f"expected ue={expected.ue_pod}, gnb={expected.gnb_pod}, pdu={expected.pdu_address}; "
                f"observed ue={current_ue}, gnb={current_gnb}, pdu={current_pdu}"
            )
            self.errors.append(detail)
            self._write_evidence()
            raise ResearchError(detail)
        return expected

    @staticmethod
    def _prove_pre_window_target(
        *,
        spec: Any,
        prove_icmp: Any,
        prove_transport: Any,
    ) -> None:
        if spec.load.enabled:
            prove_transport()
            return
        prove_icmp()

    def _restore_base_runtime(self) -> None:
        if not self.sidecar_patch_requested:
            return
        self._ensure_campaign()
        assert self.campaign_id is not None
        assert self.network_run_id is not None
        inventory = self._ensure_inventory()
        lock = self._ensure_lock()

        deployment = base_runtime._discover_ue_deployment(
            inventory,
            self.network_run_id,
        )
        base_runtime._kubectl_patch_deployment(
            inventory,
            deployment,
            canonical_edge_cleanup_patch(),
            label="campaign srsUE sidecar cleanup",
        )
        base_runtime._wait_rollout(
            inventory,
            deployment,
            label="campaign srsUE cleanup rollout",
        )
        self.final_base_state = self._original_reconcile(
            inventory,
            network_run_id=self.network_run_id,
        )
        base_runtime._delete_experiment_objects(inventory, self.campaign_id)

        report = verify_network_path(
            inventory=inventory,
            lock=lock,
            run_id=self.network_run_id,
            timeout_seconds=120,
        )
        if not report.ready:
            failing = [
                f"{check.name}: {check.detail}"
                for check in report.checks
                if not check.passed
            ]
            detail = "; ".join(failing) if failing else "network verification failed"
            raise ResearchError(
                "campaign cleanup did not restore the accepted network: " + detail
            )

    def _write_evidence(self) -> None:
        if self.evidence_path is None:
            return
        stable = self.stable_state
        final = self.final_base_state
        expected = set(self.expected_run_ids)
        observed = set(self.observed_run_ids)
        if self.cleanup_error is not None:
            status = "cleanup-failed"
        elif self.ended_at_utc is not None:
            status = (
                "complete"
                if self.command_exit_code == 0 and expected and observed == expected
                else "aborted"
            )
        elif stable is not None:
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
                "started_at_utc": self.started_at_utc,
                "ended_at_utc": self.ended_at_utc,
                "status": status,
                "command_exit_code": self.command_exit_code,
                "expected_run_ids": list(self.expected_run_ids),
                "observed_run_ids": list(self.observed_run_ids),
                "stable_runtime": (
                    {
                        "ue_pod": stable.ue_pod,
                        "gnb_pod": stable.gnb_pod,
                        "gnb_deployment": stable.gnb_deployment,
                        "pdu_address": stable.pdu_address,
                    }
                    if stable is not None
                    else None
                ),
                "restored_base_runtime": (
                    {
                        "ue_pod": final.ue_pod,
                        "gnb_pod": final.gnb_pod,
                        "gnb_deployment": final.gnb_deployment,
                        "pdu_address": final.pdu_address,
                    }
                    if final is not None
                    else None
                ),
                "cleanup_error": self.cleanup_error,
                "errors": list(self.errors),
            },
        )


def campaign_runtime_session(arguments: Sequence[str]) -> CampaignRuntimeSession:
    if not is_campaign_run(arguments):
        raise ResearchError("campaign runtime session is valid only for campaign-run")
    return CampaignRuntimeSession.from_cli_arguments(arguments)
