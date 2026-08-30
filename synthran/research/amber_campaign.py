"""Sequential Amber v2 campaign execution and analysis."""

from __future__ import annotations

import json
from pathlib import Path
import statistics
from typing import Any, Mapping, Sequence, TextIO

from synthran.ambient_contract import (
    DEFAULT_ENERGY_NODE_VARIATION,
    DEFAULT_ENERGY_POWER_SCALE,
)
from synthran.dependencies import DependencyLock
from synthran.fiveg_ansible import NetworkInventory
from synthran.research import LoadSpec, MeasurementSpec, ResearchCampaign, ResearchError, atomic_json
from synthran.research.amber_runtime import execute_amber_research_experiment
from synthran.research.campaign_runtime import CampaignRuntimeSession
from synthran.research.v2 import (
    AmberResearchSpec,
    RESEARCH_SUMMARY_SCHEMA_V2,
    require_consistent_campaign_summaries,
)


AMBER_CAMPAIGN_RESULT_SCHEMA = "synthran/research-campaign-result/v2alpha1"
AMBER_CAMPAIGN_ANALYSIS_SCHEMA = "synthran/research-analysis/v2alpha1"


def _condition(campaign: ResearchCampaign, name: str):
    for condition in campaign.conditions:
        if condition.name == name:
            return condition
    raise ResearchError(f"campaign condition {name!r} is unavailable")


def _load_for_condition(
    campaign: ResearchCampaign,
    condition_name: str,
    *,
    reference_capacity_bps: int | None,
    parallel_flows: int,
    server_port: int,
) -> LoadSpec:
    condition = _condition(campaign, condition_name)
    if condition.name == "baseline":
        return LoadSpec()
    if condition.target_bps is not None:
        return LoadSpec(
            enabled=True,
            target_bps=condition.target_bps,
            parallel_flows=parallel_flows,
            server_port=server_port,
        )
    if reference_capacity_bps is None or reference_capacity_bps <= 0:
        raise ResearchError(
            "Amber fractional-load campaign requires positive reference capacity"
        )
    return LoadSpec(
        enabled=True,
        target_fraction=condition.load_fraction,
        reference_capacity_bps=reference_capacity_bps,
        parallel_flows=parallel_flows,
        server_port=server_port,
    )


def _read_summary(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ResearchError(f"unable to read Amber run summary {path}") from exc
    if not isinstance(value, dict) or value.get("schema") != RESEARCH_SUMMARY_SCHEMA_V2:
        raise ResearchError(f"Amber run summary {path} has an unsupported schema")
    return value


def execute_amber_campaign(
    *,
    campaign: ResearchCampaign,
    iot_profile: str,
    energy_power_scale: float = DEFAULT_ENERGY_POWER_SCALE,
    energy_node_variation: float = DEFAULT_ENERGY_NODE_VARIATION,
    inventory: NetworkInventory,
    lock: DependencyLock,
    dependency_root: Path,
    network_manifest: Path,
    network_evidence: Path,
    repository_root: Path,
    run_root: Path,
    target: str,
    reference_capacity_bps: int | None,
    sensor_period_seconds: int,
    measurement: MeasurementSpec,
    parallel_flows: int,
    load_port: int,
    progress: TextIO | None = None,
) -> Path:
    """Execute one immutable campaign schedule using one fixed Amber treatment."""

    result_root = run_root.resolve()
    completed: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    result_path = result_root / f"{campaign.campaign_id}-amber-campaign-v2.json"
    energy_treatment = (
        {
            "external_power_scale": float(energy_power_scale),
            "node_variation_fraction": float(energy_node_variation),
        }
        if iot_profile == "ambient-v1"
        else None
    )

    # Campaign runtime stability belongs to campaign execution, not to the CLI
    # launcher. This scope preserves one UE/gNB/PDU epoch across all scheduled
    # treatments and restores the accepted network exactly once on exit.
    with CampaignRuntimeSession(
        campaign=campaign,
        inventory=inventory,
        lock=lock,
        run_root=result_root,
        target=target,
    ):
        for scheduled in campaign.runs:
            load = _load_for_condition(
                campaign,
                scheduled.condition,
                reference_capacity_bps=reference_capacity_bps,
                parallel_flows=parallel_flows,
                server_port=load_port,
            )
            spec = AmberResearchSpec(
                campaign_id=campaign.campaign_id,
                run_id=scheduled.run_id,
                network_run_id=campaign.network_run_id,
                condition=scheduled.condition,
                iot_profile=iot_profile,
                iot_seed=scheduled.seed,
                sensor_period_seconds=sensor_period_seconds,
                energy_power_scale=energy_power_scale,
                energy_node_variation=energy_node_variation,
                measurement=measurement,
                load=load,
                probe_target=target,
            )
            if progress is not None:
                print(
                    f"[synthran] campaign: run {scheduled.ordinal}/{len(campaign.runs)} "
                    f"{scheduled.run_id}",
                    file=progress,
                    flush=True,
                )
            try:
                summary_path = execute_amber_research_experiment(
                    spec=spec,
                    inventory=inventory,
                    lock=lock,
                    dependency_root=dependency_root,
                    network_manifest=network_manifest,
                    network_evidence=network_evidence,
                    repository_root=repository_root,
                    run_root=result_root,
                    progress=progress,
                )
                summary = _read_summary(summary_path)
            except Exception as exc:
                atomic_json(
                    result_path,
                    {
                        "schema": AMBER_CAMPAIGN_RESULT_SCHEMA,
                        "campaign_id": campaign.campaign_id,
                        "network_run_id": campaign.network_run_id,
                        "iot_source": "amber",
                        "iot_profile": iot_profile,
                        "energy_treatment": energy_treatment,
                        "completed": completed,
                        "failed_run_id": scheduled.run_id,
                        "failure": str(exc),
                    },
                )
                raise
            if summary.get("iot_seed") != scheduled.seed:
                raise ResearchError("Amber campaign summary seed does not match schedule")
            if summary.get("condition") != scheduled.condition:
                raise ResearchError("Amber campaign summary condition does not match schedule")
            if summary.get("energy_treatment") != energy_treatment:
                raise ResearchError("Amber campaign summary energy treatment does not match campaign")
            summaries.append(summary)
            identity = require_consistent_campaign_summaries(summaries)
            completed.append(
                {
                    "ordinal": scheduled.ordinal,
                    "run_id": scheduled.run_id,
                    "condition": scheduled.condition,
                    "iot_seed": scheduled.seed,
                    "summary": str(summary_path),
                }
            )
            atomic_json(
                result_path,
                {
                    "schema": AMBER_CAMPAIGN_RESULT_SCHEMA,
                    "campaign_id": campaign.campaign_id,
                    "network_run_id": campaign.network_run_id,
                    "iot_source": identity[0],
                    "iot_profile": identity[1],
                    "profile_digest": identity[2],
                    "energy_treatment": energy_treatment,
                    "completed": completed,
                    "failed_run_id": None,
                    "failure": None,
                },
            )

    if len(completed) != len(campaign.runs):
        raise ResearchError("Amber campaign did not complete every scheduled run")
    return result_path


def _number(summary: Mapping[str, Any], *path: str) -> float | None:
    current: Any = summary
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    if isinstance(current, (int, float)) and not isinstance(current, bool):
        return float(current)
    return None


def _stats(values: Sequence[float]) -> dict[str, float | None]:
    if not values:
        return {"count": 0, "mean": None, "median": None}
    data = [float(value) for value in values]
    return {
        "count": len(data),
        "mean": statistics.fmean(data),
        "median": statistics.median(data),
    }


def analyze_amber_campaign(
    *,
    campaign: ResearchCampaign,
    run_root: Path,
    output_path: Path,
) -> dict[str, Any]:
    summaries: list[dict[str, Any]] = []
    for scheduled in campaign.runs:
        path = run_root.resolve() / scheduled.run_id / "research-summary-v2.json"
        summary = _read_summary(path)
        if summary.get("campaign_id") != campaign.campaign_id:
            raise ResearchError("Amber summary belongs to a different campaign")
        if summary.get("network_run_id") != campaign.network_run_id:
            raise ResearchError("Amber summary belongs to a different network run")
        if summary.get("iot_seed") != scheduled.seed:
            raise ResearchError("Amber summary seed does not match campaign schedule")
        if summary.get("condition") != scheduled.condition:
            raise ResearchError("Amber summary condition does not match campaign schedule")
        if summary.get("infrastructure_valid") is not True:
            raise ResearchError("Amber campaign contains infrastructure-invalid run")
        summaries.append(summary)

    source, profile, digest = require_consistent_campaign_summaries(summaries)
    treatments = {json.dumps(item.get("energy_treatment"), sort_keys=True) for item in summaries}
    if len(treatments) != 1:
        raise ResearchError("Amber campaign contains mixed energy treatments")
    energy_treatment = summaries[0].get("energy_treatment")

    condition_payload: dict[str, Any] = {}
    for condition in campaign.conditions:
        subset = [item for item in summaries if item.get("condition") == condition.name]
        source_loss_ratios: list[float] = []
        measurement_events: list[float] = []
        rtts: list[float] = []
        loads: list[float] = []
        for summary in subset:
            planned = _number(summary, "source", "planned_opportunities") or 0.0
            source_loss = _number(summary, "source", "source_loss") or 0.0
            source_loss_ratios.append(source_loss / planned if planned else 0.0)
            measurement = _number(summary, "measurement_received_events")
            if measurement is not None:
                measurement_events.append(measurement)
            rtt = _number(summary, "metrics", "mean_rtt_ms")
            if rtt is not None:
                rtts.append(rtt)
            achieved = _number(summary, "metrics", "mean_achieved_load_bps")
            if achieved is not None:
                loads.append(achieved)
        condition_payload[condition.name] = {
            "runs": len(subset),
            "source_loss_ratio": _stats(source_loss_ratios),
            "measurement_received_events": _stats(measurement_events),
            "mean_rtt_ms": _stats(rtts),
            "achieved_load_bps": _stats(loads),
        }

    result = {
        "schema": AMBER_CAMPAIGN_ANALYSIS_SCHEMA,
        "campaign_id": campaign.campaign_id,
        "network_run_id": campaign.network_run_id,
        "iot_source": source,
        "iot_profile": profile,
        "profile_digest": digest,
        "energy_treatment": energy_treatment,
        "runs": len(summaries),
        "conditions": condition_payload,
    }
    atomic_json(output_path, result)
    return result
