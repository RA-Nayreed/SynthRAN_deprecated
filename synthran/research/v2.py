"""Versioned Amber research artifacts and campaign consistency checks."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from synthran.ambient_contract import (
    DEFAULT_ENERGY_NODE_VARIATION,
    DEFAULT_ENERGY_POWER_SCALE,
    consume_run_energy_treatment,
    register_run_energy_treatment,
    validate_energy_treatment,
)
from synthran.iot_source import (
    AMBER_SOURCE_ID,
    AMBIENT_PROFILE,
    SUPPORTED_PROFILES,
    TRANSPORT_PROFILE,
    IoTSourceSpec,
)
from synthran.research import (
    CONDITION_RE,
    ID_RE,
    LoadSpec,
    MeasurementSpec,
    ResearchError,
    _identifier,
    atomic_json,
)


RESEARCH_EXPERIMENT_SCHEMA_V2 = "synthran/research-experiment/v2alpha1"
RESEARCH_SUMMARY_SCHEMA_V2 = "synthran/research-summary/v2alpha1"


# The established live executor constructs IoTSourceSpec from its accepted
# transport signature. Carry the run-scoped research treatment into that
# immutable source spec at construction time, then consume it exactly once.
_ORIGINAL_IOT_SOURCE_POST_INIT = IoTSourceSpec.__post_init__
if not getattr(IoTSourceSpec, "_synthran_energy_treatment_aware", False):
    def _energy_treatment_aware_source_post_init(self: IoTSourceSpec) -> None:
        registered = consume_run_energy_treatment(self.run_id)
        if registered is not None:
            scale, variation = registered
            observed = (
                float(self.energy_power_scale),
                float(self.energy_node_variation),
            )
            defaults = (
                DEFAULT_ENERGY_POWER_SCALE,
                DEFAULT_ENERGY_NODE_VARIATION,
            )
            if observed == defaults:
                object.__setattr__(self, "energy_power_scale", scale)
                object.__setattr__(self, "energy_node_variation", variation)
            elif observed != registered:
                raise ResearchError(
                    "live Amber source energy treatment does not match its research specification"
                )
        _ORIGINAL_IOT_SOURCE_POST_INIT(self)

    IoTSourceSpec.__post_init__ = _energy_treatment_aware_source_post_init
    setattr(IoTSourceSpec, "_synthran_energy_treatment_aware", True)


@dataclass(frozen=True)
class AmberResearchSpec:
    """Backend-neutral request for one controlled Amber research run."""

    campaign_id: str
    run_id: str
    network_run_id: str
    condition: str
    iot_profile: str = TRANSPORT_PROFILE
    iot_seed: int = 424242
    sensor_period_seconds: int = 10
    sensor_count: int = 10
    energy_power_scale: float = DEFAULT_ENERGY_POWER_SCALE
    energy_node_variation: float = DEFAULT_ENERGY_NODE_VARIATION
    measurement: MeasurementSpec = MeasurementSpec()
    load: LoadSpec = LoadSpec()
    probe_target: str | None = None

    def __post_init__(self) -> None:
        _identifier(self.campaign_id, ID_RE, "campaign ID")
        _identifier(self.run_id, ID_RE, "run ID")
        _identifier(self.network_run_id, ID_RE, "network run ID")
        _identifier(self.condition, CONDITION_RE, "condition")
        if self.iot_profile not in SUPPORTED_PROFILES:
            raise ResearchError(f"unsupported Amber IoT profile: {self.iot_profile!r}")
        if self.iot_seed < 0:
            raise ResearchError("IoT seed must be non-negative")
        if not 1 <= self.sensor_period_seconds <= 3600:
            raise ResearchError("sensor period must be between 1 and 3600 seconds")
        if self.sensor_count != 10:
            raise ResearchError("controlled Amber research requires exactly 10 sensors")
        if self.iot_profile == AMBIENT_PROFILE:
            try:
                validate_energy_treatment(
                    self.energy_power_scale,
                    self.energy_node_variation,
                )
            except ValueError as exc:
                raise ResearchError(str(exc)) from exc
        elif (
            self.energy_power_scale != DEFAULT_ENERGY_POWER_SCALE
            or self.energy_node_variation != DEFAULT_ENERGY_NODE_VARIATION
        ):
            raise ResearchError(
                "energy treatment is valid only for the ambient-v1 profile"
            )
        if self.condition == "baseline" and self.load.enabled:
            raise ResearchError("baseline condition must not enable background load")
        if self.condition != "baseline" and not self.load.enabled:
            raise ResearchError("non-baseline condition requires an enabled load")
        if self.probe_target is not None and not self.probe_target.strip():
            raise ResearchError("probe target must not be empty")
        if self.iot_profile == AMBIENT_PROFILE:
            try:
                register_run_energy_treatment(
                    self.run_id,
                    self.energy_power_scale,
                    self.energy_node_variation,
                )
            except ValueError as exc:
                raise ResearchError(str(exc)) from exc

    @property
    def total_source_seconds(self) -> int:
        return self.measurement.warmup_seconds + self.measurement.duration_seconds

    @property
    def energy_treatment(self) -> dict[str, float] | None:
        if self.iot_profile != AMBIENT_PROFILE:
            return None
        return {
            "external_power_scale": float(self.energy_power_scale),
            "node_variation_fraction": float(self.energy_node_variation),
        }

    def to_request_dict(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "run_id": self.run_id,
            "network_run_id": self.network_run_id,
            "condition": self.condition,
            "iot_source": AMBER_SOURCE_ID,
            "iot_profile": self.iot_profile,
            "iot_seed": self.iot_seed,
            "sensor_period_seconds": self.sensor_period_seconds,
            "sensor_count": self.sensor_count,
            "energy_treatment": self.energy_treatment,
            "measurement": self.measurement.to_dict(),
            "load": self.load.to_dict(),
            "probe_target": self.probe_target,
        }


def research_experiment_artifact(
    spec: AmberResearchSpec,
    *,
    profile_digest: str,
    amber_commit: str,
    energy_trace_sha256: str | None,
) -> dict[str, Any]:
    if len(profile_digest) != 64:
        raise ResearchError("research profile digest must be sha256 hex")
    if len(amber_commit) != 40:
        raise ResearchError("research Amber commit must be a full Git commit")
    return {
        "schema": RESEARCH_EXPERIMENT_SCHEMA_V2,
        **spec.to_request_dict(),
        "profile_digest": profile_digest,
        "amber_commit": amber_commit,
        "energy_trace_sha256": energy_trace_sha256,
    }


def research_summary_artifact(
    spec: AmberResearchSpec,
    *,
    profile_digest: str,
    planned_opportunities: int,
    decoded_opportunities: int,
    published_events: int,
    received_events: int,
    source_loss: int,
    transport_loss: int,
    duplicate_count: int,
    measurement_received_events: int,
    infrastructure_valid: bool,
    scientific_valid: bool,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if min(
        planned_opportunities,
        decoded_opportunities,
        published_events,
        received_events,
        source_loss,
        transport_loss,
        duplicate_count,
        measurement_received_events,
    ) < 0:
        raise ResearchError("research summary counters must be non-negative")
    if planned_opportunities - decoded_opportunities != source_loss:
        raise ResearchError("research source-loss counters do not reconcile")
    if decoded_opportunities - received_events != transport_loss:
        raise ResearchError("research transport-loss counters do not reconcile")
    if spec.iot_profile == TRANSPORT_PROFILE and source_loss != 0:
        raise ResearchError("transport-v1 research summary cannot contain source loss")
    if transport_loss != 0 or duplicate_count != 0:
        infrastructure_valid = False
    value: dict[str, Any] = {
        "schema": RESEARCH_SUMMARY_SCHEMA_V2,
        "campaign_id": spec.campaign_id,
        "run_id": spec.run_id,
        "network_run_id": spec.network_run_id,
        "condition": spec.condition,
        "iot_source": AMBER_SOURCE_ID,
        "iot_profile": spec.iot_profile,
        "iot_seed": spec.iot_seed,
        "profile_digest": profile_digest,
        "sensor_period_seconds": spec.sensor_period_seconds,
        "energy_treatment": spec.energy_treatment,
        "measurement": spec.measurement.to_dict(),
        "source": {
            "planned_opportunities": planned_opportunities,
            "decoded_opportunities": decoded_opportunities,
            "source_loss": source_loss,
        },
        "transport": {
            "published_events": published_events,
            "received_events": received_events,
            "transport_loss": transport_loss,
            "duplicate_count": duplicate_count,
        },
        "measurement_received_events": measurement_received_events,
        "infrastructure_valid": infrastructure_valid,
        "scientific_valid": scientific_valid,
    }
    if extra:
        value["metrics"] = dict(extra)
    return value


def save_research_experiment_v2(
    spec: AmberResearchSpec,
    path: Path,
    *,
    profile_digest: str,
    amber_commit: str,
    energy_trace_sha256: str | None,
) -> None:
    atomic_json(
        path,
        research_experiment_artifact(
            spec,
            profile_digest=profile_digest,
            amber_commit=amber_commit,
            energy_trace_sha256=energy_trace_sha256,
        ),
    )


def save_research_summary_v2(path: Path, summary: Mapping[str, Any]) -> None:
    if summary.get("schema") != RESEARCH_SUMMARY_SCHEMA_V2:
        raise ResearchError("research summary v2 schema is required")
    atomic_json(path, summary)


def campaign_identity(summary: Mapping[str, Any]) -> tuple[str, str, str]:
    if summary.get("schema") != RESEARCH_SUMMARY_SCHEMA_V2:
        raise ResearchError(
            "new Amber campaign analysis accepts only immutable v2 summaries; legacy Cooja campaigns remain historical"
        )
    source = summary.get("iot_source")
    profile = summary.get("iot_profile")
    digest = summary.get("profile_digest")
    if source != AMBER_SOURCE_ID:
        raise ResearchError("Amber campaign summary has a different IoT source")
    if profile not in SUPPORTED_PROFILES:
        raise ResearchError("Amber campaign summary has an unsupported IoT profile")
    if not isinstance(digest, str) or len(digest) != 64:
        raise ResearchError("Amber campaign summary profile digest is invalid")
    return str(source), str(profile), digest


def require_consistent_campaign_summaries(
    summaries: Sequence[Mapping[str, Any]],
) -> tuple[str, str, str]:
    if not summaries:
        raise ResearchError("campaign analysis requires at least one summary")
    identities = [campaign_identity(summary) for summary in summaries]
    first = identities[0]
    if any(identity != first for identity in identities[1:]):
        raise ResearchError(
            "campaign analysis rejects mixed IoT source/profile/profile-digest runs"
        )
    return first


def measurement_source_bounds(spec: AmberResearchSpec) -> tuple[int, int]:
    start_ms = spec.measurement.warmup_seconds * 1000
    end_ms = (spec.measurement.warmup_seconds + spec.measurement.duration_seconds) * 1000
    return start_ms, end_ms


def select_measurement_telemetry(
    spec: AmberResearchSpec,
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Exclude warmup events using immutable source simulation timestamps."""

    start_ms, end_ms = measurement_source_bounds(spec)
    selected: list[dict[str, Any]] = []
    for record in records:
        try:
            source_ms = int(record["sensor_time_ms"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ResearchError(
                "research telemetry is missing a valid source simulation timestamp"
            ) from exc
        if start_ms <= source_ms < end_ms:
            selected.append(dict(record))
    return selected
