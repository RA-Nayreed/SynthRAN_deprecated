"""Portable IoT source contracts and offline Amber plan preparation.

This module deliberately places the portable boundary above source-specific radio
mechanisms. Cooja remains the accepted live path until the Amber transport is
proven; new v2 artifacts never claim RPL, 6LoWPAN, SLIP, a PDU address, or a UE
interface as properties of the IoT source.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from typing import Any, Iterable, Mapping, Protocol, Sequence

from synthran.dependencies import DependencyError, GitDependency, load_lock
from synthran.experiment import (
    DEFAULT_SENSOR_PERIOD_SECONDS,
    DEFAULT_TOPIC_PREFIX,
    ExperimentError,
    SENSOR_COUNT,
    SENSOR_RE,
    TELEMETRY_SCHEMA,
    validate_run_id,
)


IOT_EXPERIMENT_SCHEMA_V2 = "synthran/iot-experiment/v2alpha1"
IOT_SOURCE_EVENT_SCHEMA_V2 = "synthran/iot-source-event/v2alpha1"
PUBLISHER_EVENT_SCHEMA_V2 = "synthran/publisher-event/v2alpha1"
IOT_EVIDENCE_SCHEMA_V2 = "synthran/iot-evidence/v2alpha1"
AMBER_SOURCE_ID = "amber"
TRANSPORT_PROFILE = "transport-v1"
AMBIENT_PROFILE = "ambient-v1"
SUPPORTED_PROFILES = frozenset({TRANSPORT_PROFILE, AMBIENT_PROFILE})
DEFAULT_IOT_SEED = 424242
AMBER_ENERGY_TRACE = Path("demo_experiments/threehundred_seconds_stable.xlsx")


class IoTSourceError(ExperimentError):
    """Raised when portable source preparation or evidence is invalid."""


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), sort_keys=True, separators=(",", ":"))


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    try:
        return _sha256_bytes(path.read_bytes())
    except OSError as exc:
        raise IoTSourceError(f"unable to hash source input: {path}") from exc


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(content, encoding="utf-8", newline="\n")
    temporary.replace(path)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    _atomic_text(path, json.dumps(dict(value), indent=2, sort_keys=True) + "\n")


@dataclass(frozen=True)
class MQTTEndpoint:
    """Source-facing MQTT endpoint; backend transport details stay elsewhere."""

    host: str
    port: int

    def __post_init__(self) -> None:
        if not self.host.strip():
            raise IoTSourceError("MQTT endpoint host must be non-empty")
        if self.port <= 0 or self.port > 65535:
            raise IoTSourceError("MQTT endpoint port is invalid")


class CollectorBarrier(Protocol):
    def wait_ready(self) -> None: ...


class IoTSourceSession(Protocol):
    def stop(self) -> None: ...

    def evidence(self) -> Mapping[str, Any]: ...


class IoTSourceAdapter(Protocol):
    def prepare(
        self,
        spec: "IoTSourceSpec",
        duration_seconds: int,
        run_directory: Path,
    ) -> "PreparedIoTPlan": ...

    def start(
        self,
        plan: "PreparedIoTPlan",
        mqtt_endpoint: MQTTEndpoint,
        collector_barrier: CollectorBarrier,
    ) -> IoTSourceSession: ...


@dataclass(frozen=True)
class IoTSourceSpec:
    run_id: str
    network_run_id: str
    source: str = AMBER_SOURCE_ID
    profile: str = TRANSPORT_PROFILE
    seed: int = DEFAULT_IOT_SEED
    sensor_count: int = SENSOR_COUNT
    sensor_period_seconds: int = DEFAULT_SENSOR_PERIOD_SECONDS
    topic_prefix: str = DEFAULT_TOPIC_PREFIX

    def __post_init__(self) -> None:
        validate_run_id(self.run_id)
        validate_run_id(self.network_run_id)
        if self.source != AMBER_SOURCE_ID:
            raise IoTSourceError(f"unsupported IoT source: {self.source!r}")
        if self.profile not in SUPPORTED_PROFILES:
            raise IoTSourceError(f"unsupported Amber profile: {self.profile!r}")
        if self.seed < 0:
            raise IoTSourceError("IoT seed must be non-negative")
        if self.sensor_count != SENSOR_COUNT:
            raise IoTSourceError("Amber profiles require exactly 10 sensors")
        if self.sensor_period_seconds <= 0 or self.sensor_period_seconds > 3600:
            raise IoTSourceError("sensor period must be between 1 and 3600 seconds")
        if not self.topic_prefix or any(
            character not in "abcdefghijklmnopqrstuvwxyz0123456789_-"
            for character in self.topic_prefix
        ):
            raise IoTSourceError("topic prefix contains unsupported characters")

    @property
    def topic_root(self) -> str:
        return f"{self.topic_prefix}/{self.run_id}"

    @property
    def sensor_ids(self) -> tuple[str, ...]:
        return tuple(f"sensor-{index:02d}" for index in range(1, SENSOR_COUNT + 1))


@dataclass(frozen=True)
class IoTSourceEvent:
    run_id: str
    source: str
    profile: str
    profile_digest: str
    planned_at_ms: int
    sensor_id: str
    sequence: int
    value_milli: int
    transmitted: bool
    decoded: bool
    outcome: str
    slot_index: int | None = None
    details: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        validate_run_id(self.run_id)
        if self.source != AMBER_SOURCE_ID:
            raise IoTSourceError("source event must identify Amber")
        if self.profile not in SUPPORTED_PROFILES:
            raise IoTSourceError("source event profile is unsupported")
        if len(self.profile_digest) != 64:
            raise IoTSourceError("source event profile digest must be sha256 hex")
        if self.planned_at_ms < 0:
            raise IoTSourceError("planned source time must be non-negative")
        if not SENSOR_RE.fullmatch(self.sensor_id):
            raise IoTSourceError("source sensor ID must match sensor-01..sensor-10")
        if self.sequence < 1:
            raise IoTSourceError("source sequence must be positive")
        if self.decoded and not self.transmitted:
            raise IoTSourceError("decoded source event must have been transmitted")
        if not self.outcome:
            raise IoTSourceError("source outcome must be classified")

    @property
    def key(self) -> tuple[str, int]:
        return self.sensor_id, self.sequence

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": IOT_SOURCE_EVENT_SCHEMA_V2,
            "run_id": self.run_id,
            "source": self.source,
            "profile": self.profile,
            "profile_digest": self.profile_digest,
            "planned_at_ms": self.planned_at_ms,
            "sensor_id": self.sensor_id,
            "sequence": self.sequence,
            "value_milli": self.value_milli,
            "transmitted": self.transmitted,
            "decoded": self.decoded,
            "outcome": self.outcome,
            "slot_index": self.slot_index,
            "details": dict(self.details or {}),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "IoTSourceEvent":
        if value.get("schema") != IOT_SOURCE_EVENT_SCHEMA_V2:
            raise IoTSourceError("unsupported source-event schema")
        return cls(
            run_id=str(value.get("run_id", "")),
            source=str(value.get("source", "")),
            profile=str(value.get("profile", "")),
            profile_digest=str(value.get("profile_digest", "")),
            planned_at_ms=int(value.get("planned_at_ms", -1)),
            sensor_id=str(value.get("sensor_id", "")),
            sequence=int(value.get("sequence", 0)),
            value_milli=int(value.get("value_milli", 0)),
            transmitted=value.get("transmitted") is True,
            decoded=value.get("decoded") is True,
            outcome=str(value.get("outcome", "")),
            slot_index=(
                int(value["slot_index"])
                if value.get("slot_index") is not None
                else None
            ),
            details=value.get("details") if isinstance(value.get("details"), dict) else {},
        )


@dataclass(frozen=True)
class PreparedIoTPlan:
    spec: IoTSourceSpec
    duration_seconds: int
    amber_commit: str
    profile_digest: str
    energy_trace_sha256: str | None
    events: tuple[IoTSourceEvent, ...]
    scenario_path: Path
    source_jsonl_path: Path
    source_parquet_path: Path
    evidence_path: Path

    @property
    def planned_count(self) -> int:
        return len(self.events)

    @property
    def decoded_count(self) -> int:
        return sum(event.decoded for event in self.events)

    @property
    def source_loss_count(self) -> int:
        return self.planned_count - self.decoded_count


@dataclass(frozen=True)
class SourceTransportReconciliation:
    planned_count: int
    decoded_count: int
    source_loss_count: int
    published_count: int
    central_received_count: int
    transport_loss_count: int
    duplicate_count: int
    unexpected_central_count: int
    valid: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "planned_count": self.planned_count,
            "decoded_count": self.decoded_count,
            "source_loss_count": self.source_loss_count,
            "published_count": self.published_count,
            "central_received_count": self.central_received_count,
            "transport_loss_count": self.transport_loss_count,
            "duplicate_count": self.duplicate_count,
            "unexpected_central_count": self.unexpected_central_count,
            "valid": self.valid,
        }


def profile_descriptor(
    spec: IoTSourceSpec,
    *,
    amber_commit: str,
    energy_trace_sha256: str | None,
) -> dict[str, Any]:
    common: dict[str, Any] = {
        "source": spec.source,
        "profile": spec.profile,
        "amber_commit": amber_commit,
        "sensor_count": spec.sensor_count,
        "sensor_period_seconds": spec.sensor_period_seconds,
    }
    if spec.profile == TRANSPORT_PROFILE:
        common["model"] = {
            "energy": "ideal",
            "coverage": "ideal",
            "access": "deterministic-unicast",
            "collision": "disabled",
        }
    else:
        if not energy_trace_sha256:
            raise IoTSourceError("ambient-v1 requires a pinned energy trace digest")
        common["model"] = {
            "frequency_hz": 924_000_000.0,
            "pathloss": "macro",
            "los": True,
            "energy_mode": "hybrid",
            "energy_trace_sha256": energy_trace_sha256,
            "node_radius_m": [5.0, 40.0],
            "aloha_slots": 16,
            "slot_ms": 8,
            "command_ms": 5,
            "sic": True,
            "capture": "amber-default",
            "capacitor": {
                "capacitance_f": 0.0003,
                "r_series_ohm": 5000.0,
                "r_leakage_ohm": 100000.0,
                "dt_seconds": 0.001,
            },
            "thresholds_v": {"low": 1.3, "high": 1.7},
        }
    return common


def profile_digest(descriptor: Mapping[str, Any]) -> str:
    return _sha256_bytes(_canonical_json(descriptor).encode("utf-8"))


def scenario_record(
    spec: IoTSourceSpec,
    *,
    duration_seconds: int,
    amber_commit: str,
    resolved_profile_digest: str,
    energy_trace_sha256: str | None,
) -> dict[str, Any]:
    return {
        "schema": IOT_EXPERIMENT_SCHEMA_V2,
        "run_id": spec.run_id,
        "network_run_id": spec.network_run_id,
        "iot_source": spec.source,
        "iot_profile": spec.profile,
        "iot_seed": spec.seed,
        "duration_seconds": duration_seconds,
        "sensor_count": spec.sensor_count,
        "sensor_period_seconds": spec.sensor_period_seconds,
        "sensor_ids": list(spec.sensor_ids),
        "topic_root": spec.topic_root,
        "profile_digest": resolved_profile_digest,
        "amber_commit": amber_commit,
        "energy_trace_sha256": energy_trace_sha256,
        "data_contract": TELEMETRY_SCHEMA,
    }


def load_source_events(path: Path) -> tuple[IoTSourceEvent, ...]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise IoTSourceError(f"unable to read source plan: {path}") from exc
    events: list[IoTSourceEvent] = []
    for number, line in enumerate(lines, start=1):
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise IoTSourceError(f"source-event line {number} is invalid JSON") from exc
        if not isinstance(value, dict):
            raise IoTSourceError(f"source-event line {number} must be an object")
        events.append(IoTSourceEvent.from_dict(value))
    events.sort(key=lambda event: (event.planned_at_ms, event.sensor_id, event.sequence))
    keys = [event.key for event in events]
    if len(keys) != len(set(keys)):
        raise IoTSourceError("source plan contains duplicate sensor/sequence pairs")
    return tuple(events)


def write_source_jsonl(events: Sequence[IoTSourceEvent], path: Path) -> None:
    ordered = sorted(events, key=lambda event: (event.planned_at_ms, event.sensor_id))
    _atomic_text(
        path,
        "".join(
            json.dumps(event.to_dict(), sort_keys=True, separators=(",", ":")) + "\n"
            for event in ordered
        ),
    )


def write_source_parquet(events: Sequence[IoTSourceEvent], path: Path) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise IoTSourceError("PyArrow is required for source-plan Parquet") from exc

    records = []
    for event in sorted(events, key=lambda item: (item.planned_at_ms, item.sensor_id)):
        value = event.to_dict()
        records.append(
            {
                "schema": value["schema"],
                "run_id": value["run_id"],
                "source": value["source"],
                "profile": value["profile"],
                "profile_digest": value["profile_digest"],
                "planned_at_ms": value["planned_at_ms"],
                "sensor_id": value["sensor_id"],
                "sequence": value["sequence"],
                "value_milli": value["value_milli"],
                "transmitted": value["transmitted"],
                "decoded": value["decoded"],
                "outcome": value["outcome"],
                "slot_index": value["slot_index"],
                "details_json": _canonical_json(value["details"]),
            }
        )
    schema = pa.schema(
        [
            ("schema", pa.string()),
            ("run_id", pa.string()),
            ("source", pa.string()),
            ("profile", pa.string()),
            ("profile_digest", pa.string()),
            ("planned_at_ms", pa.int64()),
            ("sensor_id", pa.string()),
            ("sequence", pa.int64()),
            ("value_milli", pa.int64()),
            ("transmitted", pa.bool_()),
            ("decoded", pa.bool_()),
            ("outcome", pa.string()),
            ("slot_index", pa.int64()),
            ("details_json", pa.string()),
        ]
    )
    table = pa.Table.from_pylist(records, schema=schema)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        path,
        compression="zstd",
        use_dictionary=False,
        write_statistics=True,
        data_page_version="1.0",
    )


def reconcile_source_and_transport(
    events: Sequence[IoTSourceEvent],
    *,
    published_pairs: Iterable[tuple[str, int]],
    central_pairs: Sequence[tuple[str, int]],
) -> SourceTransportReconciliation:
    planned = {event.key for event in events}
    decoded = {event.key for event in events if event.decoded}
    published = set(published_pairs)
    central = set(central_pairs)
    duplicates = len(central_pairs) - len(central)
    unexpected = central - decoded
    transport_loss = decoded - central
    valid = (
        published == decoded
        and not transport_loss
        and not unexpected
        and duplicates == 0
    )
    return SourceTransportReconciliation(
        planned_count=len(planned),
        decoded_count=len(decoded),
        source_loss_count=len(planned - decoded),
        published_count=len(published),
        central_received_count=len(central),
        transport_loss_count=len(transport_loss),
        duplicate_count=duplicates,
        unexpected_central_count=len(unexpected),
        valid=valid,
    )


class AmberSourceAdapter:
    """Prepare immutable Amber event plans for the portable source runtime."""

    def __init__(
        self,
        *,
        repository_root: Path | None = None,
        dependency_root: Path | None = None,
        python_executable: str | None = None,
    ) -> None:
        self.repository_root = (
            repository_root.resolve()
            if repository_root is not None
            else Path(__file__).resolve().parents[1]
        )
        self.dependency_root = (
            dependency_root.resolve()
            if dependency_root is not None
            else self.repository_root / ".deps"
        )
        self.python_executable = python_executable or sys.executable

    def _amber_dependency(self) -> GitDependency:
        try:
            lock = load_lock(self.repository_root / "dependencies.lock.yml")
        except DependencyError as exc:
            raise IoTSourceError(str(exc)) from exc
        for dependency in lock.git:
            if dependency.name == "amber":
                return dependency
        raise IoTSourceError("dependency lock does not contain Amber")

    def _verify_amber_checkout(self) -> tuple[GitDependency, Path]:
        dependency = self._amber_dependency()
        checkout = self.dependency_root.joinpath(*dependency.checkout.parts)
        if not (checkout / ".git").exists():
            raise IoTSourceError(
                "Amber checkout is missing; run 'synthran deps sync' before preparation"
            )
        try:
            head = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=checkout,
                check=True,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ).stdout.strip()
            dirty = subprocess.run(
                ["git", "status", "--porcelain"],
                cwd=checkout,
                check=True,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError) as exc:
            raise IoTSourceError("unable to verify the Amber checkout") from exc
        if head != dependency.commit:
            raise IoTSourceError(
                f"Amber checkout drifted: expected {dependency.commit}, observed {head}"
            )
        if dirty:
            raise IoTSourceError("Amber checkout has local changes")
        return dependency, checkout

    def prepare(
        self,
        spec: IoTSourceSpec,
        duration_seconds: int,
        run_directory: Path,
    ) -> PreparedIoTPlan:
        if duration_seconds <= 0:
            raise IoTSourceError("source duration must be positive")
        dependency, amber_checkout = self._verify_amber_checkout()
        run_directory = run_directory.resolve()
        run_directory.mkdir(parents=True, exist_ok=True)

        energy_trace: Path | None = None
        energy_trace_sha256: str | None = None
        if spec.profile == AMBIENT_PROFILE:
            energy_trace = amber_checkout / AMBER_ENERGY_TRACE
            if not energy_trace.is_file():
                raise IoTSourceError("pinned Amber energy trace is missing")
            energy_trace_sha256 = _sha256_file(energy_trace)

        descriptor = profile_descriptor(
            spec,
            amber_commit=dependency.commit,
            energy_trace_sha256=energy_trace_sha256,
        )
        resolved_digest = profile_digest(descriptor)
        scenario = scenario_record(
            spec,
            duration_seconds=duration_seconds,
            amber_commit=dependency.commit,
            resolved_profile_digest=resolved_digest,
            energy_trace_sha256=energy_trace_sha256,
        )
        scenario["profile"] = descriptor

        planner_input = run_directory / "amber-plan-input.json"
        planner_output = run_directory / "amber-source-events.jsonl"
        scenario_path = run_directory / "iot-scenario-v2.json"
        parquet_path = run_directory / "amber-source-events.parquet"
        evidence_path = run_directory / "iot-evidence-v2.json"

        _atomic_json(
            planner_input,
            {
                **scenario,
                "energy_trace_path": str(energy_trace) if energy_trace is not None else None,
            },
        )
        command = [
            self.python_executable,
            "-m",
            "synthran.amber_planner",
            "--amber-root",
            str(amber_checkout),
            "--input",
            str(planner_input),
            "--output",
            str(planner_output),
        ]
        try:
            completed = subprocess.run(
                command,
                cwd=self.repository_root,
                check=True,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or "").strip()
            suffix = f": {detail}" if detail else ""
            raise IoTSourceError(f"Amber planner failed{suffix}") from exc
        except OSError as exc:
            raise IoTSourceError("unable to launch the isolated Amber planner") from exc

        events = load_source_events(planner_output)
        expected_opportunities = (
            (duration_seconds * 1000 + spec.sensor_period_seconds * 1000 - 1)
            // (spec.sensor_period_seconds * 1000)
        ) * spec.sensor_count
        if len(events) != expected_opportunities:
            raise IoTSourceError(
                "Amber planner did not emit exactly one opportunity per sensor/period"
            )
        if any(event.run_id != spec.run_id for event in events):
            raise IoTSourceError("Amber planner returned events for a different run")
        if any(event.profile_digest != resolved_digest for event in events):
            raise IoTSourceError("Amber planner returned a mismatched profile digest")
        if spec.profile == TRANSPORT_PROFILE and any(not event.decoded for event in events):
            raise IoTSourceError("transport-v1 requires every planned opportunity to decode")

        write_source_jsonl(events, planner_output)
        write_source_parquet(events, parquet_path)
        _atomic_json(scenario_path, scenario)
        _atomic_json(
            evidence_path,
            {
                "schema": IOT_EVIDENCE_SCHEMA_V2,
                "run_id": spec.run_id,
                "iot_source": spec.source,
                "iot_profile": spec.profile,
                "iot_seed": spec.seed,
                "profile_digest": resolved_digest,
                "amber_commit": dependency.commit,
                "amber_checkout_clean": True,
                "energy_trace_sha256": energy_trace_sha256,
                "planned_count": len(events),
                "decoded_count": sum(event.decoded for event in events),
                "source_loss_count": sum(not event.decoded for event in events),
                "planner_stdout": completed.stdout.strip(),
                "live_transport": None,
            },
        )
        return PreparedIoTPlan(
            spec=spec,
            duration_seconds=duration_seconds,
            amber_commit=dependency.commit,
            profile_digest=resolved_digest,
            energy_trace_sha256=energy_trace_sha256,
            events=events,
            scenario_path=scenario_path,
            source_jsonl_path=planner_output,
            source_parquet_path=parquet_path,
            evidence_path=evidence_path,
        )

    def start(
        self,
        plan: PreparedIoTPlan,
        mqtt_endpoint: MQTTEndpoint,
        collector_barrier: CollectorBarrier,
    ) -> IoTSourceSession:
        del plan, mqtt_endpoint, collector_barrier
        raise IoTSourceError(
            "live Amber replay is not enabled on the contracts-only branch"
        )
