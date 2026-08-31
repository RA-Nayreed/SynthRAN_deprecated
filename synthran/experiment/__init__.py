"""Amber IoT-to-5G experiment contracts and artifact handling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import tempfile
from typing import Any, Iterable, Mapping, Sequence


EXPERIMENT_SCHEMA = "synthran/iot-experiment/v2alpha1"
EXPERIMENT_EVIDENCE_SCHEMA = "synthran/iot-evidence/v2alpha1"
TELEMETRY_SCHEMA = "synthran/telemetry/v1alpha1"
SENSOR_COUNT = 10
DEFAULT_TOPIC_PREFIX = "synthran"
DEFAULT_SENSOR_PERIOD_SECONDS = 10
EXPECTED_PDU_NETWORK = ipaddress.ip_network("12.1.0.0/16")
RUN_ID_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
SENSOR_RE = re.compile(r"^sensor-(0[1-9]|10)$")


class ExperimentError(RuntimeError):
    """Raised when the experiment contract is violated."""


def _atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        delete=False,
    ) as temporary:
        temporary.write(content)
        temporary_path = Path(temporary.name)
    temporary_path.replace(path)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def validate_run_id(value: str) -> str:
    if not RUN_ID_RE.fullmatch(value):
        raise ExperimentError(
            "run ID must be 1-63 lowercase letters, numbers, or internal hyphens"
        )
    return value


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sha256_file(path: Path) -> str:
    try:
        return sha256_bytes(path.read_bytes())
    except OSError as exc:
        raise ExperimentError(f"unable to hash {path}") from exc


def load_path_proven_network(
    manifest_path: Path,
    evidence_path: Path,
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    """Load the persisted network/session readiness evidence used by a workload."""

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ExperimentError("accepted network evidence is missing") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExperimentError("network acceptance evidence must be readable JSON") from exc

    # The persisted network schema still uses the historical status token.  It is
    # treated here as session-readiness evidence, not as an end-to-end traffic proof.
    if not isinstance(manifest, dict) or manifest.get("status") != "path-proven":
        raise ExperimentError("experiment requires an accepted network manifest")
    if (
        not isinstance(evidence, dict)
        or evidence.get("schema") != "synthran/network-evidence/v1alpha1"
        or evidence.get("ready") is not True
    ):
        raise ExperimentError("experiment requires ready network acceptance evidence")
    if manifest.get("network_evidence") != evidence_path.name:
        raise ExperimentError("network manifest does not reference the supplied evidence")
    if manifest.get("run_id") != evidence.get("run_id"):
        raise ExperimentError("network manifest/evidence run IDs do not match")

    path = evidence.get("path")
    if not isinstance(path, dict):
        raise ExperimentError("network session evidence is malformed")
    pdu_address = path.get("pdu_address")
    pdu_network = path.get("pdu_network")
    if not isinstance(pdu_address, str) or not isinstance(pdu_network, str):
        raise ExperimentError("network evidence does not contain a PDU address/network")
    try:
        observed_address = ipaddress.ip_address(pdu_address)
        observed_network = ipaddress.ip_network(pdu_network)
    except ValueError as exc:
        raise ExperimentError("network PDU evidence contains invalid IP data") from exc
    if observed_network != EXPECTED_PDU_NETWORK or observed_address not in observed_network:
        raise ExperimentError("PDU session does not match the accepted golden path")
    if path.get("ue_interface") != "tun_srsue1":
        raise ExperimentError("accepted UE interface is not tun_srsue1")
    if (
        path.get("slice") != "slice1"
        or path.get("sst") != 1
        or path.get("dnn") != "internet"
    ):
        raise ExperimentError("slice evidence does not match the accepted golden path")
    return manifest, evidence


@dataclass(frozen=True)
class ExperimentScenario:
    """Network attachment context shared by Amber source/transport execution."""

    run_id: str
    network_run_id: str
    pdu_address: str
    sensor_count: int = SENSOR_COUNT
    sensor_period_seconds: int = DEFAULT_SENSOR_PERIOD_SECONDS
    topic_prefix: str = DEFAULT_TOPIC_PREFIX

    def __post_init__(self) -> None:
        validate_run_id(self.run_id)
        validate_run_id(self.network_run_id)
        if self.sensor_count != SENSOR_COUNT:
            raise ExperimentError("Ambient-IoT profile requires exactly 10 sensors")
        if self.sensor_period_seconds <= 0 or self.sensor_period_seconds > 3600:
            raise ExperimentError("sensor period must be between 1 and 3600 seconds")
        try:
            pdu = ipaddress.ip_address(self.pdu_address)
        except ValueError as exc:
            raise ExperimentError("PDU address is invalid") from exc
        if pdu not in EXPECTED_PDU_NETWORK:
            raise ExperimentError("PDU address is outside the accepted network")
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,31}", self.topic_prefix):
            raise ExperimentError("topic prefix contains unsupported characters")

    @property
    def topic_root(self) -> str:
        return f"{self.topic_prefix}/{self.run_id}"

    @property
    def sensor_topic(self) -> str:
        return f"{self.topic_root}/sensor/+"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": EXPERIMENT_SCHEMA,
            "run_id": self.run_id,
            "network_run_id": self.network_run_id,
            "pdu_address": self.pdu_address,
            "ue_interface": "tun_srsue1",
            "sensor_count": self.sensor_count,
            "sensor_period_seconds": self.sensor_period_seconds,
            "topic_root": self.topic_root,
            "data_contract": TELEMETRY_SCHEMA,
        }


def build_scenario(
    *,
    run_id: str,
    network_manifest: Path,
    network_evidence: Path,
    sensor_period_seconds: int = DEFAULT_SENSOR_PERIOD_SECONDS,
) -> ExperimentScenario:
    manifest, evidence = load_path_proven_network(network_manifest, network_evidence)
    path = evidence["path"]
    return ExperimentScenario(
        run_id=validate_run_id(run_id),
        network_run_id=str(manifest["run_id"]),
        pdu_address=str(path["pdu_address"]),
        sensor_period_seconds=sensor_period_seconds,
    )


def render_edge_mosquitto_config(
    scenario: ExperimentScenario,
    *,
    central_broker_address: str,
    central_broker_port: int = 1883,
) -> str:
    """Render an edge broker whose bridge is bound to the accepted UE address."""

    try:
        central = ipaddress.ip_address(central_broker_address)
    except ValueError as exc:
        raise ExperimentError("central broker address must be a literal IP address") from exc
    if central_broker_port <= 0 or central_broker_port > 65535:
        raise ExperimentError("central broker port is invalid")
    return "\n".join(
        (
            "per_listener_settings true",
            "listener 1883",
            "allow_anonymous true",
            "persistence false",
            "log_type all",
            "connection synthran-central",
            f"address {central}:{central_broker_port}",
            f"bridge_bind_address {scenario.pdu_address}",
            "bridge_protocol_version mqttv311",
            "cleansession true",
            "notifications false",
            "restart_timeout 5",
            f"topic {scenario.topic_root}/# out 1",
            "",
        )
    )


def render_central_mosquitto_config() -> str:
    return "\n".join(
        (
            "per_listener_settings true",
            "listener 1883 0.0.0.0",
            "allow_anonymous true",
            "persistence false",
            "log_type all",
            "",
        )
    )


@dataclass(frozen=True)
class TelemetryEvent:
    run_id: str
    sensor_id: str
    sequence: int
    sensor_time_ms: int
    value_milli: int

    @classmethod
    def from_payload(
        cls,
        payload: bytes | str,
        expected_run_id: str,
    ) -> "TelemetryEvent":
        validate_run_id(expected_run_id)
        try:
            text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
            value = json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ExperimentError("telemetry payload is not valid JSON") from exc
        if not isinstance(value, dict):
            raise ExperimentError("telemetry payload must be a JSON object")
        if value.get("schema") != TELEMETRY_SCHEMA:
            raise ExperimentError("telemetry payload schema is unsupported")
        if value.get("run_id") != expected_run_id:
            raise ExperimentError("telemetry event run ID does not match the active experiment")
        sensor_id = value.get("sensor_id")
        if not isinstance(sensor_id, str) or not SENSOR_RE.fullmatch(sensor_id):
            raise ExperimentError("telemetry sensor ID must match sensor-01..sensor-10")
        sequence = value.get("sequence")
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
            raise ExperimentError("telemetry sequence must be a positive integer")
        sensor_time_ms = value.get("sensor_time_ms")
        if not isinstance(sensor_time_ms, int) or isinstance(sensor_time_ms, bool) or sensor_time_ms < 0:
            raise ExperimentError("telemetry sensor_time_ms must be a non-negative integer")
        value_milli = value.get("value_milli")
        if not isinstance(value_milli, int) or isinstance(value_milli, bool):
            raise ExperimentError("telemetry value_milli must be an integer")
        return cls(
            run_id=expected_run_id,
            sensor_id=sensor_id,
            sequence=sequence,
            sensor_time_ms=sensor_time_ms,
            value_milli=value_milli,
        )

    def to_record(self, *, received_at_utc: datetime) -> dict[str, Any]:
        return {
            "schema": TELEMETRY_SCHEMA,
            "run_id": self.run_id,
            "sensor_id": self.sensor_id,
            "sequence": self.sequence,
            "sensor_time_ms": self.sensor_time_ms,
            "value_milli": self.value_milli,
            "received_at_utc": received_at_utc.astimezone(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        }


def append_jsonl(path: Path, record: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(dict(record), sort_keys=True, separators=(",", ":")) + "\n"
    try:
        with path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(line)
            stream.flush()
    except OSError as exc:
        raise ExperimentError("unable to append JSONL telemetry") from exc


def append_rejected(path: Path, *, reason: str, topic: str) -> None:
    append_jsonl(
        path,
        {
            "schema": "synthran/rejected-event/v1alpha1",
            "reason": reason,
            "topic": topic,
        },
    )


def load_jsonl(path: Path, *, expected_run_id: str) -> list[dict[str, Any]]:
    validate_run_id(expected_run_id)
    records: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise ExperimentError("unable to read JSONL telemetry") from exc
    for number, line in enumerate(lines, start=1):
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ExperimentError(f"JSONL line {number} is invalid") from exc
        if not isinstance(value, dict) or value.get("run_id") != expected_run_id:
            raise ExperimentError(f"JSONL line {number} does not belong to the run")
        if value.get("schema") != TELEMETRY_SCHEMA:
            raise ExperimentError(f"JSONL line {number} has an unsupported schema")
        sensor_id = value.get("sensor_id")
        sequence = value.get("sequence")
        if not isinstance(sensor_id, str) or not SENSOR_RE.fullmatch(sensor_id):
            raise ExperimentError(f"JSONL line {number} has an invalid sensor ID")
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1:
            raise ExperimentError(f"JSONL line {number} has an invalid sequence")
        records.append(value)
    return records


def deterministic_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    canonical = [dict(record) for record in records]
    canonical.sort(key=lambda item: (str(item["sensor_id"]), int(item["sequence"])))
    return canonical


def write_parquet(records: Sequence[Mapping[str, Any]], destination: Path) -> None:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise ExperimentError("PyArrow is required for Parquet derivation") from exc
    canonical = deterministic_records(records)
    schema = pa.schema(
        [
            ("schema", pa.string()),
            ("run_id", pa.string()),
            ("sensor_id", pa.string()),
            ("sequence", pa.int64()),
            ("sensor_time_ms", pa.int64()),
            ("value_milli", pa.int64()),
            ("received_at_utc", pa.string()),
        ]
    )
    table = pa.Table.from_pylist(canonical, schema=schema)
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        pq.write_table(
            table,
            destination,
            compression="zstd",
            use_dictionary=False,
            write_statistics=True,
            data_page_version="1.0",
        )
    except OSError as exc:
        raise ExperimentError("unable to write Parquet dataset") from exc


def validate_sequence_integrity(
    records: Sequence[Mapping[str, Any]],
    *,
    minimum_per_sensor: int = 1,
) -> tuple[str, ...]:
    if minimum_per_sensor < 1:
        raise ExperimentError("minimum_per_sensor must be positive")
    by_sensor: dict[str, list[int]] = {
        f"sensor-{index:02d}": [] for index in range(1, SENSOR_COUNT + 1)
    }
    for record in records:
        sensor_id = record.get("sensor_id")
        sequence = record.get("sequence")
        if sensor_id not in by_sensor or not isinstance(sequence, int):
            raise ExperimentError("dataset contains telemetry outside the sensor contract")
        by_sensor[str(sensor_id)].append(sequence)
    failures: list[str] = []
    for sensor_id, sequences in by_sensor.items():
        if len(sequences) < minimum_per_sensor:
            failures.append(f"{sensor_id}: fewer than {minimum_per_sensor} events")
            continue
        if len(set(sequences)) != len(sequences):
            failures.append(f"{sensor_id}: duplicate sequence")
            continue
        ordered = sorted(sequences)
        if ordered != list(range(ordered[0], ordered[-1] + 1)):
            failures.append(f"{sensor_id}: sequence gap")
    return tuple(failures)


@dataclass(frozen=True)
class ExperimentCheck:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class ExperimentEvidence:
    run_id: str
    network_run_id: str
    generated_at_utc: datetime
    scenario_sha256: str
    jsonl_sha256: str
    parquet_sha256: str
    checks: tuple[ExperimentCheck, ...]

    @property
    def ready(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": EXPERIMENT_EVIDENCE_SCHEMA,
            "run_id": self.run_id,
            "network_run_id": self.network_run_id,
            "generated_at_utc": self.generated_at_utc.astimezone(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "scenario_sha256": self.scenario_sha256,
            "jsonl_sha256": self.jsonl_sha256,
            "parquet_sha256": self.parquet_sha256,
            "checks": [
                {"name": check.name, "passed": check.passed, "detail": check.detail}
                for check in self.checks
            ],
            "ready": self.ready,
        }

    def render(self) -> str:
        lines = [f"SynthRAN experiment verification ({self.run_id})"]
        for check in self.checks:
            lines.append(f"[{'PASS' if check.passed else 'FAIL'}] {check.name}: {check.detail}")
        lines.append(f"Result: {'ACCEPTED' if self.ready else 'NOT ACCEPTED'}")
        return "\n".join(lines)


def build_data_evidence(
    *,
    scenario: ExperimentScenario,
    scenario_path: Path,
    jsonl_path: Path,
    parquet_path: Path,
    minimum_per_sensor: int = 1,
    extra_checks: Iterable[ExperimentCheck] = (),
    now: datetime | None = None,
) -> ExperimentEvidence:
    records = load_jsonl(jsonl_path, expected_run_id=scenario.run_id)
    failures = validate_sequence_integrity(records, minimum_per_sensor=minimum_per_sensor)
    sensors = {str(record["sensor_id"]) for record in records}
    checks = [
        ExperimentCheck(
            "sensor-coverage",
            sensors == set(f"sensor-{index:02d}" for index in range(1, SENSOR_COUNT + 1)),
            f"observed {len(sensors)}/10 Ambient-IoT sources",
        ),
        ExperimentCheck(
            "sequence-integrity",
            not failures,
            "no duplicates or gaps" if not failures else "; ".join(failures),
        ),
        ExperimentCheck(
            "jsonl",
            jsonl_path.is_file() and jsonl_path.stat().st_size > 0,
            "canonical append-only audit artifact exists",
        ),
        ExperimentCheck(
            "parquet",
            parquet_path.is_file() and parquet_path.stat().st_size > 0,
            "deterministic derived dataset exists",
        ),
    ]
    checks.extend(extra_checks)
    return ExperimentEvidence(
        run_id=scenario.run_id,
        network_run_id=scenario.network_run_id,
        generated_at_utc=now or datetime.now(timezone.utc),
        scenario_sha256=sha256_file(scenario_path),
        jsonl_sha256=sha256_file(jsonl_path),
        parquet_sha256=sha256_file(parquet_path),
        checks=tuple(checks),
    )


def save_experiment_evidence(report: ExperimentEvidence, destination: Path) -> None:
    _atomic_json(destination, report.to_dict())
