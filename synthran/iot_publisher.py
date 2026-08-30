"""Wall-clock paced MQTT replay for portable IoT source plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
from pathlib import Path
import threading
import time
from typing import Any, Callable, Mapping, Protocol

from synthran.experiment import TELEMETRY_SCHEMA
from synthran.iot_source import (
    IOT_EVIDENCE_SCHEMA_V2,
    PUBLISHER_EVENT_SCHEMA_V2,
    IoTSourceError,
    MQTTEndpoint,
    PreparedIoTPlan,
)


CONTROL_SCHEMA_V2 = "synthran/iot-control/v2alpha1"


class ReplayClock(Protocol):
    def monotonic(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...


class SystemReplayClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


@dataclass
class ReplayStartGate:
    """Optional research barrier placed after canaries and before source time zero."""

    released: threading.Event = field(default_factory=threading.Event)
    condition: threading.Condition = field(default_factory=threading.Condition)
    origin_monotonic_s: float | None = None


_REPLAY_GATES_LOCK = threading.Lock()
_REPLAY_GATES: dict[str, ReplayStartGate] = {}


def install_replay_start_gate(run_id: str) -> None:
    """Require explicit release before a run's source clock can advance."""

    with _REPLAY_GATES_LOCK:
        if run_id in _REPLAY_GATES:
            raise IoTSourceError(f"Amber replay start gate already exists for {run_id}")
        _REPLAY_GATES[run_id] = ReplayStartGate()


def release_replay_start_gate(run_id: str) -> None:
    """Allow a gated replay worker to establish source time zero."""

    with _REPLAY_GATES_LOCK:
        gate = _REPLAY_GATES.get(run_id)
    if gate is None:
        raise IoTSourceError(f"Amber replay start gate is missing for {run_id}")
    gate.released.set()


def wait_replay_start_origin(run_id: str, *, timeout: float = 30.0) -> float:
    """Return the exact monotonic instant chosen as source time zero."""

    with _REPLAY_GATES_LOCK:
        gate = _REPLAY_GATES.get(run_id)
    if gate is None:
        raise IoTSourceError(f"Amber replay start gate is missing for {run_id}")
    deadline = time.monotonic() + timeout
    with gate.condition:
        while gate.origin_monotonic_s is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise IoTSourceError("Amber replay source-clock origin was not observed")
            gate.condition.wait(timeout=min(0.25, remaining))
        return float(gate.origin_monotonic_s)


def remove_replay_start_gate(run_id: str) -> None:
    """Remove a research gate and release any worker still waiting on teardown."""

    with _REPLAY_GATES_LOCK:
        gate = _REPLAY_GATES.pop(run_id, None)
    if gate is not None:
        gate.released.set()


def _replay_start_gate(run_id: str) -> ReplayStartGate | None:
    with _REPLAY_GATES_LOCK:
        return _REPLAY_GATES.get(run_id)


@dataclass(frozen=True)
class PublisherEvent:
    run_id: str
    sensor_id: str
    kind: str
    scheduled_monotonic_s: float
    actual_monotonic_s: float
    lag_ms: float
    qos: int
    success: bool
    sequence: int | None = None
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": PUBLISHER_EVENT_SCHEMA_V2,
            "run_id": self.run_id,
            "sensor_id": self.sensor_id,
            "kind": self.kind,
            "sequence": self.sequence,
            "scheduled_monotonic_s": self.scheduled_monotonic_s,
            "actual_monotonic_s": self.actual_monotonic_s,
            "lag_ms": self.lag_ms,
            "qos": self.qos,
            "success": self.success,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class PublisherEvidence:
    client_count: int
    connected_clients: int
    start_canaries: int
    end_canaries: int
    decoded_events: int
    published_events: int
    publisher_errors: int
    p95_lag_ms: float
    max_lag_ms: float
    lag_limit_ms: float
    max_lag_limit_ms: float
    timing_valid: bool
    complete: bool
    cancelled: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "client_count": self.client_count,
            "connected_clients": self.connected_clients,
            "start_canaries": self.start_canaries,
            "end_canaries": self.end_canaries,
            "decoded_events": self.decoded_events,
            "published_events": self.published_events,
            "publisher_errors": self.publisher_errors,
            "p95_lag_ms": self.p95_lag_ms,
            "max_lag_ms": self.max_lag_ms,
            "lag_limit_ms": self.lag_limit_ms,
            "max_lag_limit_ms": self.max_lag_limit_ms,
            "timing_valid": self.timing_valid,
            "complete": self.complete,
            "cancelled": self.cancelled,
        }


class _PahoClientFactory:
    def __init__(self) -> None:
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise IoTSourceError("paho-mqtt is required for live Amber replay") from exc
        self._mqtt = mqtt

    def __call__(self, client_id: str) -> Any:
        return self._mqtt.Client(
            callback_api_version=self._mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=self._mqtt.MQTTv311,
            clean_session=True,
        )


def _reason_succeeded(reason_code: Any) -> bool:
    marker = getattr(reason_code, "is_failure", None)
    if marker is not None:
        try:
            marker = marker() if callable(marker) else marker
            return not bool(marker)
        except Exception:
            return False
    try:
        return int(reason_code) == 0
    except (TypeError, ValueError):
        return reason_code == 0


def _atomic_jsonl(path: Path, records: list[PublisherEvent]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        "".join(
            json.dumps(record.to_dict(), sort_keys=True, separators=(",", ":")) + "\n"
            for record in records
        ),
        encoding="utf-8",
        newline="\n",
    )
    temporary.replace(path)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(dict(value), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    temporary.replace(path)


def _nearest_rank_p95(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]


class AmberReplaySession:
    """Own ten MQTT clients and replay decoded Amber events against wall time."""

    def __init__(
        self,
        *,
        plan: PreparedIoTPlan,
        endpoint: MQTTEndpoint,
        collector_barrier: Any,
        publisher_events_path: Path | None = None,
        evidence_path: Path | None = None,
        client_factory: Callable[[str], Any] | None = None,
        clock: ReplayClock | None = None,
        connect_timeout_seconds: float = 15.0,
        publish_timeout_seconds: float = 15.0,
    ) -> None:
        self.plan = plan
        self.endpoint = endpoint
        self.collector_barrier = collector_barrier
        self.publisher_events_path = publisher_events_path or (
            plan.scenario_path.parent / "publisher-events.jsonl"
        )
        self.evidence_path = evidence_path or plan.evidence_path
        self.client_factory = client_factory or _PahoClientFactory()
        self.clock = clock or SystemReplayClock()
        self.connect_timeout_seconds = connect_timeout_seconds
        self.publish_timeout_seconds = publish_timeout_seconds
        self._condition = threading.Condition()
        self._clients: dict[str, Any] = {}
        self._connected: set[str] = set()
        self._events: list[PublisherEvent] = []
        self._errors: list[str] = []
        self._cancel = threading.Event()
        self._done = threading.Event()
        self._worker: threading.Thread | None = None
        self._replay_origin: float | None = None
        self._complete = False

    def start(self) -> "AmberReplaySession":
        self.collector_barrier.wait_ready()
        self._connect_clients()
        now = self.clock.monotonic()
        for sensor_id in self.plan.spec.sensor_ids:
            self._publish_control(sensor_id, "start-canary", now)
        if self._errors:
            self.stop()
            raise IoTSourceError(self._errors[0])
        wait_start = getattr(self.collector_barrier, "wait_start_canaries", None)
        if not callable(wait_start):
            self.stop()
            raise IoTSourceError(
                "collector does not expose the required start-canary barrier"
            )
        try:
            wait_start(self.plan.spec.sensor_ids)
        except Exception as exc:
            self.stop()
            raise IoTSourceError(
                "central collector did not observe all start canaries"
            ) from exc
        self._worker = threading.Thread(
            target=self._run,
            name=f"amber-replay-{self.plan.spec.run_id}",
            daemon=True,
        )
        self._worker.start()
        return self

    def _connect_clients(self) -> None:
        deadline = time.monotonic() + self.connect_timeout_seconds
        for sensor_id in self.plan.spec.sensor_ids:
            client_id = f"synthran-{self.plan.spec.run_id}-{sensor_id}"
            client = self.client_factory(client_id)

            def on_connect(
                callback_client: Any,
                userdata: Any,
                flags: Any,
                reason_code: Any,
                properties: Any = None,
                *,
                expected_sensor: str = sensor_id,
            ) -> None:
                del callback_client, userdata, flags, properties
                with self._condition:
                    if _reason_succeeded(reason_code):
                        self._connected.add(expected_sensor)
                    else:
                        self._errors.append(
                            f"MQTT client {expected_sensor} connection refused ({reason_code})"
                        )
                    self._condition.notify_all()

            def on_disconnect(
                callback_client: Any,
                userdata: Any,
                disconnect_flags: Any,
                reason_code: Any,
                properties: Any = None,
                *,
                expected_sensor: str = sensor_id,
            ) -> None:
                del callback_client, userdata, disconnect_flags, properties
                if self._done.is_set() or self._cancel.is_set():
                    return
                if not _reason_succeeded(reason_code):
                    with self._condition:
                        self._errors.append(
                            f"MQTT client {expected_sensor} disconnected ({reason_code})"
                        )
                        self._condition.notify_all()

            client.on_connect = on_connect
            client.on_disconnect = on_disconnect
            try:
                client.connect(self.endpoint.host, self.endpoint.port, keepalive=30)
                client.loop_start()
            except Exception as exc:
                self._errors.append(f"MQTT client {sensor_id} failed to connect: {exc}")
                break
            self._clients[sensor_id] = client

        with self._condition:
            while len(self._connected) < len(self.plan.spec.sensor_ids) and not self._errors:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._errors.append("Amber MQTT client readiness timed out")
                    break
                self._condition.wait(timeout=min(0.25, remaining))
        if len(self._connected) != len(self.plan.spec.sensor_ids) and not self._errors:
            self._errors.append("not all Amber MQTT clients became ready")

    def _publish_message(
        self,
        *,
        sensor_id: str,
        kind: str,
        topic: str,
        payload: str,
        qos: int,
        scheduled: float,
        sequence: int | None,
    ) -> bool:
        client = self._clients[sensor_id]
        actual = self.clock.monotonic()
        success = False
        detail = ""
        try:
            info = client.publish(topic, payload=payload, qos=qos, retain=False)
            wait = getattr(info, "wait_for_publish", None)
            if callable(wait):
                wait(timeout=self.publish_timeout_seconds)
            published = getattr(info, "is_published", None)
            if callable(published):
                success = bool(published())
            else:
                success = getattr(info, "rc", 0) == 0
            if not success:
                detail = f"publish did not complete (rc={getattr(info, 'rc', None)})"
        except Exception as exc:
            detail = str(exc)
        if not success:
            self._errors.append(f"{sensor_id} {kind} publish failed: {detail}")
        self._events.append(
            PublisherEvent(
                run_id=self.plan.spec.run_id,
                sensor_id=sensor_id,
                kind=kind,
                sequence=sequence,
                scheduled_monotonic_s=scheduled,
                actual_monotonic_s=actual,
                lag_ms=(actual - scheduled) * 1000.0,
                qos=qos,
                success=success,
                detail=detail,
            )
        )
        return success

    def _publish_control(self, sensor_id: str, kind: str, scheduled: float) -> bool:
        payload = json.dumps(
            {
                "schema": CONTROL_SCHEMA_V2,
                "run_id": self.plan.spec.run_id,
                "sensor_id": sensor_id,
                "kind": kind,
                "profile_digest": self.plan.profile_digest,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return self._publish_message(
            sensor_id=sensor_id,
            kind=kind,
            topic=f"{self.plan.spec.topic_root}/control/{sensor_id}",
            payload=payload,
            qos=1,
            scheduled=scheduled,
            sequence=None,
        )

    def _publish_telemetry(self, source_event: Any, scheduled: float) -> bool:
        payload = json.dumps(
            {
                "schema": TELEMETRY_SCHEMA,
                "run_id": self.plan.spec.run_id,
                "sensor_id": source_event.sensor_id,
                "sequence": source_event.sequence,
                "sensor_time_ms": source_event.planned_at_ms,
                "value_milli": source_event.value_milli,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return self._publish_message(
            sensor_id=source_event.sensor_id,
            kind="telemetry",
            topic=f"{self.plan.spec.topic_root}/sensor/{source_event.sensor_id}",
            payload=payload,
            qos=0,
            scheduled=scheduled,
            sequence=source_event.sequence,
        )

    def _wait_until(self, target: float) -> None:
        while not self._cancel.is_set():
            remaining = target - self.clock.monotonic()
            if remaining <= 0:
                return
            self.clock.sleep(min(remaining, 0.05))

    def _await_replay_start(self) -> bool:
        gate = _replay_start_gate(self.plan.spec.run_id)
        if gate is not None:
            while not self._cancel.is_set() and not gate.released.wait(timeout=0.05):
                pass
            if self._cancel.is_set():
                return False
        self._replay_origin = self.clock.monotonic()
        if gate is not None:
            with gate.condition:
                gate.origin_monotonic_s = self._replay_origin
                gate.condition.notify_all()
        return True

    def _run(self) -> None:
        try:
            if not self._await_replay_start():
                return
            assert self._replay_origin is not None
            decoded = sorted(
                (event for event in self.plan.events if event.decoded),
                key=lambda event: (event.planned_at_ms, event.sensor_id, event.sequence),
            )
            for event in decoded:
                if self._cancel.is_set() or self._errors:
                    break
                scheduled = self._replay_origin + event.planned_at_ms / 1000.0
                self._wait_until(scheduled)
                if self._cancel.is_set() or self._errors:
                    break
                self._publish_telemetry(event, scheduled)

            if not self._cancel.is_set() and not self._errors:
                end_scheduled = self._replay_origin + float(self.plan.duration_seconds)
                self._wait_until(end_scheduled)
                if self._cancel.is_set() or self._errors:
                    return
                for sensor_id in self.plan.spec.sensor_ids:
                    self._publish_control(sensor_id, "end-canary", end_scheduled)
                    if self._errors:
                        break
                self._complete = not self._errors
        finally:
            self._done.set()
            self._persist()

    def wait(self, timeout: float | None = None) -> PublisherEvidence:
        if not self._done.wait(timeout):
            raise IoTSourceError("Amber replay did not complete before timeout")
        evidence = self.evidence()
        if self._errors:
            raise IoTSourceError(self._errors[0])
        if not evidence.timing_valid:
            raise IoTSourceError(
                "Amber replay timing exceeded the accepted instrumentation bounds"
            )
        if not evidence.complete:
            raise IoTSourceError("Amber replay did not complete")
        return evidence

    def stop(self) -> None:
        self._cancel.set()
        worker = self._worker
        if worker is not None and worker.is_alive():
            worker.join(timeout=5.0)
        for client in self._clients.values():
            try:
                client.disconnect()
            except Exception:
                pass
            try:
                client.loop_stop()
            except Exception:
                pass
        self._done.set()
        self._persist()

    def published_pairs(self) -> tuple[tuple[str, int], ...]:
        return tuple(
            (event.sensor_id, int(event.sequence))
            for event in self._events
            if event.kind == "telemetry"
            and event.success
            and event.sequence is not None
        )

    def evidence(self) -> PublisherEvidence:
        telemetry = [event for event in self._events if event.kind == "telemetry"]
        lags = [max(0.0, event.lag_ms) for event in telemetry if event.success]
        p95 = _nearest_rank_p95(lags)
        maximum = max(lags, default=0.0)
        lag_limit = max(250.0, self.plan.spec.sensor_period_seconds * 100.0)
        max_lag_limit = self.plan.spec.sensor_period_seconds * 1000.0
        start_count = sum(
            event.kind == "start-canary" and event.success for event in self._events
        )
        end_count = sum(
            event.kind == "end-canary" and event.success for event in self._events
        )
        decoded_count = sum(event.decoded for event in self.plan.events)
        published_count = sum(
            event.kind == "telemetry" and event.success for event in self._events
        )
        timing_valid = p95 <= lag_limit and maximum < max_lag_limit
        complete = (
            self._complete
            and len(self._connected) == self.plan.spec.sensor_count
            and start_count == self.plan.spec.sensor_count
            and end_count == self.plan.spec.sensor_count
            and published_count == decoded_count
            and not self._errors
        )
        return PublisherEvidence(
            client_count=len(self._clients),
            connected_clients=len(self._connected),
            start_canaries=start_count,
            end_canaries=end_count,
            decoded_events=decoded_count,
            published_events=published_count,
            publisher_errors=len(self._errors),
            p95_lag_ms=p95,
            max_lag_ms=maximum,
            lag_limit_ms=lag_limit,
            max_lag_limit_ms=max_lag_limit,
            timing_valid=timing_valid,
            complete=complete,
            cancelled=self._cancel.is_set(),
        )

    def _persist(self) -> None:
        _atomic_jsonl(self.publisher_events_path, self._events)
        try:
            existing = json.loads(self.evidence_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            existing = {
                "schema": IOT_EVIDENCE_SCHEMA_V2,
                "run_id": self.plan.spec.run_id,
            }
        existing["live_transport"] = {
            "publisher": self.evidence().to_dict(),
            "publisher_events_path": self.publisher_events_path.name,
        }
        _atomic_json(self.evidence_path, existing)


def start_amber_replay(
    plan: PreparedIoTPlan,
    endpoint: MQTTEndpoint,
    collector_barrier: Any,
    **kwargs: Any,
) -> AmberReplaySession:
    return AmberReplaySession(
        plan=plan,
        endpoint=endpoint,
        collector_barrier=collector_barrier,
        **kwargs,
    ).start()