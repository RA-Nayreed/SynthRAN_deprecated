"""Subscription-aware MQTT collector session for portable IoT workloads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import threading
import time
from typing import Any, Iterable, Sequence

from synthran.experiment import (
    ExperimentError,
    SENSOR_RE,
    TelemetryEvent,
    append_jsonl,
    append_rejected,
    load_jsonl,
)


@dataclass(frozen=True)
class CollectorSessionEvidence:
    connected: bool
    subscriptions_ready: bool
    start_canaries: int
    end_canaries: int
    records: int
    rejected: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "connected": self.connected,
            "subscriptions_ready": self.subscriptions_ready,
            "start_canaries": self.start_canaries,
            "end_canaries": self.end_canaries,
            "records": self.records,
            "rejected": self.rejected,
        }


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


class PortableMqttCollectorSession:
    """Own one central-broker subscription session with explicit readiness."""

    def __init__(
        self,
        *,
        run_id: str,
        sensor_count: int,
        topic_root: str,
        host: str,
        port: int,
        jsonl_path: Path,
        rejected_path: Path,
        client_factory: Any | None = None,
        default_timeout_seconds: float = 30.0,
    ) -> None:
        if sensor_count != 10:
            raise ExperimentError("portable IoT collector requires exactly 10 sensors")
        if not topic_root.strip():
            raise ExperimentError("portable IoT topic root must be non-empty")
        self.run_id = run_id
        self.sensor_count = sensor_count
        self.topic_root = topic_root.rstrip("/")
        self.host = host
        self.port = port
        self.jsonl_path = jsonl_path
        self.rejected_path = rejected_path
        self.default_timeout_seconds = default_timeout_seconds
        self._condition = threading.Condition()
        self._connected = False
        self._subscription_acks = 0
        self._errors: list[str] = []
        self._start_canaries: set[str] = set()
        self._end_canaries: set[str] = set()
        self._rejected = 0
        self._stopped = False

        if client_factory is None:
            try:
                import paho.mqtt.client as mqtt
            except ImportError as exc:
                raise ExperimentError(
                    "paho-mqtt is required for live telemetry collection"
                ) from exc
            self._client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"synthran-portable-collector-{run_id}",
                protocol=mqtt.MQTTv311,
                clean_session=True,
            )
        else:
            self._client = client_factory(f"synthran-portable-collector-{run_id}")

        self._client.on_connect = self._on_connect
        self._client.on_subscribe = self._on_subscribe
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect

    @property
    def telemetry_topic(self) -> str:
        return f"{self.topic_root}/sensor/+"

    @property
    def control_topic(self) -> str:
        return f"{self.topic_root}/control/+"

    def start(self) -> "PortableMqttCollectorSession":
        try:
            self._client.connect(self.host, self.port, keepalive=30)
            self._client.loop_start()
        except Exception as exc:
            raise ExperimentError("central MQTT collector failed to start") from exc
        return self

    def _on_connect(
        self,
        client: Any,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        del userdata, flags, properties
        with self._condition:
            if _reason_succeeded(reason_code):
                self._connected = True
                client.subscribe(self.telemetry_topic, qos=0)
                client.subscribe(self.control_topic, qos=1)
            else:
                self._errors.append(
                    f"central MQTT connection refused ({reason_code})"
                )
            self._condition.notify_all()

    def _on_subscribe(
        self,
        client: Any,
        userdata: Any,
        mid: Any,
        reason_codes: Any,
        properties: Any = None,
    ) -> None:
        del client, userdata, mid, properties
        codes = reason_codes if isinstance(reason_codes, (list, tuple)) else [reason_codes]
        with self._condition:
            if all(_reason_succeeded(code) for code in codes):
                self._subscription_acks += 1
            else:
                self._errors.append("central MQTT subscription was rejected")
            self._condition.notify_all()

    def _on_disconnect(
        self,
        client: Any,
        userdata: Any,
        disconnect_flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        del client, userdata, disconnect_flags, properties
        if self._stopped:
            return
        if not _reason_succeeded(reason_code):
            with self._condition:
                self._errors.append(
                    f"central MQTT collector disconnected ({reason_code})"
                )
                self._condition.notify_all()

    def _reject(self, *, reason: str, topic: str) -> None:
        append_rejected(self.rejected_path, reason=reason, topic=topic)
        self._rejected += 1

    def _on_message(self, client: Any, userdata: Any, message: Any) -> None:
        del client, userdata
        topic = str(message.topic)
        try:
            if topic.startswith(f"{self.topic_root}/control/"):
                self._accept_control(topic, message.payload)
            elif topic.startswith(f"{self.topic_root}/sensor/"):
                event = TelemetryEvent.from_payload(message.payload, self.run_id)
                expected_topic = f"{self.topic_root}/sensor/{event.sensor_id}"
                if topic != expected_topic:
                    raise ExperimentError(
                        "MQTT topic does not match telemetry sensor ID"
                    )
                append_jsonl(
                    self.jsonl_path,
                    event.to_record(received_at_utc=datetime.now(timezone.utc)),
                )
            else:
                raise ExperimentError("MQTT topic is outside the run-owned topic tree")
        except (ExperimentError, ValueError, TypeError, json.JSONDecodeError) as exc:
            self._reject(reason=str(exc), topic=topic)
        finally:
            with self._condition:
                self._condition.notify_all()

    def _accept_control(self, topic: str, payload: bytes) -> None:
        value = json.loads(payload.decode("utf-8"))
        if not isinstance(value, dict):
            raise ExperimentError("control canary payload must be a JSON object")
        if value.get("run_id") != self.run_id:
            raise ExperimentError("control canary run ID does not match")
        sensor_id = value.get("sensor_id")
        if not isinstance(sensor_id, str) or not SENSOR_RE.fullmatch(sensor_id):
            raise ExperimentError("control canary sensor ID is invalid")
        if topic != f"{self.topic_root}/control/{sensor_id}":
            raise ExperimentError("control canary topic does not match sensor ID")
        kind = value.get("kind")
        if kind == "start-canary":
            self._start_canaries.add(sensor_id)
        elif kind == "end-canary":
            self._end_canaries.add(sensor_id)
        else:
            raise ExperimentError("control canary kind is invalid")

    def _wait_for(self, predicate: Any, label: str, timeout: float | None) -> None:
        timeout = self.default_timeout_seconds if timeout is None else timeout
        deadline = time.monotonic() + timeout
        with self._condition:
            while not predicate() and not self._errors:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ExperimentError(f"{label} timed out")
                self._condition.wait(timeout=min(0.25, remaining))
        if self._errors:
            raise ExperimentError(self._errors[0])

    def wait_ready(self, timeout: float | None = None) -> None:
        self._wait_for(
            lambda: self._connected and self._subscription_acks >= 2,
            "central MQTT subscription readiness",
            timeout,
        )

    def wait_start_canaries(
        self,
        sensor_ids: Iterable[str] | None = None,
        timeout: float | None = None,
    ) -> None:
        expected = set(sensor_ids or (f"sensor-{index:02d}" for index in range(1, 11)))
        self._wait_for(
            lambda: expected.issubset(self._start_canaries),
            "central start-canary collection",
            timeout,
        )

    def wait_end_canaries(
        self,
        sensor_ids: Iterable[str] | None = None,
        timeout: float | None = None,
    ) -> None:
        expected = set(sensor_ids or (f"sensor-{index:02d}" for index in range(1, 11)))
        self._wait_for(
            lambda: expected.issubset(self._end_canaries),
            "central end-canary collection",
            timeout,
        )

    def records(self) -> list[dict[str, Any]]:
        if not self.jsonl_path.is_file():
            return []
        return load_jsonl(self.jsonl_path, expected_run_id=self.run_id)

    def wait_expected_pairs(
        self,
        expected_pairs: Iterable[tuple[str, int]],
        *,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        expected = set(expected_pairs)

        def complete() -> bool:
            observed = {
                (str(record["sensor_id"]), int(record["sequence"]))
                for record in self.records()
            }
            return expected.issubset(observed)

        self._wait_for(complete, "central telemetry collection", timeout)
        return self.records()

    def evidence(self) -> CollectorSessionEvidence:
        records = self.records()
        return CollectorSessionEvidence(
            connected=self._connected,
            subscriptions_ready=self._subscription_acks >= 2,
            start_canaries=len(self._start_canaries),
            end_canaries=len(self._end_canaries),
            records=len(records),
            rejected=self._rejected,
        )

    def stop(self) -> None:
        self._stopped = True
        try:
            self._client.disconnect()
        except Exception:
            pass
        try:
            self._client.loop_stop()
        except Exception:
            pass
