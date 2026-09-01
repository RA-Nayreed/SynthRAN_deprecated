"""Fixed-window MQTT collection for controlled SynthRAN experiments."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import threading
import time
from typing import Any, Callable

from synthran.experiment import (
    ExperimentError,
    ExperimentScenario,
    TelemetryEvent,
    append_jsonl,
    append_rejected,
)
from synthran.iot_collector import _reason_succeeded


@dataclass(frozen=True)
class WindowCollectionResult:
    records: int
    sensors: int
    completed: bool
    started_at_utc: datetime
    ended_at_utc: datetime


def collect_mqtt_window(
    scenario: ExperimentScenario,
    *,
    host: str,
    port: int,
    jsonl_path: Path,
    rejected_path: Path,
    duration_seconds: int,
    warmup_seconds: int = 0,
    on_window_start: Callable[[], None] | None = None,
    health_check: Callable[[], None] | None = None,
) -> WindowCollectionResult:
    if duration_seconds < 1:
        raise ExperimentError("measurement duration must be positive")
    if warmup_seconds < 0:
        raise ExperimentError("measurement warmup must not be negative")
    try:
        import paho.mqtt.client as mqtt
    except ImportError as exc:
        raise ExperimentError("paho-mqtt is required for live telemetry collection") from exc

    condition = threading.Condition()
    last_error: list[str] = []
    connected = False
    active = False
    records = 0
    sensors: set[str] = set()

    def on_connect(
        client: Any,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        del userdata, flags, properties
        nonlocal connected
        with condition:
            connected = _reason_succeeded(reason_code)
            if connected:
                client.subscribe(scenario.sensor_topic, qos=0)
            else:
                last_error[:] = [f"central MQTT connection refused ({reason_code})"]
            condition.notify_all()

    def on_message(client: Any, userdata: Any, message: Any) -> None:
        del client, userdata
        nonlocal records
        topic = str(message.topic)
        try:
            event = TelemetryEvent.from_payload(message.payload, scenario.run_id)
            expected_topic = f"{scenario.topic_root}/sensor/{event.sensor_id}"
            if topic != expected_topic:
                raise ExperimentError("MQTT topic does not match telemetry sensor ID")
            with condition:
                should_record = active
            if should_record:
                append_jsonl(
                    jsonl_path,
                    event.to_record(received_at_utc=datetime.now(timezone.utc)),
                )
                with condition:
                    records += 1
                    sensors.add(event.sensor_id)
        except ExperimentError as exc:
            append_rejected(rejected_path, reason=str(exc), topic=topic)
        with condition:
            condition.notify_all()

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"synthran-research-{scenario.run_id}",
        protocol=mqtt.MQTTv311,
        clean_session=True,
    )
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(host, port, keepalive=30)
        client.loop_start()
        connect_deadline = time.monotonic() + 30
        with condition:
            while not connected and not last_error:
                remaining = connect_deadline - time.monotonic()
                if remaining <= 0:
                    raise ExperimentError("central MQTT collector connection timed out")
                condition.wait(timeout=min(1.0, remaining))
        if last_error:
            raise ExperimentError(last_error[0])
        if warmup_seconds:
            time.sleep(warmup_seconds)
        if on_window_start is not None:
            on_window_start()

        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        jsonl_path.touch(exist_ok=True)
        started_at = datetime.now(timezone.utc)
        with condition:
            active = True
        deadline = time.monotonic() + duration_seconds
        try:
            while True:
                if health_check is not None:
                    health_check()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                with condition:
                    condition.wait(timeout=min(1.0, remaining))
        finally:
            with condition:
                active = False
        ended_at = datetime.now(timezone.utc)
        return WindowCollectionResult(
            records=records,
            sensors=len(sensors),
            completed=True,
            started_at_utc=started_at,
            ended_at_utc=ended_at,
        )
    finally:
        try:
            client.disconnect()
        except Exception:
            pass
        client.loop_stop()
