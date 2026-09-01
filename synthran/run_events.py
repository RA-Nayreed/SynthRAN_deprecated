"""Canonical lifecycle and child-runtime events for ``synthran run``.

SynthRAN owns experiment lifecycle rendering.  5g-Ansible owns deployment
progress semantics and supplies them as versioned ``fiveg/event/v1`` records;
SynthRAN relays those records without parsing Ansible text.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Mapping, TextIO


PREFIX = "[synthran]"
FIVEG_EVENT_SCHEMA = "fiveg/event/v1"
PUBLIC_STAGES = frozenset(
    {"provider", "infrastructure", "network", "workload", "acceptance", "cleanup"}
)

_FIVEG_PHASES = {
    "provider": ("provider", "SLICES provider"),
    "reservation": ("infrastructure", "SLICES reservation"),
    "r2lab-authority": ("infrastructure", "R2Lab authority"),
    "collections": ("infrastructure", "deployment dependencies"),
    "r2lab-deployment": ("infrastructure", "physical resources"),
    "deployment": ("infrastructure", "5G deployment"),
    "scenario": ("network", "5G scenario"),
    "cleanup": ("cleanup", "5G cleanup"),
}
_FIVEG_MARKERS = {
    "started": "→",
    "completed": "✓",
    "failed": "✗",
    "resumed": "↻",
    "skipped": "–",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _strip_child_prefix(message: str) -> str:
    value = message.rstrip("\r").strip()
    if value.startswith(PREFIX):
        value = value[len(PREFIX):].lstrip()
    return value


def normalize_child_message(message: str) -> str | None:
    """Normalize only experiment-owned Amber/research child progress."""

    value = _strip_child_prefix(message)
    if not value:
        return None
    if value.startswith("Amber energy treatment: "):
        return "  " + value
    if value.startswith("Amber source: preparing immutable event plan"):
        return "  → Ambient-IoT source plan"
    if value.startswith("Amber source: "):
        return "    " + value.removeprefix("Amber source: ")
    if value.startswith("Amber source outcomes: "):
        return "    outcomes: " + value.removeprefix("Amber source outcomes: ")
    if value.startswith("error: "):
        return "✗ " + value.removeprefix("error: ")
    if value.startswith((
        "research:",
        "Amber research summary:",
        "Amber campaign result:",
        "Campaign schedule:",
        "Capacity evidence:",
    )):
        return "  " + value
    return None


def _event_kind(message: str) -> str:
    value = message.lstrip()
    if value.startswith("→ "):
        return "started"
    if value.startswith("✓ "):
        return "completed"
    if value.startswith("↻ "):
        return "resumed"
    if value.startswith("– "):
        return "skipped"
    if value.startswith("✗ "):
        return "failed"
    if value.startswith("… "):
        return "heartbeat"
    return "detail"


class RunEventStream:
    """TextIO-compatible canonical experiment event stream."""

    def __init__(
        self,
        *,
        run_id: str,
        radio: str,
        terminal: TextIO | None = None,
        terminal_enabled: bool = True,
        root: Path = Path(".synthran/events"),
    ) -> None:
        self.run_id = run_id
        self.radio = radio
        self.terminal = terminal if terminal is not None else sys.stderr
        self.terminal_enabled = terminal_enabled
        self.path = root.expanduser().resolve() / f"{run_id}.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._buffer = ""

    def writable(self) -> bool:
        return True

    def emit(
        self,
        message: str,
        *,
        event: str | None = None,
        stage: str | None = None,
        component: str | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> None:
        value = message.rstrip("\r\n")
        if not value:
            return
        if self.terminal_enabled:
            print(f"{PREFIX} {value}", file=self.terminal, flush=True)
        payload: dict[str, Any] = {
            "schema": "synthran/run-event/v2",
            "time": _utc_now(),
            "run_id": self.run_id,
            "radio": self.radio,
            "event": event or _event_kind(value),
            "stage": stage,
            "message": value,
        }
        if component is not None:
            payload["component"] = component
        if detail:
            payload["detail"] = dict(detail)
        with self.path.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(payload, sort_keys=True) + "\n")

    def write(self, text: str) -> int:
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            normalized = normalize_child_message(line)
            if normalized is not None:
                self.emit(normalized)
        return len(text)

    def flush(self) -> None:
        if self._buffer:
            normalized = normalize_child_message(self._buffer)
            self._buffer = ""
            if normalized is not None:
                self.emit(normalized)
        self.terminal.flush()


class RunProgress:
    """Render experiment stages and relay already-semantic upstream events."""

    def __init__(
        self,
        *,
        run_id: str,
        radio: str,
        enabled: bool = True,
        terminal: TextIO | None = None,
    ) -> None:
        self.stream = RunEventStream(
            run_id=run_id,
            radio=radio,
            terminal=terminal,
            terminal_enabled=enabled,
        )
        self.event_path = self.stream.path
        self.current_stage: str | None = None

    @property
    def child_stream(self) -> TextIO:
        return self.stream

    @staticmethod
    def _validate_stage(stage: str) -> None:
        if stage not in PUBLIC_STAGES:
            raise ValueError(f"unsupported public lifecycle stage: {stage}")

    def relay_fiveg_event(self, upstream: Mapping[str, Any]) -> None:
        """Relay one versioned 5g-Ansible progress event without inferring truth."""

        if upstream.get("schema") != FIVEG_EVENT_SCHEMA:
            return
        if upstream.get("deployment_id") != self.stream.run_id:
            return
        phase = upstream.get("phase")
        event = upstream.get("event")
        if not isinstance(phase, str) or phase not in _FIVEG_PHASES:
            return
        if not isinstance(event, str) or event not in _FIVEG_MARKERS:
            return
        stage, label = _FIVEG_PHASES[phase]
        marker = _FIVEG_MARKERS[event]
        component = upstream.get("component")
        component_value = component if isinstance(component, str) and component else None
        self.stream.emit(
            f"  {marker} {label}",
            stage=stage,
            event=event,
            component=component_value,
            detail={"upstream": dict(upstream)},
        )

    def start(self, stage: str, detail: str | None = None) -> None:
        self._validate_stage(stage)
        self.current_stage = stage
        suffix = f": {detail}" if detail else ""
        self.stream.emit(f"→ {stage}{suffix}", stage=stage, event="started")

    def done(self, stage: str, detail: str | None = None) -> None:
        self._validate_stage(stage)
        suffix = f": {detail}" if detail else ""
        self.stream.emit(f"✓ {stage}{suffix}", stage=stage, event="completed")
        if self.current_stage == stage:
            self.current_stage = None

    def resumed(self, stage: str, detail: str | None = None) -> None:
        self._validate_stage(stage)
        suffix = f": {detail}" if detail else ""
        self.stream.emit(f"↻ {stage}{suffix}", stage=stage, event="resumed")
        if self.current_stage == stage:
            self.current_stage = None

    def skipped(self, stage: str, detail: str | None = None) -> None:
        self._validate_stage(stage)
        suffix = f": {detail}" if detail else ""
        self.stream.emit(f"– {stage}{suffix}", stage=stage, event="skipped")
        if self.current_stage == stage:
            self.current_stage = None

    def fail(self, detail: str) -> None:
        stage = self.current_stage or "run"
        self.stream.emit(f"✗ {stage}: {detail}", stage=stage, event="failed")
        self.current_stage = None

    def close(self) -> None:
        self.stream.flush()
