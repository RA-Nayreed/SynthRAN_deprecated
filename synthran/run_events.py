"""Canonical lifecycle and child-runtime events for ``synthran run``.

Backends do the work; this module owns the operator view. Every human-facing
runtime line goes through one ``[synthran]`` renderer and is also persisted as a
structured JSONL event. Child Ansible/AMBER streams are filtered here so
implementation chatter never becomes lifecycle state.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Mapping, TextIO


PREFIX = "[synthran]"

_INTERNAL_PREFIXES = (
    "preparation started:",
    "controller-preflight:",
    "isolated-worktree:",
    "verify-worktree:",
    "upstream-overlay:",
    "ansible-collections:",
    "ansible-syntax:",
    "upstream-syntax:",
    "network-foundation-syntax:",
    "tool-preparation-syntax:",
    "reservation-inspection:",
    "allocation-inspection:",
    "allocation-reclaim-",
    "allocation-create:",
    "allocation-verification-",
    "upstream-resource-preparation:",
    "network-foundation-reconciliation:",
    "locked-tool-preparation:",
    "resource preparation:",
    "network deployment started:",
    "ansible-deployment:",
    "network deployment:",
)
_INTERNAL_OUTPUT_PREFIXES = (
    "SLICES resources prepared for run ",
    "Generated inventory:",
    "Private authority:",
    "Sanitized manifest:",
    "Sanitized log:",
    "Sanitized evidence:",
    "Deployment completed for run ",
    "Run directory:",
)
_VALIDATOR_PREFIXES = (
    "SynthRAN doctor (",
    "SynthRAN network verification (",
    "[PASS] ",
    "Result: READY",
    "Result: NOT READY",
    "Result: PATH PROVEN",
    "Result: NOT PROVEN",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _strip_child_prefix(message: str) -> str:
    value = message.rstrip("\r").strip()
    if value.startswith(PREFIX):
        value = value[len(PREFIX):].lstrip()
    return value


def normalize_child_message(message: str) -> str | None:
    """Return one meaningful child-runtime event, or ``None`` for noise."""

    value = _strip_child_prefix(message)
    if not value:
        return None
    lower = value.lower()
    if any(lower.startswith(prefix.lower()) for prefix in _INTERNAL_PREFIXES):
        return None
    if any(value.startswith(prefix) for prefix in _INTERNAL_OUTPUT_PREFIXES):
        return None
    if any(value.startswith(prefix) for prefix in _VALIDATOR_PREFIXES):
        return None
    if value.startswith(("PLAY: ", "TASK: ", "HANDLER: ")):
        return None

    if value == "reservation-discovery: active owned reservation selected":
        return "  ✓ active owned reservation selected"
    if value == "network prerequisite: verifying path-proven baseline...":
        return None
    if value == "network and Amber transport prerequisites: OK":
        return "  ✓ current 5G session and AMBER transport prerequisites ready"
    if value.startswith("experiment: "):
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

    # The Ansible adapter emits only these three event forms.
    if value.startswith("→ "):
        return "  " + value
    if value.startswith("… "):
        return "    " + value
    if value.startswith("✗ "):
        return "  " + value

    # Controlled research emits compact scientific progress already.
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
    """TextIO-compatible canonical runtime stream."""

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


def _load_json(path: Path) -> Mapping[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _network_summary(network_root: Path, run_id: str) -> tuple[str, ...]:
    evidence = _load_json(network_root.expanduser().resolve() / run_id / "network-evidence.json")
    if evidence is None:
        return ("  ✓ 5G session readiness verified",)
    path = evidence.get("path")
    pdu = path.get("pdu_address") if isinstance(path, dict) else None
    lines = ["  ✓ gNB cell active"]
    if isinstance(pdu, str) and pdu:
        lines.append(f"  ✓ PDU session · {pdu}")
    lines.append("  ✓ srsUE session and UPF route ready")
    return tuple(lines)


def _amber_summary(experiment_root: Path, run_id: str) -> tuple[str, ...]:
    directory = experiment_root.expanduser().resolve() / run_id
    wrapper = _load_json(directory / "experiment-evidence.json")
    if wrapper is None:
        return ()
    iot_name = wrapper.get("iot_evidence")
    if not isinstance(iot_name, str) or not iot_name:
        return ()
    iot = _load_json(directory / iot_name)
    if iot is None:
        return ()
    live = iot.get("live_transport")
    reconciliation = live.get("reconciliation") if isinstance(live, dict) else None
    if not isinstance(reconciliation, dict):
        return ()

    def count(name: str) -> int | None:
        value = reconciliation.get(name)
        return value if isinstance(value, int) and not isinstance(value, bool) else None

    planned = count("planned_count")
    decoded = count("decoded_count")
    source_loss = count("source_loss_count")
    published = count("published_count")
    received = count("central_received_count")
    transport_loss = count("transport_loss_count")
    duplicates = count("duplicate_count")

    lines: list[str] = ["  ✓ PDU-bound TCP transport gate passed"]
    if None not in (planned, decoded, source_loss):
        lines.append(
            f"  ✓ source · planned={planned} · decoded={decoded} · source-loss={source_loss}"
        )
    if None not in (published, received, transport_loss, duplicates):
        lines.append(
            "  ✓ transport · "
            f"published={published} · received={received} · "
            f"loss={transport_loss} · duplicates={duplicates}"
        )
    return tuple(lines)


class RunProgress:
    """Normalize backend-specific progress into one lifecycle."""

    _R2LAB_NETWORK_STAGES = {
        "live resume",
        "foundation",
        "gNB staging",
        "gNB/N2",
        "UE path",
    }

    def __init__(
        self,
        *,
        run_id: str,
        radio: str,
        enabled: bool = True,
        terminal: TextIO | None = None,
        network_root: Path = Path(".synthran/runs"),
        experiment_root: Path = Path(".synthran/experiments"),
    ) -> None:
        self.stream = RunEventStream(
            run_id=run_id,
            radio=radio,
            terminal=terminal,
            terminal_enabled=enabled,
        )
        self.event_path = self.stream.path
        self.run_id = run_id
        self.radio = radio
        self.network_root = network_root
        self.experiment_root = experiment_root
        self.current_stage: str | None = None
        self._infrastructure_open = False
        self._network_open = False

    @property
    def child_stream(self) -> TextIO:
        return self.stream

    def _emit(
        self,
        message: str,
        *,
        stage: str | None = None,
        event: str | None = None,
        component: str | None = None,
    ) -> None:
        self.stream.emit(message, stage=stage, event=event, component=component)

    def _finish_infrastructure(self) -> None:
        if self._infrastructure_open:
            self._emit("✓ infrastructure", stage="infrastructure", event="completed")
            self._infrastructure_open = False

    def _ensure_network(self) -> None:
        if not self._network_open:
            self._finish_infrastructure()
            self._emit("→ network", stage="network", event="started")
            self._network_open = True

    def _finish_network(self) -> None:
        if self._network_open:
            self._emit("✓ network: READY", stage="network", event="completed")
            self._network_open = False

    def start(self, stage: str, detail: str | None = None) -> None:
        self.current_stage = stage
        if stage == "provider":
            self._emit("→ provider", stage="provider", event="started")
            return
        if stage == "resources":
            self._emit(
                "→ infrastructure: allocate, bootstrap and prepare selected nodes",
                stage="infrastructure",
                event="started",
            )
            self._infrastructure_open = True
            return
        if stage == "preflight":
            self._emit("  → verify authority and deployment prerequisites", stage="infrastructure")
            return
        if stage == "network":
            self._ensure_network()
            return
        if stage == "path":
            self._ensure_network()
            self._emit("  → verify live 5G session readiness", stage="network")
            return
        if stage in self._R2LAB_NETWORK_STAGES:
            self._ensure_network()
            suffix = f": {detail}" if detail else ""
            self._emit(
                f"  → {stage}{suffix}",
                stage="network",
                component=stage,
            )
            return
        if stage == "workload":
            self._finish_network()
            self._finish_infrastructure()
            suffix = f": {detail}" if detail else ""
            self._emit(f"→ workload{suffix}", stage="workload", event="started")
            return
        if stage == "acceptance":
            return
        if stage == "cleanup":
            suffix = f": {detail}" if detail else ""
            self._emit(f"→ cleanup{suffix}", stage="cleanup", event="started")
            return
        suffix = f": {detail}" if detail else ""
        self._emit(f"→ {stage}{suffix}", stage=stage, event="started")

    def done(self, stage: str, detail: str | None = None) -> None:
        if stage == "provider":
            self._emit(
                f"✓ provider: {detail}" if detail else "✓ provider",
                stage="provider",
                event="completed",
            )
        elif stage == "resources":
            self._emit("  ✓ compute resources and node bootstrap ready", stage="infrastructure")
        elif stage == "preflight":
            self._emit("  ✓ authority and deployment prerequisites verified", stage="infrastructure")
            self._finish_infrastructure()
        elif stage == "network":
            self._emit("  ✓ Open5GS + srsRAN deployed", stage="network")
        elif stage == "path":
            for line in _network_summary(self.network_root, self.run_id):
                self._emit(line, stage="network")
            self._finish_network()
        elif stage in self._R2LAB_NETWORK_STAGES:
            suffix = f": {detail}" if detail else ""
            self._emit(f"  ✓ {stage}{suffix}", stage="network", component=stage)
        elif stage == "workload":
            if self.radio == "rfsim":
                for line in _amber_summary(self.experiment_root, self.run_id):
                    self._emit(line, stage="workload")
            self._emit("✓ workload: accepted", stage="workload", event="completed")
        elif stage == "acceptance":
            self._emit("✓ experiment accepted", stage="acceptance", event="completed")
            if detail:
                self._emit(f"  evidence: {detail}", stage="acceptance")
        elif stage == "cleanup":
            self._emit(
                f"✓ cleanup: {detail}" if detail else "✓ cleanup",
                stage="cleanup",
                event="completed",
            )
        else:
            suffix = f": {detail}" if detail else ""
            self._emit(f"✓ {stage}{suffix}", stage=stage, event="completed")
        if self.current_stage == stage:
            self.current_stage = None

    def resumed(self, stage: str, detail: str | None = None) -> None:
        if stage == "resources":
            self._emit("→ infrastructure", stage="infrastructure", event="started")
            self._emit("  ↻ existing resource preparation retained", stage="infrastructure", event="resumed")
            self._infrastructure_open = True
        elif stage == "preflight":
            self._emit("  ↻ authority verification already current", stage="infrastructure", event="resumed")
            self._finish_infrastructure()
        elif stage == "network":
            self._ensure_network()
            self._emit("  ↻ existing network deployment retained", stage="network", event="resumed")
        elif stage == "path":
            self._ensure_network()
            for line in _network_summary(self.network_root, self.run_id):
                self._emit(line, stage="network")
            self._finish_network()
        elif stage in self._R2LAB_NETWORK_STAGES:
            self._ensure_network()
            suffix = f": {detail}" if detail else ""
            self._emit(f"  ↻ {stage}{suffix}", stage="network", component=stage, event="resumed")
        elif stage == "workload":
            self._emit("↻ workload: accepted evidence retained", stage="workload", event="resumed")
        else:
            suffix = f": {detail}" if detail else ""
            self._emit(f"↻ {stage}{suffix}", stage=stage, event="resumed")
        if self.current_stage == stage:
            self.current_stage = None

    def skipped(self, stage: str, detail: str | None = None) -> None:
        suffix = f": {detail}" if detail else ""
        self._emit(f"– {stage}{suffix}", stage=stage, event="skipped")
        if self.current_stage == stage:
            self.current_stage = None

    def fail(self, detail: str) -> None:
        stage = self.current_stage or "run"
        normalized = "network" if stage == "path" or stage in self._R2LAB_NETWORK_STAGES else stage
        self._emit(f"✗ {normalized}: {detail}", stage=normalized, event="failed")
        self.current_stage = None

    def close(self) -> None:
        self._finish_network()
        self._finish_infrastructure()
        self.stream.flush()
