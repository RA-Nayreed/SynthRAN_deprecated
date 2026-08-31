"""Streaming Ansible execution translated into meaningful SynthRAN events."""

from __future__ import annotations

import json
from pathlib import Path
import queue
import re
import subprocess
import threading
from time import monotonic
from typing import Callable, Mapping, Sequence

from synthran.live_preflight import CommandResult


PLAY_RE = re.compile(r"^PLAY\s+\[(.*)\](?:\s*.*)?$")
TASK_RE = re.compile(r"^TASK\s+\[(.*)\](?:\s*.*)?$")
HANDLER_RE = re.compile(r"^RUNNING HANDLER\s+\[(.*)\](?:\s*.*)?$")
HOST_STATUS_RE = re.compile(
    r"^(ok|changed|failed|fatal|skipping|unreachable):\s+\[([^\]]+)\]",
    re.IGNORECASE,
)

STATUS_MAP = {
    "ok": "OK",
    "changed": "CHANGED",
    "failed": "FAILED",
    "fatal": "FATAL",
    "skipping": "SKIPPED",
    "unreachable": "UNREACHABLE",
}

FRIENDLY_MAPPINGS: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(r"^Deploy the locked Open5GS core into the ready cluster$", re.I),
        "Open5GS core",
    ),
    (
        re.compile(r"^Deploy the srsRAN gNB and srsUE into the ready cluster$", re.I),
        "srsRAN RFSIM",
    ),
    (
        re.compile(r"^Replace every reviewed mutable Open5GS image reference$", re.I),
        "Open5GS locked images",
    ),
    (
        re.compile(r"^Replace every reviewed mutable srsRAN image reference$", re.I),
        "srsRAN locked images",
    ),
    (
        re.compile(r"^Check job status$", re.I),
        "node bootstrap",
    ),
    (
        re.compile(r"^setup/ovs\s*:\s*Ensure OVS is installed$", re.I),
        "node networking setup",
    ),
    (
        re.compile(r"^setup/cni\s*:\s*Apply NetworkAddonsConfig from /tmp$", re.I),
        "Multus/OVS network setup",
    ),
    (
        re.compile(r"^5g/open5gs/config\s*:\s*(.*)$", re.I),
        r"Open5GS config: \1",
    ),
    (
        re.compile(r"^5g/open5gs/deploy\s*:\s*(.*)$", re.I),
        r"Open5GS: \1",
    ),
    (
        re.compile(r"^5g/srsRAN/deploy\s*:\s*(.*)$", re.I),
        r"srsRAN: \1",
    ),
]

VISIBLE_TASK_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^(?:Open5GS|srsRAN) locked images$", re.I),
    re.compile(r"^Open5GS: Wait for Open5GS Core NFs pods Ready$", re.I),
    re.compile(r"^Open5GS: Wait for MongoDB pod to be Ready$", re.I),
    re.compile(r"^srsRAN: Deploy srsRAN UE pod via Helm.*$", re.I),
    re.compile(r"^srsRAN: Wait for UE pod to be Ready$", re.I),
    re.compile(r"^srsRAN: Wait for gNB cell to be activated$", re.I),
    re.compile(r"^srsRAN: Wait for GNU Radio broker to be ready$", re.I),
    re.compile(r"^srsRAN: Wait until each UE process is up$", re.I),
    re.compile(r"^Wait for .+ to become Ready$", re.I),
)

_GENERIC_HEARTBEAT_TASKS = {
    "command",
    "shell",
    "file",
    "k8s",
    "assert",
    "debug",
    "include_tasks",
    "include_role",
    "set_fact",
}


def format_duration(seconds: float) -> str:
    """Format elapsed seconds as human-readable string."""

    total = int(max(0, seconds))
    mins = total // 60
    secs = total % 60
    if mins == 0:
        return f"{secs}s"
    if secs == 0:
        return f"{mins}m"
    return f"{mins}m {secs}s"


def _clean_ansible_title(raw_name: str) -> str:
    name = raw_name.strip()
    match = re.search(r"(?:\s+|:\s*)[a-zA-Z_][a-zA-Z0-9_]*\s*=", name)
    if match:
        cleaned = name[: match.start()].strip().rstrip(":")
        if cleaned:
            return cleaned.strip()
    return name


def friendly_task_name(name: str) -> str:
    """Map raw upstream names to concise operator-facing labels."""

    cleaned = _clean_ansible_title(name)
    for pattern, replacement in FRIENDLY_MAPPINGS:
        if pattern.search(cleaned):
            return pattern.sub(replacement, cleaned).strip()
    return cleaned


def is_ugly_template_task(name: str) -> bool:
    """Detect upstream template-noise names that should never reach operators."""

    lower = name.lower()
    return "<<" in name and ">>" in name and ("error" in lower or "undefined" in lower)


def _is_visible_task(name: str) -> bool:
    friendly = friendly_task_name(name)
    return any(pattern.search(friendly) for pattern in VISIBLE_TASK_PATTERNS)


def _heartbeat_label(name: str | None) -> str | None:
    if name is None:
        return None
    friendly = friendly_task_name(name)
    if not friendly or friendly.lower() in _GENERIC_HEARTBEAT_TASKS:
        return None
    if is_ugly_template_task(friendly):
        return None
    return friendly


def _sanitize_reason(value: str) -> str:
    text = " ".join(value.replace("\r", " ").replace("\n", " ").split())
    text = re.sub(r"(?i)\b[0-9a-f]{32,}\b", "<redacted>", text)
    text = re.sub(r"\b[0-9]{14,16}\b", "<redacted>", text)
    if len(text) > 240:
        text = text[:237].rstrip() + "..."
    return text


def _failure_reason(line: str) -> str | None:
    marker = "=>"
    if marker not in line:
        return None
    raw = line.split(marker, 1)[1].strip()
    if not raw.startswith("{"):
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    for key in ("msg", "stderr", "module_stderr"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return _sanitize_reason(value)
    return None


def parse_ansible_line(line: str, current_task: str | None = None) -> str | None:
    """Translate one completed Ansible status line into a SynthRAN event.

    PLAY and TASK headers are intentionally not rendered here. A task header
    alone does not prove execution because Ansible may immediately mark that
    task skipped. The streaming runner promotes a visible task only after a
    non-skipped host status or a live heartbeat proves it is actually running.
    """

    stripped = line.strip()
    if not stripped:
        return None

    if PLAY_RE.match(stripped) or TASK_RE.match(stripped):
        return None

    match = HANDLER_RE.match(stripped)
    if match:
        raw_name = _clean_ansible_title(match.group(1))
        return f"→ {friendly_task_name(raw_name)}"

    match = HOST_STATUS_RE.match(stripped)
    if match:
        raw_status = match.group(1).lower()
        host = match.group(2).strip()
        status = STATUS_MAP.get(raw_status, raw_status.upper())
        if status in ("OK", "CHANGED", "SKIPPED"):
            return None
        task = current_task or "Ansible task"
        lines = [f"✗ {task}", f"  host: {host}", f"  state: {status}"]
        reason = _failure_reason(stripped)
        if reason:
            lines.append(f"  reason: {reason}")
        return "\n".join(lines)

    return None


def _kill_process_tree(process: subprocess.Popen) -> None:
    """Terminate and reap child process upon timeout or cancellation."""

    try:
        process.terminate()
        try:
            process.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3.0)
    except OSError:
        pass
    finally:
        if process.stdout and not process.stdout.closed:
            try:
                process.stdout.close()
            except OSError:
                pass


def run_streaming_ansible_command(
    command: Sequence[str],
    cwd: Path,
    environment: Mapping[str, str] | None,
    timeout_seconds: int,
    *,
    report: Callable[[str], None] | None = None,
    heartbeat_interval_seconds: float = 30.0,
    poll_interval_seconds: float = 0.5,
) -> CommandResult:
    """Stream truthful, sparse progress while preserving complete raw output."""

    process = subprocess.Popen(
        list(command),
        cwd=cwd,
        env=dict(environment) if environment is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    output_queue: queue.Queue[str | None] = queue.Queue()

    def _reader() -> None:
        try:
            if process.stdout:
                for line in process.stdout:
                    output_queue.put(line)
        finally:
            output_queue.put(None)
            if process.stdout and not process.stdout.closed:
                try:
                    process.stdout.close()
                except OSError:
                    pass

    reader_thread = threading.Thread(target=_reader, daemon=True)
    reader_thread.start()

    output_lines: list[str] = []
    deferred_failures: list[str] = []
    started = monotonic()
    task_started = started
    current_task: str | None = None
    current_task_visible = False
    current_task_announced = False
    next_heartbeat = heartbeat_interval_seconds
    deadline = started + timeout_seconds

    def announce_current() -> None:
        nonlocal current_task_announced
        if (
            report is not None
            and current_task is not None
            and current_task_visible
            and not current_task_announced
        ):
            report(f"→ {current_task}")
            current_task_announced = True

    try:
        while True:
            now = monotonic()
            if now > deadline:
                _kill_process_tree(process)
                reader_thread.join(timeout=2.0)
                raise subprocess.TimeoutExpired(
                    cmd=list(command),
                    timeout=timeout_seconds,
                    output="".join(output_lines),
                )

            task_elapsed = now - task_started
            if task_elapsed >= next_heartbeat:
                label = _heartbeat_label(current_task)
                if report is not None and label is not None:
                    announce_current()
                    report(f"… {label} · {format_duration(task_elapsed)}")
                next_heartbeat += heartbeat_interval_seconds

            timeout_to_next = max(0.05, min(poll_interval_seconds, deadline - now))
            try:
                line = output_queue.get(timeout=timeout_to_next)
            except queue.Empty:
                if process.poll() is not None and not reader_thread.is_alive():
                    break
                continue

            if line is None:
                break

            output_lines.append(line)
            stripped = line.strip()
            task_match = TASK_RE.match(stripped)
            play_match = PLAY_RE.match(stripped)
            handler_match = HANDLER_RE.match(stripped)

            if task_match:
                raw_name = _clean_ansible_title(task_match.group(1))
                current_task = friendly_task_name(raw_name)
                current_task_visible = _is_visible_task(raw_name)
                current_task_announced = False
                task_started = monotonic()
                next_heartbeat = heartbeat_interval_seconds
                continue
            if play_match:
                current_task = None
                current_task_visible = False
                current_task_announced = False
                task_started = monotonic()
                next_heartbeat = heartbeat_interval_seconds
                continue
            if handler_match:
                raw_name = _clean_ansible_title(handler_match.group(1))
                current_task = friendly_task_name(raw_name)
                current_task_visible = True
                current_task_announced = False
                task_started = monotonic()
                next_heartbeat = heartbeat_interval_seconds
                continue

            if stripped == "...ignoring" and deferred_failures:
                deferred_failures.pop()
                continue

            status_match = HOST_STATUS_RE.match(stripped)
            if status_match:
                status = status_match.group(1).lower()
                if status in {"ok", "changed"}:
                    announce_current()
                    continue
                if status == "skipping":
                    # A TASK header followed only by skipping statuses was never
                    # executed. Do not promote it to the operator stream.
                    continue
                if status in {"failed", "fatal", "unreachable"}:
                    parsed = parse_ansible_line(line, current_task=current_task)
                    if parsed is not None:
                        deferred_failures.append(parsed)
                    continue

            if report is not None:
                parsed = parse_ansible_line(line, current_task=current_task)
                if parsed is not None:
                    report(parsed)

        returncode = process.wait()
        reader_thread.join(timeout=2.0)
        if report is not None and returncode != 0:
            for failure in deferred_failures:
                report(failure)
        return CommandResult(
            returncode=returncode,
            stdout="".join(output_lines),
            stderr="",
        )
    finally:
        if process.stdout and not process.stdout.closed:
            try:
                process.stdout.close()
            except OSError:
                pass
