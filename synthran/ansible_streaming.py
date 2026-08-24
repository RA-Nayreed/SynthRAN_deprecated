"""Streaming subprocess execution and live progress formatting for Ansible stages."""

from __future__ import annotations

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
        "Pinning locked Open5GS images",
    ),
    (
        re.compile(r"^Replace every reviewed mutable srsRAN image reference$", re.I),
        "Pinning locked srsRAN images",
    ),
    (
        re.compile(r"^Attach the run ID to the deployed network resources$", re.I),
        "Recording run ownership",
    ),
    (
        re.compile(r"^5g/open5gs/config\s*:\s*(.*)$", re.I),
        r"Open5GS config: \1",
    ),
    (
        re.compile(r"^5g/open5gs/deploy\s*:\s*(.*)$", re.I),
        r"Open5GS deploy: \1",
    ),
    (
        re.compile(r"^5g/srsRAN/deploy\s*:\s*(.*)$", re.I),
        r"srsRAN deploy: \1",
    ),
]

# Tasks that are always visible even without a friendly mapping.
VISIBLE_TASK_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"^Wait for .+ to become Ready$", re.I),
    re.compile(r"^Pin(ning)? locked .+ images?$", re.I),
    re.compile(r"^Recording run ownership$", re.I),
]


def format_duration(seconds: float) -> str:
    """Format elapsed seconds as human-readable string (e.g. 30s, 1m, 2m 30s)."""
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
    """Map raw internal Ansible task/play names to concise operator-facing labels."""
    cleaned = _clean_ansible_title(name)
    for pattern, replacement in FRIENDLY_MAPPINGS:
        if pattern.search(cleaned):
            return pattern.sub(replacement, cleaned).strip()
    return cleaned


def is_ugly_template_task(name: str) -> bool:
    """Detect skipped upstream tasks with Jinja template error noise."""
    lower = name.lower()
    return "<<" in name and ">>" in name and ("error" in lower or "undefined" in lower)


def _is_visible_task(name: str) -> bool:
    """Return True if a task name should be visible to the operator.

    A task is visible if it has a friendly mapping or matches a visible pattern.
    Unmapped internal tasks (gather facts, command, file, assert, etc.) are suppressed.
    """
    cleaned = _clean_ansible_title(name)
    for pattern, _replacement in FRIENDLY_MAPPINGS:
        if pattern.search(cleaned):
            return True
    for pattern in VISIBLE_TASK_PATTERNS:
        if pattern.search(cleaned):
            return True
    return False


def parse_ansible_line(line: str, current_task: str | None = None) -> str | None:
    """Parse one raw Ansible line into a sanitized high-level event or None.

    Routine host lines (OK, CHANGED, SKIPPED) and ugly skipped template errors are suppressed.
    Unmapped internal TASK names are suppressed during normal execution.
    Failures (FAILED, FATAL, UNREACHABLE) remain surfaced with task and host context,
    including the cleaned task name even if the TASK header itself was suppressed.
    """
    stripped = line.strip()
    if not stripped:
        return None

    match = PLAY_RE.match(stripped)
    if match:
        raw_name = _clean_ansible_title(match.group(1))
        name = friendly_task_name(raw_name)
        return f"  PLAY: {name}"

    match = TASK_RE.match(stripped)
    if match:
        raw_name = _clean_ansible_title(match.group(1))
        if is_ugly_template_task(raw_name):
            return None
        name = friendly_task_name(raw_name)
        if _is_visible_task(raw_name):
            return f"  TASK: {name}"
        # Suppress unmapped internal TASK lines during normal execution
        return None

    match = HANDLER_RE.match(stripped)
    if match:
        raw_name = _clean_ansible_title(match.group(1))
        name = friendly_task_name(raw_name)
        return f"  HANDLER: {name}"

    match = HOST_STATUS_RE.match(stripped)
    if match:
        raw_status = match.group(1).lower()
        host = match.group(2).strip()
        status = STATUS_MAP.get(raw_status, raw_status.upper())
        if status in ("OK", "CHANGED", "SKIPPED"):
            # Suppress routine host chatter from normal CLI output
            return None
        if current_task:
            return f"    [FAIL] {current_task}\n           host: {host}\n           state: {status}"
        return f"    [FAIL] host: {host}\n           state: {status}"

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
    """Stream sanitized progress for long-running Ansible stages with heartbeats."""
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
    started = monotonic()
    task_started = started
    current_task: str | None = None
    next_heartbeat = heartbeat_interval_seconds
    deadline = started + timeout_seconds

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
                if report is not None:
                    label = current_task or "Ansible stage"
                    report(f"    {label} · {format_duration(task_elapsed)}")
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
                task_started = monotonic()
                next_heartbeat = heartbeat_interval_seconds
            elif play_match:
                raw_name = _clean_ansible_title(play_match.group(1))
                current_task = friendly_task_name(raw_name)
                task_started = monotonic()
                next_heartbeat = heartbeat_interval_seconds
            elif handler_match:
                raw_name = _clean_ansible_title(handler_match.group(1))
                current_task = friendly_task_name(raw_name)
                task_started = monotonic()
                next_heartbeat = heartbeat_interval_seconds

            if report is not None:
                parsed = parse_ansible_line(line, current_task=current_task)
                if parsed is not None:
                    report(parsed)

        returncode = process.wait()
        reader_thread.join(timeout=2.0)
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
