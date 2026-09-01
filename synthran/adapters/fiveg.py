"""Thin process boundary around the pinned 5g-Ansible machine interface.

SynthRAN does not implement or interpret 5G deployment mechanics here. It
validates the locked checkout, invokes ``bin/fiveg`` with declarative input,
relays versioned upstream progress events, and consumes the final machine JSON.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import threading
from typing import Any, Callable, Mapping, Sequence

from synthran.dependencies import DependencyLock


FIVEG_SPEC_SCHEMA = "fiveg/deployment/v1"
FIVEG_EVENT_SCHEMA = "fiveg/event/v1"


class FiveGAdapterError(RuntimeError):
    """Raised when the pinned 5g-Ansible machine boundary cannot be used."""


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str = ""


EventSink = Callable[[Mapping[str, Any]], None]
Runner = Callable[
    [Sequence[str], Path, Mapping[str, str] | None, int, EventSink | None],
    CommandResult,
]


def _decode_event_line(line: str) -> Mapping[str, Any] | None:
    try:
        value = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(value, dict) or value.get("schema") != FIVEG_EVENT_SCHEMA:
        return None
    return value


def subprocess_runner(
    command: Sequence[str],
    cwd: Path,
    environment: Mapping[str, str] | None,
    timeout_seconds: int,
    event_sink: EventSink | None,
) -> CommandResult:
    """Run one machine verb while relaying its JSONL event channel live."""

    try:
        process = subprocess.Popen(
            list(command),
            cwd=cwd,
            env=dict(environment) if environment is not None else None,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise FiveGAdapterError("required 5g-Ansible executable was not found") from exc

    if process.stdout is None or process.stderr is None:
        process.kill()
        raise FiveGAdapterError("5g-Ansible machine pipes could not be opened")

    stdout_parts: list[str] = []
    stderr_parts: list[str] = []
    reader_errors: list[BaseException] = []

    def read_stdout() -> None:
        try:
            stdout_parts.extend(process.stdout.readlines())
        except BaseException as exc:  # pragma: no cover - OS pipe failure
            reader_errors.append(exc)

    def read_stderr() -> None:
        try:
            for line in process.stderr:
                event = _decode_event_line(line.strip())
                if event is not None:
                    if event_sink is not None:
                        event_sink(event)
                    continue
                stderr_parts.append(line)
        except BaseException as exc:  # includes event-sink contract failures
            reader_errors.append(exc)

    stdout_thread = threading.Thread(target=read_stdout, daemon=True)
    stderr_thread = threading.Thread(target=read_stderr, daemon=True)
    stdout_thread.start()
    stderr_thread.start()
    try:
        returncode = process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        process.kill()
        process.wait()
        stdout_thread.join(timeout=5)
        stderr_thread.join(timeout=5)
        raise FiveGAdapterError("5g-Ansible machine operation exceeded its timeout") from exc
    stdout_thread.join(timeout=5)
    stderr_thread.join(timeout=5)
    if stdout_thread.is_alive() or stderr_thread.is_alive():
        raise FiveGAdapterError("5g-Ansible machine output stream did not close")
    if reader_errors:
        raise FiveGAdapterError("5g-Ansible progress event stream could not be consumed") from reader_errors[0]
    return CommandResult(returncode, "".join(stdout_parts), "".join(stderr_parts))


def _locked_dependency(lock: DependencyLock):
    dependency = next((item for item in lock.git if item.name == "fiveg_ansible"), None)
    if dependency is None:
        raise FiveGAdapterError("dependency lock does not define fiveg_ansible")
    return dependency


def _git_output(args: Sequence[str], *, cwd: Path) -> str:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=cwd,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        raise FiveGAdapterError("Git is required for dependency validation") from exc
    except subprocess.CalledProcessError as exc:
        raise FiveGAdapterError("unable to validate the locked fiveg_ansible checkout") from exc
    return completed.stdout.strip()


def validate_checkout(lock: DependencyLock, dependency_root: Path) -> Path:
    """Return the exact immutable 5g-Ansible checkout declared by the lock."""

    dependency = _locked_dependency(lock)
    checkout = dependency_root.expanduser().resolve().joinpath(*dependency.checkout.parts)
    if not checkout.is_dir():
        raise FiveGAdapterError(
            "locked fiveg_ansible checkout is missing; run synthran deps sync"
        )
    if _git_output(("rev-parse", "HEAD"), cwd=checkout) != dependency.commit:
        raise FiveGAdapterError("fiveg_ansible checkout is not at the locked commit")
    if _git_output(("status", "--porcelain"), cwd=checkout):
        raise FiveGAdapterError("fiveg_ansible checkout contains local changes")
    if _git_output(("rev-parse", "--abbrev-ref", "HEAD"), cwd=checkout) != "HEAD":
        raise FiveGAdapterError("fiveg_ansible checkout must be detached")
    if (
        _git_output(("remote", "get-url", "origin"), cwd=checkout).rstrip("/")
        != dependency.url.rstrip("/")
    ):
        raise FiveGAdapterError("fiveg_ansible checkout origin does not match the lock")
    for relative in ("bin/fiveg", "tools/fiveg_machine.py", "tools/fiveg_events.py"):
        if not (checkout / relative).is_file():
            raise FiveGAdapterError(
                f"locked fiveg_ansible checkout is missing machine interface: {relative}"
            )
    return checkout


def write_spec(path: Path, spec: Mapping[str, Any]) -> Path:
    """Persist one declarative upstream spec without interpreting its topology."""

    if spec.get("schema") != FIVEG_SPEC_SCHEMA:
        raise FiveGAdapterError(f"deployment spec schema must be {FIVEG_SPEC_SCHEMA}")
    deployment_id = spec.get("id")
    if not isinstance(deployment_id, str) or not deployment_id:
        raise FiveGAdapterError("deployment spec requires a non-empty id")
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    temporary.write_text(
        json.dumps(dict(spec), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    temporary.replace(destination)
    return destination


def load_spec(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FiveGAdapterError("deployment spec must be readable JSON") from exc
    if not isinstance(value, dict) or value.get("schema") != FIVEG_SPEC_SCHEMA:
        raise FiveGAdapterError(f"deployment spec schema must be {FIVEG_SPEC_SCHEMA}")
    return value


def _failure_detail(result: CommandResult) -> str:
    """Return one concise upstream diagnostic without replaying progress JSONL."""

    lines = [*result.stderr.splitlines(), *result.stdout.splitlines()]
    for item in reversed(lines):
        line = item.strip()
        if not line or _decode_event_line(line) is not None:
            continue
        return line[-1000:]
    return ""


@dataclass(frozen=True)
class FiveGAdapter:
    """Invoke one locked 5g-Ansible checkout through ``bin/fiveg`` only."""

    checkout: Path
    state_root: Path
    timeout_seconds: int = 3600
    runner: Runner = subprocess_runner
    environment: Mapping[str, str] | None = None
    event_sink: EventSink | None = None

    @classmethod
    def from_lock(
        cls,
        *,
        lock: DependencyLock,
        dependency_root: Path,
        state_root: Path,
        timeout_seconds: int = 3600,
        runner: Runner = subprocess_runner,
        environment: Mapping[str, str] | None = None,
        event_sink: EventSink | None = None,
    ) -> "FiveGAdapter":
        if timeout_seconds < 1 or timeout_seconds > 14400:
            raise FiveGAdapterError("5g-Ansible timeout must be between 1 and 14400 seconds")
        return cls(
            checkout=validate_checkout(lock, dependency_root),
            state_root=state_root.expanduser().resolve(),
            timeout_seconds=timeout_seconds,
            runner=runner,
            environment=environment,
            event_sink=event_sink,
        )

    def _environment(self) -> dict[str, str]:
        value = dict(os.environ)
        if self.environment is not None:
            value.update(self.environment)
        return value

    def _invoke(
        self,
        *arguments: str,
        expected_schema: str,
    ) -> Mapping[str, Any]:
        command = [
            str(self.checkout / "bin" / "fiveg"),
            *arguments,
            "--json",
        ]
        if self.event_sink is not None:
            command.append("--events")
        result = self.runner(
            command,
            self.checkout,
            self._environment(),
            self.timeout_seconds,
            self.event_sink,
        )
        if result.returncode != 0:
            operation = arguments[0] if arguments else "operation"
            detail = _failure_detail(result)
            suffix = f": {detail}" if detail else ""
            raise FiveGAdapterError(f"5g-Ansible {operation} failed{suffix}")
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise FiveGAdapterError("5g-Ansible machine output was not JSON") from exc
        if not isinstance(payload, dict) or payload.get("schema") != expected_schema:
            raise FiveGAdapterError(
                f"5g-Ansible returned an unexpected schema; expected {expected_schema}"
            )
        return payload

    def capabilities(self) -> Mapping[str, Any]:
        return self._invoke("capabilities", expected_schema="fiveg/capabilities/v1")

    def plan(self, spec_path: Path) -> Mapping[str, Any]:
        return self._invoke(
            "plan",
            "--spec",
            str(spec_path.expanduser().resolve()),
            "--state-root",
            str(self.state_root),
            expected_schema="fiveg/deployment-plan/v1",
        )

    def up(self, spec_path: Path, *, resume: bool = False) -> Mapping[str, Any]:
        arguments = [
            "up",
            "--spec",
            str(spec_path.expanduser().resolve()),
            "--state-root",
            str(self.state_root),
        ]
        if resume:
            arguments.append("--resume")
        return self._invoke(
            *arguments,
            expected_schema="fiveg/deployment-manifest/v1",
        )

    def status(self, deployment_id: str) -> Mapping[str, Any]:
        if not deployment_id:
            raise FiveGAdapterError("deployment id must not be empty")
        return self._invoke(
            "status",
            "--deployment",
            deployment_id,
            "--state-root",
            str(self.state_root),
            expected_schema="fiveg/deployment-status/v1",
        )

    def down(self, deployment_id: str) -> Mapping[str, Any]:
        if not deployment_id:
            raise FiveGAdapterError("deployment id must not be empty")
        return self._invoke(
            "down",
            "--deployment",
            deployment_id,
            "--state-root",
            str(self.state_root),
            expected_schema="fiveg/deployment-down/v1",
        )

    def scenario(
        self,
        deployment_id: str,
        *,
        scenario_type: str | None = None,
        no_setup: bool = False,
    ) -> Mapping[str, Any]:
        if not deployment_id:
            raise FiveGAdapterError("deployment id must not be empty")
        arguments = [
            "scenario",
            "--deployment",
            deployment_id,
            "--state-root",
            str(self.state_root),
        ]
        if scenario_type is not None:
            arguments.extend(("--type", scenario_type))
        if no_setup:
            arguments.append("--no-setup")
        return self._invoke(
            *arguments,
            expected_schema="fiveg/scenario-result/v1",
        )
