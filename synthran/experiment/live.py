"""Shared process, SSH, and Kubernetes primitives for live experiments.

This module contains no 5G reconciliation or deployment repair. Infrastructure
is upstream-owned; these helpers support only observation and run-scoped
experiment resources/processes.
"""

from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import time
from typing import Any, Mapping, Sequence, TextIO

from synthran.experiment import ExperimentError
from synthran.experiment.resources import RUN_LABEL
from synthran.fiveg_ansible import NetworkInventory
from synthran.live_preflight import CommandResult, LivePreflightError, ssh_command


DEFAULT_COLLECTION_SECONDS = 180
DEFAULT_MINIMUM_PER_SENSOR = 3
LOCAL_CENTRAL_FORWARD_PORT = 18885
KUBERNETES_NAMESPACE = "open5gs"


@dataclass(frozen=True)
class ExperimentRunResult:
    run_id: str
    run_directory: Path
    evidence_path: Path
    ready: bool


@dataclass
class ManagedProcess:
    name: str
    process: subprocess.Popen[str]
    log_path: Path
    log_stream: TextIO

    def stop(self) -> None:
        if self.process.poll() is None:
            pid = getattr(self.process, "pid", None)
            if isinstance(pid, int) and pid > 1 and hasattr(os, "killpg"):
                try:
                    os.killpg(pid, signal.SIGTERM)
                except (ProcessLookupError, PermissionError):
                    pass
                try:
                    self.process.wait(timeout=8)
                except Exception:
                    try:
                        os.killpg(pid, signal.SIGKILL)
                    except (ProcessLookupError, PermissionError):
                        pass
                    try:
                        self.process.wait(timeout=5)
                    except Exception:
                        pass
            else:
                try:
                    self.process.terminate()
                except Exception:
                    pass
                try:
                    self.process.wait(timeout=8)
                except Exception:
                    try:
                        self.process.kill()
                    except Exception:
                        pass
                    try:
                        self.process.wait(timeout=5)
                    except Exception:
                        pass
        try:
            self.log_stream.close()
        except Exception:
            pass


def _run(
    command: Sequence[str],
    *,
    timeout_seconds: int = 60,
    cwd: Path | None = None,
    input_text: str | None = None,
) -> CommandResult:
    try:
        completed = subprocess.run(
            list(command),
            cwd=cwd,
            input=input_text,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
            check=False,
        )
    except FileNotFoundError as exc:
        raise ExperimentError(f"required command was not found: {command[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise ExperimentError("experiment command timed out") from exc
    return CommandResult(completed.returncode, completed.stdout, completed.stderr)


def _checked(
    command: Sequence[str],
    *,
    label: str,
    timeout_seconds: int = 60,
    cwd: Path | None = None,
    input_text: str | None = None,
) -> str:
    result = _run(
        command,
        timeout_seconds=timeout_seconds,
        cwd=cwd,
        input_text=input_text,
    )
    if result.returncode != 0:
        reason = (result.stderr or result.stdout).strip()
        raise ExperimentError(f"{label} failed" + (f": {reason}" if reason else ""))
    return result.stdout


def _remote(
    inventory: NetworkInventory,
    *remote_command: str,
    label: str,
    timeout_seconds: int = 60,
) -> str:
    try:
        command = ssh_command(inventory.core_node, *remote_command)
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    return _checked(command, label=label, timeout_seconds=timeout_seconds)


def _remote_json(
    inventory: NetworkInventory,
    command: str,
    *,
    label: str,
    timeout_seconds: int = 60,
) -> Mapping[str, Any]:
    output = _remote(
        inventory,
        "sh",
        "-c",
        command,
        label=label,
        timeout_seconds=timeout_seconds,
    )
    try:
        value = json.loads(output)
    except json.JSONDecodeError as exc:
        raise ExperimentError(f"{label} did not return JSON") from exc
    if not isinstance(value, dict):
        raise ExperimentError(f"{label} did not return one JSON object")
    return value


def _transfer_file(
    inventory: NetworkInventory,
    source_file: Path,
    remote_path: str,
    *,
    label: str,
) -> None:
    try:
        content = source_file.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ExperimentError(f"{label} source is not readable UTF-8 text") from exc
    try:
        command = ssh_command(
            inventory.core_node,
            "sh",
            "-c",
            f"cat > {shlex.quote(remote_path)}",
        )
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    result = _run(command, input_text=content, timeout_seconds=30)
    if result.returncode != 0:
        reason = (result.stderr or result.stdout).strip()
        raise ExperimentError(f"{label} failed" + (f": {reason}" if reason else ""))


def _core_address(inventory: NetworkInventory) -> str:
    value = inventory.core_node.variables.get("ip")
    if not value:
        raise ExperimentError("upstream inventory is missing the core node IP address")
    try:
        ipaddress.ip_address(value)
    except ValueError as exc:
        raise ExperimentError("upstream inventory has an invalid core node IP address") from exc
    return value


def _probe_ssh_forwarding(
    inventory: NetworkInventory,
    *,
    timeout_seconds: int = 15,
) -> None:
    host = inventory.core_node
    try:
        command = ssh_command(host, "sshd", "-T")
    except LivePreflightError as exc:
        raise ExperimentError(f"SSH forwarding probe failed on {host.name}: {exc}") from exc
    result = _run(command, timeout_seconds=timeout_seconds)
    if result.returncode != 0:
        raise ExperimentError(f"SSH forwarding is unavailable on {host.name}")
    forwarding: str | None = None
    for line in result.stdout.splitlines():
        parts = line.strip().lower().split(None, 1)
        if len(parts) == 2 and parts[0] == "allowtcpforwarding":
            forwarding = parts[1]
            break
    if forwarding not in {"yes", "all"}:
        raise ExperimentError(f"SSH forwarding is disabled on {host.name}")


def _wait_tcp(
    host: str,
    port: int,
    *,
    timeout_seconds: int = 60,
    family: int = socket.AF_INET,
    process: ManagedProcess | None = None,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if process is not None and process.process.poll() is not None:
            try:
                process.log_stream.flush()
            except Exception:
                pass
            raise ExperimentError(
                f"{process.name} exited before TCP endpoint {host}:{port} became ready; "
                f"see {process.log_path}"
            )
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.settimeout(1.0)
        try:
            sock.connect((host, port))
            return
        except OSError:
            time.sleep(0.5)
        finally:
            sock.close()
    raise ExperimentError(f"TCP endpoint {host}:{port} did not become ready")


def _start_process(
    name: str,
    command: Sequence[str],
    *,
    cwd: Path,
    log_path: Path,
    env: Mapping[str, str] | None = None,
) -> ManagedProcess:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    stream = log_path.open("w", encoding="utf-8", newline="\n")
    try:
        process = subprocess.Popen(
            list(command),
            cwd=cwd,
            env=dict(env) if env is not None else None,
            text=True,
            stdin=subprocess.DEVNULL,
            stdout=stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except (FileNotFoundError, OSError) as exc:
        stream.close()
        raise ExperimentError(f"unable to start {name}") from exc
    return ManagedProcess(name, process, log_path, stream)


def _ssh_tunnel_command(
    inventory: NetworkInventory,
    *,
    local_port: int,
    remote_port: int,
    remote_command: str,
) -> tuple[str, ...]:
    try:
        base = list(ssh_command(inventory.core_node))
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    if not base:
        raise ExperimentError("unable to construct strict SSH tunnel")
    target = base.pop()
    return tuple(
        base
        + [
            "-o",
            "ExitOnForwardFailure=yes",
            "-L",
            f"127.0.0.1:{local_port}:127.0.0.1:{remote_port}",
            target,
            remote_command,
        ]
    )


def _kubectl_apply_object(
    inventory: NetworkInventory,
    value: Mapping[str, Any],
    *,
    label: str,
) -> None:
    try:
        command = ssh_command(
            inventory.core_node,
            "sh",
            "-c",
            "KUBECONFIG=/etc/kubernetes/admin.conf kubectl apply -f -",
        )
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    result = _run(command, input_text=json.dumps(value), timeout_seconds=60)
    if result.returncode != 0:
        raise ExperimentError(f"{label} failed")


def _wait_rollout(inventory: NetworkInventory, deployment: str, *, label: str) -> None:
    _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl rollout status deployment/"
        f"{shlex.quote(deployment)} -n {KUBERNETES_NAMESPACE} --timeout=180s",
        label=label,
        timeout_seconds=200,
    )


def _delete_experiment_objects(inventory: NetworkInventory, run_id: str) -> None:
    _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl delete deployment,configmap "
        f"-n {KUBERNETES_NAMESPACE} -l {RUN_LABEL}={shlex.quote(run_id)} "
        "--ignore-not-found=true --wait=true",
        label="exact-run Kubernetes cleanup",
        timeout_seconds=180,
    )


def _remote_path_exists(
    inventory: NetworkInventory,
    path: str,
    *,
    timeout_seconds: int = 10,
) -> bool:
    try:
        command = ssh_command(inventory.core_node, "test", "-f", path)
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    return _run(command, timeout_seconds=timeout_seconds).returncode == 0


def _remote_process_reap(
    inventory: NetworkInventory,
    *,
    patterns: Sequence[str],
    orphan_only: bool,
    label: str,
) -> None:
    """Terminate only remote processes whose complete cmdline matches a pattern."""

    if not patterns:
        return
    script = r'''
import json, os, re, signal, sys, time
patterns = [re.compile(value) for value in json.loads(sys.argv[1])]
orphan_only = sys.argv[2] == '1'

def matches():
    result = []
    for entry in os.listdir('/proc'):
        if not entry.isdigit() or int(entry) <= 1:
            continue
        pid = int(entry)
        try:
            raw = open(f'/proc/{pid}/cmdline', 'rb').read()
            cmd = ' '.join(part.decode('utf-8', 'replace') for part in raw.split(b'\0') if part)
            status = open(f'/proc/{pid}/status', encoding='utf-8', errors='replace').read().splitlines()
        except OSError:
            continue
        if not cmd or not any(pattern.search(cmd) for pattern in patterns):
            continue
        ppid = None
        for line in status:
            if line.startswith('PPid:'):
                try: ppid = int(line.split()[1])
                except (IndexError, ValueError): ppid = None
                break
        if orphan_only and ppid != 1:
            continue
        result.append(pid)
    return result

for signum, delay in ((signal.SIGTERM, 1.5), (signal.SIGKILL, 0.5)):
    targets = matches()
    if not targets:
        raise SystemExit(0)
    for pid in targets:
        try: os.kill(pid, signum)
        except (ProcessLookupError, PermissionError): pass
    deadline = time.monotonic() + delay
    while time.monotonic() < deadline:
        if not matches():
            raise SystemExit(0)
        time.sleep(0.1)
raise SystemExit(4 if matches() else 0)
'''.strip()
    try:
        command = ssh_command(
            inventory.core_node,
            "python3",
            "-c",
            script,
            json.dumps(list(patterns)),
            "1" if orphan_only else "0",
        )
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    result = _run(command, timeout_seconds=15)
    if result.returncode != 0:
        raise ExperimentError(f"{label} failed")
