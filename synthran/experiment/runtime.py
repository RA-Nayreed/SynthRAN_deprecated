"""Operator-triggered integrated IoT-to-5G experiment runtime.

The runner consumes an already path-proven network deployment. It creates only
run-scoped experiment resources on the controller and selected root experiment host,
temporarily adds an MQTT sidecar to the run-owned srsUE Deployment, collects
deterministic telemetry, restores the srsUE Deployment, and reproves the accepted
network after cleanup.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import io
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import time
from typing import Any, Mapping, Sequence, TextIO

from synthran.dependencies import DependencyLock
from synthran.experiment import (
    ExperimentCheck,
    ExperimentError,
    ExperimentScenario,
    build_data_evidence,
    build_scenario,
    load_jsonl,
    render_edge_mosquitto_config,
    save_experiment_evidence,
    validate_run_id,
    write_parquet,
)
from synthran.experiment_resources import (
    CENTRAL_PORT,
    EDGE_CONTAINER,
    RUN_LABEL,
    json_document,
    names,
    render_edge_cleanup_patch,
    render_edge_patch,
    render_experiment_objects,
)
from synthran.fiveg_ansible import NetworkInventory
from synthran.ingress import IngressSnapshot
from synthran.iot import write_run_inputs
from synthran.live_preflight import CommandResult, LivePreflightError, ssh_command
from synthran.mqtt_collector import collect_mqtt
from synthran.network_runtime import (
    NetworkVerificationReport,
    sanitize_deployment_text,
    verify_network_path,
)
from synthran.rfsim_runtime import reconcile_rfsim_runtime


DEFAULT_RUN_ROOT = Path(".synthran/experiments")
DEFAULT_COLLECTION_SECONDS = 180
DEFAULT_MINIMUM_PER_SENSOR = 3
REMOTE_EDGE_FORWARD_PORT = 18883
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
                except (subprocess.TimeoutExpired, Exception):
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
                except (subprocess.TimeoutExpired, Exception):
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


def _run_bytes(
    command: Sequence[str],
    *,
    input_bytes: bytes,
    timeout_seconds: int = 60,
    cwd: Path | None = None,
) -> CommandResult:
    try:
        completed = subprocess.run(
            list(command),
            cwd=cwd,
            input=input_bytes,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
            check=False,
        )
    except FileNotFoundError as exc:
        raise ExperimentError(f"required command was not found: {command[0]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise ExperimentError("experiment command timed out") from exc
    return CommandResult(
        completed.returncode,
        completed.stdout.decode("utf-8", errors="replace"),
        completed.stderr.decode("utf-8", errors="replace"),
    )


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
        raise ExperimentError(f"{label} failed")
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


def _remote_path_exists(
    inventory: NetworkInventory,
    path: str,
    *,
    timeout_seconds: int = 5,
) -> bool:
    try:
        command = ssh_command(inventory.core_node, "test", "-e", path)
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    result = _run(command, timeout_seconds=timeout_seconds)
    if result.returncode == 0:
        return True
    if result.returncode == 1:
        return False
    raise ExperimentError(
        f"remote existence probe for {path} failed with exit code {result.returncode}"
    )


def _remote_process_reap(
    inventory: NetworkInventory,
    *,
    patterns: Sequence[str],
    orphan_only: bool,
    remove_proven_workspaces: bool = False,
    remove_tun_if_tunslip: bool = False,
    label: str,
) -> Mapping[str, Any]:
    """Reap only processes matching explicit SynthRAN runtime signatures.

    For stale recovery, matching processes must already be orphaned (PPID 1),
    or be a child of a matching wrapper whose PPID is 1.  Exact-run cleanup
    can set ``orphan_only`` false because every pattern is run-scoped.
    """
    payload = json.dumps(
        {
            "patterns": list(patterns),
            "orphan_only": orphan_only,
            "remove_proven_workspaces": remove_proven_workspaces,
            "remove_tun_if_tunslip": remove_tun_if_tunslip,
        },
        sort_keys=True,
    )
    reaper = r'''
import json, os, re, shutil, signal, subprocess, sys, time

cfg = json.loads(sys.argv[1])
patterns = [re.compile(value) for value in cfg["patterns"]]
self_pid = os.getpid()

def read_process(pid):
    if pid == self_pid:
        return None
    try:
        raw = open(f"/proc/{pid}/cmdline", "rb").read()
        if not raw:
            return None
        cmd = raw.replace(b"\0", b" ").decode("utf-8", "replace").strip()
        ppid = None
        with open(f"/proc/{pid}/status", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                if line.startswith("PPid:"):
                    ppid = int(line.split()[1])
                    break
        if ppid is None:
            return None
        return {"pid": pid, "ppid": ppid, "cmd": cmd}
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError, ValueError):
        return None

records = {}
for entry in os.listdir("/proc"):
    if not entry.isdigit():
        continue
    record = read_process(int(entry))
    if record is not None and any(pattern.search(record["cmd"]) for pattern in patterns):
        records[record["pid"]] = record

def orphaned(record):
    if record["ppid"] == 1:
        return True
    parent = records.get(record["ppid"])
    return parent is not None and parent["ppid"] == 1

blocked = sorted(
    record["pid"]
    for record in records.values()
    if cfg["orphan_only"] and not orphaned(record)
)
if blocked:
    print(json.dumps({"killed": [], "blocked": blocked, "remaining": [], "workspaces": []}))
    raise SystemExit(0)

targets = sorted(records)
target_commands = [records[pid]["cmd"] for pid in targets]
for pid in targets:
    try:
        os.kill(pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass

def alive(pid):
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8", errors="replace") as handle:
            fields = handle.read().split()
        return len(fields) > 2 and fields[2] != "Z"
    except (FileNotFoundError, ProcessLookupError, PermissionError, OSError):
        return False

deadline = time.monotonic() + 5.0
while time.monotonic() < deadline and any(alive(pid) for pid in targets):
    time.sleep(0.1)
for pid in targets:
    if alive(pid):
        try:
            os.kill(pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
deadline = time.monotonic() + 2.0
while time.monotonic() < deadline and any(alive(pid) for pid in targets):
    time.sleep(0.1)

remaining = sorted(pid for pid in targets if alive(pid))
workspaces = sorted(
    set(
        match.group(0)
        for cmd in target_commands
        for match in re.finditer(r"/tmp/synthran/[A-Za-z0-9._:-]+", cmd)
    )
)

if not remaining and cfg["remove_tun_if_tunslip"] and any("/serial-io/tunslip6 " in cmd for cmd in target_commands):
    if os.path.exists("/sys/class/net/tun0"):
        subprocess.run(["ip", "link", "delete", "dev", "tun0"], check=False)

if not remaining and cfg["remove_proven_workspaces"]:
    for workspace in workspaces:
        shutil.rmtree(workspace, ignore_errors=True)

print(json.dumps({"killed": targets, "blocked": [], "remaining": remaining, "workspaces": workspaces}))
'''
    output = _remote(
        inventory,
        "python3",
        "-c",
        reaper,
        payload,
        label=label,
        timeout_seconds=20,
    )
    try:
        result = json.loads(output)
    except json.JSONDecodeError as exc:
        raise ExperimentError(f"{label} did not return JSON") from exc
    if not isinstance(result, dict):
        raise ExperimentError(f"{label} returned malformed state")
    blocked = result.get("blocked")
    remaining = result.get("remaining")
    if blocked:
        raise ExperimentError(
            "[FAIL] experiment-host: an active SynthRAN runtime already owns reserved resources; "
            "refusing to terminate a non-orphaned process"
        )
    if remaining:
        raise ExperimentError(f"{label} left matching remote processes alive: {remaining}")
    return result


def _reclaim_stale_experiment_runtime(inventory: NetworkInventory) -> int:
    """Automatically reclaim only orphaned processes with SynthRAN signatures."""
    patterns = (
        r"kubectl port-forward -n open5gs pod/srsran-ue-[A-Za-z0-9.-]+ 18883:1883 --address 127\.0\.0\.1(?: |$)",
        r"kubectl port-forward -n open5gs deployment/synthran-exp-central-[A-Za-z0-9.-]+ 18885:18884 --address 127\.0\.0\.1(?: |$)",
        r"python3 /tmp/synthran/[A-Za-z0-9._:-]+/ingress\.py --listen-host fd00::1 --listen-port 1883 --target-host 127\.0\.0\.1 --target-port 18883(?: |$)",
        r"/tmp/synthran/[A-Za-z0-9._:-]+/serial-io/tunslip6 -a 127\.0\.0\.1 -p 60001 -t tun0 fd00::1/64(?: |$)",
    )
    result = _remote_process_reap(
        inventory,
        patterns=patterns,
        orphan_only=True,
        remove_proven_workspaces=True,
        remove_tun_if_tunslip=True,
        label="stale SynthRAN runtime recovery",
    )
    killed = result.get("killed", [])
    return len(killed) if isinstance(killed, list) else 0


def _cleanup_remote_run_processes(
    inventory: NetworkInventory,
    *,
    remote_workspace: str,
    ue_pod: str | None,
    central_deployment: str | None,
) -> None:
    """Terminate every remote process created by the exact experiment run."""
    patterns = [
        re.escape(f"python3 {remote_workspace}/ingress.py "),
        re.escape(f"{remote_workspace}/serial-io/tunslip6 "),
    ]
    if ue_pod is not None:
        patterns.append(
            re.escape(
                "kubectl port-forward -n open5gs "
                f"pod/{ue_pod} {REMOTE_EDGE_FORWARD_PORT}:1883 --address 127.0.0.1"
            )
        )
    if central_deployment is not None:
        patterns.append(
            re.escape(
                "kubectl port-forward -n open5gs "
                f"deployment/{central_deployment} "
                f"{LOCAL_CENTRAL_FORWARD_PORT}:{CENTRAL_PORT} --address 127.0.0.1"
            )
        )
    _remote_process_reap(
        inventory,
        patterns=tuple(patterns),
        orphan_only=False,
        label="exact-run remote process cleanup",
    )


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


def _one_name(payload: Mapping[str, Any], *, label: str) -> str:
    items = payload.get("items")
    if not isinstance(items, list):
        raise ExperimentError(f"{label} discovery returned malformed data")
    if not all(isinstance(item, dict) for item in items):
        raise ExperimentError(f"{label} metadata is malformed")
    active_items = [
        item
        for item in items
        if not (
            isinstance(item.get("metadata"), dict)
            and item["metadata"].get("deletionTimestamp") is not None
        )
    ]
    if not active_items:
        raise ExperimentError(f"no {label} was found")
    if len(active_items) > 1:
        raise ExperimentError(
            f"multiple {label} resources were found; refusing to choose one"
        )
    item = active_items[0]
    metadata = item.get("metadata")
    if not isinstance(metadata, dict) or not isinstance(
        metadata.get("name"), str
    ):
        raise ExperimentError(f"{label} metadata is malformed")
    return str(metadata["name"])


def _core_address(inventory: NetworkInventory) -> str:
    value = inventory.core_node.variables.get("ip")
    if not value:
        raise ExperimentError("prepared inventory is missing the core node IP address")
    try:
        ipaddress.ip_address(value)
    except ValueError as exc:
        raise ExperimentError(
            "prepared inventory has an invalid core node IP address; expected a literal IPv4 or IPv6 address"
        ) from exc
    return str(value)


def _probe_experiment_host(
    inventory: NetworkInventory,
    *,
    required_ports: Sequence[int] = (60001, REMOTE_EDGE_FORWARD_PORT, LOCAL_CENTRAL_FORWARD_PORT),
    timeout_seconds: int = 30,
) -> None:
    """Perform early capability check on the privileged experiment host before any live mutation."""
    host = inventory.core_node
    host_name = host.name

    probe_code = (
        "import os, shutil, socket, stat, sys, json\n"
        "rep = {'uid': os.geteuid(), 'tun_exists': False, 'tun_dev': False, 'missing_tools': [], 'busy_ports': []}\n"
        "if os.path.exists('/dev/net/tun'):\n"
        "    try:\n"
        "        st = os.stat('/dev/net/tun')\n"
        "        rep['tun_dev'] = stat.S_ISCHR(st.st_mode)\n"
        "    except Exception:\n"
        "        pass\n"
        "for tool in ['python3', 'ip', 'gcc', 'make', 'tar', 'ifconfig']:\n"
        "    if shutil.which(tool) is None:\n"
        "        rep['missing_tools'].append(tool)\n"
        "if os.path.exists('/sys/class/net/tun0'):\n"
        "    rep['tun_exists'] = True\n"
        f"for p in {list(required_ports)}:\n"
        "    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n"
        "    try:\n"
        "        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n"
        "        s.bind(('127.0.0.1', p))\n"
        "    except OSError:\n"
        "        rep['busy_ports'].append(p)\n"
        "    finally:\n"
        "        s.close()\n"
        "print(json.dumps(rep))\n"
    )

    try:
        cmd = ssh_command(host, "python3", "-c", probe_code)
    except LivePreflightError as exc:
        raise ExperimentError(f"[FAIL] experiment-host: SSH connection error: {exc}") from exc

    result = _run(cmd, timeout_seconds=timeout_seconds)
    if result.returncode != 0:
        if "python3" in (result.stderr or "").lower() or "not found" in (result.stderr or "").lower():
            raise ExperimentError(f"[FAIL] experiment-host: python3 is missing on {host_name}")
        raise ExperimentError(
            f"[FAIL] experiment-host: capability probe failed on {host_name}: {result.stderr or result.stdout}"
        )

    try:
        data = json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError) as exc:
        raise ExperimentError(
            f"[FAIL] experiment-host: capability probe did not return valid JSON on {host_name}"
        ) from exc

    if data.get("uid") != 0:
        raise ExperimentError(
            f"[FAIL] experiment-host: remote host user is not root (uid={data.get('uid')}) on {host_name}"
        )

    if not data.get("tun_dev"):
        raise ExperimentError(
            f"[FAIL] experiment-host: /dev/net/tun is unavailable on {host_name}"
        )

    missing = data.get("missing_tools")
    if missing:
        raise ExperimentError(
            f"[FAIL] experiment-host: required tools {missing} are missing on {host_name}"
        )

    if data.get("tun_exists"):
        raise ExperimentError(
            f"[FAIL] experiment-host: tun0 already exists on {host_name}; refusing to adopt or delete it"
        )

    busy = data.get("busy_ports")
    if busy:
        raise ExperimentError(
            f"[FAIL] experiment-host: required ports {busy} are already in use on {host_name}"
        )


def _probe_ssh_forwarding(
    inventory: NetworkInventory,
    *,
    timeout_seconds: int = 15,
) -> None:
    """Verify that the experiment host allows both local and remote SSH forwarding."""
    host = inventory.core_node
    host_name = host.name
    try:
        cmd = ssh_command(host, "sshd", "-T")
    except LivePreflightError as exc:
        raise ExperimentError(
            f"[FAIL] experiment-host: SSH forwarding probe failed on {host_name}: {exc}"
        ) from exc
    result = _run(cmd, timeout_seconds=timeout_seconds)
    if result.returncode != 0:
        raise ExperimentError(
            f"[FAIL] experiment-host: SSH forwarding required by the experiment "
            f"is disabled on {host_name}"
        )
    forwarding_value: str | None = None
    for line in result.stdout.splitlines():
        parts = line.strip().lower().split(None, 1)
        if len(parts) == 2 and parts[0] == "allowtcpforwarding":
            forwarding_value = parts[1]
            break
    if forwarding_value not in ("yes", "all"):
        raise ExperimentError(
            f"[FAIL] experiment-host: SSH forwarding required by the experiment "
            f"is disabled on {host_name}"
        )


def _wait_remote_tcp(
    inventory: NetworkInventory,
    *,
    host: str,
    port: int,
    timeout_seconds: int = 30,
    process: ManagedProcess | None = None,
) -> None:
    """Wait until a remote TCP port is connectable via an SSH-based Python probe."""
    probe_code = (
        "import socket, sys; "
        "s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); "
        "s.settimeout(2); "
        f"s.connect(('{host}', {port})); "
        "s.close(); "
        "print('ok')"
    )
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if process is not None and process.process.poll() is not None:
            exit_code = process.process.poll()
            raise ExperimentError(
                f"{process.name} exited with code {exit_code} before "
                f"remote TCP endpoint {host}:{port} became ready"
            )
        try:
            cmd = ssh_command(inventory.core_node, "python3", "-c", probe_code)
        except LivePreflightError as exc:
            raise ExperimentError(str(exc)) from exc
        result = _run(cmd, timeout_seconds=5)
        if result.returncode == 0 and "ok" in result.stdout:
            return
        time.sleep(0.5)
    if process is not None and process.process.poll() is not None:
        exit_code = process.process.poll()
        raise ExperimentError(
            f"{process.name} exited with code {exit_code} before "
            f"remote TCP endpoint {host}:{port} became ready"
        )
    raise ExperimentError(
        f"remote TCP endpoint {host}:{port} did not become ready"
    )


def _transfer_directory(
    inventory: NetworkInventory,
    source_dir: Path,
    remote_dir: str,
    *,
    label: str,
) -> None:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for file_path in sorted(source_dir.rglob("*")):
            if file_path.is_file():
                rel_path = file_path.relative_to(source_dir).as_posix()
                tar.add(file_path, arcname=rel_path)
    tar_bytes = buffer.getvalue()

    try:
        cmd = ssh_command(
            inventory.core_node,
            "sh",
            "-c",
            f"mkdir -p {shlex.quote(remote_dir)} && tar -xzf - -C {shlex.quote(remote_dir)}",
        )
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc

    result = _run_bytes(cmd, input_bytes=tar_bytes, timeout_seconds=60)
    if result.returncode != 0:
        raise ExperimentError(f"{label} failed: {result.stderr or result.stdout}")


def _transfer_file(
    inventory: NetworkInventory,
    source_file: Path,
    remote_path: str,
    *,
    label: str,
) -> None:
    content = source_file.read_text(encoding="utf-8")
    try:
        cmd = ssh_command(
            inventory.core_node,
            "sh",
            "-c",
            f"cat > {shlex.quote(remote_path)}",
        )
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc

    result = _run(cmd, input_text=content, timeout_seconds=30)
    if result.returncode != 0:
        raise ExperimentError(f"{label} failed: {result.stderr or result.stdout}")


def _validate_contiki_checkout(lock: DependencyLock, dependency_root: Path) -> Path:
    dependency = next((item for item in lock.git if item.name == "contiki_ng"), None)
    if dependency is None:
        raise ExperimentError("dependency lock does not define Contiki-NG")
    checkout = dependency_root.resolve() / Path(str(dependency.checkout))
    if not checkout.is_dir():
        raise ExperimentError("pinned Contiki-NG checkout is missing; run synthran deps sync")
    head = _checked(
        ("git", "-C", str(checkout), "rev-parse", "HEAD"),
        label="Contiki-NG commit check",
    ).strip()
    if head != dependency.commit:
        raise ExperimentError("Contiki-NG checkout is not at the locked commit")
    status = _checked(
        (
            "git",
            "-C",
            str(checkout),
            "status",
            "--porcelain",
            "--untracked-files=no",
        ),
        label="Contiki-NG cleanliness check",
    )
    if status.strip():
        raise ExperimentError("Contiki-NG checkout has tracked modifications")
    return checkout


def _prepare_cooja_checkout(contiki: Path) -> Path:
    cooja_directory = contiki / "tools" / "cooja"
    _checked(
        (
            "git",
            "-C",
            str(contiki),
            "submodule",
            "update",
            "--init",
            "--checkout",
            "--",
            "tools/cooja",
        ),
        label="Cooja submodule preparation",
        timeout_seconds=600,
    )
    expected = _checked(
        ("git", "-C", str(contiki), "rev-parse", "HEAD:tools/cooja"),
        label="Cooja expected revision check",
    ).strip()
    actual = _checked(
        ("git", "-C", str(cooja_directory), "rev-parse", "HEAD"),
        label="Cooja actual revision check",
    ).strip()
    if not expected or not actual or expected != actual:
        raise ExperimentError("Cooja checkout does not match the revision pinned by Contiki-NG")
    return cooja_directory


def _copy_sensor_source(repository_root: Path, run_directory: Path) -> None:
    source = repository_root.resolve() / "deploy" / "iot" / "sensor"
    destination = run_directory / "sensor"
    destination.mkdir(parents=True, exist_ok=True)
    for name in ("Makefile", "synthran-sensor.c", "project-conf.h"):
        candidate = source / name
        if not candidate.is_file():
            raise ExperimentError(f"sensor source is missing: {name}")
        shutil.copy2(candidate, destination / name)


def _validate_java_runtime() -> Path:
    """Ensure Java 21 is available in the synthran environment and return JAVA_HOME."""
    java_executable = shutil.which("java")
    if java_executable is None:
        raise ExperimentError("Cooja requires Java 21 in the synthran environment")

    java_path = Path(java_executable).resolve()
    try:
        completed = subprocess.run(
            [str(java_path), "-version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10,
            check=False,
        )
    except Exception as exc:
        raise ExperimentError(
            "Cooja requires Java 21 in the synthran environment"
        ) from exc

    if completed.returncode != 0:
        raise ExperimentError("Cooja requires Java 21 in the synthran environment")

    output = f"{completed.stdout}\n{completed.stderr}"
    match = re.search(r'version "(\d+)(?:\.|\+|-|_|")', output)
    if not match or match.group(1) != "21":
        raise ExperimentError("Cooja requires Java 21 in the synthran environment")

    java_home = java_path.parent.parent
    if (java_home / "lib" / "jvm").is_dir():
        java_home = java_home / "lib" / "jvm"
    return java_home


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
            exit_code = process.process.poll()
            raise ExperimentError(
                f"{process.name} exited with code {exit_code} before TCP endpoint {host}:{port} became ready; "
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
    if process is not None and process.process.poll() is not None:
        try:
            process.log_stream.flush()
        except Exception:
            pass
        exit_code = process.process.poll()
        raise ExperimentError(
            f"{process.name} exited with code {exit_code} before TCP endpoint {host}:{port} became ready; "
            f"see {process.log_path}"
        )
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
    base.extend(
        (
            "-o",
            "ExitOnForwardFailure=yes",
            "-L",
            f"127.0.0.1:{local_port}:127.0.0.1:{remote_port}",
            target,
            remote_command,
        )
    )
    return tuple(base)


def _ssh_reverse_tunnel_command(
    inventory: NetworkInventory,
    *,
    remote_port: int,
    local_port: int,
) -> tuple[str, ...]:
    try:
        base = list(ssh_command(inventory.core_node))
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    if not base:
        raise ExperimentError("unable to construct strict SSH tunnel")
    target = base.pop()
    base.extend(
        (
            "-N",
            "-o",
            "ExitOnForwardFailure=yes",
            "-R",
            f"127.0.0.1:{remote_port}:127.0.0.1:{local_port}",
            target,
        )
    )
    return tuple(base)


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


def _kubectl_patch_deployment(
    inventory: NetworkInventory,
    deployment: str,
    patch: Mapping[str, Any],
    *,
    label: str,
) -> None:
    patch_text = shlex.quote(json_document(patch))
    _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl patch deployment "
        f"{shlex.quote(deployment)} -n {KUBERNETES_NAMESPACE} "
        f"--type=strategic -p {patch_text}",
        label=label,
    )


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


def _collect_rollout_diagnostics(
    inventory: NetworkInventory,
    *,
    network_run_id: str,
    log_path: Path,
    private_paths: Sequence[Path],
    verification: NetworkVerificationReport | None = None,
) -> None:
    """Collect sanitized pod status, events, sidecar logs, and verification checks on failure."""

    log_parts: list[str] = [
        f"=== SynthRAN Rollout Diagnostics ({datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}) ===",
        f"Network Run ID: {network_run_id}",
        "",
    ]
    if verification is not None:
        log_parts.append("=== Network Verification Checks ===")
        log_parts.append(verification.render())
        log_parts.append("")

    def _safe_remote(command_str: str) -> str:
        try:
            cmd = ssh_command(inventory.core_node, "sh", "-c", command_str)
            result = _run(cmd, timeout_seconds=30)
            output = result.stdout
            if result.stderr:
                output = (
                    f"{output}\n[stderr]\n{result.stderr}"
                    if output
                    else f"[stderr]\n{result.stderr}"
                )
            return output.strip()
        except Exception as exc:
            return f"<diagnostic command failed: {exc}>"

    log_parts.append("=== kubectl get pods (srsran-ue) ===")
    log_parts.append(
        _safe_remote(
            f"KUBECONFIG=/etc/kubernetes/admin.conf kubectl get pods -n {KUBERNETES_NAMESPACE} "
            f"-l app=srsran,component=ue,synthran.run/id={shlex.quote(network_run_id)} -o wide"
        )
    )
    log_parts.append("")

    log_parts.append("=== kubectl get pods (namespace) ===")
    log_parts.append(
        _safe_remote(
            f"KUBECONFIG=/etc/kubernetes/admin.conf kubectl get pods -n {KUBERNETES_NAMESPACE} -o wide"
        )
    )
    log_parts.append("")

    log_parts.append("=== kubectl get events ===")
    log_parts.append(
        _safe_remote(
            f"KUBECONFIG=/etc/kubernetes/admin.conf kubectl get events -n {KUBERNETES_NAMESPACE} "
            "--sort-by=.metadata.creationTimestamp"
        )
    )
    log_parts.append("")

    pod_names_raw = _safe_remote(
        f"KUBECONFIG=/etc/kubernetes/admin.conf kubectl get pods -n {KUBERNETES_NAMESPACE} "
        f"-l app=srsran,component=ue,synthran.run/id={shlex.quote(network_run_id)} "
        "-o jsonpath='{.items[*].metadata.name}'"
    )
    pod_names: list[str] = []
    if not pod_names_raw.startswith("<"):
        pod_names = [
            name.strip("'\"")
            for name in pod_names_raw.split()
            if name and not name.startswith("<")
        ]
    if not pod_names:
        fallback_names = _safe_remote(
            f"KUBECONFIG=/etc/kubernetes/admin.conf kubectl get pods -n {KUBERNETES_NAMESPACE} "
            "-o jsonpath='{.items[*].metadata.name}'"
        )
        if not fallback_names.startswith("<"):
            pod_names = [
                name.strip("'\"")
                for name in fallback_names.split()
                if name and not name.startswith("<")
            ]

    for pod_name in pod_names:
        log_parts.append(f"=== kubectl describe pod {pod_name} ===")
        log_parts.append(
            _safe_remote(
                f"KUBECONFIG=/etc/kubernetes/admin.conf kubectl describe pod "
                f"{shlex.quote(pod_name)} -n {KUBERNETES_NAMESPACE}"
            )
        )
        log_parts.append("")
        log_parts.append(f"=== kubectl logs {pod_name} -c {EDGE_CONTAINER} --tail=100 ===")
        log_parts.append(
            _safe_remote(
                f"KUBECONFIG=/etc/kubernetes/admin.conf kubectl logs "
                f"{shlex.quote(pod_name)} -n {KUBERNETES_NAMESPACE} "
                f"-c {EDGE_CONTAINER} --tail=100"
            )
        )
        log_parts.append("")

    raw_text = "\n".join(log_parts)
    sanitized = sanitize_deployment_text(raw_text, private_paths)
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(sanitized, encoding="utf-8", newline="\n")
    except OSError:
        pass


def _discover_ue_deployment(
    inventory: NetworkInventory,
    network_run_id: str,
) -> str:
    payload = _remote_json(
        inventory,
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl get deployments "
        f"-n {KUBERNETES_NAMESPACE} "
        f"-l app.kubernetes.io/name=srsran-ue,"
        f"synthran.run/id={shlex.quote(network_run_id)} "
        "-o json",
        label="srsUE Deployment discovery",
    )
    return _one_name(payload, label="run-owned srsUE Deployment")


def _discover_ue_pod(inventory: NetworkInventory, network_run_id: str) -> str:
    payload = _remote_json(
        inventory,
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl get pods "
        f"-n {KUBERNETES_NAMESPACE} "
        f"-l app=srsran,component=ue,synthran.run/id={shlex.quote(network_run_id)} "
        "-o json",
        label="srsUE pod discovery",
    )
    return _one_name(payload, label="run-owned srsUE pod")


def _interface_counter(
    inventory: NetworkInventory,
    pod: str,
    interface: str,
    counter: str,
) -> int:
    if counter not in {"rx_bytes", "tx_bytes"}:
        raise ExperimentError("unsupported interface counter")
    output = _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
        f"-n {KUBERNETES_NAMESPACE} {shlex.quote(pod)} -c ue -- "
        f"cat /sys/class/net/{shlex.quote(interface)}/statistics/{counter}",
        label=f"{interface} {counter} probe",
    ).strip()
    if not output.isdigit():
        raise ExperimentError(f"{interface} {counter} probe returned invalid data")
    return int(output)


def _add_ue_route(inventory: NetworkInventory, pod: str, core_address: str) -> None:
    destination = f"{core_address}/32"
    _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
        f"-n {KUBERNETES_NAMESPACE} {shlex.quote(pod)} -c ue -- "
        f"ip route replace {shlex.quote(destination)} dev tun_srsue1",
        label="UE experiment route installation",
    )
    route = _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
        f"-n {KUBERNETES_NAMESPACE} {shlex.quote(pod)} -c ue -- "
        f"ip -j route get {shlex.quote(core_address)}",
        label="UE experiment route proof",
    )
    try:
        payload = json.loads(route)
    except json.JSONDecodeError as exc:
        raise ExperimentError("UE experiment route proof did not return JSON") from exc
    if not isinstance(payload, list) or not any(
        isinstance(item, dict) and item.get("dev") == "tun_srsue1" for item in payload
    ):
        raise ExperimentError("central MQTT destination is not routed through tun_srsue1")


def _replace_edge_runtime_config(
    inventory: NetworkInventory,
    pod: str,
    config: str,
) -> None:
    try:
        command = ssh_command(
            inventory.core_node,
            "sh",
            "-c",
            "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec -i "
            f"-n {KUBERNETES_NAMESPACE} {shlex.quote(pod)} -c {EDGE_CONTAINER} -- "
            "/bin/sh -c 'cat > /synthran/mosquitto.conf'",
        )
    except LivePreflightError as exc:
        raise ExperimentError(str(exc)) from exc
    result = _run(command, input_text=config, timeout_seconds=30)
    if result.returncode != 0:
        raise ExperimentError("edge MQTT runtime config refresh failed")


def _restart_edge_sidecar(inventory: NetworkInventory, pod: str) -> None:
    _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
        f"-n {KUBERNETES_NAMESPACE} {shlex.quote(pod)} -c {EDGE_CONTAINER} -- "
        "sh -c 'kill -TERM 1' || true",
        label="edge MQTT sidecar restart",
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


def _render_manifest(
    scenario: ExperimentScenario,
    *,
    status: str,
    scenario_path: Path,
    failure: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": "synthran/experiment-run/v1alpha1",
        "run_id": scenario.run_id,
        "network_run_id": scenario.network_run_id,
        "status": status,
        "scenario": scenario_path.name,
        "updated_at_utc": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "reservation_action": "none",
        "network_deployment_action": "none",
    }
    if failure:
        payload["failure"] = failure
    return payload


def _save_manifest(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _cleanup_live_resources(
    *,
    inventory: NetworkInventory,
    lock: DependencyLock,
    scenario: ExperimentScenario,
    ue_deployment: str | None,
    cleanup_errors: Sequence[str] = (),
    remote_cleanup_errors: Sequence[str] = (),
) -> ExperimentCheck:
    errors: list[str] = [*cleanup_errors, *remote_cleanup_errors]
    cleanup_rollout_completed = False
    if ue_deployment is not None:
        try:
            _kubectl_patch_deployment(
                inventory,
                ue_deployment,
                render_edge_cleanup_patch(),
                label="srsUE sidecar cleanup",
            )
            _wait_rollout(inventory, ue_deployment, label="srsUE cleanup rollout")
            cleanup_rollout_completed = True
        except Exception as exc:
            errors.append(f"sidecar restore: {exc}")
    if cleanup_rollout_completed:
        try:
            reconcile_rfsim_runtime(
                inventory,
                network_run_id=scenario.network_run_id,
            )
        except Exception as exc:
            errors.append(f"RFSIM runtime restore: {exc}")
    try:
        _delete_experiment_objects(inventory, scenario.run_id)
    except Exception as exc:
        errors.append(f"run-scoped object cleanup: {exc}")
    try:
        restored = verify_network_path(
            inventory=inventory,
            lock=lock,
            run_id=scenario.network_run_id,
            timeout_seconds=120,
        )
        if not restored.ready:
            errors.append("accepted network path did not reprove after cleanup")
    except Exception as exc:
        errors.append(f"accepted network reproof: {exc}")
    if errors:
        return ExperimentCheck(
            "cleanup-base-network",
            False,
            "cleanup failed closed: " + "; ".join(errors),
        )
    return ExperimentCheck(
        "cleanup-base-network",
        True,
        "experiment resources removed and accepted network path reproven",
    )


def execute_experiment(
    *,
    inventory: NetworkInventory,
    lock: DependencyLock,
    dependency_root: Path,
    network_manifest: Path,
    network_evidence: Path,
    run_id: str,
    repository_root: Path,
    run_root: Path = DEFAULT_RUN_ROOT,
    collection_seconds: int = DEFAULT_COLLECTION_SECONDS,
    minimum_per_sensor: int = DEFAULT_MINIMUM_PER_SENSOR,
    progress: TextIO | None = None,
) -> ExperimentRunResult:
    """Run the complete ten-sensor path against a proven network baseline."""

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] {message}", file=progress, flush=True)

    if sys.platform != "linux":
        raise ExperimentError("live experiment execution requires Linux")
    if os.environ.get("CONDA_DEFAULT_ENV") != "synthran":
        raise ExperimentError(
            "live experiment execution requires the active synthran Conda environment"
        )
    if collection_seconds < 30 or collection_seconds > 3600:
        raise ExperimentError("collection duration must be between 30 and 3600 seconds")
    if minimum_per_sensor < 1 or minimum_per_sensor > 100:
        raise ExperimentError("minimum events per sensor must be between 1 and 100")

    scenario = build_scenario(
        run_id=validate_run_id(run_id),
        network_manifest=network_manifest,
        network_evidence=network_evidence,
    )
    contiki = _validate_contiki_checkout(lock, dependency_root)
    java_home = _validate_java_runtime()
    core_address = _core_address(inventory)
    core_host = inventory.core_node

    report(f"experiment: {scenario.run_id}")
    report("network prerequisite: verifying path-proven baseline...")
    base = verify_network_path(
        inventory=inventory,
        lock=lock,
        run_id=scenario.network_run_id,
        timeout_seconds=120,
    )
    if not base.ready:
        raise ExperimentError("accepted network no longer satisfies path proof")
    report("network prerequisite: OK")

    report(f"experiment host: checking {core_host.name}...")
    reclaimed = _reclaim_stale_experiment_runtime(inventory)
    if reclaimed:
        report(f"experiment host: reclaimed {reclaimed} stale SynthRAN process(es)")
    _probe_experiment_host(
        inventory,
        required_ports=(
            scenario.serial_socket_port,
            REMOTE_EDGE_FORWARD_PORT,
            LOCAL_CENTRAL_FORWARD_PORT,
        ),
    )
    _probe_ssh_forwarding(inventory)
    report("experiment host: OK")

    run_root = run_root.resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    run_directory = run_root / scenario.run_id
    try:
        run_directory.mkdir()
    except FileExistsError as exc:
        raise ExperimentError("experiment run directory already exists; choose a new run ID") from exc

    logs = run_directory / "logs"
    logs.mkdir()
    _, csc, scenario_path = write_run_inputs(
        scenario,
        run_directory=run_directory,
        contiki_directory=contiki,
    )
    _copy_sensor_source(repository_root, run_directory)
    manifest_path = run_directory / "manifest.json"
    evidence_path = run_directory / "experiment-evidence.json"
    jsonl_path = run_directory / "telemetry.jsonl"
    rejected_path = run_directory / "rejected-events.jsonl"
    parquet_path = run_directory / "telemetry.parquet"
    _save_manifest(
        manifest_path,
        _render_manifest(scenario, status="running", scenario_path=scenario_path),
    )

    processes: list[ManagedProcess] = []
    ue_deployment: str | None = None
    ue_pod: str | None = None
    central_deployment: str | None = None
    extra_checks: list[ExperimentCheck] = []
    failure: str | None = None
    remote_workspace = f"/tmp/synthran/{scenario.run_id}"
    remote_workspace_created = False
    tun_state = "absent"  # proven by prerequisite: no pre-existing tun0

    try:
        report("Cooja dependency: preparing pinned checkout...")
        _prepare_cooja_checkout(contiki)
        report("Cooja dependency: OK")

        _remote(
            inventory,
            "mkdir",
            "-p",
            f"{remote_workspace}/serial-io",
            label="remote workspace creation",
        )
        remote_workspace_created = True

        _transfer_directory(
            inventory,
            contiki / "tools" / "serial-io",
            f"{remote_workspace}/serial-io",
            label="serial-io transfer",
        )
        _transfer_file(
            inventory,
            repository_root.resolve() / "synthran" / "ingress.py",
            f"{remote_workspace}/ingress.py",
            label="ingress helper transfer",
        )

        report("remote tunslip6 build: running...")
        _remote(
            inventory,
            "make",
            "-C",
            f"{remote_workspace}/serial-io",
            "tunslip6",
            label="remote tunslip6 build",
            timeout_seconds=180,
        )
        report("remote tunslip6 build: OK")

        report(f"accepted PDU: {scenario.pdu_address}")
        report("preparing UE MQTT sidecar...")

        ue_deployment = _discover_ue_deployment(inventory, scenario.network_run_id)
        resource_names = names(scenario)
        central_deployment = resource_names["central_deployment"]
        for index, value in enumerate(
            render_experiment_objects(
                scenario,
                lock=lock,
                core_node=inventory.core_node.name,
                core_address=core_address,
            ),
            start=1,
        ):
            _kubectl_apply_object(
                inventory,
                value,
                label=f"experiment Kubernetes object {index}",
            )

        _remote(
            inventory,
            "sh",
            "-c",
            "KUBECONFIG=/etc/kubernetes/admin.conf kubectl rollout status deployment/"
            f"{resource_names['central_deployment']} -n {KUBERNETES_NAMESPACE} "
            "--timeout=180s",
            label="central MQTT rollout",
            timeout_seconds=200,
        )

        _kubectl_patch_deployment(
            inventory,
            ue_deployment,
            render_edge_patch(scenario, lock=lock, core_address=core_address),
            label="srsUE MQTT sidecar patch",
        )
        try:
            _wait_rollout(inventory, ue_deployment, label="srsUE MQTT rollout")
        except Exception as exc:
            report("srsUE MQTT rollout: FAILED")
            _collect_rollout_diagnostics(
                inventory,
                network_run_id=scenario.network_run_id,
                log_path=logs / "srsue-mqtt-rollout-diagnostics.log",
                private_paths=(
                    repository_root,
                    dependency_root,
                    run_directory,
                    inventory.path,
                ),
            )
            raise ExperimentError(
                "edge MQTT sidecar did not become Ready; diagnostic log saved"
            ) from exc

        report("reconciling RFSIM...")
        try:
            runtime_state = reconcile_rfsim_runtime(
                inventory,
                network_run_id=scenario.network_run_id,
            )
        except Exception as exc:
            report("rfsim-runtime: FAILED")
            _collect_rollout_diagnostics(
                inventory,
                network_run_id=scenario.network_run_id,
                log_path=logs / "rfsim-runtime-diagnostics.log",
                private_paths=(
                    repository_root,
                    dependency_root,
                    run_directory,
                    inventory.path,
                ),
            )
            raise ExperimentError(
                "RFSIM runtime did not recover after srsUE rollout; diagnostic log saved"
            ) from exc

        ue_pod = runtime_state.ue_pod
        if runtime_state.pdu_address != scenario.pdu_address:
            report(f"runtime PDU: {runtime_state.pdu_address}")
        else:
            report(f"runtime PDU: {runtime_state.pdu_address} (unchanged)")

        scenario = replace(scenario, pdu_address=runtime_state.pdu_address)
        _, csc, scenario_path = write_run_inputs(
            scenario,
            run_directory=run_directory,
            contiki_directory=contiki,
        )
        edge_config = render_edge_mosquitto_config(
            scenario,
            central_broker_address=core_address,
            central_broker_port=CENTRAL_PORT,
        )
        _replace_edge_runtime_config(inventory, ue_pod, edge_config)
        _restart_edge_sidecar(inventory, ue_pod)
        time.sleep(3)

        after_patch = verify_network_path(
            inventory=inventory,
            lock=lock,
            run_id=scenario.network_run_id,
            timeout_seconds=120,
        )
        if not after_patch.ready:
            report("srsUE post-patch network verification: FAILED")
            for check in after_patch.checks:
                if check.passed:
                    report(f"[PASS] {check.name}")
                else:
                    report(f"[FAIL] {check.name}: {check.detail}")
            _collect_rollout_diagnostics(
                inventory,
                network_run_id=scenario.network_run_id,
                log_path=logs / "srsue-mqtt-rollout-diagnostics.log",
                private_paths=(
                    repository_root,
                    dependency_root,
                    run_directory,
                    inventory.path,
                ),
                verification=after_patch,
            )
            failing = [f"{c.name}: {c.detail}" for c in after_patch.checks if not c.passed]
            failing_summary = "; ".join(failing) if failing else "verification checks failed"
            raise ExperimentError(
                f"srsUE sidecar patch broke the accepted network path ({failing_summary})"
            )

        _add_ue_route(inventory, ue_pod, core_address)
        tx_before = _interface_counter(inventory, ue_pod, "tun_srsue1", "tx_bytes")
        rx_before = _interface_counter(inventory, ue_pod, "tun_srsue1", "rx_bytes")

        # Start remote edge port-forward on core node
        edge_forward_cmd = ssh_command(
            inventory.core_node,
            "sh",
            "-c",
            "KUBECONFIG=/etc/kubernetes/admin.conf kubectl port-forward "
            f"-n {KUBERNETES_NAMESPACE} pod/{shlex.quote(ue_pod)} "
            f"{REMOTE_EDGE_FORWARD_PORT}:1883 --address 127.0.0.1",
        )
        edge_forward = _start_process(
            "edge MQTT port-forward",
            edge_forward_cmd,
            cwd=repository_root,
            log_path=logs / "edge-port-forward.log",
        )
        processes.append(edge_forward)
        _wait_remote_tcp(
            inventory,
            host="127.0.0.1",
            port=REMOTE_EDGE_FORWARD_PORT,
            timeout_seconds=30,
            process=edge_forward,
        )

        central_forward = _start_process(
            "central MQTT port-forward",
            _ssh_tunnel_command(
                inventory,
                local_port=LOCAL_CENTRAL_FORWARD_PORT,
                remote_port=LOCAL_CENTRAL_FORWARD_PORT,
                remote_command=(
                    "KUBECONFIG=/etc/kubernetes/admin.conf kubectl port-forward "
                    f"-n {KUBERNETES_NAMESPACE} "
                    f"deployment/{resource_names['central_deployment']} "
                    f"{LOCAL_CENTRAL_FORWARD_PORT}:{CENTRAL_PORT} "
                    "--address 127.0.0.1"
                ),
            ),
            cwd=repository_root,
            log_path=logs / "central-port-forward.log",
        )
        processes.append(central_forward)
        _wait_tcp(
            "127.0.0.1",
            LOCAL_CENTRAL_FORWARD_PORT,
            timeout_seconds=30,
            process=central_forward,
        )

        report("Cooja: starting 10-sensor simulation...")
        cooja_env = os.environ.copy()
        cooja_env["JAVA_HOME"] = str(java_home)
        cooja = _start_process(
            "Cooja",
            (
                str(contiki / "tools" / "cooja" / "gradlew"),
                "--no-daemon",
                "--console=plain",
                "run",
                f"--args=--no-gui {csc}",
            ),
            cwd=contiki / "tools" / "cooja",
            log_path=logs / "cooja.log",
            env=cooja_env,
        )
        processes.append(cooja)
        _wait_tcp(
            "127.0.0.1",
            scenario.serial_socket_port,
            timeout_seconds=180,
            process=cooja,
        )
        extra_checks.append(
            ExperimentCheck(
                "cooja",
                True,
                "deterministic 10-sensor simulation exposed its serial socket",
            )
        )

        # Reverse SSH tunnel to expose controller's SerialSocket on remote experiment host
        reverse_tunnel = _start_process(
            "SerialSocket reverse SSH tunnel",
            _ssh_reverse_tunnel_command(
                inventory,
                remote_port=scenario.serial_socket_port,
                local_port=scenario.serial_socket_port,
            ),
            cwd=repository_root,
            log_path=logs / "serial-reverse-tunnel.log",
        )
        processes.append(reverse_tunnel)
        time.sleep(1)
        if reverse_tunnel.process.poll() is not None:
            raise ExperimentError("SerialSocket reverse SSH tunnel failed to start")

        # Launch remote tunslip6 as root through SSH
        tunslip_cmd = ssh_command(
            inventory.core_node,
            "sh",
            "-c",
            f"exec {shlex.quote(remote_workspace)}/serial-io/tunslip6 "
            "-a 127.0.0.1 "
            f"-p {scenario.serial_socket_port} "
            "-t tun0 "
            "fd00::1/64",
        )
        tun_state = "creation-attempted"
        tunslip = _start_process(
            "tunslip6",
            tunslip_cmd,
            cwd=repository_root,
            log_path=logs / "tunslip6.log",
        )
        processes.append(tunslip)

        deadline = time.monotonic() + 30
        tun0_ready = False
        while time.monotonic() < deadline:
            if tunslip.process.poll() is not None:
                report(
                    f"[FAIL] serial bridge: remote tunslip6 exited\n"
                    f"       host: {core_host.name}\n"
                    f"       log: {logs / 'tunslip6.log'}"
                )
                raise ExperimentError(
                    f"tunslip6 exited before tun0 became ready; see {logs / 'tunslip6.log'}"
                )
            result = _run(
                ssh_command(inventory.core_node, "ip", "-j", "address", "show", "dev", "tun0"),
                timeout_seconds=5,
            )
            if result.returncode == 0 and "fd00::1" in result.stdout:
                tun0_ready = True
                break
            time.sleep(1)
        if not tun0_ready:
            if tunslip.process.poll() is not None:
                report(
                    f"[FAIL] serial bridge: remote tunslip6 exited\n"
                    f"       host: {core_host.name}\n"
                    f"       log: {logs / 'tunslip6.log'}"
                )
                raise ExperimentError(
                    f"tunslip6 exited before tun0 became ready; see {logs / 'tunslip6.log'}"
                )
            raise ExperimentError("tun0 did not become UP with fd00::1 on remote experiment host")
        tun_state = "ready"

        report(f"serial bridge: ready on {core_host.name}")
        report("RPL border router: tun0 ready")
        extra_checks.append(
            ExperimentCheck(
                "rpl-border-router",
                True,
                "Cooja serial socket is bridged through remote tunslip6/tun0",
            )
        )

        # Launch remote CountedTcpIngress through SSH
        snapshot_remote_path = f"{remote_workspace}/ingress-snapshot.json"
        ingress_remote_cmd = (
            f"exec python3 {shlex.quote(remote_workspace)}/ingress.py "
            "--listen-host fd00::1 "
            "--listen-port 1883 "
            "--target-host 127.0.0.1 "
            f"--target-port {REMOTE_EDGE_FORWARD_PORT} "
            f"--snapshot-path {shlex.quote(snapshot_remote_path)}"
        )
        ingress_proc = _start_process(
            "CountedTcpIngress",
            ssh_command(inventory.core_node, "sh", "-c", ingress_remote_cmd),
            cwd=repository_root,
            log_path=logs / "ingress.log",
        )
        processes.append(ingress_proc)
        time.sleep(1)
        if ingress_proc.process.poll() is not None:
            raise ExperimentError("remote CountedTcpIngress failed to start")

        report("collector: waiting for 10 sensor streams...")
        collection = collect_mqtt(
            scenario,
            host="127.0.0.1",
            port=LOCAL_CENTRAL_FORWARD_PORT,
            jsonl_path=jsonl_path,
            rejected_path=rejected_path,
            minimum_per_sensor=minimum_per_sensor,
            timeout_seconds=collection_seconds,
        )
        if not collection.completed:
            raise ExperimentError(
                "collector timed out after observing "
                f"{collection.sensors}/10 sensors and {collection.records} events"
            )
        report(f"collector: OK ({collection.records} events from 10 sensors)")

        snapshot_data = _remote_json(
            inventory,
            f"cat {shlex.quote(snapshot_remote_path)}",
            label="remote ingress snapshot probe",
        )
        ingress_snapshot = IngressSnapshot.from_dict(snapshot_data)
        if (
            ingress_snapshot.accepted_connections < scenario.sensor_count
            or ingress_snapshot.upstream_bytes <= 0
        ):
            raise ExperimentError("Cooja MQTT ingress was not proven through tun0")
        extra_checks.append(
            ExperimentCheck(
                "edge-mqtt",
                True,
                f"{ingress_snapshot.accepted_connections} sensor MQTT connections crossed the remote tun0 ingress",
            )
        )
        extra_checks.append(
            ExperimentCheck(
                "ue-binding",
                True,
                f"edge bridge is bound to live UE PDU address {scenario.pdu_address}",
            )
        )

        tx_after = _interface_counter(inventory, ue_pod, "tun_srsue1", "tx_bytes")
        rx_after = _interface_counter(inventory, ue_pod, "tun_srsue1", "rx_bytes")
        if tx_after <= tx_before:
            raise ExperimentError("tun_srsue1 TX counter did not increase during MQTT delivery")
        extra_checks.append(
            ExperimentCheck(
                "5g-egress",
                True,
                "tun_srsue1 counters increased "
                f"(tx +{tx_after - tx_before}, rx +{max(0, rx_after - rx_before)})",
            )
        )

        live_network = verify_network_path(
            inventory=inventory,
            lock=lock,
            run_id=scenario.network_run_id,
            timeout_seconds=120,
        )
        if not live_network.ready:
            raise ExperimentError("accepted UPF path was not valid after telemetry delivery")
        extra_checks.append(
            ExperimentCheck(
                "upf-path",
                True,
                "accepted slice-one UPF route remains path-proven",
            )
        )
        extra_checks.append(
            ExperimentCheck(
                "central-mqtt",
                True,
                "central broker delivered all 10 deterministic sensor streams",
            )
        )

        records = load_jsonl(jsonl_path, expected_run_id=scenario.run_id)
        write_parquet(records, parquet_path)
        data_evidence = build_data_evidence(
            scenario=scenario,
            scenario_path=scenario_path,
            jsonl_path=jsonl_path,
            parquet_path=parquet_path,
            minimum_per_sensor=minimum_per_sensor,
            extra_checks=extra_checks,
        )
        if not data_evidence.ready:
            raise ExperimentError("experiment data evidence is incomplete")

    except Exception as exc:
        failure = str(exc)
        report(f"error: {failure}")
    finally:
        cleanup_errors: list[str] = []
        for managed in reversed(processes):
            try:
                managed.stop()
            except Exception as exc:
                cleanup_errors.append(f"process stop ({managed.name}): {exc}")

        # Local SSH/session teardown is not sufficient: remote kubectl and
        # helper processes can survive and be reparented to PID 1.  Reap the
        # exact run signatures before removing tun0 or the remote workspace.
        try:
            _cleanup_remote_run_processes(
                inventory,
                remote_workspace=remote_workspace,
                ue_pod=ue_pod,
                central_deployment=central_deployment,
            )
        except Exception as exc:
            cleanup_errors.append(f"remote process cleanup: {exc}")

        if tun_state in ("creation-attempted", "ready"):
            tun0_exists = False
            try:
                tun0_exists = _remote_path_exists(
                    inventory,
                    "/sys/class/net/tun0",
                    timeout_seconds=5,
                )
            except Exception as exc:
                cleanup_errors.append(f"remote tun0 existence check: {exc}")
            else:
                if tun0_exists:
                    try:
                        _remote(
                            inventory,
                            "ip",
                            "link",
                            "delete",
                            "dev",
                            "tun0",
                            label="remote tun0 cleanup",
                            timeout_seconds=10,
                        )
                    except Exception as exc:
                        cleanup_errors.append(f"remote tun0 cleanup: {exc}")

            # Postcondition: verify tun0 is absent
            try:
                if _remote_path_exists(
                    inventory,
                    "/sys/class/net/tun0",
                    timeout_seconds=5,
                ):
                    cleanup_errors.append(
                        "remote tun0 cleanup postcondition: tun0 still exists"
                    )
            except Exception as exc:
                cleanup_errors.append(
                    f"remote tun0 cleanup postcondition: {exc}"
                )

        if remote_workspace_created:
            try:
                _remote(
                    inventory,
                    "rm",
                    "-rf",
                    remote_workspace,
                    label="remote workspace cleanup",
                    timeout_seconds=10,
                )
            except Exception as exc:
                cleanup_errors.append(
                    f"remote workspace cleanup: {exc}"
                )
            # Postcondition: verify run-scoped workspace is absent
            try:
                if _remote_path_exists(
                    inventory,
                    remote_workspace,
                    timeout_seconds=5,
                ):
                    cleanup_errors.append(
                        f"remote workspace cleanup postcondition: "
                        f"{remote_workspace} still exists"
                    )
            except Exception as exc:
                cleanup_errors.append(
                    f"remote workspace cleanup postcondition: {exc}"
                )

        # Cleanup is not successful merely because Kubernetes reproves.  The
        # host must also be back to the pre-experiment runtime state.
        try:
            _probe_experiment_host(
                inventory,
                required_ports=(
                    scenario.serial_socket_port,
                    REMOTE_EDGE_FORWARD_PORT,
                    LOCAL_CENTRAL_FORWARD_PORT,
                ),
                timeout_seconds=30,
            )
        except Exception as exc:
            cleanup_errors.append(f"remote runtime cleanup postcondition: {exc}")

        cleanup_check = _cleanup_live_resources(
            inventory=inventory,
            lock=lock,
            scenario=scenario,
            ue_deployment=ue_deployment,
            cleanup_errors=cleanup_errors,
        )
        if cleanup_check.passed:
            report(f"[PASS] cleanup-base-network: {cleanup_check.detail}")
        else:
            report(f"[FAIL] cleanup-base-network: {cleanup_check.detail}")

    final = None
    if jsonl_path.is_file() and parquet_path.is_file():
        final = build_data_evidence(
            scenario=scenario,
            scenario_path=scenario_path,
            jsonl_path=jsonl_path,
            parquet_path=parquet_path,
            minimum_per_sensor=minimum_per_sensor,
            extra_checks=(*extra_checks, cleanup_check),
        )
        save_experiment_evidence(final, evidence_path)

    ready = failure is None and final is not None and final.ready
    if failure is None and not cleanup_check.passed:
        failure = cleanup_check.detail
        ready = False

    _save_manifest(
        manifest_path,
        _render_manifest(
            scenario,
            status=(
                "iot-to-5g-path-proven"
                if ready
                else "failed"
                if failure is not None
                else "completed-unverified"
            ),
            scenario_path=scenario_path,
            failure=failure,
        ),
    )
    report("IOT-TO-5G PATH PROVEN" if ready else "experiment path NOT PROVEN")
    return ExperimentRunResult(scenario.run_id, run_directory, evidence_path, ready)
