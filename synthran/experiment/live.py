"""Shared live RFSIM experiment mechanics used by the Amber workload.

This module deliberately contains no source-model logic.  It owns the narrow
SSH/Kubernetes/process primitives needed to attach an accepted Amber workload to
an already-ready RFSIM network and to restore that network afterwards.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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

from synthran.dependencies import DependencyLock
from synthran.experiment import ExperimentCheck, ExperimentError, ExperimentScenario
from synthran.experiment.resources import (
    EDGE_CONTAINER,
    RUN_LABEL,
    json_document,
    render_edge_cleanup_patch,
)
from synthran.fiveg_ansible import NetworkInventory
from synthran.live_preflight import CommandResult, LivePreflightError, ssh_command
from synthran.network.runtime import (
    NetworkVerificationReport,
    sanitize_deployment_text,
    verify_network_path,
)
from synthran.network.rfsim import reconcile_rfsim_runtime


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


def _remote_path_exists(
    inventory: NetworkInventory,
    path: str,
    *,
    timeout_seconds: int = 5,
) -> bool:
    """Check one exact remote path without conflating absence and probe failure."""

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
    label: str,
) -> Mapping[str, Any]:
    """Reap only remote processes matching explicit SynthRAN signatures."""

    payload = json.dumps(
        {
            "patterns": list(patterns),
            "orphan_only": orphan_only,
        },
        sort_keys=True,
    )
    reaper = r'''
import json, os, re, signal, sys, time

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
    print(json.dumps({"killed": [], "blocked": blocked, "remaining": []}))
    raise SystemExit(0)

targets = sorted(records)
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
print(json.dumps({"killed": targets, "blocked": [], "remaining": remaining}))
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
            "an active SynthRAN runtime already owns reserved resources; "
            "refusing to terminate a non-orphaned process"
        )
    if remaining:
        raise ExperimentError(f"{label} left matching remote processes alive: {remaining}")
    return result


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
    if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
        raise ExperimentError(f"{label} discovery returned malformed data")
    active = [
        item
        for item in items
        if not (
            isinstance(item.get("metadata"), dict)
            and item["metadata"].get("deletionTimestamp") is not None
        )
    ]
    if not active:
        raise ExperimentError(f"no {label} was found")
    if len(active) != 1:
        raise ExperimentError(f"multiple {label} resources were found; refusing to choose one")
    metadata = active[0].get("metadata")
    name = metadata.get("name") if isinstance(metadata, dict) else None
    if not isinstance(name, str) or not name:
        raise ExperimentError(f"{label} metadata is malformed")
    return name


def _core_address(inventory: NetworkInventory) -> str:
    value = inventory.core_node.variables.get("ip")
    if not value:
        raise ExperimentError("prepared inventory is missing the core node IP address")
    try:
        ipaddress.ip_address(value)
    except ValueError as exc:
        raise ExperimentError("prepared inventory has an invalid core node IP address") from exc
    return str(value)


def _probe_ssh_forwarding(
    inventory: NetworkInventory,
    *,
    timeout_seconds: int = 15,
) -> None:
    host = inventory.core_node
    try:
        command = ssh_command(host, "sshd", "-T")
    except LivePreflightError as exc:
        raise ExperimentError(
            f"SSH forwarding probe failed on {host.name}: {exc}"
        ) from exc
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
                f"{process.name} exited with code {process.process.poll()} before "
                f"TCP endpoint {host}:{port} became ready; see {process.log_path}"
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


def _wait_remote_tcp(
    inventory: NetworkInventory,
    *,
    host: str,
    port: int,
    timeout_seconds: int = 30,
    process: ManagedProcess | None = None,
) -> None:
    """Wait until a remote TCP port is connectable through the selected core."""

    probe = (
        "import socket; "
        "s=socket.socket(socket.AF_INET,socket.SOCK_STREAM); "
        "s.settimeout(2); "
        f"s.connect(({host!r},{port})); s.close(); print('ok')"
    )
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if process is not None and process.process.poll() is not None:
            raise ExperimentError(
                f"{process.name} exited before remote TCP endpoint {host}:{port} became ready"
            )
        try:
            command = ssh_command(inventory.core_node, "python3", "-c", probe)
        except LivePreflightError as exc:
            raise ExperimentError(str(exc)) from exc
        result = _run(command, timeout_seconds=5)
        if result.returncode == 0 and "ok" in result.stdout:
            return
        time.sleep(0.5)
    raise ExperimentError(f"remote TCP endpoint {host}:{port} did not become ready")


def _transfer_file(
    inventory: NetworkInventory,
    source_file: Path,
    remote_path: str,
    *,
    label: str,
) -> None:
    """Transfer one UTF-8 helper through the existing strict SSH boundary."""

    content = source_file.read_text(encoding="utf-8")
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
        raise ExperimentError(f"{label} failed")


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
    """Build a strict reverse tunnel through the selected core host."""

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
    _remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl patch deployment "
        f"{shlex.quote(deployment)} -n {KUBERNETES_NAMESPACE} "
        f"--type=strategic -p {shlex.quote(json_document(patch))}",
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
    parts: list[str] = [
        f"=== SynthRAN Rollout Diagnostics ({datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')}) ===",
        f"Network Run ID: {network_run_id}",
        "",
    ]
    if verification is not None:
        parts.extend(("=== Network Verification Checks ===", verification.render(), ""))

    def safe(command: str) -> str:
        try:
            result = _run(
                ssh_command(inventory.core_node, "sh", "-c", command),
                timeout_seconds=30,
            )
            output = result.stdout
            if result.stderr:
                output = f"{output}\n[stderr]\n{result.stderr}" if output else f"[stderr]\n{result.stderr}"
            return output.strip()
        except Exception as exc:
            return f"<diagnostic command failed: {exc}>"

    parts.extend(
        (
            "=== kubectl get pods ===",
            safe(
                f"KUBECONFIG=/etc/kubernetes/admin.conf kubectl get pods -n {KUBERNETES_NAMESPACE} -o wide"
            ),
            "",
            "=== kubectl get events ===",
            safe(
                f"KUBECONFIG=/etc/kubernetes/admin.conf kubectl get events -n {KUBERNETES_NAMESPACE} --sort-by=.metadata.creationTimestamp"
            ),
            "",
        )
    )
    names = safe(
        f"KUBECONFIG=/etc/kubernetes/admin.conf kubectl get pods -n {KUBERNETES_NAMESPACE} "
        f"-l app=srsran,component=ue,synthran.run/id={shlex.quote(network_run_id)} "
        "-o jsonpath='{.items[*].metadata.name}'"
    )
    if not names.startswith("<"):
        for pod in (name.strip("'\"") for name in names.split() if name):
            parts.extend(
                (
                    f"=== kubectl describe pod {pod} ===",
                    safe(
                        f"KUBECONFIG=/etc/kubernetes/admin.conf kubectl describe pod {shlex.quote(pod)} -n {KUBERNETES_NAMESPACE}"
                    ),
                    "",
                    f"=== kubectl logs {pod} -c {EDGE_CONTAINER} --tail=100 ===",
                    safe(
                        f"KUBECONFIG=/etc/kubernetes/admin.conf kubectl logs {shlex.quote(pod)} -n {KUBERNETES_NAMESPACE} -c {EDGE_CONTAINER} --tail=100"
                    ),
                    "",
                )
            )
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            sanitize_deployment_text("\n".join(parts), private_paths),
            encoding="utf-8",
            newline="\n",
        )
    except OSError:
        pass


def _discover_ue_deployment(inventory: NetworkInventory, network_run_id: str) -> str:
    payload = _remote_json(
        inventory,
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl get deployments "
        f"-n {KUBERNETES_NAMESPACE} "
        f"-l app.kubernetes.io/name=srsran-ue,synthran.run/id={shlex.quote(network_run_id)} -o json",
        label="srsUE Deployment discovery",
    )
    return _one_name(payload, label="run-owned srsUE Deployment")


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


def _replace_edge_runtime_config(inventory: NetworkInventory, pod: str, config: str) -> None:
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


def _cleanup_live_resources(
    *,
    inventory: NetworkInventory,
    lock: DependencyLock,
    scenario: ExperimentScenario,
    ue_deployment: str | None,
    cleanup_errors: Sequence[str] = (),
) -> ExperimentCheck:
    errors = list(cleanup_errors)
    rollout_restored = False
    if ue_deployment is not None:
        try:
            _kubectl_patch_deployment(
                inventory,
                ue_deployment,
                render_edge_cleanup_patch(),
                label="srsUE sidecar cleanup",
            )
            _wait_rollout(inventory, ue_deployment, label="srsUE cleanup rollout")
            rollout_restored = True
        except Exception as exc:
            errors.append(f"sidecar restore: {exc}")
    if rollout_restored:
        try:
            reconcile_rfsim_runtime(inventory, network_run_id=scenario.network_run_id)
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
            errors.append("accepted network session did not revalidate after cleanup")
    except Exception as exc:
        errors.append(f"accepted network revalidation: {exc}")
    if errors:
        return ExperimentCheck(
            "cleanup-base-network",
            False,
            "cleanup failed closed: " + "; ".join(errors),
        )
    return ExperimentCheck(
        "cleanup-base-network",
        True,
        "experiment resources removed and accepted network session revalidated",
    )
