"""Safe retry migration for the terminal physical workload stage.

A physical run keeps one stable run ID. Individual deterministic workload attempts
are immutable. A failed workload stage may be reopened only when cleanup was
proven by that attempt or can be re-proven from the current live state without
adopting ambiguous resources.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Callable, Sequence

from synthran.live_preflight import CommandResult, subprocess_runner
from synthran.network.runtime import atomic_json, validate_run_id
from synthran.r2lab.acceptance import (
    PhysicalAcceptance,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
)
from synthran.r2lab.live_cluster import cluster_command
from synthran.r2lab.resources import load_topology


RETRY_SCHEMA = "synthran/r2lab-workload-retry/v1alpha1"
CLEANUP_RECOVERY_SCHEMA = "synthran/r2lab-workload-cleanup-recovery/v1alpha1"
DEFAULT_EXPERIMENT_ROOT = Path(".synthran/experiments-r2lab")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_LABEL = "synthran.experiment/run"
_KUBERNETES_NAMESPACE = "open5gs"
_RESERVED_CORE_PORTS = (60001, 18883, 18885, 18884)
Runner = Callable[[Sequence[str], int], CommandResult]


class R2LabWorkloadRetryError(RuntimeError):
    """Raised when a failed physical workload cannot be retried safely."""


def _load_previous_result(path: Path, *, run_id: str) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise R2LabWorkloadRetryError(
            "previous failed workload has no persisted result; refusing same-run retry"
        ) from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2LabWorkloadRetryError(
            "previous failed workload result is unreadable; refusing same-run retry"
        ) from exc
    if not isinstance(payload, dict):
        raise R2LabWorkloadRetryError("previous failed workload result is malformed")
    workload_id = payload.get("workload_id")
    digest = payload.get("evidence_sha256")
    if (
        payload.get("run_id") != run_id
        or payload.get("backend") != "r2lab"
        or payload.get("interface") != "wwan0"
        or not isinstance(workload_id, str)
        or validate_run_id(workload_id) != workload_id
        or not isinstance(digest, str)
        or _DIGEST_RE.fullmatch(digest) is None
    ):
        raise R2LabWorkloadRetryError(
            "previous failed workload result is not bound to this physical run"
        )
    if payload.get("accepted") is not False:
        raise R2LabWorkloadRetryError(
            "failed workload acceptance conflicts with the persisted workload result"
        )
    if not isinstance(payload.get("cleanup_proven"), bool):
        raise R2LabWorkloadRetryError("previous failed workload cleanup state is malformed")
    return payload


def _read_attempt_manifest(
    experiment_root: Path,
    *,
    workload_id: str,
    physical_run_id: str,
) -> Path:
    directory = experiment_root.expanduser().resolve() / workload_id
    path = directory / "manifest.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise R2LabWorkloadRetryError(
            "previous failed workload cleanup was not proven and its run directory is missing"
        ) from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2LabWorkloadRetryError(
            "previous failed workload cleanup was not proven and its manifest is unreadable"
        ) from exc
    if (
        not isinstance(payload, dict)
        or payload.get("run_id") != workload_id
        or payload.get("physical_run_id") != physical_run_id
        or payload.get("backend") != "r2lab"
    ):
        raise R2LabWorkloadRetryError(
            "previous failed workload run directory is not bound to this physical run"
        )
    return directory


def _require_early_failure_scope(directory: Path) -> None:
    """Accept only failures that did not advance beyond central broker staging."""

    logs = directory / "logs"
    if logs.is_dir() and any(path.is_file() for path in logs.iterdir()):
        raise R2LabWorkloadRetryError(
            "previous failed workload reached a later runtime stage; automatic cleanup recovery is unsafe"
        )
    for name in (
        "telemetry.jsonl",
        "rejected-events.jsonl",
        "telemetry.parquet",
        "experiment-evidence.json",
    ):
        if (directory / name).exists():
            raise R2LabWorkloadRetryError(
                "previous failed workload reached data collection; automatic cleanup recovery is unsafe"
            )


def _checked_cluster(
    *,
    topology,
    runner: Runner,
    timeout_seconds: int,
    label: str,
    remote: Sequence[str],
) -> CommandResult:
    try:
        result = runner(cluster_command(topology, *tuple(remote)), timeout_seconds)
    except Exception as exc:
        raise R2LabWorkloadRetryError(f"{label} could not complete") from exc
    if result.returncode != 0:
        raise R2LabWorkloadRetryError(f"{label} returned nonzero")
    return result


def _recover_early_cleanup(
    *,
    physical_run_id: str,
    workload_id: str,
    previous_evidence_sha256: str,
    run_root: Path,
    experiment_root: Path,
    runner: Runner,
) -> Path:
    """Re-prove cleanup for an early central-broker-stage workload failure."""

    attempt_directory = _read_attempt_manifest(
        experiment_root,
        workload_id=workload_id,
        physical_run_id=physical_run_id,
    )
    _require_early_failure_scope(attempt_directory)
    topology = load_topology(run_root=run_root, run_id=physical_run_id).validate()
    selector = f"{_RUN_LABEL}={workload_id}"

    _checked_cluster(
        topology=topology,
        runner=runner,
        timeout_seconds=180,
        label="exact failed-workload Kubernetes cleanup",
        remote=(
            "kubectl",
            "delete",
            "deployment,configmap",
            "-n",
            _KUBERNETES_NAMESPACE,
            "-l",
            selector,
            "--ignore-not-found=true",
            "--wait=true",
        ),
    )
    remaining = _checked_cluster(
        topology=topology,
        runner=runner,
        timeout_seconds=60,
        label="failed-workload Kubernetes cleanup postcondition",
        remote=(
            "kubectl",
            "get",
            "deployment,configmap",
            "-n",
            _KUBERNETES_NAMESPACE,
            "-l",
            selector,
            "-o",
            "name",
        ),
    )
    if remaining.stdout.strip():
        raise R2LabWorkloadRetryError(
            "failed-workload Kubernetes objects remain after exact cleanup"
        )

    workspace = f"/tmp/synthran/{workload_id}"
    _checked_cluster(
        topology=topology,
        runner=runner,
        timeout_seconds=30,
        label="failed-workload remote workspace postcondition",
        remote=("test", "!", "-e", workspace),
    )

    probe = "\n".join(
        (
            "import json, os, socket",
            "ports = [60001, 18883, 18885, 18884]",
            "busy = []",
            "for port in ports:",
            "    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)",
            "    try:",
            "        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)",
            "        sock.bind(('127.0.0.1', port))",
            "    except OSError:",
            "        busy.append(port)",
            "    finally:",
            "        sock.close()",
            "print(json.dumps({'tun_exists': os.path.exists('/sys/class/net/tun0'), 'busy_ports': busy}))",
        )
    )
    observed = _checked_cluster(
        topology=topology,
        runner=runner,
        timeout_seconds=30,
        label="failed-workload core cleanup postcondition",
        remote=("python3", "-c", probe),
    )
    try:
        state = json.loads(observed.stdout)
    except json.JSONDecodeError as exc:
        raise R2LabWorkloadRetryError(
            "failed-workload core cleanup postcondition returned malformed state"
        ) from exc
    if not isinstance(state, dict):
        raise R2LabWorkloadRetryError(
            "failed-workload core cleanup postcondition returned malformed state"
        )
    if state.get("tun_exists") is not False:
        raise R2LabWorkloadRetryError(
            "tun0 exists after the failed workload; refusing to adopt or delete it"
        )
    if state.get("busy_ports") != []:
        raise R2LabWorkloadRetryError(
            "reserved core workload ports remain busy after exact cleanup"
        )

    directory = run_root.expanduser().resolve() / physical_run_id / "physical" / "workload-cleanup-recoveries"
    recovery_index, recovery_path = _next_retry_record(directory)
    atomic_json(
        recovery_path,
        {
            "schema": CLEANUP_RECOVERY_SCHEMA,
            "physical_run_id": physical_run_id,
            "previous_workload_id": workload_id,
            "previous_evidence_sha256": previous_evidence_sha256,
            "recovery_index": recovery_index,
            "scope": "central-broker-stage-only",
            "cleanup_proven": True,
            "checks": {
                "run_scoped_kubernetes_objects_absent": True,
                "remote_workspace_absent": True,
                "later_stage_local_logs_absent": True,
                "tun0_absent": True,
                "reserved_core_ports_free": list(_RESERVED_CORE_PORTS),
            },
            "recovered_at_utc": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        },
    )
    return recovery_path


def _next_retry_record(directory: Path) -> tuple[int, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    for index in range(1, 1000):
        candidate = directory / f"retry-{index:03d}.json"
        if not candidate.exists():
            return index, candidate
    raise R2LabWorkloadRetryError("physical workload retry history is exhausted")


def recover_failed_workload(
    *,
    evidence: PhysicalRunEvidence,
    run_root: Path,
    experiment_root: Path = DEFAULT_EXPERIMENT_ROOT,
    cluster_runner: Runner = subprocess_runner,
) -> tuple[PhysicalRunEvidence, bool]:
    """Reopen only a terminal WORKLOAD failure whose cleanup is currently proven.

    Historical cleanup evidence remains immutable. If the failed attempt exited
    before proving cleanup, only the narrow central-broker-stage recovery is
    eligible for live re-proof. Ambiguous later-stage leftovers remain fail-closed.
    """

    if evidence.acceptance.failed_stage is not PhysicalAcceptanceStage.WORKLOAD:
        return evidence, False
    if not evidence.acceptance.evidence:
        raise R2LabWorkloadRetryError("failed workload acceptance evidence is missing")

    run_directory = run_root.expanduser().resolve() / evidence.run_id
    result_path = run_directory / "physical" / "physical-workload-result.json"
    previous = _load_previous_result(result_path, run_id=evidence.run_id)
    historical_cleanup = previous["cleanup_proven"] is True
    recovery_path: Path | None = None
    if not historical_cleanup:
        try:
            recovery_path = _recover_early_cleanup(
                physical_run_id=evidence.run_id,
                workload_id=str(previous["workload_id"]),
                previous_evidence_sha256=str(previous["evidence_sha256"]),
                run_root=run_root,
                experiment_root=experiment_root,
                runner=cluster_runner,
            )
        except R2LabWorkloadRetryError as exc:
            raise R2LabWorkloadRetryError(
                f"previous failed workload cleanup was not proven; live cleanup recovery failed: {exc}"
            ) from exc

    retry_directory = run_directory / "physical" / "workload-retries"
    retry_index, retry_path = _next_retry_record(retry_directory)
    failed_item = evidence.acceptance.evidence[-1]
    atomic_json(
        retry_path,
        {
            "schema": RETRY_SCHEMA,
            "physical_run_id": evidence.run_id,
            "retry_index": retry_index,
            "previous_workload_id": previous["workload_id"],
            "previous_evidence_sha256": previous["evidence_sha256"],
            "previous_failure_source": failed_item.source,
            "previous_accepted": False,
            "previous_cleanup_proven": historical_cleanup,
            "cleanup_recovered_live": recovery_path is not None,
            "cleanup_recovery_path": str(recovery_path) if recovery_path is not None else None,
            "reopened_stage": PhysicalAcceptanceStage.WORKLOAD.value,
            "recovered_at_utc": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        },
    )

    trimmed = PhysicalAcceptance(evidence=evidence.acceptance.evidence[:-1])
    recovered = PhysicalRunEvidence(
        run_id=evidence.run_id,
        staged=evidence.staged,
        gnb_start=evidence.gnb_start,
        acceptance=trimmed,
    )
    recovered.write_json(run_directory / "physical-run.json")
    return recovered, True


def next_workload_attempt_id(
    requested_id: str,
    *,
    physical_run_id: str,
    physical_run_root: Path,
    experiment_root: Path,
) -> str:
    """Return an immutable workload-attempt ID without changing the physical run ID."""

    requested_id = validate_run_id(requested_id)
    validate_run_id(physical_run_id)
    retry_directory = (
        physical_run_root.expanduser().resolve()
        / physical_run_id
        / "physical"
        / "workload-retries"
    )
    retry_records = sorted(retry_directory.glob("retry-*.json")) if retry_directory.is_dir() else []
    experiment_root = experiment_root.expanduser().resolve()

    if not retry_records:
        if (experiment_root / requested_id).exists():
            raise R2LabWorkloadRetryError(
                "workload directory already exists without cleanup-proven retry authorization"
            )
        return requested_id

    for attempt in range(2, 1000):
        suffix = f"-w{attempt}"
        stem = requested_id[: 63 - len(suffix)].rstrip("-")
        if not stem:
            raise R2LabWorkloadRetryError("workload ID cannot be shortened for retry")
        candidate = validate_run_id(f"{stem}{suffix}")
        if not (experiment_root / candidate).exists():
            return candidate
    raise R2LabWorkloadRetryError("physical workload attempt namespace is exhausted")
