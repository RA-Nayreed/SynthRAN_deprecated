"""Safe retry migration for the terminal physical workload stage.

A physical run keeps one stable run ID. Individual deterministic workload attempts
are immutable. A failed workload stage may be reopened only when the previous
attempt's own persisted result proves exact cleanup; the failed attempt remains
preserved and a subsequent attempt receives a distinct workload ID.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re

from synthran.network.runtime import atomic_json, validate_run_id
from synthran.r2lab.acceptance import (
    PhysicalAcceptance,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
)


RETRY_SCHEMA = "synthran/r2lab-workload-retry/v1alpha1"
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


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
    if payload.get("cleanup_proven") is not True:
        raise R2LabWorkloadRetryError(
            "previous failed workload cleanup was not proven; refusing same-run retry"
        )
    return payload


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
) -> tuple[PhysicalRunEvidence, bool]:
    """Reopen only a cleanup-proven terminal WORKLOAD failure.

    The failed attempt is preserved in a retry audit record before the terminal
    failed acceptance entry is trimmed. All earlier physical acceptance evidence
    remains byte-for-byte represented by the same immutable evidence objects.
    """

    if evidence.acceptance.failed_stage is not PhysicalAcceptanceStage.WORKLOAD:
        return evidence, False
    if not evidence.acceptance.evidence:
        raise R2LabWorkloadRetryError("failed workload acceptance evidence is missing")

    run_directory = run_root.expanduser().resolve() / evidence.run_id
    result_path = run_directory / "physical" / "physical-workload-result.json"
    previous = _load_previous_result(result_path, run_id=evidence.run_id)
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
            "previous_cleanup_proven": True,
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
