"""Retire local R2Lab claims when the owning lease is no longer current.

Lease loss removes SynthRAN's authority to mutate physical hardware. It must not
also leave a workspace permanently locked by an obsolete local claim. This
module verifies Faraday is reachable, proves that the configured slice no
longer holds a current lease, archives the exact claim, records why it was
retired, and removes only the workspace-level active marker.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Callable, Mapping, Sequence

from synthran.live_preflight import CommandResult
from synthran.network.runtime import validate_run_id
from synthran.r2lab.controller import gateway_command, subprocess_runner
from synthran.r2lab.hardware import PhysicalTopology
from synthran.r2lab.resources import R2LabTopologyResourceError


Runner = Callable[[Sequence[str], int], CommandResult]
CLAIM_SCHEMA = "synthran/r2lab-claim/v1alpha2"
RETIREMENT_SCHEMA = "synthran/r2lab-claim-retirement/v1"


def _now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _slice_fingerprint(slice_name: str) -> str:
    if not slice_name or len(slice_name) > 64 or any(
        not (character.isalnum() or character in "._-") for character in slice_name
    ):
        raise R2LabTopologyResourceError("R2Lab slice name is malformed")
    return hashlib.sha256(slice_name.encode("utf-8")).hexdigest()


def _read_json(path: Path, label: str) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise R2LabTopologyResourceError(f"{label} is missing") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2LabTopologyResourceError(f"{label} is not readable JSON") from exc
    if not isinstance(payload, dict):
        raise R2LabTopologyResourceError(f"{label} must contain one JSON object")
    return payload


def _atomic_json(path: Path, payload: Mapping[str, object]) -> Path:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=path.parent,
            delete=False,
        ) as stream:
            json.dump(dict(payload), stream, indent=2, sort_keys=True)
            stream.write("\n")
            temporary = Path(stream.name)
        temporary.replace(path)
    except OSError as exc:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise R2LabTopologyResourceError(
            "stale R2Lab claim retirement evidence could not be persisted"
        ) from exc
    return path


@dataclass(frozen=True)
class StaleClaimRetirement:
    run_id: str
    archived_claim_path: Path
    evidence_path: Path
    retired_at_utc: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": RETIREMENT_SCHEMA,
            "run_id": self.run_id,
            "retired": True,
            "reason": "current-r2lab-lease-not-held",
            "hardware_mutated": False,
            "archived_claim_path": str(self.archived_claim_path),
            "evidence_path": str(self.evidence_path),
            "retired_at_utc": self.retired_at_utc,
        }


def retire_stale_claim(
    *,
    run_root: Path,
    run_id: str,
    slice_name: str,
    topology: PhysicalTopology,
) -> StaleClaimRetirement:
    """Archive and retire only the matching local active claim.

    This function never contacts or mutates provider hardware. Callers must
    separately establish that the configured slice no longer holds a current
    lease before invoking it.
    """

    validate_run_id(run_id)
    topology = topology.validate()
    root = run_root.expanduser().resolve()
    active = root / "active.json"
    claim = _read_json(active, "active physical resource claim")
    expected = {
        "schema": CLAIM_SCHEMA,
        "run_id": run_id,
        "slice_fingerprint": _slice_fingerprint(slice_name),
        "core_node": topology.core_node,
        "ran_node": topology.ran_node,
        "radio": topology.radio,
        "ue": topology.ue,
    }
    if any(claim.get(key) != value for key, value in expected.items()):
        raise R2LabTopologyResourceError(
            "active physical claim does not match the requested stale run"
        )

    run_directory = root / run_id
    if not run_directory.is_dir():
        raise R2LabTopologyResourceError(
            "stale physical claim has no matching preserved run directory"
        )

    retired_at = _now()
    archived = _atomic_json(run_directory / "retired-claim.json", claim)
    evidence = _atomic_json(
        run_directory / "claim-retirement.json",
        {
            "schema": RETIREMENT_SCHEMA,
            "run_id": run_id,
            "retired": True,
            "reason": "current-r2lab-lease-not-held",
            "hardware_mutated": False,
            "retired_at_utc": retired_at,
            "topology": topology.to_dict(),
        },
    )

    # Re-read immediately before removing the active marker so a concurrent
    # claim replacement cannot be mistaken for the claim we just archived.
    if _read_json(active, "active physical resource claim") != claim:
        raise R2LabTopologyResourceError(
            "active physical claim changed while stale retirement was in progress"
        )
    try:
        active.unlink()
    except OSError as exc:
        raise R2LabTopologyResourceError(
            "stale physical claim could not be retired from the workspace"
        ) from exc

    return StaleClaimRetirement(
        run_id=run_id,
        archived_claim_path=archived,
        evidence_path=evidence,
        retired_at_utc=retired_at,
    )


def retire_if_lease_absent(
    *,
    run_root: Path,
    run_id: str,
    slice_name: str,
    topology: PhysicalTopology,
    runner: Runner = subprocess_runner,
    timeout_seconds: int = 30,
) -> StaleClaimRetirement | None:
    """Retire a matching local claim only after explicit lease loss is proven.

    A transport or Faraday-access failure remains fail-closed. A reachable
    Faraday gateway plus a non-zero ``rhubarbe leases --check`` means this slice
    no longer has mutation authority, so the local claim is archival state only.
    """

    try:
        gateway = runner(gateway_command(slice_name, "true"), timeout_seconds)
    except Exception as exc:
        raise R2LabTopologyResourceError(
            "R2Lab gateway could not be verified; stale claim was not retired"
        ) from exc
    if gateway.returncode != 0:
        raise R2LabTopologyResourceError(
            "R2Lab gateway could not be verified; stale claim was not retired"
        )

    try:
        lease = runner(
            gateway_command(slice_name, "rhubarbe", "leases", "--check"),
            timeout_seconds,
        )
    except Exception as exc:
        raise R2LabTopologyResourceError(
            "current R2Lab lease state could not be verified; stale claim was not retired"
        ) from exc
    if lease.returncode == 0:
        return None

    return retire_stale_claim(
        run_root=run_root,
        run_id=run_id,
        slice_name=slice_name,
        topology=topology,
    )
