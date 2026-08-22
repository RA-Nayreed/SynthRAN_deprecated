"""Fail-closed R2Lab resource controller with evidence-backed hardware state.

The controller separates authority, mutation, and resulting-state evidence.
Physical mutations are always scoped to the selected resources, every mutation
is preceded by an active-lease check, and cleanup claims are released only when
both the selected UE and radio are proven off.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
from typing import Callable, Mapping, Sequence, TextIO

from synthran.live_preflight import CommandResult
from synthran.network.runtime import validate_run_id
from synthran.r2lab.deployment import (
    PhysicalGnbStartResult,
    PhysicalStagingResult,
    PhysicalStartAuthority,
    execute_authorized_physical_gnb_start,
)
from synthran.r2lab.provider import (
    CleanupEvidence,
    CleanupState,
    PowerState,
    R2LabPowerStateError,
    R2LabQfitStateError,
    ReleaseAssessment,
    VerifiedPduOperation,
    VerifiedQfitOperation,
    execute_verified_pdu_transition,
    execute_verified_qfit_transition,
    parse_pdu_status,
    release_assessment,
)
from synthran.workspace.model import WorkspaceError
from synthran.workspace.store import (
    DEFAULT_PROFILE_NAME,
    load_profile,
    resolve_identity_reference,
    verify_profile_identity,
)


R2LAB_GATEWAY = "faraday.inria.fr"
R2LAB_SCHEMA = "synthran/r2lab-resource/v1alpha1"
R2LAB_PLAN_SCHEMA = "synthran/r2lab-plan/v1alpha1"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_REACHABILITY_ATTEMPTS = 12
DEFAULT_REACHABILITY_DELAY_SECONDS = 10.0
DEFAULT_POWER_SETTLE_SECONDS = 20.0

SUPPORTED_RADIOS = frozenset({"n300", "n320"})
SUPPORTED_QHATS = frozenset(
    {
        "qhat01",
        "qhat02",
        "qhat03",
        "qhat10",
        "qhat11",
        "qhat20",
        "qhat21",
        "qhat22",
    }
)
SUPPORTED_QFITS = frozenset(
    {"qfit07", "qfit09", "qfit18", "qfit29", "qfit32", "qfit34"}
)
QMI_QHATS = frozenset({"qhat20", "qhat21", "qhat22"})


class R2LabResourceError(RuntimeError):
    """Raised when R2Lab authority or selected-resource state is unsafe."""


Runner = Callable[[Sequence[str], int], CommandResult]
Sleeper = Callable[[float], None]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _validate_slice(value: str) -> str:
    if not value or len(value) > 64:
        raise R2LabResourceError("R2Lab slice name must contain 1-64 safe characters")
    if any(not (character.isalnum() or character in "._-") for character in value):
        raise R2LabResourceError(
            "R2Lab slice name may contain only letters, numbers, '.', '_', or '-'"
        )
    return value


def _validate_radio(value: str) -> str:
    radio = value.strip().lower()
    if radio not in SUPPORTED_RADIOS:
        supported = ", ".join(sorted(SUPPORTED_RADIOS))
        raise R2LabResourceError(f"unsupported R2Lab radio; choose one of: {supported}")
    return radio


def _validate_ue(value: str) -> str:
    ue = value.strip().lower()
    if ue not in SUPPORTED_QHATS and ue not in SUPPORTED_QFITS:
        supported = ", ".join(sorted(SUPPORTED_QHATS | SUPPORTED_QFITS))
        raise R2LabResourceError(f"unsupported R2Lab UE; choose one of: {supported}")
    return ue


def _ue_kind(ue: str) -> str:
    return "qhat" if ue in SUPPORTED_QHATS else "qfit"


def _ue_mode(ue: str) -> str:
    return "qmi" if ue in QMI_QHATS else "mbim"


def _validate_timeout(value: int) -> int:
    if value < 5 or value > 300:
        raise R2LabResourceError("R2Lab command timeout must be between 5 and 300 seconds")
    return value


def _validate_run(value: str) -> str:
    try:
        run_id = validate_run_id(value)
    except Exception as exc:
        raise R2LabResourceError(str(exc)) from exc
    if run_id == "active":
        raise R2LabResourceError("run ID 'active' is reserved by the R2Lab provider")
    return run_id


def _configured_identity(slice_name: str) -> Path | None:
    """Resolve private R2Lab SSH identity without exposing it in public evidence."""

    override = os.environ.get("SYNTHRAN_R2LAB_IDENTITY")
    if override:
        return Path(override).expanduser().resolve()

    try:
        profile = load_profile(DEFAULT_PROFILE_NAME)
    except WorkspaceError:
        return None
    if profile.r2lab_identity is None:
        return None
    if profile.r2lab_slice is not None and profile.r2lab_slice != slice_name:
        raise R2LabResourceError(
            "configured R2Lab SSH identity belongs to a different profile slice"
        )
    try:
        verify_profile_identity(profile)
    except WorkspaceError as exc:
        raise R2LabResourceError("configured R2Lab SSH identity could not be verified") from exc
    return resolve_identity_reference(profile.r2lab_identity)


def subprocess_runner(command: Sequence[str], timeout_seconds: int) -> CommandResult:
    """Execute one argv-only local command and capture its result."""

    try:
        completed = subprocess.run(
            list(command),
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout_seconds,
        )
    except FileNotFoundError as exc:
        raise R2LabResourceError("ssh is required for R2Lab control") from exc
    except subprocess.TimeoutExpired as exc:
        raise R2LabResourceError("R2Lab command timed out") from exc
    return CommandResult(completed.returncode, completed.stdout or "", completed.stderr or "")


def gateway_command(slice_name: str, *remote: str) -> tuple[str, ...]:
    """Build the strict public-key SSH boundary used for all gateway actions."""

    slice_name = _validate_slice(slice_name)
    identity = _configured_identity(slice_name)
    identity_options: tuple[str, ...] = ()
    if identity is not None:
        identity_options = (
            "-o",
            "IdentitiesOnly=yes",
            "-i",
            str(identity),
        )
    return (
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        *identity_options,
        "--",
        f"{slice_name}@{R2LAB_GATEWAY}",
        *remote,
    )


@dataclass(frozen=True)
class R2LabSelection:
    """One exact physical-radio resource selection."""

    slice_name: str
    radio: str
    ue: str

    @classmethod
    def build(cls, *, slice_name: str, radio: str, ue: str) -> "R2LabSelection":
        return cls(
            slice_name=_validate_slice(slice_name),
            radio=_validate_radio(radio),
            ue=_validate_ue(ue),
        )

    @property
    def ue_kind(self) -> str:
        return _ue_kind(self.ue)

    @property
    def ue_mode(self) -> str:
        return _ue_mode(self.ue)

    @property
    def slice_fingerprint(self) -> str:
        return _fingerprint(self.slice_name)

    def public_summary(self) -> dict[str, str]:
        return {
            "gateway": R2LAB_GATEWAY,
            "slice_fingerprint": self.slice_fingerprint,
            "radio": self.radio,
            "ue": self.ue,
            "ue_kind": self.ue_kind,
            "ue_mode": self.ue_mode,
        }


@dataclass(frozen=True)
class R2LabPlan:
    run_id: str
    selection: R2LabSelection

    def to_dict(self) -> dict[str, object]:
        ue_status = (
            f"ssh <r2lab-slice>@faraday.inria.fr rhubarbe pdu status {self.selection.ue}"
            if self.selection.ue_kind == "qhat"
            else "ssh <r2lab-slice>@faraday.inria.fr rhubarbe status <qfit-node>"
        )
        return {
            "schema": R2LAB_PLAN_SCHEMA,
            "execution_enabled": False,
            "run_id": self.run_id,
            "lease_action": "reuse-active",
            "resources": self.selection.public_summary(),
            "commands": [
                "ssh <r2lab-slice>@faraday.inria.fr rhubarbe leases --check",
                f"ssh <r2lab-slice>@faraday.inria.fr rhubarbe pdu on {self.selection.radio}",
                f"ssh <r2lab-slice>@faraday.inria.fr rhubarbe pdu status {self.selection.radio}",
                (
                    f"ssh <r2lab-slice>@faraday.inria.fr rhubarbe pdu off {self.selection.ue}"
                    if self.selection.ue_kind == "qhat"
                    else f"ssh <r2lab-slice>@faraday.inria.fr qfit off {self.selection.ue}"
                ),
                ue_status,
                (
                    f"ssh <r2lab-slice>@faraday.inria.fr rhubarbe pdu on {self.selection.ue}"
                    if self.selection.ue_kind == "qhat"
                    else f"ssh <r2lab-slice>@faraday.inria.fr qfit on {self.selection.ue}"
                ),
                ue_status,
                f"ssh <r2lab-slice>@faraday.inria.fr ping -c 1 -W 1 {self.selection.ue}",
            ],
            "safety": {
                "global_power_off": False,
                "password_storage": False,
                "automatic_lease_booking": False,
                "one_active_selection_per_workspace": True,
                "mutation_returncode_is_state_truth": False,
                "claim_release_requires_proven_clean_state": True,
            },
        }

    def render(self, *, as_json: bool = False) -> str:
        payload = self.to_dict()
        if as_json:
            return json.dumps(payload, indent=2, sort_keys=True)
        resources = payload["resources"]
        assert isinstance(resources, dict)
        return "\n".join(
            (
                "SynthRAN R2Lab resource plan (NON-EXECUTING)",
                f"Run ID: {self.run_id}",
                f"Radio: {resources['radio']}",
                f"UE: {resources['ue']} ({resources['ue_kind']}, {resources['ue_mode']})",
                "Lease: require and reuse the active R2Lab lease",
                "Credentials: SSH key only; no R2Lab password is stored",
                "State proof: verify exact provider state after every power mutation",
                "Cleanup: exact selected radio and UE only; global power-off is forbidden",
            )
        )


def build_plan(*, run_id: str, selection: R2LabSelection) -> R2LabPlan:
    return R2LabPlan(run_id=_validate_run(run_id), selection=selection)


@dataclass(frozen=True)
class R2LabCheck:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class R2LabDoctorReport:
    selection: R2LabSelection
    checks: tuple[R2LabCheck, ...]

    @property
    def ready(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)

    def render(self) -> str:
        lines = ["SynthRAN R2Lab doctor (read-only)"]
        for check in self.checks:
            lines.append(
                f"[{'PASS' if check.passed else 'FAIL'}] {check.name}: {check.detail}"
            )
        lines.append(f"Result: {'READY' if self.ready else 'NOT READY'}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-doctor/v1alpha1",
            "ready": self.ready,
            "resources": self.selection.public_summary(),
            "checks": [
                {"name": check.name, "passed": check.passed, "detail": check.detail}
                for check in self.checks
            ],
        }


def run_doctor(
    *,
    selection: R2LabSelection,
    runner: Runner = subprocess_runner,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> R2LabDoctorReport:
    """Verify gateway access and an active owned lease without mutation."""

    timeout_seconds = _validate_timeout(timeout_seconds)
    checks: list[R2LabCheck] = [
        R2LabCheck(
            "selection",
            True,
            f"supported {selection.radio} + {selection.ue} resource pair",
        )
    ]

    try:
        gateway = runner(gateway_command(selection.slice_name, "true"), timeout_seconds)
    except (R2LabResourceError, OSError):
        checks.append(R2LabCheck("gateway", False, "strict public-key SSH to Faraday failed"))
        return R2LabDoctorReport(selection, tuple(checks))
    gateway_ok = gateway.returncode == 0
    checks.append(
        R2LabCheck(
            "gateway",
            gateway_ok,
            "strict public-key SSH to Faraday succeeded"
            if gateway_ok
            else "strict public-key SSH to Faraday failed",
        )
    )
    if not gateway_ok:
        return R2LabDoctorReport(selection, tuple(checks))

    try:
        lease = runner(
            gateway_command(selection.slice_name, "rhubarbe", "leases", "--check"),
            timeout_seconds,
        )
    except (R2LabResourceError, OSError):
        checks.append(R2LabCheck("lease", False, "no active R2Lab lease could be verified"))
        return R2LabDoctorReport(selection, tuple(checks))
    lease_ok = lease.returncode == 0
    checks.append(
        R2LabCheck(
            "lease",
            lease_ok,
            "active R2Lab lease verified"
            if lease_ok
            else "no active R2Lab lease could be verified",
        )
    )
    return R2LabDoctorReport(selection, tuple(checks))


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        delete=False,
    ) as temporary:
        temporary.write(content)
        temporary_path = Path(temporary.name)
    temporary_path.replace(path)


def _manifest_payload(
    *,
    run_id: str,
    selection: R2LabSelection,
    status: str,
    updated_at: datetime,
    claim_held: bool,
    failure_stage: str | None = None,
    cleanup: ReleaseAssessment | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema": R2LAB_SCHEMA,
        "run_id": run_id,
        "status": status,
        "updated_at_utc": _format_time(updated_at),
        "lease_action": "reuse-active",
        "resources": selection.public_summary(),
        "resource_claim": "held" if claim_held else "released",
        "password_storage": False,
        "global_power_off": False,
    }
    if failure_stage is not None:
        payload["failure_stage"] = failure_stage
    if cleanup is not None:
        payload["cleanup"] = cleanup.to_dict()
    return payload


def _load_json(path: Path, label: str) -> Mapping[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise R2LabResourceError(f"{label} was not found") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2LabResourceError(f"{label} must be readable JSON") from exc
    if not isinstance(payload, dict):
        raise R2LabResourceError(f"{label} must contain one JSON object")
    return payload


def _selection_from_manifest(
    payload: Mapping[str, object], *, slice_name: str, run_id: str
) -> R2LabSelection:
    if payload.get("schema") != R2LAB_SCHEMA or payload.get("run_id") != run_id:
        raise R2LabResourceError("R2Lab manifest does not match the requested run")
    resources = payload.get("resources")
    if not isinstance(resources, dict):
        raise R2LabResourceError("R2Lab manifest resource selection is malformed")
    fingerprint = resources.get("slice_fingerprint")
    if fingerprint != _fingerprint(_validate_slice(slice_name)):
        raise R2LabResourceError("R2Lab slice authority does not match the run manifest")
    radio = resources.get("radio")
    ue = resources.get("ue")
    if not isinstance(radio, str) or not isinstance(ue, str):
        raise R2LabResourceError("R2Lab manifest resource selection is incomplete")
    return R2LabSelection.build(slice_name=slice_name, radio=radio, ue=ue)


def _claim_path(run_root: Path) -> Path:
    return run_root.resolve() / "active.json"


def _write_claim(path: Path, *, run_id: str, selection: R2LabSelection) -> None:
    if path.exists():
        raise R2LabResourceError(
            "another R2Lab resource claim exists in this workspace; release or inspect it first"
        )
    payload = {
        "schema": "synthran/r2lab-claim/v1alpha1",
        "run_id": run_id,
        "slice_fingerprint": selection.slice_fingerprint,
        "radio": selection.radio,
        "ue": selection.ue,
        "created_at_utc": _format_time(_utc_now()),
    }
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise R2LabResourceError("another R2Lab resource claim appeared concurrently") from exc
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
    except OSError:
        path.unlink(missing_ok=True)
        raise


def _require_claim(path: Path, *, run_id: str, selection: R2LabSelection) -> None:
    payload = _load_json(path, "active R2Lab resource claim")
    expected = {
        "schema": "synthran/r2lab-claim/v1alpha1",
        "run_id": run_id,
        "slice_fingerprint": selection.slice_fingerprint,
        "radio": selection.radio,
        "ue": selection.ue,
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise R2LabResourceError("active R2Lab resource claim does not match the requested run")


def _claim_sha256(payload: Mapping[str, object]) -> str:
    try:
        encoded = json.dumps(
            dict(payload),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise R2LabResourceError("active R2Lab resource claim is not canonical JSON data") from exc
    return hashlib.sha256(encoded).hexdigest()


def authorize_physical_start(
    *,
    run_id: str,
    slice_name: str,
    run_root: Path = Path(".synthran/r2lab"),
    runner: Runner = subprocess_runner,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> PhysicalStartAuthority:
    """Mint one sanitized start authority from the active claim and live N300 state."""

    run_id = _validate_run(run_id)
    slice_name = _validate_slice(slice_name)
    timeout_seconds = _validate_timeout(timeout_seconds)
    run_root = run_root.resolve()
    manifest = _load_json(run_root / run_id / "manifest.json", "R2Lab run manifest")
    selection = _selection_from_manifest(manifest, slice_name=slice_name, run_id=run_id)
    claim_path = _claim_path(run_root)
    _require_claim(claim_path, run_id=run_id, selection=selection)
    claim = _load_json(claim_path, "active R2Lab resource claim")
    claim_digest = _claim_sha256(claim)

    if manifest.get("status") != "ready" or manifest.get("resource_claim") != "held":
        raise R2LabResourceError("R2Lab run is not in a ready, claimed state")
    if selection.radio != "n300":
        raise R2LabResourceError("current physical gNB start boundary requires n300")
    if selection.ue_kind != "qfit":
        raise R2LabResourceError("current physical gNB start boundary requires a qfit UE")

    try:
        lease = runner(
            gateway_command(slice_name, "rhubarbe", "leases", "--check"),
            timeout_seconds,
        )
    except (R2LabResourceError, OSError) as exc:
        raise R2LabResourceError("fresh R2Lab lease authority could not be verified") from exc
    if lease.returncode != 0:
        raise R2LabResourceError("fresh R2Lab lease authority was not verified")

    try:
        radio = runner(
            gateway_command(slice_name, "rhubarbe", "pdu", "status", selection.radio),
            timeout_seconds,
        )
    except (R2LabResourceError, OSError) as exc:
        raise R2LabResourceError("selected N300 state could not be observed") from exc
    observed = parse_pdu_status(
        "\n".join(part for part in (radio.stdout, radio.stderr) if part),
        resource=selection.radio,
    )
    if observed.state is not PowerState.ON:
        raise R2LabResourceError("selected N300 is not proven on for physical gNB start")

    # The filesystem claim is part of the authority boundary, so verify that it
    # did not change while the remote lease and radio observations were in flight.
    _require_claim(claim_path, run_id=run_id, selection=selection)
    refreshed_claim = _load_json(claim_path, "active R2Lab resource claim")
    if _claim_sha256(refreshed_claim) != claim_digest:
        raise R2LabResourceError(
            "active R2Lab resource claim changed during authority verification"
        )

    return PhysicalStartAuthority(
        run_id=run_id,
        radio=selection.radio,
        ue=selection.ue,
        ue_kind=selection.ue_kind,
        claim_sha256=claim_digest,
        lease_verified=True,
        radio_state=observed.state.value,
    ).validate()


@dataclass(frozen=True)
class R2LabResult:
    run_id: str
    run_directory: Path
    manifest_path: Path
    log_path: Path
    status: str


def _remote_runner(
    *, slice_name: str, runner: Runner
) -> Callable[[Sequence[str], int], CommandResult]:
    def run(command: Sequence[str], timeout_seconds: int) -> CommandResult:
        return runner(gateway_command(slice_name, *tuple(command)), timeout_seconds)

    return run


def _cleanup_state(state: PowerState) -> CleanupState:
    if state is PowerState.OFF:
        return CleanupState.PROVEN_OFF
    if state is PowerState.ON:
        return CleanupState.PROVEN_ON
    return CleanupState.UNKNOWN


def _pdu_cleanup_evidence(
    *, resource: str, stage: str, operation: VerifiedPduOperation
) -> CleanupEvidence:
    source = "pdu-status"
    if operation.status_transport_error:
        source = "status-transport-error"
    elif operation.mutation_transport_error:
        source = "pdu-status-after-mutation-transport-error"
    return CleanupEvidence(
        resource=resource,
        stage=stage,
        state=_cleanup_state(operation.evidence.observed_state),
        source=source,
    )


def _qfit_cleanup_evidence(
    *, resource: str, stage: str, operation: VerifiedQfitOperation
) -> CleanupEvidence:
    source = "qfit-provider-status"
    if operation.status_transport_error:
        source = "status-transport-error"
    elif operation.mutation_transport_error:
        source = "qfit-status-after-mutation-transport-error"
    return CleanupEvidence(
        resource=resource,
        stage=stage,
        state=_cleanup_state(operation.observed_state),
        source=source,
    )


def execute_prepare(
    *,
    plan: R2LabPlan,
    run_root: Path = Path(".synthran/r2lab"),
    runner: Runner = subprocess_runner,
    sleeper: Sleeper = time.sleep,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    power_settle_seconds: float = DEFAULT_POWER_SETTLE_SECONDS,
    reachability_attempts: int = DEFAULT_REACHABILITY_ATTEMPTS,
    reachability_delay_seconds: float = DEFAULT_REACHABILITY_DELAY_SECONDS,
    progress: TextIO | None = None,
) -> R2LabResult:
    """Claim and prove one exact R2Lab radio/UE pair under an active lease."""

    timeout_seconds = _validate_timeout(timeout_seconds)
    if power_settle_seconds < 0 or reachability_delay_seconds < 0:
        raise R2LabResourceError("R2Lab wait intervals must not be negative")
    if reachability_attempts < 1 or reachability_attempts > 60:
        raise R2LabResourceError("R2Lab reachability attempts must be between 1 and 60")

    run_root = run_root.resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    run_directory = run_root / plan.run_id
    try:
        run_directory.mkdir()
    except FileExistsError as exc:
        raise R2LabResourceError("R2Lab run directory already exists; choose a new run ID") from exc

    manifest_path = run_directory / "manifest.json"
    log_path = run_directory / "r2lab.log"
    claim_path = _claim_path(run_root)
    log_lines: list[str] = []
    claim_held = False
    provider = _remote_runner(slice_name=plan.selection.slice_name, runner=runner)

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] {message}", file=progress, flush=True)

    def write_manifest(status: str, failure_stage: str | None = None) -> None:
        _atomic_json(
            manifest_path,
            _manifest_payload(
                run_id=plan.run_id,
                selection=plan.selection,
                status=status,
                updated_at=_utc_now(),
                claim_held=claim_held,
                failure_stage=failure_stage,
            ),
        )

    def finish_log() -> None:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")

    def fail(stage: str, message: str) -> None:
        log_lines.append(f"{stage}: FAIL - {message}")
        write_manifest("failed", stage)
        finish_log()

    def remote_required(stage: str, *command: str) -> CommandResult:
        report(f"{stage}: running...")
        try:
            result = runner(
                gateway_command(plan.selection.slice_name, *command), timeout_seconds
            )
        except (R2LabResourceError, OSError) as exc:
            report(f"{stage}: FAILED")
            fail(stage, "gateway command could not complete")
            raise R2LabResourceError(
                f"R2Lab stage {stage} failed; see the sanitized run log"
            ) from exc
        if result.returncode != 0:
            report(f"{stage}: FAILED")
            fail(stage, "gateway command returned nonzero")
            raise R2LabResourceError(
                f"R2Lab stage {stage} failed; see the sanitized run log"
            )
        log_lines.append(f"{stage}: OK")
        report(f"{stage}: OK")
        return result

    def require_lease(stage: str) -> None:
        remote_required(stage, "rhubarbe", "leases", "--check")

    def require_pdu(stage: str, resource: str, requested: PowerState) -> None:
        report(f"{stage}: running...")
        try:
            operation = execute_verified_pdu_transition(
                resource=resource,
                requested_state=requested,
                runner=provider,
                timeout_seconds=timeout_seconds,
            )
        except (R2LabPowerStateError, R2LabResourceError, OSError) as exc:
            report(f"{stage}: FAILED")
            fail(stage, "provider state could not be resolved")
            raise R2LabResourceError(
                f"R2Lab stage {stage} failed; selected resource state is unresolved"
            ) from exc
        if not operation.confirmed:
            report(f"{stage}: FAILED")
            fail(stage, f"provider did not prove requested {requested.value} state")
            raise R2LabResourceError(
                f"R2Lab stage {stage} failed; selected resource state is unresolved"
            )
        log_lines.append(
            f"{stage}: OK - state={operation.evidence.observed_state.value}"
        )
        report(f"{stage}: OK")

    def require_qfit(stage: str, requested: PowerState) -> None:
        report(f"{stage}: running...")
        try:
            operation = execute_verified_qfit_transition(
                qfit=plan.selection.ue,
                requested_state=requested,
                runner=provider,
                timeout_seconds=timeout_seconds,
            )
        except (R2LabQfitStateError, R2LabResourceError, OSError, ValueError) as exc:
            report(f"{stage}: FAILED")
            fail(stage, "provider qfit state could not be resolved")
            raise R2LabResourceError(
                f"R2Lab stage {stage} failed; selected qfit state is unresolved"
            ) from exc
        if not operation.confirmed:
            report(f"{stage}: FAILED")
            fail(stage, f"provider did not prove requested {requested.value} state")
            raise R2LabResourceError(
                f"R2Lab stage {stage} failed; selected qfit state is unresolved"
            )
        log_lines.append(f"{stage}: OK - state={operation.observed_state.value}")
        report(f"{stage}: OK")

    write_manifest("running")
    require_lease("lease-check")
    try:
        _write_claim(claim_path, run_id=plan.run_id, selection=plan.selection)
    except (OSError, R2LabResourceError) as exc:
        fail("resource-claim", "unable to acquire the workspace resource claim")
        raise R2LabResourceError("unable to claim R2Lab resources safely") from exc
    claim_held = True
    write_manifest("running")
    log_lines.append("resource-claim: OK")

    require_lease("lease-before-radio")
    require_pdu("radio-power-on", plan.selection.radio, PowerState.ON)

    require_lease("lease-before-ue-off")
    if plan.selection.ue_kind == "qhat":
        require_pdu("ue-power-off", plan.selection.ue, PowerState.OFF)
    else:
        require_qfit("ue-power-off", PowerState.OFF)

    sleeper(power_settle_seconds)
    require_lease("lease-before-ue-on")
    if plan.selection.ue_kind == "qhat":
        require_pdu("ue-power-on", plan.selection.ue, PowerState.ON)
    else:
        require_qfit("ue-power-on", PowerState.ON)

    report("ue-reachability: running...")
    reachable = False
    for attempt in range(1, reachability_attempts + 1):
        try:
            probe = runner(
                gateway_command(
                    plan.selection.slice_name,
                    "ping",
                    "-c",
                    "1",
                    "-W",
                    "1",
                    plan.selection.ue,
                ),
                timeout_seconds,
            )
        except (R2LabResourceError, OSError) as exc:
            report("ue-reachability: FAILED")
            fail("ue-reachability", "management reachability probe could not complete")
            raise R2LabResourceError(
                "selected R2Lab UE reachability could not be verified"
            ) from exc
        if probe.returncode == 0:
            reachable = True
            log_lines.append(f"ue-reachability: OK on attempt {attempt}")
            report("ue-reachability: OK")
            break
        if attempt < reachability_attempts:
            sleeper(reachability_delay_seconds)
    if not reachable:
        report("ue-reachability: FAILED")
        fail("ue-reachability", "selected UE did not become reachable")
        raise R2LabResourceError("selected R2Lab UE did not become reachable")

    require_lease("lease-final")
    write_manifest("ready")
    finish_log()
    report("R2Lab resources: READY")
    return R2LabResult(plan.run_id, run_directory, manifest_path, log_path, "ready")


def execute_physical_gnb_start(
    *,
    run_id: str,
    slice_name: str,
    staging: PhysicalStagingResult,
    owner: str,
    reservation_id: str,
    allocation_id: str,
    known_hosts: Path,
    now: datetime,
    run_root: Path = Path(".synthran/r2lab"),
    r2lab_runner: Runner = subprocess_runner,
    cluster_runner: Runner = subprocess_runner,
    sleeper: Sleeper = time.sleep,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> PhysicalGnbStartResult:
    """Start the exact staged gNB while refreshing both provider authority boundaries."""

    timeout_seconds = _validate_timeout(timeout_seconds)
    if timeout_seconds < 30:
        raise R2LabResourceError("physical gNB start timeout must be at least 30 seconds")
    authority = authorize_physical_start(
        run_id=run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=timeout_seconds,
    )

    def refresh() -> PhysicalStartAuthority:
        return authorize_physical_start(
            run_id=run_id,
            slice_name=slice_name,
            run_root=run_root,
            runner=r2lab_runner,
            timeout_seconds=timeout_seconds,
        )

    try:
        return execute_authorized_physical_gnb_start(
            authority=authority,
            staging=staging,
            owner=owner,
            reservation_id=reservation_id,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            now=now,
            runner=cluster_runner,
            refresh_r2lab_authority=refresh,
            sleeper=sleeper,
            timeout_seconds=timeout_seconds,
        )
    except RuntimeError as exc:
        raise R2LabResourceError("physical gNB start did not satisfy the safety boundary") from exc


def execute_release(
    *,
    run_id: str,
    slice_name: str,
    run_root: Path = Path(".synthran/r2lab"),
    runner: Runner = subprocess_runner,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    progress: TextIO | None = None,
) -> R2LabResult:
    """Prove both exact resources off before releasing one local claim."""

    run_id = _validate_run(run_id)
    slice_name = _validate_slice(slice_name)
    timeout_seconds = _validate_timeout(timeout_seconds)
    run_root = run_root.resolve()
    run_directory = run_root / run_id
    manifest_path = run_directory / "manifest.json"
    log_path = run_directory / "r2lab.log"
    payload = _load_json(manifest_path, "R2Lab run manifest")
    selection = _selection_from_manifest(payload, slice_name=slice_name, run_id=run_id)
    claim_path = _claim_path(run_root)
    _require_claim(claim_path, run_id=run_id, selection=selection)
    provider = _remote_runner(slice_name=slice_name, runner=runner)

    try:
        existing_log = log_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        existing_log = ""
    except (OSError, UnicodeDecodeError) as exc:
        raise R2LabResourceError("R2Lab run log is not readable") from exc
    log_lines = [line for line in existing_log.splitlines() if line]

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] {message}", file=progress, flush=True)

    def write_manifest(
        status: str,
        failure_stage: str | None = None,
        cleanup: ReleaseAssessment | None = None,
    ) -> None:
        _atomic_json(
            manifest_path,
            _manifest_payload(
                run_id=run_id,
                selection=selection,
                status=status,
                updated_at=_utc_now(),
                claim_held=claim_path.exists(),
                failure_stage=failure_stage,
                cleanup=cleanup,
            ),
        )

    def finish_log() -> None:
        log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")

    def lease_ok(stage: str) -> bool:
        report(f"{stage}: running...")
        try:
            result = runner(
                gateway_command(slice_name, "rhubarbe", "leases", "--check"),
                timeout_seconds,
            )
        except (R2LabResourceError, OSError):
            log_lines.append(f"{stage}: FAIL - lease authority could not be verified")
            report(f"{stage}: FAILED")
            return False
        if result.returncode != 0:
            log_lines.append(f"{stage}: FAIL - active lease was not verified")
            report(f"{stage}: FAILED")
            return False
        log_lines.append(f"{stage}: OK")
        report(f"{stage}: OK")
        return True

    def unknown(resource: str, stage: str, source: str) -> CleanupEvidence:
        return CleanupEvidence(
            resource=resource,
            stage=stage,
            state=CleanupState.UNKNOWN,
            source=source,
        )

    if not lease_ok("lease-before-release"):
        assessment = release_assessment(
            ue=unknown(selection.ue, "ue-power-off-release", "authority-unavailable"),
            radio=unknown(selection.radio, "radio-power-off-release", "authority-unavailable"),
        )
        write_manifest("release-failed", "lease-before-release", assessment)
        finish_log()
        raise R2LabResourceError(
            "R2Lab release could not verify lease authority; resource claim was retained"
        )

    report("ue-power-off-release: running...")
    ue_evidence: CleanupEvidence
    if selection.ue_kind == "qhat":
        try:
            ue_operation = execute_verified_pdu_transition(
                resource=selection.ue,
                requested_state=PowerState.OFF,
                runner=provider,
                timeout_seconds=timeout_seconds,
            )
        except (R2LabPowerStateError, R2LabResourceError, OSError):
            ue_evidence = unknown(
                selection.ue,
                "ue-power-off-release",
                "provider-state-unresolved",
            )
        else:
            ue_evidence = _pdu_cleanup_evidence(
                resource=selection.ue,
                stage="ue-power-off-release",
                operation=ue_operation,
            )
    else:
        try:
            qfit_operation = execute_verified_qfit_transition(
                qfit=selection.ue,
                requested_state=PowerState.OFF,
                runner=provider,
                timeout_seconds=timeout_seconds,
            )
        except (R2LabQfitStateError, R2LabResourceError, OSError, ValueError):
            ue_evidence = unknown(
                selection.ue,
                "ue-power-off-release",
                "provider-state-unresolved",
            )
        else:
            ue_evidence = _qfit_cleanup_evidence(
                resource=selection.ue,
                stage="ue-power-off-release",
                operation=qfit_operation,
            )
    log_lines.append(
        f"ue-power-off-release: {'OK' if ue_evidence.clean else 'UNRESOLVED'} - state={ue_evidence.state.value}"
    )
    report(f"ue-power-off-release: {'OK' if ue_evidence.clean else 'UNRESOLVED'}")

    if lease_ok("lease-before-radio-off"):
        report("radio-power-off-release: running...")
        try:
            radio_operation = execute_verified_pdu_transition(
                resource=selection.radio,
                requested_state=PowerState.OFF,
                runner=provider,
                timeout_seconds=timeout_seconds,
            )
        except (R2LabPowerStateError, R2LabResourceError, OSError):
            radio_evidence = unknown(
                selection.radio,
                "radio-power-off-release",
                "provider-state-unresolved",
            )
        else:
            radio_evidence = _pdu_cleanup_evidence(
                resource=selection.radio,
                stage="radio-power-off-release",
                operation=radio_operation,
            )
        log_lines.append(
            f"radio-power-off-release: {'OK' if radio_evidence.clean else 'UNRESOLVED'} - state={radio_evidence.state.value}"
        )
        report(
            f"radio-power-off-release: {'OK' if radio_evidence.clean else 'UNRESOLVED'}"
        )
    else:
        radio_evidence = unknown(
            selection.radio,
            "radio-power-off-release",
            "authority-unavailable",
        )
        log_lines.append("radio-power-off-release: UNRESOLVED - lease authority unavailable")

    assessment = release_assessment(ue=ue_evidence, radio=radio_evidence)
    if not assessment.claim_releasable:
        write_manifest("release-failed", "cleanup-unresolved", assessment)
        finish_log()
        unresolved = ", ".join(assessment.unresolved_resources)
        raise R2LabResourceError(
            f"R2Lab release left unresolved selected resources ({unresolved}); resource claim was retained"
        )

    try:
        claim_path.unlink()
    except OSError as exc:
        write_manifest("release-failed", "resource-claim-release", assessment)
        finish_log()
        raise R2LabResourceError(
            "resources were proven off but the local R2Lab claim could not be removed"
        ) from exc

    write_manifest("released", cleanup=assessment)
    finish_log()
    report("R2Lab resources: RELEASED")
    return R2LabResult(run_id, run_directory, manifest_path, log_path, "released")
