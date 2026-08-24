"""Public composition boundary for stopped gNB staging and N2 proof."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import shutil
import tempfile
import time
from typing import Mapping

from synthran.dependencies import DependencyLock, load_lock
from synthran.live_preflight import subprocess_runner
from synthran.r2lab.acceptance import (
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
    R2LabAcceptanceError,
)
from synthran.r2lab.controller import (
    authorize_physical_start,
    execute_physical_gnb_start,
    subprocess_runner as r2lab_subprocess_runner,
)
from synthran.r2lab.deployment import (
    PHYSICAL_CHART_PATH,
    PhysicalChartBindings,
    PhysicalGnbStartResult,
    PhysicalStagingResult,
    R2LabPhysicalArtifactError,
    R2LabPhysicalChartError,
    R2LabPhysicalHelmError,
    R2LabPhysicalStagingError,
    R2LabPhysicalStartError,
    build_physical_chart_bundle,
    build_physical_deployment_plan,
    discover_physical_chart_bindings,
    execute_authorized_physical_gnb_stop,
    execute_stopped_physical_staging,
    materialize_physical_chart_workspace,
    package_physical_chart,
    render_physical_chart_offline,
)
from synthran.r2lab.runtime import (
    GnbN2Evidence,
    PhysicalGnbN2VerificationResult,
    R2LabRuntimeVerificationError,
    execute_physical_gnb_n2_verification,
)


class R2LabPhysicalGnbError(RuntimeError):
    """Raised when public physical gNB composition cannot proceed safely."""


DEFAULT_N2_STABILITY_OBSERVATIONS = 6


@dataclass(frozen=True)
class PhysicalGnbStagingSummary:
    run_id: str
    physical_directory: Path
    evidence_path: Path
    staging: PhysicalStagingResult

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "status": "staged-stopped",
            "next_stage": "singleton-start",
            "physical_directory": str(self.physical_directory),
            "evidence_path": str(self.evidence_path),
            "staging": self.staging.to_dict(),
        }


@dataclass(frozen=True)
class PhysicalGnbN2Summary:
    run_id: str
    evidence_path: Path
    observation_path: Path
    verification: PhysicalGnbN2VerificationResult

    @property
    def proven(self) -> bool:
        return self.verification.proven

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "status": "gnb-n2-ready" if self.proven else "gnb-n2-not-proven",
            "next_stage": "ue-management" if self.proven else None,
            "attempts": self.verification.attempts,
            "stability": {
                "consecutive_proofs": self.verification.consecutive_proofs,
                "required_consecutive_proofs": (
                    self.verification.required_consecutive_proofs
                ),
                "proven": self.verification.proven,
            },
            "evidence_path": str(self.evidence_path),
            "observation_path": str(self.observation_path),
            "gnb_n2": self.verification.gnb_n2.to_dict(),
        }


def _read_json(path: Path, label: str) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise R2LabPhysicalGnbError(f"{label} is missing") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2LabPhysicalGnbError(f"{label} could not be read") from exc
    if not isinstance(payload, dict):
        raise R2LabPhysicalGnbError(f"{label} must be one JSON object")
    return payload


def _write_json(path: Path, payload: Mapping[str, object]) -> Path:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(dict(payload), indent=2, sort_keys=True) + "\n"
    temporary_path: Path | None = None
    try:
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
    except OSError as exc:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        raise R2LabPhysicalGnbError("physical gNB evidence could not be persisted") from exc
    return path


def _physical_paths(run_root: Path, run_id: str) -> tuple[Path, Path, Path]:
    run_directory = run_root.expanduser().resolve() / run_id
    return (
        run_directory / "physical-run.json",
        run_directory / "physical",
        run_directory,
    )


def _require_staging_boundary(evidence: PhysicalRunEvidence) -> None:
    if evidence.acceptance.next_stage is not PhysicalAcceptanceStage.GNB_N2:
        raise R2LabPhysicalGnbError(
            "physical gNB staging requires accepted resource, SLICES, Kubernetes, and Open5GS evidence"
        )
    if evidence.gnb_start is not None:
        raise R2LabPhysicalGnbError("physical gNB has already been started for this run")


def _chart_checkout(lock: DependencyLock, deps_root: Path) -> tuple[Path, str]:
    dependency = next(
        (item for item in lock.git if item.name == "srsran_helm"),
        None,
    )
    if dependency is None:
        raise R2LabPhysicalGnbError("dependency lock is missing srsran_helm")
    return deps_root.expanduser().resolve() / dependency.checkout, dependency.commit


def _verify_checkout(checkout: Path, commit: str, runner) -> None:
    if not checkout.is_dir():
        raise R2LabPhysicalGnbError(
            "locked srsran_helm checkout is missing; run dependency synchronization first"
        )
    try:
        head = runner(("git", "-C", str(checkout), "rev-parse", "HEAD"), 30)
        status = runner(("git", "-C", str(checkout), "status", "--short"), 30)
    except Exception as exc:
        raise R2LabPhysicalGnbError("locked chart checkout could not be inspected") from exc
    if head.returncode != 0 or head.stdout.strip() != commit:
        raise R2LabPhysicalGnbError("srsran_helm checkout is not at the locked commit")
    if status.returncode != 0 or status.stdout.strip():
        raise R2LabPhysicalGnbError("srsran_helm checkout is not clean")


def _recover_staging(
    *,
    evidence: PhysicalRunEvidence,
    evidence_path: Path,
    physical_directory: Path,
) -> PhysicalGnbStagingSummary:
    staging = PhysicalStagingResult.from_dict(
        _read_json(physical_directory / "physical-staging.json", "physical staging result")
    )
    if staging.run_id != evidence.run_id:
        raise R2LabPhysicalGnbError("stored physical staging belongs to another run")
    if evidence.staged is None:
        evidence = evidence.bind_staging(staging.to_dict())
        evidence.write_json(evidence_path)
    elif (
        evidence.staged.package_sha256 != staging.package_sha256
        or evidence.staged.values_sha256 != staging.values_sha256
        or evidence.staged.render_sha256 != staging.render_sha256
    ):
        raise R2LabPhysicalGnbError(
            "stored physical staging does not match immutable run evidence"
        )
    return PhysicalGnbStagingSummary(
        run_id=evidence.run_id,
        physical_directory=physical_directory,
        evidence_path=evidence_path,
        staging=staging,
    )


def execute_physical_gnb_staging(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    reservation_id: str | None,
    allocation_id: str | None,
    known_hosts: Path,
    now: datetime,
    bindings: PhysicalChartBindings | None = None,
    lock_path: Path = Path("dependencies.lock.yml"),
    deps_root: Path = Path(".deps"),
    run_root: Path = Path(".synthran/r2lab"),
    runner=subprocess_runner,
    r2lab_runner=r2lab_subprocess_runner,
    timeout_seconds: int = 120,
) -> PhysicalGnbStagingSummary:
    """Render, package, and stage one immutable physical chart at zero pods."""

    evidence_path, physical_directory, run_directory = _physical_paths(
        run_root, run_id
    )
    try:
        evidence = PhysicalRunEvidence.read_json(evidence_path)
        _require_staging_boundary(evidence)
        initial_authority = authorize_physical_start(
            run_id=run_id,
            slice_name=slice_name,
            run_root=run_root,
            runner=r2lab_runner,
            timeout_seconds=timeout_seconds,
        )
        if physical_directory.exists():
            return _recover_staging(
                evidence=evidence,
                evidence_path=evidence_path,
                physical_directory=physical_directory,
            )

        lock = load_lock(lock_path.expanduser().resolve())
        checkout, commit = _chart_checkout(lock, deps_root)
        _verify_checkout(checkout, commit, runner)
        bindings = (
            bindings.validate()
            if bindings is not None
            else discover_physical_chart_bindings(
                known_hosts=known_hosts,
                runner=runner,
                timeout_seconds=min(timeout_seconds, 300),
            )
        )
        plan = build_physical_deployment_plan(run_id=run_id)
        bundle = build_physical_chart_bundle(
            lock=lock,
            plan=plan,
            bindings=bindings,
        )

        temporary_directory = Path(
            tempfile.mkdtemp(prefix=".physical-", dir=run_directory)
        )
        try:
            isolated_root = temporary_directory / "workspace"
            destination_chart = isolated_root / PHYSICAL_CHART_PATH
            destination_chart.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(checkout / PHYSICAL_CHART_PATH, destination_chart)
            workspace = materialize_physical_chart_workspace(
                checkout_root=isolated_root,
                lock=lock,
                bundle=bundle,
            )
            rendered_text, render_evidence = render_physical_chart_offline(
                lock=lock,
                bundle=bundle,
                workspace=workspace,
                runner=runner,
                timeout_seconds=min(timeout_seconds, 300),
            )
            artifact = package_physical_chart(
                workspace=workspace,
                run_id=run_id,
                destination=temporary_directory / "artifacts",
            )
            _write_json(temporary_directory / "physical-chart.json", bundle.to_dict())
            (temporary_directory / "physical-render.yaml").write_text(
                rendered_text,
                encoding="utf-8",
                newline="\n",
            )
            _write_json(
                temporary_directory / "physical-render-evidence.json",
                render_evidence.to_dict(),
            )
            _write_json(
                temporary_directory / "physical-artifact.json",
                artifact.to_dict(),
            )
            staging = execute_stopped_physical_staging(
                lock=lock,
                artifact=artifact,
                render_evidence=render_evidence,
                run_id=run_id,
                owner=owner,
                reservation_id=reservation_id,
                allocation_id=allocation_id,
                known_hosts=known_hosts,
                now=now,
                runner=runner,
                timeout_seconds=timeout_seconds,
            )
            refreshed_authority = authorize_physical_start(
                run_id=run_id,
                slice_name=slice_name,
                run_root=run_root,
                runner=r2lab_runner,
                timeout_seconds=timeout_seconds,
            )
            if refreshed_authority != initial_authority:
                raise R2LabPhysicalGnbError(
                    "R2Lab authority changed during stopped physical staging"
                )
            _write_json(
                temporary_directory / "physical-staging.json",
                staging.to_dict(),
            )
            temporary_directory.replace(physical_directory)
        finally:
            if temporary_directory.exists():
                shutil.rmtree(temporary_directory)

        evidence = evidence.bind_staging(staging.to_dict())
        evidence.write_json(evidence_path)
        return PhysicalGnbStagingSummary(
            run_id=run_id,
            physical_directory=physical_directory,
            evidence_path=evidence_path,
            staging=staging,
        )
    except (
        R2LabAcceptanceError,
        R2LabPhysicalArtifactError,
        R2LabPhysicalChartError,
        R2LabPhysicalHelmError,
        R2LabPhysicalStagingError,
        OSError,
    ) as exc:
        if isinstance(exc, R2LabPhysicalGnbError):
            raise
        raise R2LabPhysicalGnbError(str(exc)) from exc


def load_expected_gnb_n2_peer(physical_directory: Path) -> str:
    bundle = _read_json(
        physical_directory / "physical-chart.json", "physical chart evidence"
    )
    values = bundle.get("values")
    peer = values.get("gnbIp") if isinstance(values, dict) else None
    if not isinstance(peer, str) or not peer:
        raise R2LabPhysicalGnbError(
            "physical chart evidence does not contain the expected gNB N2 peer"
        )
    return peer


def _bind_existing_start(
    *,
    evidence: PhysicalRunEvidence,
    evidence_path: Path,
    start_path: Path,
) -> PhysicalRunEvidence:
    started = PhysicalGnbStartResult.from_dict(
        _read_json(start_path, "physical gNB start result")
    )
    if evidence.gnb_start is None:
        evidence = evidence.bind_gnb_start(started.to_dict())
        evidence.write_json(evidence_path)
    return evidence


def _stop_gnb_after_unsuccessful_proof(
    *,
    staging: PhysicalStagingResult,
    owner: str,
    reservation_id: str | None,
    allocation_id: str | None,
    known_hosts: Path,
    now: datetime,
    cluster_runner,
    timeout_seconds: int,
    stop_path: Path,
) -> None:
    stopped = execute_authorized_physical_gnb_stop(
        staging=staging,
        owner=owner,
        reservation_id=reservation_id,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        now=now,
        runner=cluster_runner,
        sleeper=time.sleep,
        timeout_seconds=timeout_seconds,
    )
    _write_json(stop_path, stopped.to_dict())


def execute_physical_gnb_n2_acceptance(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    reservation_id: str | None,
    allocation_id: str | None,
    known_hosts: Path,
    now: datetime,
    run_root: Path = Path(".synthran/r2lab"),
    r2lab_runner=r2lab_subprocess_runner,
    cluster_runner=subprocess_runner,
    timeout_seconds: int = 120,
    attempts: int = 12,
    poll_interval_seconds: float = 5.0,
) -> PhysicalGnbN2Summary:
    """Start one artifact-bound gNB and persist a bounded N2 proof."""

    evidence_path, physical_directory, _run_directory = _physical_paths(
        run_root, run_id
    )
    start_path = physical_directory / "physical-gnb-start.json"
    observation_path = physical_directory / "physical-gnb-n2.json"
    stop_path = physical_directory / "physical-gnb-stop.json"
    staging: PhysicalStagingResult | None = None
    started_bound = False
    try:
        evidence = PhysicalRunEvidence.read_json(evidence_path)
        if evidence.staged is None:
            raise R2LabPhysicalGnbError(
                "physical gNB start requires immutable stopped staging evidence"
            )
        staging = PhysicalStagingResult.from_dict(
            _read_json(
                physical_directory / "physical-staging.json",
                "physical staging result",
            )
        )

        if evidence.acceptance.next_stage is PhysicalAcceptanceStage.UE_MANAGEMENT:
            observation = GnbN2Evidence.from_dict(
                _read_json(observation_path, "physical gNB/N2 evidence")
            )
            verification = PhysicalGnbN2VerificationResult(
                evidence=evidence,
                gnb_n2=observation,
                attempts=0,
            )
            return PhysicalGnbN2Summary(
                run_id=run_id,
                evidence_path=evidence_path,
                observation_path=observation_path,
                verification=verification,
            )
        if evidence.acceptance.next_stage is not PhysicalAcceptanceStage.GNB_N2:
            raise R2LabPhysicalGnbError(
                "physical run evidence is not at the gNB/N2 acceptance boundary"
            )

        started_bound = start_path.exists() or evidence.gnb_start is not None
        if start_path.exists():
            evidence = _bind_existing_start(
                evidence=evidence,
                evidence_path=evidence_path,
                start_path=start_path,
            )
        elif evidence.gnb_start is not None:
            raise R2LabPhysicalGnbError(
                "bound gNB start evidence is missing its persisted start result"
            )
        else:
            started = execute_physical_gnb_start(
                run_id=run_id,
                slice_name=slice_name,
                staging=staging,
                owner=owner,
                reservation_id=reservation_id,
                allocation_id=allocation_id,
                known_hosts=known_hosts,
                now=now,
                run_root=run_root,
                r2lab_runner=r2lab_runner,
                cluster_runner=cluster_runner,
                timeout_seconds=timeout_seconds,
            )
            started_bound = True
            _write_json(start_path, started.to_dict())
            evidence = evidence.bind_gnb_start(started.to_dict())
            evidence.write_json(evidence_path)

        verification = execute_physical_gnb_n2_verification(
            evidence=evidence,
            slice_name=slice_name,
            run_root=run_root,
            known_hosts=known_hosts,
            r2lab_runner=r2lab_runner,
            cluster_runner=cluster_runner,
            expected_gnb_n2_peer=load_expected_gnb_n2_peer(physical_directory),
            evidence_path=evidence_path,
            timeout_seconds=min(timeout_seconds, 60),
            attempts=attempts,
            required_consecutive_proofs=min(
                DEFAULT_N2_STABILITY_OBSERVATIONS,
                attempts,
            ),
            poll_interval_seconds=poll_interval_seconds,
        )
        _write_json(observation_path, verification.gnb_n2.to_dict())
        if not verification.proven:
            _stop_gnb_after_unsuccessful_proof(
                staging=staging,
                owner=owner,
                reservation_id=reservation_id,
                allocation_id=allocation_id,
                known_hosts=known_hosts,
                now=now,
                cluster_runner=cluster_runner,
                timeout_seconds=timeout_seconds,
                stop_path=stop_path,
            )
        return PhysicalGnbN2Summary(
            run_id=run_id,
            evidence_path=evidence_path,
            observation_path=observation_path,
            verification=verification,
        )
    except (
        R2LabPhysicalGnbError,
        R2LabAcceptanceError,
        R2LabPhysicalStagingError,
        R2LabPhysicalStartError,
        R2LabRuntimeVerificationError,
        OSError,
    ) as exc:
        if started_bound and staging is not None and not stop_path.exists():
            try:
                _stop_gnb_after_unsuccessful_proof(
                    staging=staging,
                    owner=owner,
                    reservation_id=reservation_id,
                    allocation_id=allocation_id,
                    known_hosts=known_hosts,
                    now=now,
                    cluster_runner=cluster_runner,
                    timeout_seconds=timeout_seconds,
                    stop_path=stop_path,
                )
            except (R2LabPhysicalStartError, R2LabPhysicalGnbError) as stop_exc:
                raise R2LabPhysicalGnbError(
                    "physical gNB acceptance failed and exact scale-to-zero recovery is unresolved"
                ) from stop_exc
        if isinstance(exc, R2LabPhysicalGnbError):
            raise
        raise R2LabPhysicalGnbError(str(exc)) from exc
