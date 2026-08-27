from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Mapping, TextIO

from synthran.dependencies import load_lock
from synthran.live_preflight import CommandResult, Runner, subprocess_runner
from synthran.network.resources import SUPPORTED_NODES
from synthran.network.runtime import atomic_json, run_command
from synthran.r2lab.acceptance import (
    AcceptanceOutcome,
    PhysicalAcceptance,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
)
from synthran.r2lab.deployment import materialize_locked_helm, parse_gnb_pods_json
from synthran.r2lab.foundation_topology import (
    NAMESPACE,
    RELEASE,
    RUN_LABEL,
    REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS,
    _handoff_namespace,
    _namespace_owner,
    _open5gs_ready,
    _physical_networks_ready,
    _ready_nodes,
    reconcile_open5gs_topology,
)
from synthran.r2lab.n3xx import (
    GNB_SELECTOR,
    PACKAGE_ANNOTATION,
    RENDER_ANNOTATION,
    RUN_ANNOTATION,
    VALUES_ANNOTATION,
    _checked,
    _cluster_ssh,
    _load_artifact,
    _scp_base,
    _sha256_file,
)
from synthran.r2lab.resources import (
    claim_selected_allocation,
    load_topology,
    verify_physical_authority,
    verify_selected_allocation,
)
from synthran.r2lab.ue import (
    R2LabPhysicalUeError,
    prove_physical_user_plane,
    verify_current_n3xx_n2,
)
from synthran.r2lab.ue_activation import activate_physical_ue


RESUME_SCHEMA = "synthran/r2lab-live-resume/v1alpha1"


class R2LabLiveReconciliationError(RuntimeError):
    pass


@dataclass(frozen=True)
class LiveResumeResult:
    run_id: str
    allocation_id: str
    foundation_reconciled: bool
    gnb_restarted: bool
    ue_status: str | None
    user_plane_proven: bool
    evidence_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": RESUME_SCHEMA,
            "run_id": self.run_id,
            "allocation_id": self.allocation_id,
            "foundation_reconciled": self.foundation_reconciled,
            "gnb_restarted": self.gnb_restarted,
            "ue_status": self.ue_status,
            "user_plane_proven": self.user_plane_proven,
            "evidence_path": str(self.evidence_path),
        }


def _report(progress: TextIO | None, message: str) -> None:
    if progress is not None:
        print(f"[synthran] {message}", file=progress, flush=True)


def _cluster(
    *,
    topology,
    known_hosts: Path,
    runner: Runner,
    timeout_seconds: int,
    remote: tuple[str, ...],
    label: str,
) -> CommandResult:
    try:
        result = runner(
            _cluster_ssh(topology, known_hosts, *remote),
            min(timeout_seconds, 60),
        )
    except Exception as exc:
        raise R2LabLiveReconciliationError(f"{label} could not complete") from exc
    if result.returncode != 0:
        raise R2LabLiveReconciliationError(f"{label} returned nonzero")
    return result


def _expected_annotations(run_id: str, artifact) -> dict[str, str]:
    return {
        RUN_ANNOTATION: run_id,
        PACKAGE_ANNOTATION: artifact.package_sha256,
        VALUES_ANNOTATION: artifact.values_sha256,
        RENDER_ANNOTATION: artifact.render_sha256,
    }


def _require_exact_deployment(
    *, payload: Mapping[str, object], run_id: str, artifact
) -> int:
    metadata = payload.get("metadata")
    spec = payload.get("spec")
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
    desired = spec.get("replicas") if isinstance(spec, dict) else None
    if not isinstance(labels, dict) or labels.get(RUN_LABEL) != run_id:
        raise R2LabLiveReconciliationError("current physical gNB is not owned by this run")
    expected = _expected_annotations(run_id, artifact)
    if not isinstance(annotations, dict) or any(
        annotations.get(key) != value for key, value in expected.items()
    ):
        raise R2LabLiveReconciliationError(
            "current physical gNB does not match the accepted immutable artifact"
        )
    if desired not in {0, 1}:
        raise R2LabLiveReconciliationError("current physical gNB replica state is invalid")
    return int(desired)


def _deployment(
    *, topology, known_hosts: Path, runner: Runner, timeout_seconds: int
) -> dict[str, object] | None:
    result = _cluster(
        topology=topology,
        known_hosts=known_hosts,
        runner=runner,
        timeout_seconds=timeout_seconds,
        remote=(
            "kubectl",
            "get",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            "--ignore-not-found",
            "-o",
            "json",
        ),
        label="current physical gNB Deployment query",
    )
    if not result.stdout.strip():
        return None
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise R2LabLiveReconciliationError(
            "current physical gNB Deployment returned malformed JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise R2LabLiveReconciliationError(
            "current physical gNB Deployment returned malformed JSON"
        )
    return payload


def _wait_zero(
    *, topology, known_hosts: Path, runner: Runner, timeout_seconds: int
) -> None:
    for attempt in range(30):
        result = _cluster(
            topology=topology,
            known_hosts=known_hosts,
            runner=runner,
            timeout_seconds=timeout_seconds,
            remote=(
                "kubectl",
                "get",
                "pods",
                "-n",
                NAMESPACE,
                "-l",
                GNB_SELECTOR,
                "-o",
                "json",
            ),
            label="current physical gNB pod query",
        )
        if parse_gnb_pods_json(result.stdout).zero:
            return
        if attempt < 29:
            time.sleep(2)
    raise R2LabLiveReconciliationError("current physical gNB did not reach zero pods")


def _foundation(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    timeout_seconds: int,
    progress: TextIO | None,
) -> tuple[str, bool]:
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    allocation = claim_selected_allocation(
        run_id=run_id,
        slice_name=slice_name,
        topology=topology,
        r2lab_runner=r2lab_runner,
        allocation_runner=cluster_runner,
        owner=owner,
        allocation_id=allocation_id,
        timeout_seconds=min(timeout_seconds, 300),
        run_root=run_root,
    )
    verify_physical_authority(
        run_id=run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    _ready_nodes(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )

    current_owner = _namespace_owner(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 60),
    )
    ownership_changed = False
    if current_owner != run_id:
        ownership_changed = _handoff_namespace(
            run_id=run_id,
            previous_run_id=None,
            slice_name=slice_name,
            topology=topology,
            run_root=run_root,
            known_hosts=known_hosts,
            r2lab_runner=r2lab_runner,
            cluster_runner=cluster_runner,
            timeout_seconds=min(timeout_seconds, 300),
        )

    healthy, _ = _open5gs_ready(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    networks_ready, ready_networks = _physical_networks_ready(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    reconciled = not (healthy and networks_ready)
    if reconciled:
        _report(progress, "resume-foundation: reconciling Open5GS")

        def authority() -> object:
            return verify_physical_authority(
                run_id=run_id,
                slice_name=slice_name,
                run_root=run_root,
                runner=r2lab_runner,
                timeout_seconds=min(timeout_seconds, 300),
            )

        reconcile_open5gs_topology(
            run_id=run_id,
            slice_name=slice_name,
            topology=topology,
            known_hosts=known_hosts,
            authority_verifier=authority,
            lock_path=lock_path,
            dependency_root=deps_root,
            run_root=run_root,
            repository_root=Path("."),
            runner=run_command,
            timeout_seconds=timeout_seconds,
            progress=progress,
        )
        healthy, _ = _open5gs_ready(
            topology=topology,
            known_hosts=known_hosts,
            runner=cluster_runner,
            timeout_seconds=min(timeout_seconds, 300),
        )
        networks_ready, ready_networks = _physical_networks_ready(
            topology=topology,
            known_hosts=known_hosts,
            runner=cluster_runner,
            timeout_seconds=min(timeout_seconds, 300),
        )
    if not healthy:
        raise R2LabLiveReconciliationError("current Open5GS AMF/SMF/UPF set is not ready")
    if not networks_ready:
        missing = ", ".join(
            name
            for name in REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS
            if name not in ready_networks
        )
        raise R2LabLiveReconciliationError(
            f"current physical Multus networks are missing: {missing}"
        )
    if _namespace_owner(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 60),
    ) != run_id:
        raise R2LabLiveReconciliationError(
            "current Open5GS namespace ownership is not proven"
        )
    _report(progress, "resume-foundation: current Kubernetes/Open5GS foundation proven")
    return allocation, bool(ownership_changed or reconciled)


def _local_artifact(run_root: Path, run_id: str):
    artifact = _load_artifact(run_root, run_id)
    for path, expected in (
        (artifact.package_path, artifact.package_sha256),
        (artifact.source_values_path, artifact.source_values_sha256),
        (artifact.generated_values_path, artifact.values_sha256),
    ):
        if not path.is_file() or path.is_symlink() or _sha256_file(path) != expected:
            raise R2LabLiveReconciliationError(
                "stored N3xx artifact bytes are unavailable or changed"
            )
    return artifact


def _stage_exact_gnb(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str,
    known_hosts: Path,
    lock_path: Path,
    run_root: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    timeout_seconds: int,
) -> None:
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    artifact = _local_artifact(run_root, run_id)
    current = _deployment(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=timeout_seconds,
    )
    if current is not None:
        desired = _require_exact_deployment(
            payload=current, run_id=run_id, artifact=artifact
        )
        if desired == 1:
            _cluster(
                topology=topology,
                known_hosts=known_hosts,
                runner=cluster_runner,
                timeout_seconds=timeout_seconds,
                remote=(
                    "kubectl",
                    "scale",
                    f"deployment/{RELEASE}",
                    "-n",
                    NAMESPACE,
                    "--replicas=0",
                ),
                label="resume physical gNB scale-to-zero",
            )
        _wait_zero(
            topology=topology,
            known_hosts=known_hosts,
            runner=cluster_runner,
            timeout_seconds=timeout_seconds,
        )
        return

    verify_physical_authority(
        run_id=run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    verify_selected_allocation(
        topology=topology,
        runner=cluster_runner,
        owner=owner,
        allocation_id=allocation_id,
        timeout_seconds=min(timeout_seconds, 300),
    )
    helm = materialize_locked_helm(
        lock=load_lock(lock_path),
        destination=run_root / run_id / "tools",
        timeout_seconds=min(timeout_seconds, 300),
    )
    remote_root = f"/root/.synthran/{run_id}/n3xx"
    remote_package = f"{remote_root}/{artifact.package_path.name}"
    remote_source = f"{remote_root}/{artifact.source_values_path.name}"
    remote_generated = f"{remote_root}/{artifact.generated_values_path.name}"
    remote_helm = f"{remote_root}/helm"
    _checked(
        cluster_runner,
        _cluster_ssh(topology, known_hosts, "mkdir", "-p", remote_root),
        min(timeout_seconds, 60),
        "resume N3xx artifact directory",
    )
    _checked(
        cluster_runner,
        (
            *_scp_base(known_hosts),
            str(artifact.package_path),
            str(artifact.source_values_path),
            str(artifact.generated_values_path),
            str(helm),
            f"root@{topology.core_node}:{remote_root}/",
        ),
        timeout_seconds,
        "resume N3xx artifact transfer",
    )
    hashes = _checked(
        cluster_runner,
        _cluster_ssh(
            topology,
            known_hosts,
            "sha256sum",
            remote_package,
            remote_source,
            remote_generated,
            remote_helm,
        ),
        min(timeout_seconds, 60),
        "resume N3xx artifact verification",
    ).stdout
    for expected in (
        artifact.package_sha256,
        artifact.source_values_sha256,
        artifact.values_sha256,
        _sha256_file(helm),
    ):
        if expected not in hashes:
            raise R2LabLiveReconciliationError(
                "remote N3xx resume artifact bytes do not match local evidence"
            )
    _checked(
        cluster_runner,
        _cluster_ssh(topology, known_hosts, "chmod", "0755", remote_helm),
        min(timeout_seconds, 60),
        "resume Helm permission preparation",
    )
    _checked(
        cluster_runner,
        _cluster_ssh(
            topology,
            known_hosts,
            remote_helm,
            "upgrade",
            "--install",
            RELEASE,
            remote_package,
            "--namespace",
            NAMESPACE,
            "--values",
            remote_source,
            "--values",
            remote_generated,
            "--wait",
            "--atomic",
            "--timeout",
            "120s",
        ),
        timeout_seconds,
        "resume stopped N3xx staging",
    )
    _checked(
        cluster_runner,
        _cluster_ssh(
            topology,
            known_hosts,
            "kubectl",
            "label",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            f"{RUN_LABEL}={run_id}",
            "--overwrite",
        ),
        min(timeout_seconds, 60),
        "resume physical gNB ownership binding",
    )
    _checked(
        cluster_runner,
        _cluster_ssh(
            topology,
            known_hosts,
            "kubectl",
            "annotate",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            *(
                f"{key}={value}"
                for key, value in _expected_annotations(run_id, artifact).items()
            ),
            "--overwrite",
        ),
        min(timeout_seconds, 60),
        "resume physical gNB artifact binding",
    )
    current = _deployment(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=timeout_seconds,
    )
    if current is None or _require_exact_deployment(
        payload=current, run_id=run_id, artifact=artifact
    ) != 0:
        raise R2LabLiveReconciliationError(
            "resume staging did not prove an exact zero-replica gNB"
        )
    _wait_zero(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=timeout_seconds,
    )


def _n2(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str,
    known_hosts: Path,
    lock_path: Path,
    run_root: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    timeout_seconds: int,
    n2_attempts: int,
    n2_convergence_attempts: int,
    n2_interval: float,
    progress: TextIO | None,
) -> bool:
    try:
        if verify_current_n3xx_n2(
            run_id=run_id,
            known_hosts=known_hosts,
            run_root=run_root,
            runner=cluster_runner,
            timeout_seconds=min(timeout_seconds, 60),
        ):
            _report(progress, "resume-gNB/N2: current stable path already present")
            return False
    except R2LabPhysicalUeError:
        pass

    _stage_exact_gnb(
        run_id=run_id,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        lock_path=lock_path,
        run_root=run_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        timeout_seconds=timeout_seconds,
    )
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    _cluster(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=timeout_seconds,
        remote=(
            "kubectl",
            "scale",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            "--replicas=1",
        ),
        label="resume physical gNB singleton start",
    )
    total = n2_attempts + n2_convergence_attempts - 1
    if n2_attempts < 1 or n2_convergence_attempts < 1 or total > 120:
        raise R2LabLiveReconciliationError("N2 resume proof counts are invalid")
    consecutive = 0
    for attempt in range(1, total + 1):
        try:
            proven = verify_current_n3xx_n2(
                run_id=run_id,
                known_hosts=known_hosts,
                run_root=run_root,
                runner=cluster_runner,
                timeout_seconds=min(timeout_seconds, 60),
            )
        except R2LabPhysicalUeError:
            proven = False
        consecutive = consecutive + 1 if proven else 0
        if consecutive >= n2_attempts:
            _report(progress, f"resume-gNB/N2: stable current N2 proven ({attempt} observations)")
            return True
        if attempt < total:
            time.sleep(n2_interval)
    try:
        _cluster(
            topology=topology,
            known_hosts=known_hosts,
            runner=cluster_runner,
            timeout_seconds=timeout_seconds,
            remote=(
                "kubectl",
                "scale",
                f"deployment/{RELEASE}",
                "-n",
                NAMESPACE,
                "--replicas=0",
            ),
            label="failed resume gNB scale-to-zero",
        )
    except R2LabLiveReconciliationError:
        pass
    raise R2LabLiveReconciliationError(
        "stable current gNB/N2 proof was not re-established"
    )


def _synthetic_gnb_boundary(evidence: PhysicalRunEvidence) -> PhysicalRunEvidence:
    if evidence.staged is None or evidence.gnb_start is None:
        raise R2LabLiveReconciliationError(
            "accepted UE path history is missing immutable gNB evidence"
        )
    prefix = tuple(
        item
        for item in evidence.acceptance.evidence
        if item.stage.value
        in {
            PhysicalAcceptanceStage.RESOURCE_AUTHORITY.value,
            PhysicalAcceptanceStage.SLICES_FOUNDATION.value,
            PhysicalAcceptanceStage.KUBERNETES.value,
            PhysicalAcceptanceStage.OPEN5GS.value,
            PhysicalAcceptanceStage.GNB_N2.value,
        }
    )
    if len(prefix) != 5 or prefix[-1].stage is not PhysicalAcceptanceStage.GNB_N2:
        raise R2LabLiveReconciliationError(
            "accepted UE path history does not include gNB/N2"
        )
    if any(item.outcome is not AcceptanceOutcome.PASSED for item in prefix):
        raise R2LabLiveReconciliationError(
            "accepted UE path history contains a failed prerequisite"
        )
    return PhysicalRunEvidence(
        run_id=evidence.run_id,
        staged=evidence.staged,
        gnb_start=evidence.gnb_start,
        acceptance=PhysicalAcceptance(evidence=prefix),
    )


def _ue_path(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    owner: str,
    allocation_id: str,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    timeout_seconds: int,
    progress: TextIO | None,
) -> tuple[str, bool]:
    state, activation = activate_physical_ue(
        evidence=_synthetic_gnb_boundary(evidence),
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        lock_path=lock_path,
        deps_root=deps_root,
        run_root=run_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        evidence_path=None,
        activation_evidence_path=None,
        timeout_seconds=min(timeout_seconds, 300),
        progress=progress,
    )
    if state.acceptance.next_stage is not PhysicalAcceptanceStage.USER_PLANE:
        raise R2LabLiveReconciliationError(
            "current UE registration/PDU path was not re-established"
        )
    topology = load_topology(run_root=run_root, run_id=evidence.run_id).validate()
    peer = SUPPORTED_NODES[topology.ran_node].ip
    user_plane = prove_physical_user_plane(
        evidence=state,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        peer=peer,
        run_root=run_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        evidence_path=None,
        timeout_seconds=min(timeout_seconds, 300),
    )
    if not user_plane.probe.proven:
        raise R2LabLiveReconciliationError(
            "current physical user plane was not re-proven"
        )
    _report(progress, "resume-UE path: registration, PDU and user plane re-proven")
    return activation.status, True


def reconcile_live_resume(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    r2lab_runner: Runner = subprocess_runner,
    cluster_runner: Runner = subprocess_runner,
    timeout_seconds: int = 1800,
    n2_attempts: int = 12,
    n2_convergence_attempts: int = 12,
    n2_interval: float = 5.0,
    progress: TextIO | None = None,
) -> LiveResumeResult:
    run_root = run_root.expanduser().resolve()
    known_hosts = known_hosts.expanduser().resolve()
    evidence_path = run_root / run_id / "physical-run.json"
    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if evidence.acceptance.accepted:
        raise R2LabLiveReconciliationError(
            "accepted physical run does not require live resume reproof"
        )
    if evidence.acceptance.outcome_for(
        PhysicalAcceptanceStage.OPEN5GS
    ) is not AcceptanceOutcome.PASSED:
        raise R2LabLiveReconciliationError(
            "live resume reproof requires previously accepted Open5GS foundation"
        )

    _report(progress, "resume: re-proving current state behind historical acceptance")
    allocation, foundation_reconciled = _foundation(
        run_id=run_id,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        lock_path=lock_path,
        deps_root=deps_root,
        run_root=run_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        timeout_seconds=timeout_seconds,
        progress=progress,
    )

    gnb_restarted = False
    if evidence.acceptance.outcome_for(
        PhysicalAcceptanceStage.GNB_N2
    ) is AcceptanceOutcome.PASSED:
        gnb_restarted = _n2(
            run_id=run_id,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation,
            known_hosts=known_hosts,
            lock_path=lock_path,
            run_root=run_root,
            r2lab_runner=r2lab_runner,
            cluster_runner=cluster_runner,
            timeout_seconds=timeout_seconds,
            n2_attempts=n2_attempts,
            n2_convergence_attempts=n2_convergence_attempts,
            n2_interval=n2_interval,
            progress=progress,
        )

    ue_status: str | None = None
    user_plane_proven = False
    if evidence.acceptance.outcome_for(
        PhysicalAcceptanceStage.USER_PLANE
    ) is AcceptanceOutcome.PASSED:
        ue_status, user_plane_proven = _ue_path(
            evidence=evidence,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation,
            known_hosts=known_hosts,
            lock_path=lock_path,
            deps_root=deps_root,
            run_root=run_root,
            r2lab_runner=r2lab_runner,
            cluster_runner=cluster_runner,
            timeout_seconds=timeout_seconds,
            progress=progress,
        )

    resume_path = run_root / run_id / "physical" / "live-resume.json"
    atomic_json(
        resume_path,
        {
            "schema": RESUME_SCHEMA,
            "run_id": run_id,
            "observed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "historical_acceptance_unchanged": True,
            "foundation_reconciled": foundation_reconciled,
            "gnb_restarted": gnb_restarted,
            "ue_status": ue_status,
            "user_plane_proven": user_plane_proven,
        },
    )
    _report(progress, "resume: current live prerequisites re-proven")
    return LiveResumeResult(
        run_id=run_id,
        allocation_id=allocation,
        foundation_reconciled=foundation_reconciled,
        gnb_restarted=gnb_restarted,
        ue_status=ue_status,
        user_plane_proven=user_plane_proven,
        evidence_path=resume_path,
    )
