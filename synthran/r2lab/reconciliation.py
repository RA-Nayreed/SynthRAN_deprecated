from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import TextIO

from synthran.live_preflight import CommandResult, Runner, subprocess_runner
from synthran.network.resources import SUPPORTED_NODES
from synthran.network.runtime import atomic_json, run_command
from synthran.r2lab.acceptance import (
    AcceptanceOutcome,
    PhysicalAcceptance,
    PhysicalAcceptanceStage,
    PhysicalRunEvidence,
)
from synthran.r2lab.deployment import parse_gnb_pods_json
from synthran.r2lab.foundation_topology import (
    NAMESPACE,
    RELEASE,
    RUN_LABEL,
    REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS,
    R2LabTopologyFoundationError,
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
    _cluster_ssh,
    _load_artifact,
    _scp_base,
    _sha256_file,
)
from synthran.r2lab.resources import (
    claim_selected_allocation,
    load_topology,
    verify_physical_authority,
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
    topology,
    known_hosts: Path,
    runner: Runner,
    timeout_seconds: int,
    label: str,
    *remote: str,
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


def _namespace_exists(
    topology, known_hosts: Path, runner: Runner, timeout_seconds: int
) -> bool:
    result = _cluster(
        topology,
        known_hosts,
        runner,
        timeout_seconds,
        "Open5GS namespace query",
        "kubectl",
        "get",
        "namespace",
        NAMESPACE,
        "--ignore-not-found",
        "-o",
        "name",
    )
    return bool(result.stdout.strip())


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

    exists = _namespace_exists(topology, known_hosts, cluster_runner, timeout_seconds)
    current_owner = (
        _namespace_owner(
            topology=topology,
            known_hosts=known_hosts,
            runner=cluster_runner,
            timeout_seconds=min(timeout_seconds, 60),
        )
        if exists
        else None
    )
    if current_owner not in {None, run_id}:
        raise R2LabLiveReconciliationError(
            "current Open5GS namespace belongs to another run"
        )

    healthy = False
    networks_ready = False
    ready_networks: tuple[str, ...] = ()
    if exists:
        try:
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
        except R2LabTopologyFoundationError:
            healthy = False
            networks_ready = False

    reconciled = not (exists and healthy and networks_ready)
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
        exists = _namespace_exists(topology, known_hosts, cluster_runner, timeout_seconds)
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
    if not exists or not healthy:
        raise R2LabLiveReconciliationError(
            "current Open5GS AMF/SMF/UPF set is not ready"
        )
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
    return allocation, bool(reconciled or ownership_changed)


def _accepted_render(run_root: Path, run_id: str):
    artifact = _load_artifact(run_root, run_id)
    render = run_root / run_id / "physical" / "physical-render.yaml"
    if not render.is_file() or render.is_symlink():
        raise R2LabLiveReconciliationError("accepted N3xx render is unavailable")
    if _sha256_file(render) != artifact.render_sha256:
        raise R2LabLiveReconciliationError("accepted N3xx render bytes changed")
    return artifact, render


def _deployment(
    topology, known_hosts: Path, runner: Runner, timeout_seconds: int
) -> dict[str, object] | None:
    result = _cluster(
        topology,
        known_hosts,
        runner,
        timeout_seconds,
        "current physical gNB Deployment query",
        "kubectl",
        "get",
        f"deployment/{RELEASE}",
        "-n",
        NAMESPACE,
        "--ignore-not-found",
        "-o",
        "json",
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


def _require_run_owned(payload: dict[str, object], run_id: str) -> None:
    metadata = payload.get("metadata")
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    if not isinstance(labels, dict) or labels.get(RUN_LABEL) != run_id:
        raise R2LabLiveReconciliationError(
            "current physical gNB Deployment is not owned by this run"
        )


def _zero_gnb(
    topology, known_hosts: Path, runner: Runner, timeout_seconds: int
) -> None:
    _cluster(
        topology,
        known_hosts,
        runner,
        timeout_seconds,
        "physical gNB scale-to-zero",
        "kubectl",
        "scale",
        f"deployment/{RELEASE}",
        "-n",
        NAMESPACE,
        "--replicas=0",
    )
    for attempt in range(30):
        pods = _cluster(
            topology,
            known_hosts,
            runner,
            timeout_seconds,
            "physical gNB zero-pod query",
            "kubectl",
            "get",
            "pods",
            "-n",
            NAMESPACE,
            "-l",
            GNB_SELECTOR,
            "-o",
            "json",
        )
        if parse_gnb_pods_json(pods.stdout).zero:
            return
        if attempt < 29:
            time.sleep(2)
    raise R2LabLiveReconciliationError("physical gNB did not reach zero pods")


def _replay_gnb(
    *,
    run_id: str,
    topology,
    known_hosts: Path,
    runner: Runner,
    run_root: Path,
    timeout_seconds: int,
) -> None:
    artifact, render = _accepted_render(run_root, run_id)
    current = _deployment(topology, known_hosts, runner, timeout_seconds)
    if current is not None:
        _require_run_owned(current, run_id)
        _zero_gnb(topology, known_hosts, runner, timeout_seconds)

    remote_root = f"/root/.synthran/{run_id}/n3xx"
    remote_render = f"{remote_root}/physical-render.yaml"
    _cluster(
        topology,
        known_hosts,
        runner,
        timeout_seconds,
        "N3xx resume directory",
        "mkdir",
        "-p",
        remote_root,
    )
    try:
        transfer = runner(
            (
                *_scp_base(known_hosts),
                str(render),
                f"root@{topology.core_node}:{remote_render}",
            ),
            min(timeout_seconds, 300),
        )
    except Exception as exc:
        raise R2LabLiveReconciliationError(
            "accepted N3xx render transfer could not complete"
        ) from exc
    if transfer.returncode != 0:
        raise R2LabLiveReconciliationError(
            "accepted N3xx render transfer returned nonzero"
        )
    remote_hash = _cluster(
        topology,
        known_hosts,
        runner,
        timeout_seconds,
        "accepted N3xx render verification",
        "sha256sum",
        remote_render,
    ).stdout
    if artifact.render_sha256 not in remote_hash:
        raise R2LabLiveReconciliationError(
            "remote N3xx render does not match accepted bytes"
        )
    _cluster(
        topology,
        known_hosts,
        runner,
        timeout_seconds,
        "accepted N3xx render apply",
        "kubectl",
        "apply",
        "-f",
        remote_render,
        "-n",
        NAMESPACE,
    )
    _cluster(
        topology,
        known_hosts,
        runner,
        timeout_seconds,
        "physical gNB ownership binding",
        "kubectl",
        "label",
        f"deployment/{RELEASE}",
        "-n",
        NAMESPACE,
        f"{RUN_LABEL}={run_id}",
        "--overwrite",
    )
    annotations = {
        RUN_ANNOTATION: run_id,
        PACKAGE_ANNOTATION: artifact.package_sha256,
        VALUES_ANNOTATION: artifact.values_sha256,
        RENDER_ANNOTATION: artifact.render_sha256,
    }
    _cluster(
        topology,
        known_hosts,
        runner,
        timeout_seconds,
        "physical gNB artifact binding",
        "kubectl",
        "annotate",
        f"deployment/{RELEASE}",
        "-n",
        NAMESPACE,
        *(f"{key}={value}" for key, value in annotations.items()),
        "--overwrite",
    )
    _zero_gnb(topology, known_hosts, runner, timeout_seconds)


def _n2(
    *,
    run_id: str,
    known_hosts: Path,
    run_root: Path,
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
            _report(progress, "resume-gNB/N2: current path already proven")
            return False
    except R2LabPhysicalUeError:
        pass

    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    _replay_gnb(
        run_id=run_id,
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        run_root=run_root,
        timeout_seconds=timeout_seconds,
    )
    _cluster(
        topology,
        known_hosts,
        cluster_runner,
        timeout_seconds,
        "physical gNB singleton start",
        "kubectl",
        "scale",
        f"deployment/{RELEASE}",
        "-n",
        NAMESPACE,
        "--replicas=1",
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
            _report(progress, f"resume-gNB/N2: stable N2 proven ({attempt} observations)")
            return True
        if attempt < total:
            time.sleep(n2_interval)
    try:
        _zero_gnb(topology, known_hosts, cluster_runner, timeout_seconds)
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
    prefix = evidence.acceptance.evidence[:5]
    if (
        len(prefix) != 5
        or prefix[-1].stage is not PhysicalAcceptanceStage.GNB_N2
        or any(item.outcome is not AcceptanceOutcome.PASSED for item in prefix)
    ):
        raise R2LabLiveReconciliationError(
            "accepted UE path history does not contain a valid gNB/N2 boundary"
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
        timeout_seconds=min(timeout_seconds, 300),
        progress=progress,
    )
    if state.acceptance.next_stage is not PhysicalAcceptanceStage.USER_PLANE:
        raise R2LabLiveReconciliationError(
            "current UE registration/PDU path was not re-established"
        )
    topology = load_topology(run_root=run_root, run_id=evidence.run_id).validate()
    user_plane = prove_physical_user_plane(
        evidence=state,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        peer=SUPPORTED_NODES[topology.ran_node].ip,
        run_root=run_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
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
            "live resume requires previously accepted Open5GS history"
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
            known_hosts=known_hosts,
            run_root=run_root,
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
