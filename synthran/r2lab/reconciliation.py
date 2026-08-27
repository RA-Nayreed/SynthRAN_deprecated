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
    R2LabN3xxError,
    _checked as _n3xx_checked,
    _cluster_ssh,
    _load_artifact,
    _scp_base,
    _sha256_file,
)
from synthran.r2lab.resources import (
    R2LabTopologyResourceError,
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


def _current_time() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _run_cluster(
    *,
    topology,
    known_hosts: Path,
    runner: Runner,
    timeout_seconds: int,
    remote: tuple[str, ...],
) -> CommandResult:
    try:
        result = runner(
            _cluster_ssh(topology, known_hosts, *remote),
            timeout_seconds,
        )
    except Exception as exc:
        raise R2LabLiveReconciliationError("current Kubernetes state could not be observed") from exc
    return result


def _deployment_payload(text: str) -> dict[str, object] | None:
    text = text.strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise R2LabLiveReconciliationError("current physical gNB Deployment returned malformed JSON") from exc
    if not isinstance(payload, dict):
        raise R2LabLiveReconciliationError("current physical gNB Deployment returned malformed JSON")
    return payload


def _expected_bindings(run_id: str, artifact) -> dict[str, str]:
    return {
        RUN_ANNOTATION: run_id,
        PACKAGE_ANNOTATION: artifact.package_sha256,
        VALUES_ANNOTATION: artifact.values_sha256,
        RENDER_ANNOTATION: artifact.render_sha256,
    }


def _require_exact_deployment(
    *,
    payload: Mapping[str, object],
    run_id: str,
    artifact,
) -> int:
    metadata = payload.get("metadata")
    spec = payload.get("spec")
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
    desired = spec.get("replicas") if isinstance(spec, dict) else None
    if not isinstance(labels, dict) or labels.get(RUN_LABEL) != run_id:
        raise R2LabLiveReconciliationError("current physical gNB is not owned by this run")
    expected = _expected_bindings(run_id, artifact)
    if not isinstance(annotations, dict) or any(
        annotations.get(key) != value for key, value in expected.items()
    ):
        raise R2LabLiveReconciliationError("current physical gNB does not match the accepted immutable artifact")
    if desired not in {0, 1}:
        raise R2LabLiveReconciliationError("current physical gNB replica state is invalid")
    return int(desired)


def _wait_zero_gnb(
    *,
    topology,
    known_hosts: Path,
    runner: Runner,
    timeout_seconds: int,
    attempts: int = 30,
) -> None:
    for attempt in range(attempts):
        result = _run_cluster(
            topology=topology,
            known_hosts=known_hosts,
            runner=runner,
            timeout_seconds=min(timeout_seconds, 60),
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
        )
        if result.returncode != 0:
            raise R2LabLiveReconciliationError("current physical gNB pod query returned nonzero")
        if parse_gnb_pods_json(result.stdout).zero:
            return
        if attempt + 1 < attempts:
            time.sleep(2)
    raise R2LabLiveReconciliationError("current physical gNB did not reach zero pods")


def _quiesce_exact_gnb(
    *,
    run_id: str,
    topology,
    artifact,
    known_hosts: Path,
    runner: Runner,
    timeout_seconds: int,
) -> bool:
    query = _run_cluster(
        topology=topology,
        known_hosts=known_hosts,
        runner=runner,
        timeout_seconds=min(timeout_seconds, 60),
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
    )
    if query.returncode != 0:
        namespace = _run_cluster(
            topology=topology,
            known_hosts=known_hosts,
            runner=runner,
            timeout_seconds=min(timeout_seconds, 60),
            remote=(
                "kubectl",
                "get",
                "namespace",
                NAMESPACE,
                "--ignore-not-found",
                "-o",
                "name",
            ),
        )
        if namespace.returncode == 0 and not namespace.stdout.strip():
            return False
        raise R2LabLiveReconciliationError("current physical gNB Deployment query returned nonzero")
    payload = _deployment_payload(query.stdout)
    if payload is None:
        return False
    desired = _require_exact_deployment(payload=payload, run_id=run_id, artifact=artifact)
    if desired == 1:
        scale = _run_cluster(
            topology=topology,
            known_hosts=known_hosts,
            runner=runner,
            timeout_seconds=min(timeout_seconds, 60),
            remote=(
                "kubectl",
                "scale",
                f"deployment/{RELEASE}",
                "-n",
                NAMESPACE,
                "--replicas=0",
            ),
        )
        if scale.returncode != 0:
            raise R2LabLiveReconciliationError("current exact physical gNB could not be stopped")
    _wait_zero_gnb(
        topology=topology,
        known_hosts=known_hosts,
        runner=runner,
        timeout_seconds=timeout_seconds,
    )
    return True


def _reconcile_foundation(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    repository_root: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    timeout_seconds: int,
    progress: TextIO | None,
) -> tuple[str, bool]:
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    try:
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
    except R2LabTopologyResourceError as exc:
        raise R2LabLiveReconciliationError(str(exc)) from exc

    _report(progress, "resume-foundation: allocation authority verified")
    _ready_nodes(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    artifact = _load_artifact(run_root, run_id)
    _quiesce_exact_gnb(
        run_id=run_id,
        topology=topology,
        artifact=artifact,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=timeout_seconds,
    )
    changed = _handoff_namespace(
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
            repository_root=repository_root,
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
            name for name in REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS if name not in ready_networks
        )
        raise R2LabLiveReconciliationError(f"current physical Multus networks are missing: {missing}")
    if _namespace_owner(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=min(timeout_seconds, 60),
    ) != run_id:
        raise R2LabLiveReconciliationError("current Open5GS namespace ownership is not proven")
    _report(progress, "resume-foundation: current Kubernetes/Open5GS foundation proven")
    return allocation, bool(changed or reconciled)


def _restore_and_start_gnb(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path,
    run_root: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    timeout_seconds: int,
    required_consecutive_proofs: int,
    convergence_attempts: int,
    poll_interval_seconds: float,
    progress: TextIO | None,
) -> bool:
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    artifact = _load_artifact(run_root, run_id)
    evidence_path = run_root.expanduser().resolve() / run_id / "physical-run.json"
    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if evidence.staged is None or evidence.gnb_start is None:
        raise R2LabLiveReconciliationError("accepted gNB/N2 history is missing immutable staging/start evidence")
    if (
        evidence.staged.package_sha256 != artifact.package_sha256
        or evidence.staged.values_sha256 != artifact.values_sha256
        or evidence.staged.render_sha256 != artifact.render_sha256
    ):
        raise R2LabLiveReconciliationError("stored N3xx artifact does not match accepted staging evidence")
    for path, expected in (
        (artifact.package_path, artifact.package_sha256),
        (artifact.source_values_path, artifact.source_values_sha256),
        (artifact.generated_values_path, artifact.values_sha256),
    ):
        if not path.is_file() or path.is_symlink() or _sha256_file(path) != expected:
            raise R2LabLiveReconciliationError("stored N3xx artifact bytes are unavailable or changed")

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
    namespace = _n3xx_checked(
        cluster_runner,
        _cluster_ssh(
            topology,
            known_hosts,
            "kubectl",
            "get",
            "namespace",
            NAMESPACE,
            "-o",
            "jsonpath={.metadata.labels.synthran\\.run/id}",
        ),
        min(timeout_seconds, 60),
        "resume Open5GS namespace ownership query",
    ).stdout.strip()
    if namespace != run_id:
        raise R2LabLiveReconciliationError("current Open5GS namespace is not owned by this run")

    existing = _n3xx_checked(
        cluster_runner,
        _cluster_ssh(
            topology,
            known_hosts,
            "kubectl",
            "get",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            "--ignore-not-found",
            "-o",
            "json",
        ),
        min(timeout_seconds, 60),
        "resume physical gNB Deployment query",
    ).stdout
    payload = _deployment_payload(existing)
    if payload is not None:
        desired = _require_exact_deployment(payload=payload, run_id=run_id, artifact=artifact)
        if desired == 1:
            _n3xx_checked(
                cluster_runner,
                _cluster_ssh(
                    topology,
                    known_hosts,
                    "kubectl",
                    "scale",
                    f"deployment/{RELEASE}",
                    "-n",
                    NAMESPACE,
                    "--replicas=0",
                ),
                min(timeout_seconds, 60),
                "resume physical gNB scale-to-zero",
            )
        _wait_zero_gnb(
            topology=topology,
            known_hosts=known_hosts,
            runner=cluster_runner,
            timeout_seconds=timeout_seconds,
        )

    lock = load_lock(lock_path)
    helm = materialize_locked_helm(
        lock=lock,
        destination=run_root.expanduser().resolve() / run_id / "tools",
        timeout_seconds=min(timeout_seconds, 300),
    )
    remote_root = f"/root/.synthran/{run_id}/n3xx"
    remote_package = f"{remote_root}/{artifact.package_path.name}"
    remote_source = f"{remote_root}/{artifact.source_values_path.name}"
    remote_generated = f"{remote_root}/{artifact.generated_values_path.name}"
    remote_helm = f"{remote_root}/helm"
    _n3xx_checked(
        cluster_runner,
        _cluster_ssh(topology, known_hosts, "mkdir", "-p", remote_root),
        min(timeout_seconds, 60),
        "resume N3xx artifact directory",
    )
    _n3xx_checked(
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
    hashes = _n3xx_checked(
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
            raise R2LabLiveReconciliationError("remote N3xx resume artifact bytes do not match local evidence")
    _n3xx_checked(
        cluster_runner,
        _cluster_ssh(topology, known_hosts, "chmod", "0755", remote_helm),
        min(timeout_seconds, 60),
        "resume Helm permission preparation",
    )
    verify_physical_authority(
        run_id=run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    _n3xx_checked(
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
    _n3xx_checked(
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
    bindings = _expected_bindings(run_id, artifact)
    _n3xx_checked(
        cluster_runner,
        _cluster_ssh(
            topology,
            known_hosts,
            "kubectl",
            "annotate",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            *(f"{key}={value}" for key, value in bindings.items()),
            "--overwrite",
        ),
        min(timeout_seconds, 60),
        "resume physical gNB artifact binding",
    )
    staged_payload = _deployment_payload(
        _n3xx_checked(
            cluster_runner,
            _cluster_ssh(
                topology,
                known_hosts,
                "kubectl",
                "get",
                f"deployment/{RELEASE}",
                "-n",
                NAMESPACE,
                "-o",
                "json",
            ),
            min(timeout_seconds, 60),
            "resume staged physical gNB verification",
        ).stdout
    )
    if staged_payload is None or _require_exact_deployment(
        payload=staged_payload,
        run_id=run_id,
        artifact=artifact,
    ) != 0:
        raise R2LabLiveReconciliationError("resume staging did not prove an exact zero-replica gNB")
    _wait_zero_gnb(
        topology=topology,
        known_hosts=known_hosts,
        runner=cluster_runner,
        timeout_seconds=timeout_seconds,
    )
    _n3xx_checked(
        cluster_runner,
        _cluster_ssh(
            topology,
            known_hosts,
            "kubectl",
            "scale",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            "--replicas=1",
        ),
        min(timeout_seconds, 60),
        "resume physical gNB singleton start",
    )
    _report(progress, "resume-gNB/N2: exact accepted artifact restarted")

    if required_consecutive_proofs < 1 or convergence_attempts < 1:
        raise R2LabLiveReconciliationError("N2 resume proof counts must be positive")
    total_attempts = required_consecutive_proofs + convergence_attempts - 1
    if total_attempts > 120:
        raise R2LabLiveReconciliationError("combined N2 resume proof attempts exceed 120")
    consecutive = 0
    last_error: Exception | None = None
    for attempt in range(1, total_attempts + 1):
        try:
            proven = verify_current_n3xx_n2(
                run_id=run_id,
                known_hosts=known_hosts,
                run_root=run_root,
                runner=cluster_runner,
                timeout_seconds=min(timeout_seconds, 60),
            )
        except R2LabPhysicalUeError as exc:
            proven = False
            last_error = exc
        consecutive = consecutive + 1 if proven else 0
        if consecutive >= required_consecutive_proofs:
            _report(progress, f"resume-gNB/N2: stable current N2 proven ({attempt} observations)")
            return True
        if attempt < total_attempts:
            time.sleep(poll_interval_seconds)

    try:
        _n3xx_checked(
            cluster_runner,
            _cluster_ssh(
                topology,
                known_hosts,
                "kubectl",
                "scale",
                f"deployment/{RELEASE}",
                "-n",
                NAMESPACE,
                "--replicas=0",
            ),
            min(timeout_seconds, 60),
            "failed resume gNB scale-to-zero",
        )
    except R2LabN3xxError:
        pass
    if last_error is not None:
        raise R2LabLiveReconciliationError("stable current gNB/N2 proof was not re-established") from last_error
    raise R2LabLiveReconciliationError("stable current gNB/N2 proof was not re-established")


def _synthetic_gnb_boundary(evidence: PhysicalRunEvidence) -> PhysicalRunEvidence:
    if evidence.staged is None or evidence.gnb_start is None:
        raise R2LabLiveReconciliationError("accepted UE path history is missing immutable gNB evidence")
    prefix = []
    for item in evidence.acceptance.evidence:
        prefix.append(item)
        if item.stage is PhysicalAcceptanceStage.GNB_N2:
            break
    if not prefix or prefix[-1].stage is not PhysicalAcceptanceStage.GNB_N2:
        raise R2LabLiveReconciliationError("accepted UE path history does not include gNB/N2")
    if any(item.outcome is not AcceptanceOutcome.PASSED for item in prefix):
        raise R2LabLiveReconciliationError("accepted UE path history contains a failed prerequisite")
    return PhysicalRunEvidence(
        run_id=evidence.run_id,
        staged=evidence.staged,
        gnb_start=evidence.gnb_start,
        acceptance=PhysicalAcceptance(evidence=tuple(prefix)),
    )


def _reprove_live_ue_path(
    *,
    evidence: PhysicalRunEvidence,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    peer: str,
    lock_path: Path,
    deps_root: Path,
    run_root: Path,
    r2lab_runner: Runner,
    cluster_runner: Runner,
    timeout_seconds: int,
    progress: TextIO | None,
) -> tuple[str, bool]:
    synthetic = _synthetic_gnb_boundary(evidence)
    state, activation = activate_physical_ue(
        evidence=synthetic,
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
        raise R2LabLiveReconciliationError("current UE registration/PDU path was not re-established")
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
    if not user_plane.probe.proven or user_plane.evidence.acceptance.next_stage is not PhysicalAcceptanceStage.WORKLOAD:
        raise R2LabLiveReconciliationError("current physical user plane was not re-proven")
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
    repository_root: Path = Path("."),
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
    if not known_hosts.is_file():
        raise R2LabLiveReconciliationError("strict SLICES known-hosts file is missing")
    evidence_path = run_root / run_id / "physical-run.json"
    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if evidence.acceptance.accepted:
        raise R2LabLiveReconciliationError("accepted physical run does not require live resume reproof")
    if evidence.acceptance.outcome_for(PhysicalAcceptanceStage.OPEN5GS) is not AcceptanceOutcome.PASSED:
        raise R2LabLiveReconciliationError("live resume reproof requires previously accepted Open5GS foundation")

    _report(progress, "resume: re-proving live state behind persisted acceptance")
    allocation, foundation_reconciled = _reconcile_foundation(
        run_id=run_id,
        slice_name=slice_name,
        owner=owner,
        allocation_id=allocation_id,
        known_hosts=known_hosts,
        lock_path=lock_path,
        deps_root=deps_root,
        run_root=run_root,
        repository_root=repository_root,
        r2lab_runner=r2lab_runner,
        cluster_runner=cluster_runner,
        timeout_seconds=timeout_seconds,
        progress=progress,
    )

    gnb_restarted = False
    if evidence.acceptance.outcome_for(PhysicalAcceptanceStage.GNB_N2) is AcceptanceOutcome.PASSED:
        gnb_restarted = _restore_and_start_gnb(
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
            required_consecutive_proofs=n2_attempts,
            convergence_attempts=n2_convergence_attempts,
            poll_interval_seconds=n2_interval,
            progress=progress,
        )

    ue_status: str | None = None
    user_plane_proven = False
    if evidence.acceptance.outcome_for(PhysicalAcceptanceStage.USER_PLANE) is AcceptanceOutcome.PASSED:
        topology = load_topology(run_root=run_root, run_id=run_id).validate()
        peer = SUPPORTED_NODES[topology.ran_node].ip
        ue_status, user_plane_proven = _reprove_live_ue_path(
            evidence=evidence,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation,
            known_hosts=known_hosts,
            peer=peer,
            lock_path=lock_path,
            deps_root=deps_root,
            run_root=run_root,
            r2lab_runner=r2lab_runner,
            cluster_runner=cluster_runner,
            timeout_seconds=timeout_seconds,
            progress=progress,
        )

    resume_path = run_root / run_id / "physical" / "live-resume.json"
    payload = {
        "schema": RESUME_SCHEMA,
        "run_id": run_id,
        "observed_at_utc": _current_time(),
        "historical_evidence_path": str(evidence_path),
        "historical_acceptance_unchanged": True,
        "allocation_id": allocation,
        "foundation_reconciled": foundation_reconciled,
        "gnb_restarted": gnb_restarted,
        "ue_status": ue_status,
        "user_plane_proven": user_plane_proven,
    }
    atomic_json(resume_path, payload)
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
