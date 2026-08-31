"""Thin N3xx compatibility boundary backed by pinned 5g-Ansible roles.

SynthRAN owns run identity, evidence ordering, and current-state proof.  The
physical gNB deployment lifecycle itself is delegated to the pinned upstream
5g-Ansible/srsRAN roles in :mod:`synthran.r2lab.upstream_roles`.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import tempfile
import time
from typing import Mapping, Sequence

from synthran.dependencies import load_lock
from synthran.live_preflight import CommandResult, Runner, subprocess_runner
from synthran.r2lab.acceptance import PhysicalAcceptanceStage, PhysicalRunEvidence
from synthran.r2lab.resources import load_topology
from synthran.r2lab.ue import R2LabPhysicalUeError, verify_current_n3xx_n2
from synthran.r2lab.upstream_roles import (
    R2LabUpstreamRoleError,
    converge_physical_gnb,
    stop_role_managed_gnb,
)
from synthran.utils.ssh import strict_ssh_command


NAMESPACE = "open5gs"
RELEASE = "srsran-gnb"
GNB_SELECTOR = "app=srsran,component=gnb"
RUN_LABEL = "synthran.run/id"
RUN_ANNOTATION = "synthran.io/run-id"
OPEN5GS_AMF_N2_ADDRESS = "10.10.3.200"
OPEN5GS_GNB_N2_N3_ADDRESS = "10.10.3.234"
DEFAULT_TIMEOUT_SECONDS = 120


class R2LabN3xxError(RuntimeError):
    """Raised when the upstream-managed physical gNB cannot be proven."""


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as exc:
        raise R2LabN3xxError("physical evidence file could not be hashed") from exc
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(dict(payload), indent=2, sort_keys=True) + "\n"
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="\n", dir=path.parent, delete=False
        ) as stream:
            stream.write(text)
            temporary = Path(stream.name)
        temporary.replace(path)
    except OSError as exc:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise R2LabN3xxError("physical gNB evidence could not be persisted") from exc
    return path


def _dependency_commit(lock, name: str) -> str:
    item = next((entry for entry in lock.git if entry.name == name), None)
    if item is None:
        raise R2LabN3xxError(f"dependency lock is missing {name}")
    return item.commit


def _cluster_command(topology, known_hosts: Path, *remote: str) -> tuple[str, ...]:
    try:
        return strict_ssh_command(
            f"root@{topology.core_node}",
            *remote,
            known_hosts=known_hosts,
            isolated_config=True,
            quote_remote=True,
        )
    except ValueError as exc:
        raise R2LabN3xxError(str(exc)) from exc


def _checked(
    runner: Runner,
    command: Sequence[str],
    timeout_seconds: int,
    label: str,
) -> CommandResult:
    try:
        result = runner(command, timeout_seconds)
    except Exception as exc:
        raise R2LabN3xxError(f"{label} could not complete") from exc
    if result.returncode != 0:
        raise R2LabN3xxError(f"{label} returned nonzero")
    return result


def _upstream_identity(*, topology, lock) -> tuple[str, str, str]:
    fiveg_commit = _dependency_commit(lock, "fiveg_ansible")
    srsran_commit = _dependency_commit(lock, "srsran_helm")
    topology_text = json.dumps(topology.to_dict(), sort_keys=True, separators=(",", ":"))
    package = _sha256_text(f"fiveg_ansible:{fiveg_commit}|srsran_helm:{srsran_commit}")
    values = _sha256_text(f"{topology_text}|fiveg_ansible:{fiveg_commit}")
    render = _sha256_text(
        f"upstream-role:r2lab-srsran-gnb|{topology_text}|srsran_helm:{srsran_commit}"
    )
    return package, values, render


@dataclass(frozen=True)
class N3xxArtifact:
    run_id: str
    radio: str
    package_sha256: str
    values_sha256: str
    render_sha256: str
    expected_gnb_peer: str
    deployment_authority: str
    evidence_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-n3xx-upstream/v1alpha1",
            "run_id": self.run_id,
            "radio": self.radio,
            "package_sha256": self.package_sha256,
            "values_sha256": self.values_sha256,
            "render_sha256": self.render_sha256,
            "expected_gnb_peer": self.expected_gnb_peer,
            "deployment_authority": self.deployment_authority,
            "evidence_path": str(self.evidence_path),
        }

    @classmethod
    def read(cls, path: Path) -> "N3xxArtifact":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise R2LabN3xxError("upstream gNB artifact identity is unavailable") from exc
        required = (
            "run_id",
            "radio",
            "package_sha256",
            "values_sha256",
            "render_sha256",
            "expected_gnb_peer",
            "deployment_authority",
        )
        if payload.get("schema") != "synthran/r2lab-n3xx-upstream/v1alpha1" or any(
            not isinstance(payload.get(key), str) or not payload.get(key) for key in required
        ):
            raise R2LabN3xxError("upstream gNB artifact identity is malformed")
        return cls(
            run_id=str(payload["run_id"]),
            radio=str(payload["radio"]),
            package_sha256=str(payload["package_sha256"]),
            values_sha256=str(payload["values_sha256"]),
            render_sha256=str(payload["render_sha256"]),
            expected_gnb_peer=str(payload["expected_gnb_peer"]),
            deployment_authority=str(payload["deployment_authority"]),
            evidence_path=path,
        )


@dataclass(frozen=True)
class N3xxStartSummary:
    run_id: str
    radio: str
    attempts: int
    consecutive_n2_proofs: int
    evidence_path: Path
    n2_path: Path
    deployment_authority: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-n3xx-start/v1alpha2",
            "run_id": self.run_id,
            "radio": self.radio,
            "status": "gnb-n2-ready",
            "attempts": self.attempts,
            "consecutive_n2_proofs": self.consecutive_n2_proofs,
            "deployment_authority": self.deployment_authority,
            "evidence_path": str(self.evidence_path),
            "n2_path": str(self.n2_path),
            "next_stage": PhysicalAcceptanceStage.UE_MANAGEMENT.value,
        }


def stage_n3xx_gnb(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    lock_path: Path = Path("dependencies.lock.yml"),
    deps_root: Path = Path(".deps"),
    run_root: Path = Path(".synthran/r2lab"),
    runner: Runner = subprocess_runner,
    r2lab_runner: Runner = subprocess_runner,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> N3xxArtifact:
    """Bind acceptance provenance to the pinned upstream role without deploying the gNB."""

    del slice_name, owner, allocation_id, deps_root, r2lab_runner
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    if topology.radio_profile.family != "n3xx":
        raise R2LabN3xxError("selected radio is not an N3xx profile")
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabN3xxError("strict SLICES known-hosts file is missing")
    evidence_path = run_root.expanduser().resolve() / run_id / "physical-run.json"
    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if evidence.acceptance.next_stage is not PhysicalAcceptanceStage.GNB_N2:
        raise R2LabN3xxError("physical foundation is not ready for gNB deployment")
    if evidence.staged is not None:
        raise R2LabN3xxError("physical gNB provenance is already bound for this run")

    namespace_owner = _checked(
        runner,
        _cluster_command(
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
        "Open5GS namespace ownership query",
    ).stdout.strip()
    if namespace_owner != run_id:
        raise R2LabN3xxError("Open5GS namespace is not owned by this run")

    pods = _checked(
        runner,
        _cluster_command(
            topology,
            known_hosts,
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
        min(timeout_seconds, 60),
        "pre-deploy physical gNB pod query",
    )
    try:
        pod_items = json.loads(pods.stdout).get("items", [])
    except (json.JSONDecodeError, AttributeError) as exc:
        raise R2LabN3xxError("pre-deploy physical gNB pod query was malformed") from exc
    if pod_items:
        raise R2LabN3xxError("upstream physical gNB deployment requires zero existing gNB pods")

    lock = load_lock(lock_path)
    package_sha256, values_sha256, render_sha256 = _upstream_identity(
        topology=topology, lock=lock
    )
    fiveg_commit = _dependency_commit(lock, "fiveg_ansible")
    physical_dir = run_root.expanduser().resolve() / run_id / "physical"
    artifact_path = physical_dir / "n3xx-artifact.json"
    artifact = N3xxArtifact(
        run_id=run_id,
        radio=topology.radio,
        package_sha256=package_sha256,
        values_sha256=values_sha256,
        render_sha256=render_sha256,
        expected_gnb_peer=OPEN5GS_GNB_N2_N3_ADDRESS,
        deployment_authority=f"fiveg_ansible:{fiveg_commit}",
        evidence_path=artifact_path,
    )
    _atomic_json(artifact_path, artifact.to_dict())

    staging_payload = {
        "run_id": run_id,
        "package_sha256": package_sha256,
        "values_sha256": values_sha256,
        "render_sha256": render_sha256,
        "namespace_owned": True,
        "desired_replicas": 0,
        "gnb_pod_count": 0,
        "deployment_bound": True,
        "status": "staged-stopped",
        "hardware_mutation": False,
    }
    _atomic_json(physical_dir / "physical-staging.json", staging_payload)
    evidence.bind_staging(staging_payload).write_json(evidence_path)
    return artifact


def start_n3xx_gnb(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    run_root: Path = Path(".synthran/r2lab"),
    runner: Runner = subprocess_runner,
    r2lab_runner: Runner = subprocess_runner,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    required_consecutive_proofs: int = 12,
    convergence_attempts: int = 12,
    poll_interval_seconds: float = 5.0,
    lock_path: Path = Path("dependencies.lock.yml"),
    deps_root: Path = Path(".deps"),
) -> N3xxStartSummary:
    """Deploy the physical gNB through pinned 5g-Ansible and prove stable N2."""

    del r2lab_runner
    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    artifact_path = run_root.expanduser().resolve() / run_id / "physical" / "n3xx-artifact.json"
    artifact = N3xxArtifact.read(artifact_path)
    evidence_path = run_root.expanduser().resolve() / run_id / "physical-run.json"
    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if evidence.staged is None or evidence.gnb_start is not None:
        raise R2LabN3xxError("N3xx start requires bound upstream provenance and no prior start")
    if required_consecutive_proofs < 1 or convergence_attempts < 1:
        raise R2LabN3xxError("N2 proof attempt counts must be positive")
    total_attempts = required_consecutive_proofs + convergence_attempts - 1
    if total_attempts > 120:
        raise R2LabN3xxError("combined N2 convergence/stability attempts exceed 120")

    try:
        converge_physical_gnb(
            run_id=run_id,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            lock_path=lock_path,
            deps_root=deps_root,
            run_root=run_root,
            timeout_seconds=timeout_seconds,
            progress=None,
        )
    except R2LabUpstreamRoleError as exc:
        raise R2LabN3xxError(str(exc)) from exc

    consecutive = 0
    attempts = 0
    for attempt in range(1, total_attempts + 1):
        attempts = attempt
        try:
            proven = verify_current_n3xx_n2(
                run_id=run_id,
                known_hosts=known_hosts,
                run_root=run_root,
                runner=runner,
                timeout_seconds=min(timeout_seconds, 60),
            )
        except R2LabPhysicalUeError:
            proven = False
        consecutive = consecutive + 1 if proven else 0
        if consecutive >= required_consecutive_proofs:
            break
        if attempt < total_attempts:
            time.sleep(poll_interval_seconds)
    else:
        raise R2LabN3xxError("stable physical gNB/N2 proof was not established")

    claim_path = run_root.expanduser().resolve() / "active.json"
    start_payload = {
        "run_id": run_id,
        "package_sha256": artifact.package_sha256,
        "values_sha256": artifact.values_sha256,
        "render_sha256": artifact.render_sha256,
        "claim_sha256": _sha256_file(claim_path),
        "maximum_observed_pods": 1,
        "started_exactly_one": True,
        "status": "gnb-started",
        "hardware_mutation": True,
    }
    physical_dir = run_root.expanduser().resolve() / run_id / "physical"
    _atomic_json(physical_dir / "physical-gnb-start.json", start_payload)
    n2_path = _atomic_json(
        physical_dir / "physical-gnb-n2.json",
        {
            "run_id": run_id,
            "radio": topology.radio,
            "status": "proven",
            "consecutive_proofs": consecutive,
            "attempts": attempts,
            "deployment_authority": artifact.deployment_authority,
        },
    )
    evidence = evidence.bind_gnb_start(start_payload)
    evidence = evidence.pass_stage(
        PhysicalAcceptanceStage.GNB_N2,
        source="current-upstream-fiveg-ansible-stable-n2",
    )
    evidence.write_json(evidence_path)
    return N3xxStartSummary(
        run_id=run_id,
        radio=topology.radio,
        attempts=attempts,
        consecutive_n2_proofs=consecutive,
        evidence_path=evidence_path,
        n2_path=n2_path,
        deployment_authority=artifact.deployment_authority,
    )


def stop_n3xx_gnb(
    *,
    run_id: str,
    slice_name: str,
    owner: str,
    allocation_id: str | None,
    known_hosts: Path,
    run_root: Path = Path(".synthran/r2lab"),
    runner: Runner = subprocess_runner,
    r2lab_runner: Runner = subprocess_runner,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    lock_path: Path = Path("dependencies.lock.yml"),
) -> dict[str, object]:
    """Stop only the run-bound role-managed gNB."""

    del runner, r2lab_runner
    try:
        return stop_role_managed_gnb(
            run_id=run_id,
            slice_name=slice_name,
            owner=owner,
            allocation_id=allocation_id,
            known_hosts=known_hosts,
            lock_path=lock_path,
            run_root=run_root,
            timeout_seconds=timeout_seconds,
        )
    except R2LabUpstreamRoleError as exc:
        raise R2LabN3xxError(str(exc)) from exc
