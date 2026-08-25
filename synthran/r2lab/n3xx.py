"""Selected-topology N300/N320 srsRAN gNB adapter.

The pinned radio values file remains the RF source of truth. SynthRAN overlays
the Open5GS network profile plus lifecycle properties it owns (zero-pod staging,
Recreate, selected RAN node, resource envelope, log-sidecar policy, and optional
immutable image identity). Runtime authority is current lease/allocation/ownership
state; hashes below identify transferred deployment bytes and are not treated as
lease truth.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import tarfile
import tempfile
import time
from typing import Callable, Mapping, Sequence

from synthran.dependencies import DependencyLock, load_lock
from synthran.live_preflight import CommandResult, subprocess_runner
from synthran.r2lab.acceptance import PhysicalAcceptanceStage, PhysicalRunEvidence, R2LabAcceptanceError
from synthran.r2lab.deployment import materialize_locked_helm, parse_gnb_pods_json
from synthran.r2lab.hardware import PhysicalTopology, RadioProfile
from synthran.r2lab.n2 import build_amf_n2_evidence
from synthran.r2lab.resources import (
    R2LabTopologyResourceError,
    load_topology,
    verify_physical_authority,
    verify_selected_allocation,
)
from synthran.r2lab.runtime import N2State, parse_n2_log_state


Runner = Callable[[Sequence[str], int], CommandResult]
NAMESPACE = "open5gs"
RELEASE = "srsran-gnb"
GNB_SELECTOR = "app=srsran,component=gnb"
RUN_LABEL = "synthran.run/id"
RUN_ANNOTATION = "synthran.io/run-id"
PACKAGE_ANNOTATION = "synthran.io/package-sha256"
VALUES_ANNOTATION = "synthran.io/values-sha256"
RENDER_ANNOTATION = "synthran.io/render-sha256"
CHART_PATH = "charts/srsran-gnb"
DEPLOYMENT_TEMPLATE = f"{CHART_PATH}/templates/deployment.yaml"
GENERATED_VALUES = "synthran-physical-values.json"
CPU_COUNT = 8
MEMORY = "4Gi"
DEFAULT_TIMEOUT_SECONDS = 120
OPEN5GS_AMF_N2_ADDRESS = "10.10.3.200"
OPEN5GS_GNB_N2_N3_ADDRESS = "10.10.3.234"
OPEN5GS_N3_NETWORK = "n3network"
OPEN5GS_RU_NETWORK = "ru-network"
OPEN5GS_AMF_PORT = 38412


class R2LabN3xxError(RuntimeError):
    """Raised when the selected N3xx deployment cannot be proven safe."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as exc:
        raise R2LabN3xxError("physical deployment artifact could not be hashed") from exc
    return digest.hexdigest()


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _dependency(lock: DependencyLock, name: str):
    item = next((entry for entry in lock.git if entry.name == name), None)
    if item is None:
        raise R2LabN3xxError(f"dependency lock is missing {name}")
    return item


def _chart_checkout(lock: DependencyLock, deps_root: Path, runner: Runner) -> Path:
    dependency = _dependency(lock, "srsran_helm")
    checkout = deps_root.expanduser().resolve() / dependency.checkout
    if not checkout.is_dir():
        raise R2LabN3xxError("locked srsran_helm checkout is missing; run synthran deps sync")
    head = runner(("git", "-C", str(checkout), "rev-parse", "HEAD"), 30)
    status = runner(("git", "-C", str(checkout), "status", "--short"), 30)
    if head.returncode != 0 or head.stdout.strip() != dependency.commit:
        raise R2LabN3xxError("srsran_helm checkout is not at the locked commit")
    if status.returncode != 0 or status.stdout.strip():
        raise R2LabN3xxError("srsran_helm checkout is not clean")
    return checkout


def _normalize_image(value: str) -> str:
    return value.removeprefix("docker.io/")


def _locked_image(lock: DependencyLock, profile: RadioProfile) -> tuple[str, str, str | None]:
    if profile.image_repository is None or profile.image_tag is None:
        raise R2LabN3xxError(f"radio {profile.name} has no pinned upstream image reference")
    repository = profile.image_repository
    tag = profile.image_tag
    digest: str | None = None
    key = profile.container_lock_key
    if key is not None:
        containers = lock.raw.get("containers")
        entry = containers.get(key) if isinstance(containers, dict) else None
        if not isinstance(entry, dict):
            raise R2LabN3xxError(f"dependency lock is missing container {key}")
        image = entry.get("image")
        locked_tag = entry.get("tag")
        locked_digest = entry.get("digest")
        if (
            not isinstance(image, str)
            or not isinstance(locked_tag, str)
            or not isinstance(locked_digest, str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", locked_digest)
        ):
            raise R2LabN3xxError(f"container lock {key} is incomplete")
        if _normalize_image(image) != _normalize_image(repository) or locked_tag != tag:
            raise R2LabN3xxError(f"container lock {key} does not match the selected radio profile")
        digest = locked_digest
    else:
        optional_key = f"srsran_gnb_physical_{profile.name}"
        containers = lock.raw.get("containers")
        entry = containers.get(optional_key) if isinstance(containers, dict) else None
        if isinstance(entry, dict):
            image = entry.get("image")
            locked_tag = entry.get("tag")
            locked_digest = entry.get("digest")
            if (
                isinstance(image, str)
                and isinstance(locked_tag, str)
                and isinstance(locked_digest, str)
                and re.fullmatch(r"sha256:[0-9a-f]{64}", locked_digest)
                and _normalize_image(image) == _normalize_image(repository)
                and locked_tag == tag
            ):
                digest = locked_digest
    return repository, tag, digest


def _overlay_template(source: str) -> str:
    replacements = {
        "spec:\n  selector:\n": (
            "spec:\n"
            "  strategy:\n"
            "    type: {{ .Values.deploymentStrategy }}\n"
            "  selector:\n"
        ),
        "  replicas: 1\n": "  replicas: {{ .Values.replicas }}\n",
        '          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"\n': (
            '          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}'
            '{{- if .Values.image.digest }}@{{ .Values.image.digest }}{{- end }}"\n'
        ),
    }
    result = source
    for anchor, replacement in replacements.items():
        if result.count(anchor) != 1:
            raise R2LabN3xxError("pinned srsRAN Deployment template changed unexpectedly")
        result = result.replace(anchor, replacement, 1)
    return result


def _radio_address(source_values: str) -> str:
    match = re.search(r"(?m)^\s*device_args:\s*[^\r\n]*?(?:^|,)addr=([^,\s]+)", source_values)
    if match is None:
        match = re.search(r"(?m)^\s*device_args:\s*.*?addr=([^,\s]+)", source_values)
    if match is None:
        raise R2LabN3xxError("selected radio values do not expose a UHD address")
    return match.group(1)


def _gnb_address(source_values: str) -> str:
    match = re.search(r"(?m)^gnbIp:\s*([^\s#]+)", source_values)
    if match is None:
        raise R2LabN3xxError("selected radio values do not expose gnbIp")
    return match.group(1)


def _ru_pod_address(source_values: str) -> str:
    match = re.search(r"(?m)^ruPodIp:\s*([^\s#]+)", source_values)
    if match is None:
        raise R2LabN3xxError("selected radio values do not expose ruPodIp")
    return match.group(1)


def _generated_values(
    *, topology: PhysicalTopology, lock: DependencyLock
) -> dict[str, object]:
    repository, tag, digest = _locked_image(lock, topology.radio_profile)
    return {
        "image": {
            "repository": repository,
            "tag": tag,
            "digest": digest or "",
            "pullPolicy": "IfNotPresent",
        },
        "replicas": 0,
        "deploymentStrategy": "Recreate",
        "resources": {
            "define": True,
            "requests": {"tcpdump": {"cpu": str(CPU_COUNT), "memory": MEMORY}},
            "limits": {"tcpdump": {"cpu": str(CPU_COUNT), "memory": MEMORY}},
        },
        "start": {"gnb": True, "logs": False},
        "nodeName": topology.ran_node,
        "namespace": NAMESPACE,
        "n3networkName": OPEN5GS_N3_NETWORK,
        "gnbIp": OPEN5GS_GNB_N2_N3_ADDRESS,
        "gnbConfig": {
            "cu_cp": {
                "amf": {
                    "addr": OPEN5GS_AMF_N2_ADDRESS,
                    "port": OPEN5GS_AMF_PORT,
                    "bind_addr": OPEN5GS_GNB_N2_N3_ADDRESS,
                }
            },
            "cu_up": {
                "ngu": {
                    "socket": [{"bind_addr": OPEN5GS_GNB_N2_N3_ADDRESS}],
                }
            },
        },
    }


@dataclass(frozen=True)
class N3xxArtifact:
    run_id: str
    radio: str
    package_path: Path
    source_values_path: Path
    generated_values_path: Path
    package_sha256: str
    source_values_sha256: str
    values_sha256: str
    render_sha256: str
    expected_image_digest: str | None
    expected_gnb_peer: str
    radio_address: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-n3xx-artifact/v1alpha1",
            "run_id": self.run_id,
            "radio": self.radio,
            "package_file": self.package_path.name,
            "source_values_file": self.source_values_path.name,
            "generated_values_file": self.generated_values_path.name,
            "package_sha256": self.package_sha256,
            "source_values_sha256": self.source_values_sha256,
            "values_sha256": self.values_sha256,
            "render_sha256": self.render_sha256,
            "expected_image_digest": self.expected_image_digest,
            "expected_gnb_peer": self.expected_gnb_peer,
            "radio_address": self.radio_address,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object], directory: Path) -> "N3xxArtifact":
        required = (
            "run_id",
            "radio",
            "package_file",
            "source_values_file",
            "generated_values_file",
            "package_sha256",
            "source_values_sha256",
            "values_sha256",
            "render_sha256",
            "expected_gnb_peer",
            "radio_address",
        )
        if payload.get("schema") != "synthran/r2lab-n3xx-artifact/v1alpha1" or any(
            not isinstance(payload.get(key), str) or not payload.get(key) for key in required
        ):
            raise R2LabN3xxError("stored N3xx artifact metadata is malformed")
        digest = payload.get("expected_image_digest")
        if digest is not None and (
            not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
        ):
            raise R2LabN3xxError("stored N3xx image digest is malformed")
        result = cls(
            run_id=str(payload["run_id"]),
            radio=str(payload["radio"]),
            package_path=directory / str(payload["package_file"]),
            source_values_path=directory / str(payload["source_values_file"]),
            generated_values_path=directory / str(payload["generated_values_file"]),
            package_sha256=str(payload["package_sha256"]),
            source_values_sha256=str(payload["source_values_sha256"]),
            values_sha256=str(payload["values_sha256"]),
            render_sha256=str(payload["render_sha256"]),
            expected_image_digest=digest if isinstance(digest, str) else None,
            expected_gnb_peer=str(payload["expected_gnb_peer"]),
            radio_address=str(payload["radio_address"]),
        )
        for value in (
            result.package_sha256,
            result.source_values_sha256,
            result.values_sha256,
            result.render_sha256,
        ):
            if not re.fullmatch(r"[0-9a-f]{64}", value):
                raise R2LabN3xxError("stored N3xx artifact digest is malformed")
        return result


def _write_json(path: Path, payload: Mapping[str, object]) -> Path:
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


def _package_chart(chart_root: Path, destination: Path, run_id: str) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    files = sorted(path for path in chart_root.rglob("*") if path.is_file())
    if not files or any(path.is_symlink() for path in files):
        raise R2LabN3xxError("isolated physical chart contains unsafe files")
    try:
        with destination.open("wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
                    for path in files:
                        relative = path.relative_to(chart_root)
                        info = archive.gettarinfo(str(path), arcname=(Path("srsran-gnb") / relative).as_posix())
                        info.uid = 0
                        info.gid = 0
                        info.uname = ""
                        info.gname = ""
                        info.mtime = 0
                        info.mode = 0o644
                        with path.open("rb") as stream:
                            archive.addfile(info, stream)
    except OSError as exc:
        destination.unlink(missing_ok=True)
        raise R2LabN3xxError("physical chart package could not be produced") from exc
    return destination


def _cluster_ssh(topology: PhysicalTopology, known_hosts: Path, *remote: str) -> tuple[str, ...]:
    return (
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        f"root@{topology.core_node}",
        shlex.join(remote),
    )


def _scp_base(known_hosts: Path) -> tuple[str, ...]:
    return (
        "scp",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
    )


def _checked(runner: Runner, command: Sequence[str], timeout_seconds: int, label: str) -> CommandResult:
    try:
        result = runner(command, timeout_seconds)
    except Exception as exc:
        raise R2LabN3xxError(f"{label} could not complete") from exc
    if result.returncode != 0:
        raise R2LabN3xxError(f"{label} returned nonzero")
    return result


def _validate_render(
    *,
    text: str,
    topology: PhysicalTopology,
    repository: str,
    tag: str,
    digest: str | None,
    ru_pod_address: str,
) -> None:
    if not re.search(r"(?m)^\s*replicas:\s*0\s*$", text):
        raise R2LabN3xxError("offline physical render is not staged at zero replicas")
    if not re.search(r"(?m)^\s*type:\s*Recreate\s*$", text):
        raise R2LabN3xxError("offline physical render does not use Recreate")
    if f"nodeName: {topology.ran_node}" not in text:
        raise R2LabN3xxError("offline physical render does not target the selected RAN node")
    expected = f"{repository}:{tag}" + (f"@{digest}" if digest else "")
    if expected not in text:
        raise R2LabN3xxError("offline physical render does not use the selected radio image")
    if "name: gnb-logs" in text:
        raise R2LabN3xxError("physical render unexpectedly contains the unpinned log sidecar")
    network_requirements = (
        (rf'"name"\s*:\s*"{re.escape(OPEN5GS_N3_NETWORK)}"', "Open5GS N3 network attachment"),
        (rf'"ips"\s*:\s*\[\s*"{re.escape(OPEN5GS_GNB_N2_N3_ADDRESS)}/24"\s*\]', "Open5GS gNB N3 address"),
        (rf'"name"\s*:\s*"{re.escape(OPEN5GS_RU_NETWORK)}"', "N3xx RU network attachment"),
        (rf'"ips"\s*:\s*\[\s*"{re.escape(ru_pod_address)}/24"\s*\]', "N3xx RU pod address"),
    )
    for pattern, label in network_requirements:
        if re.search(pattern, text) is None:
            raise R2LabN3xxError(f"offline physical render is missing the required {label}")


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
    """Render, package and stage the selected N300/N320 gNB at zero pods."""

    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    if topology.radio_profile.family != "n3xx":
        raise R2LabN3xxError("selected radio is not an N3xx profile")
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabN3xxError("strict SLICES known-hosts file is missing")
    evidence_path = run_root.expanduser().resolve() / run_id / "physical-run.json"
    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if evidence.acceptance.next_stage is not PhysicalAcceptanceStage.GNB_N2:
        raise R2LabN3xxError("physical foundation is not ready for gNB staging")
    if evidence.staged is not None:
        raise R2LabN3xxError("physical gNB is already staged for this run")

    verify_physical_authority(
        run_id=run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    verify_selected_allocation(
        topology=topology,
        runner=runner,
        owner=owner,
        allocation_id=allocation_id,
        timeout_seconds=min(timeout_seconds, 300),
    )

    lock = load_lock(lock_path)
    checkout = _chart_checkout(lock, deps_root, runner)
    profile = topology.radio_profile
    assert profile.values_file is not None
    source_values = checkout / profile.values_file
    if not source_values.is_file():
        raise R2LabN3xxError("selected pinned radio values file is missing")
    repository, tag, digest = _locked_image(lock, profile)
    physical_dir = run_root.expanduser().resolve() / run_id / "physical"
    if physical_dir.exists():
        raise R2LabN3xxError("physical deployment directory already exists")
    run_dir = physical_dir.parent
    temporary = Path(tempfile.mkdtemp(prefix=".n3xx-", dir=run_dir))
    try:
        chart_copy = temporary / CHART_PATH
        chart_copy.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(checkout / CHART_PATH, chart_copy)
        template = chart_copy / "templates" / "deployment.yaml"
        source_template = template.read_text(encoding="utf-8")
        template.write_text(_overlay_template(source_template), encoding="utf-8", newline="\n")
        source_copy = temporary / f"values-{topology.radio}.yaml"
        source_text = source_values.read_text(encoding="utf-8")
        source_copy.write_text(source_text, encoding="utf-8", newline="\n")
        generated = temporary / GENERATED_VALUES
        generated_text = json.dumps(_generated_values(topology=topology, lock=lock), indent=2, sort_keys=True) + "\n"
        generated.write_text(generated_text, encoding="utf-8", newline="\n")
        (chart_copy / GENERATED_VALUES).write_text(generated_text, encoding="utf-8", newline="\n")
        helm = materialize_locked_helm(
            lock=lock,
            destination=run_dir / "tools",
            timeout_seconds=min(timeout_seconds, 300),
        )
        render = _checked(
            runner,
            (
                str(helm),
                "template",
                RELEASE,
                str(chart_copy),
                "--namespace",
                NAMESPACE,
                "--values",
                str(source_copy),
                "--values",
                str(generated),
            ),
            min(timeout_seconds, 300),
            "offline N3xx Helm render",
        ).stdout
        _validate_render(
            text=render,
            topology=topology,
            repository=repository,
            tag=tag,
            digest=digest,
            ru_pod_address=_ru_pod_address(source_text),
        )
        render_path = temporary / "physical-render.yaml"
        render_path.write_text(render, encoding="utf-8", newline="\n")
        package = _package_chart(chart_copy, temporary / f"srsran-gnb-{run_id}.tgz", run_id)
        artifact = N3xxArtifact(
            run_id=run_id,
            radio=topology.radio,
            package_path=package,
            source_values_path=source_copy,
            generated_values_path=generated,
            package_sha256=_sha256_file(package),
            source_values_sha256=_sha256_file(source_copy),
            values_sha256=_sha256_file(generated),
            render_sha256=_sha256_text(render),
            expected_image_digest=digest,
            expected_gnb_peer=OPEN5GS_GNB_N2_N3_ADDRESS,
            radio_address=_radio_address(source_text),
        )
        _write_json(temporary / "n3xx-artifact.json", artifact.to_dict())

        remote_root = f"/root/.synthran/{run_id}/n3xx"
        remote_package = f"{remote_root}/{package.name}"
        remote_source = f"{remote_root}/{source_copy.name}"
        remote_generated = f"{remote_root}/{generated.name}"
        remote_helm = f"{remote_root}/helm"

        def authority() -> None:
            verify_physical_authority(
                run_id=run_id,
                slice_name=slice_name,
                run_root=run_root,
                runner=r2lab_runner,
                timeout_seconds=min(timeout_seconds, 300),
            )
            verify_selected_allocation(
                topology=topology,
                runner=runner,
                owner=owner,
                allocation_id=allocation_id,
                timeout_seconds=min(timeout_seconds, 300),
            )

        authority()
        _checked(
            runner,
            _cluster_ssh(topology, known_hosts, "mkdir", "-p", remote_root),
            min(timeout_seconds, 60),
            "remote N3xx artifact directory creation",
        )
        _checked(
            runner,
            (
                *_scp_base(known_hosts),
                str(package),
                str(source_copy),
                str(generated),
                str(helm),
                f"root@{topology.core_node}:{remote_root}/",
            ),
            timeout_seconds,
            "N3xx artifact transfer",
        )
        remote_hashes = _checked(
            runner,
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
            "remote N3xx artifact verification",
        ).stdout
        for expected in (
            artifact.package_sha256,
            artifact.source_values_sha256,
            artifact.values_sha256,
            _sha256_file(helm),
        ):
            if expected not in remote_hashes:
                raise R2LabN3xxError("remote N3xx artifact bytes do not match local staging")
        _checked(
            runner,
            _cluster_ssh(topology, known_hosts, "chmod", "0755", remote_helm),
            min(timeout_seconds, 60),
            "remote Helm permission preparation",
        )
        namespace_owner = _checked(
            runner,
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
            "Open5GS namespace ownership query",
        ).stdout.strip()
        if namespace_owner != run_id:
            raise R2LabN3xxError("Open5GS namespace is not owned by this run")
        existing = _checked(
            runner,
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
            "existing physical gNB query",
        ).stdout.strip()
        if existing:
            payload = json.loads(existing)
            desired = payload.get("spec", {}).get("replicas") if isinstance(payload, dict) else None
            if desired != 0:
                raise R2LabN3xxError("existing physical gNB is not stopped")
        pods = parse_gnb_pods_json(
            _checked(
                runner,
                _cluster_ssh(
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
                "pre-stage physical gNB pod query",
            ).stdout
        )
        if not pods.zero:
            raise R2LabN3xxError("physical staging requires zero existing gNB pods")
        authority()
        _checked(
            runner,
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
            "stopped N3xx Helm staging",
        )
        _checked(
            runner,
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
            "physical gNB run binding",
        )
        annotations = {
            RUN_ANNOTATION: run_id,
            PACKAGE_ANNOTATION: artifact.package_sha256,
            VALUES_ANNOTATION: artifact.values_sha256,
            RENDER_ANNOTATION: artifact.render_sha256,
        }
        _checked(
            runner,
            _cluster_ssh(
                topology,
                known_hosts,
                "kubectl",
                "annotate",
                f"deployment/{RELEASE}",
                "-n",
                NAMESPACE,
                *(f"{key}={value}" for key, value in annotations.items()),
                "--overwrite",
            ),
            min(timeout_seconds, 60),
            "physical gNB artifact binding",
        )
        deployment = json.loads(
            _checked(
                runner,
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
                "staged physical gNB verification",
            ).stdout
        )
        labels = deployment.get("metadata", {}).get("labels", {})
        observed_annotations = deployment.get("metadata", {}).get("annotations", {})
        desired = deployment.get("spec", {}).get("replicas")
        if labels.get(RUN_LABEL) != run_id or desired != 0 or any(
            observed_annotations.get(key) != value for key, value in annotations.items()
        ):
            raise R2LabN3xxError("staged physical gNB ownership/binding was not proven")
        pods = parse_gnb_pods_json(
            _checked(
                runner,
                _cluster_ssh(
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
                "staged physical gNB pod verification",
            ).stdout
        )
        if not pods.zero:
            raise R2LabN3xxError("staged physical gNB did not remain at zero pods")

        temporary.replace(physical_dir)
        artifact = N3xxArtifact.from_dict(
            json.loads((physical_dir / "n3xx-artifact.json").read_text(encoding="utf-8")),
            physical_dir,
        )
        staging_payload = {
            "run_id": run_id,
            "package_sha256": artifact.package_sha256,
            "values_sha256": artifact.values_sha256,
            "render_sha256": artifact.render_sha256,
            "namespace_owned": True,
            "desired_replicas": 0,
            "gnb_pod_count": 0,
            "deployment_bound": True,
            "status": "staged-stopped",
            "hardware_mutation": False,
        }
        _write_json(physical_dir / "physical-staging.json", staging_payload)
        evidence.bind_staging(staging_payload).write_json(evidence_path)
        return artifact
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)
        raise


def _load_artifact(run_root: Path, run_id: str) -> N3xxArtifact:
    directory = run_root.expanduser().resolve() / run_id / "physical"
    try:
        payload = json.loads((directory / "n3xx-artifact.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise R2LabN3xxError("N3xx artifact metadata is missing or malformed") from exc
    if not isinstance(payload, dict):
        raise R2LabN3xxError("N3xx artifact metadata is malformed")
    return N3xxArtifact.from_dict(payload, directory)


def _deployment_proven(
    *, topology: PhysicalTopology, run_id: str, artifact: N3xxArtifact, known_hosts: Path, runner: Runner
) -> bool:
    try:
        payload = json.loads(
            _checked(
                runner,
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
                60,
                "physical gNB deployment query",
            ).stdout
        )
    except (json.JSONDecodeError, R2LabN3xxError):
        return False
    metadata = payload.get("metadata") if isinstance(payload, dict) else None
    spec = payload.get("spec") if isinstance(payload, dict) else None
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    annotations = metadata.get("annotations") if isinstance(metadata, dict) else None
    return (
        isinstance(labels, dict)
        and isinstance(annotations, dict)
        and isinstance(spec, dict)
        and labels.get(RUN_LABEL) == run_id
        and annotations.get(RUN_ANNOTATION) == run_id
        and annotations.get(PACKAGE_ANNOTATION) == artifact.package_sha256
        and annotations.get(VALUES_ANNOTATION) == artifact.values_sha256
        and annotations.get(RENDER_ANNOTATION) == artifact.render_sha256
        and spec.get("replicas") == 1
    )


def _pod_identity(text: str) -> tuple[str, str, str] | None:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return None
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or len(items) != 1 or not isinstance(items[0], dict):
        return None
    pod = items[0]
    metadata = pod.get("metadata")
    status = pod.get("status")
    if not isinstance(metadata, dict) or not isinstance(status, dict):
        return None
    name = metadata.get("name")
    created = metadata.get("creationTimestamp")
    statuses = status.get("containerStatuses")
    if not isinstance(name, str) or not isinstance(created, str) or not isinstance(statuses, list):
        return None
    gnb = next((item for item in statuses if isinstance(item, dict) and item.get("name") == "gnb"), None)
    image_id = gnb.get("imageID") if isinstance(gnb, dict) else None
    if not isinstance(image_id, str) or not image_id:
        return None
    return name, created, image_id


def _image_digest(image_id: str) -> str | None:
    match = re.search(r"sha256:[0-9a-f]{64}", image_id.lower())
    return match.group(0) if match else None


@dataclass(frozen=True)
class N3xxStartSummary:
    run_id: str
    radio: str
    attempts: int
    consecutive_n2_proofs: int
    observed_image_digest: str
    evidence_path: Path
    n2_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-n3xx-start/v1alpha1",
            "run_id": self.run_id,
            "radio": self.radio,
            "status": "gnb-n2-ready",
            "attempts": self.attempts,
            "consecutive_n2_proofs": self.consecutive_n2_proofs,
            "observed_image_digest": self.observed_image_digest,
            "evidence_path": str(self.evidence_path),
            "n2_path": str(self.n2_path),
            "next_stage": PhysicalAcceptanceStage.UE_MANAGEMENT.value,
        }


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
) -> N3xxStartSummary:
    """Start exactly one selected N3xx gNB and require stable current N2 proof."""

    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    artifact = _load_artifact(run_root, run_id)
    evidence_path = run_root.expanduser().resolve() / run_id / "physical-run.json"
    evidence = PhysicalRunEvidence.read_json(evidence_path)
    if evidence.staged is None or evidence.gnb_start is not None:
        raise R2LabN3xxError("N3xx start requires one stopped staged artifact and no prior start")
    if required_consecutive_proofs < 1 or convergence_attempts < 1:
        raise R2LabN3xxError("N2 proof attempt counts must be positive")
    total_attempts = required_consecutive_proofs + convergence_attempts - 1
    if total_attempts > 120:
        raise R2LabN3xxError("combined N2 convergence/stability attempts exceed 120")

    def authority() -> None:
        verify_physical_authority(
            run_id=run_id,
            slice_name=slice_name,
            run_root=run_root,
            runner=r2lab_runner,
            timeout_seconds=min(timeout_seconds, 300),
        )
        verify_selected_allocation(
            topology=topology,
            runner=runner,
            owner=owner,
            allocation_id=allocation_id,
            timeout_seconds=min(timeout_seconds, 300),
        )

    known_hosts = known_hosts.expanduser().resolve()
    authority()
    deployment = json.loads(
        _checked(
            runner,
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
            60,
            "staged physical gNB query",
        ).stdout
    )
    if deployment.get("metadata", {}).get("labels", {}).get(RUN_LABEL) != run_id or deployment.get("spec", {}).get("replicas") != 0:
        raise R2LabN3xxError("staged physical gNB is not stopped and owned by this run")
    authority()
    _checked(
        runner,
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
        60,
        "physical gNB singleton start",
    )
    started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    consecutive = 0
    attempts = 0
    observed_image_digest: str | None = None
    n2_source = "not-observed"
    peer_fingerprint: str | None = None
    last_observation: dict[str, object] = {}
    try:
        for attempt in range(1, total_attempts + 1):
            attempts = attempt
            pods_text = _checked(
                runner,
                _cluster_ssh(
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
                60,
                "current physical gNB pod query",
            ).stdout
            observation = parse_gnb_pods_json(pods_text)
            if observation.total_count > 1:
                raise R2LabN3xxError("overlapping physical gNB owners were observed")
            identity = _pod_identity(pods_text) if observation.exactly_one_ready else None
            deployment_bound = _deployment_proven(
                topology=topology,
                run_id=run_id,
                artifact=artifact,
                known_hosts=known_hosts,
                runner=runner,
            )
            n2_state = N2State.NOT_OBSERVED
            n2_source = "not-observed"
            peer_fingerprint = None
            log_observed = False
            image_ok = False
            if identity is not None:
                pod_name, _created, image_id = identity
                observed_image_digest = _image_digest(image_id)
                image_ok = observed_image_digest is not None
                if artifact.expected_image_digest is not None:
                    image_ok = observed_image_digest == artifact.expected_image_digest
                logs = _checked(
                    runner,
                    _cluster_ssh(
                        topology,
                        known_hosts,
                        "kubectl",
                        "logs",
                        f"pod/{pod_name}",
                        "-n",
                        NAMESPACE,
                        "-c",
                        "gnb",
                        "--tail=400",
                    ),
                    60,
                    "current physical gNB log query",
                )
                log_observed = True
                n2_state = parse_n2_log_state(logs.stdout)
                if n2_state is N2State.ESTABLISHED:
                    n2_source = "gnb-log"
                else:
                    amf_pods = json.loads(
                        _checked(
                            runner,
                            _cluster_ssh(
                                topology,
                                known_hosts,
                                "kubectl",
                                "get",
                                "pods",
                                "-n",
                                NAMESPACE,
                                "-l",
                                "nf=amf",
                                "-o",
                                "json",
                            ),
                            60,
                            "AMF pod query",
                        ).stdout
                    )
                    items = amf_pods.get("items", []) if isinstance(amf_pods, dict) else []
                    if len(items) == 1 and isinstance(items[0], dict):
                        amf_name = items[0].get("metadata", {}).get("name")
                        if isinstance(amf_name, str):
                            amf_logs = _checked(
                                runner,
                                _cluster_ssh(
                                    topology,
                                    known_hosts,
                                    "kubectl",
                                    "logs",
                                    f"pod/{amf_name}",
                                    "-n",
                                    NAMESPACE,
                                    "--tail=400",
                                    f"--since-time={started_at}",
                                ),
                                60,
                                "current AMF N2 log query",
                            )
                            amf = build_amf_n2_evidence(
                                text=amf_logs.stdout,
                                expected_peer=artifact.expected_gnb_peer,
                            )
                            if amf.proven:
                                n2_state = N2State.ESTABLISHED
                                n2_source = "amf-exact-peer"
                                peer_fingerprint = amf.peer_fingerprint
            proven = (
                observation.exactly_one_ready
                and deployment_bound
                and image_ok
                and n2_state is N2State.ESTABLISHED
            )
            last_observation = {
                "namespace_owned": True,
                "deployment_bound": deployment_bound,
                "desired_replicas": 1,
                "pod_count": observation.total_count,
                "ready_running_count": observation.ready_running_count,
                "n2_state": n2_state.value,
                "n2_source": n2_source,
                "peer_fingerprint": peer_fingerprint,
                "log_observed": log_observed,
                "transport_error": False,
                "proven": proven,
                "observed_image_digest": observed_image_digest,
                "radio": topology.radio,
                "ran_node": topology.ran_node,
            }
            consecutive = consecutive + 1 if proven else 0
            if consecutive >= required_consecutive_proofs:
                break
            if attempt < total_attempts:
                time.sleep(poll_interval_seconds)
        else:
            raise R2LabN3xxError("stable physical gNB/N2 proof was not established")

        assert observed_image_digest is not None
        claim_path = run_root.expanduser().resolve() / "active.json"
        claim_digest = _sha256_file(claim_path)
        start_payload = {
            "run_id": run_id,
            "package_sha256": artifact.package_sha256,
            "values_sha256": artifact.values_sha256,
            "render_sha256": artifact.render_sha256,
            "claim_sha256": claim_digest,
            "maximum_observed_pods": 1,
            "started_exactly_one": True,
            "status": "gnb-started",
            "hardware_mutation": True,
        }
        physical_dir = run_root.expanduser().resolve() / run_id / "physical"
        _write_json(physical_dir / "physical-gnb-start.json", start_payload)
        n2_path = _write_json(physical_dir / "physical-gnb-n2.json", last_observation)
        evidence = evidence.bind_gnb_start(start_payload)
        evidence = evidence.pass_stage(
            PhysicalAcceptanceStage.GNB_N2,
            source=f"current-{topology.radio}-singleton-stable-n2",
        )
        evidence.write_json(evidence_path)
        return N3xxStartSummary(
            run_id=run_id,
            radio=topology.radio,
            attempts=attempts,
            consecutive_n2_proofs=consecutive,
            observed_image_digest=observed_image_digest,
            evidence_path=evidence_path,
            n2_path=n2_path,
        )
    except Exception:
        physical_dir = run_root.expanduser().resolve() / run_id / "physical"
        if last_observation:
            failure_payload = {
                **last_observation,
                "status": "gnb-n2-not-proven",
                "attempts": attempts,
                "required_consecutive_proofs": required_consecutive_proofs,
                "convergence_attempts": convergence_attempts,
            }
            try:
                _write_json(physical_dir / "physical-gnb-n2-failure.json", failure_payload)
            except R2LabN3xxError:
                pass
        try:
            _checked(
                runner,
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
                60,
                "failed physical gNB scale-to-zero",
            )
            for _ in range(30):
                pods = parse_gnb_pods_json(
                    _checked(
                        runner,
                        _cluster_ssh(
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
                        60,
                        "failed physical gNB recovery query",
                    ).stdout
                )
                if pods.zero:
                    break
                time.sleep(2)
        except Exception as recovery_error:
            raise R2LabN3xxError(
                "physical gNB start failed and exact scale-to-zero recovery is unresolved"
            ) from recovery_error
        raise


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
) -> dict[str, object]:
    """Scale only the run-owned selected gNB to zero and prove no matching pods remain."""

    topology = load_topology(run_root=run_root, run_id=run_id).validate()
    artifact = _load_artifact(run_root, run_id)
    verify_physical_authority(
        run_id=run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    verify_selected_allocation(
        topology=topology,
        runner=runner,
        owner=owner,
        allocation_id=allocation_id,
        timeout_seconds=min(timeout_seconds, 300),
    )
    known_hosts = known_hosts.expanduser().resolve()
    deployment = json.loads(
        _checked(
            runner,
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
            60,
            "physical gNB stop ownership query",
        ).stdout
    )
    metadata = deployment.get("metadata", {}) if isinstance(deployment, dict) else {}
    labels = metadata.get("labels", {}) if isinstance(metadata, dict) else {}
    annotations = metadata.get("annotations", {}) if isinstance(metadata, dict) else {}
    if (
        labels.get(RUN_LABEL) != run_id
        or annotations.get(RUN_ANNOTATION) != run_id
        or annotations.get(PACKAGE_ANNOTATION) != artifact.package_sha256
    ):
        raise R2LabN3xxError("physical gNB stop refuses an unowned or foreign Deployment")
    _checked(
        runner,
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
        60,
        "physical gNB scale-to-zero",
    )
    for _ in range(30):
        pods = parse_gnb_pods_json(
            _checked(
                runner,
                _cluster_ssh(
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
                60,
                "physical gNB stopped-state query",
            ).stdout
        )
        if pods.zero:
            result = {
                "schema": "synthran/r2lab-n3xx-stop/v1alpha1",
                "run_id": run_id,
                "radio": topology.radio,
                "desired_replicas": 0,
                "gnb_pod_count": 0,
                "status": "gnb-stopped",
            }
            _write_json(run_root.expanduser().resolve() / run_id / "physical" / "physical-gnb-stop.json", result)
            return result
        time.sleep(2)
    raise R2LabN3xxError("physical gNB did not reach proven zero-pod state")
