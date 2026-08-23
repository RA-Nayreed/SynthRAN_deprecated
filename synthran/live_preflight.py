"""Fail-closed, read-only checks for the SLICES golden-path deployment boundary."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import hashlib
import json
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile
from typing import Any, Callable, Mapping, Sequence
import urllib.parse
import urllib.request

from synthran.dependencies import DependencyLock
from synthran.fiveg_ansible import InventoryHost, NetworkInventory
from synthran.slices_controller import (
    SlicesControllerError,
    SlicesControllerReport,
    dependency_lock_sha256,
    fingerprint as context_fingerprint,
    validate_context,
    verify_slices_controller,
)


LIVE_PREFLIGHT_SCHEMA = "synthran/live-preflight/v1alpha2"
DEFAULT_TIMEOUT_SECONDS = 15
DEFAULT_EVIDENCE_MAX_AGE = timedelta(minutes=15)
SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
IMAGE_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
POS_TIMEZONE = ZoneInfo("Europe/Berlin")


class LivePreflightError(RuntimeError):
    """Raised when live readiness cannot be established safely."""


@dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str = ""


@dataclass(frozen=True)
class LiveCheck:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class LivePreflightReport:
    generated_at_utc: datetime
    dependency_lock_sha256: str
    slices_controller: SlicesControllerReport | None
    inventory_sha256: str
    owner_fingerprint: str
    reservation_fingerprint: str
    allocation_fingerprint: str
    checks: tuple[LiveCheck, ...]

    @property
    def ready(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)

    def render(self) -> str:
        lines = ["SynthRAN doctor (live read-only)"]
        for check in self.checks:
            state = "PASS" if check.passed else "FAIL"
            lines.append(f"[{state}] {check.name}: {check.detail}")
        lines.append(f"Result: {'READY' if self.ready else 'NOT READY'}")
        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": LIVE_PREFLIGHT_SCHEMA,
            "generated_at_utc": _format_time(self.generated_at_utc),
            "ready": self.ready,
            "dependency_lock_sha256": self.dependency_lock_sha256,
            "slices_controller": (
                self.slices_controller.to_dict() if self.slices_controller else None
            ),
            "inventory_sha256": self.inventory_sha256,
            "owner_fingerprint": self.owner_fingerprint,
            "reservation_fingerprint": self.reservation_fingerprint,
            "allocation_fingerprint": self.allocation_fingerprint,
            "checks": [
                {"name": item.name, "passed": item.passed, "detail": item.detail}
                for item in self.checks
            ],
        }


Runner = Callable[[Sequence[str], int], CommandResult]
Which = Callable[[str], str | None]
ImageProbe = Callable[[str, int], None]


def subprocess_runner(command: Sequence[str], timeout_seconds: int) -> CommandResult:
    """Execute an argv-only probe without invoking a local shell."""

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
        raise LivePreflightError("a required preflight executable was not found") from exc
    except subprocess.TimeoutExpired as exc:
        raise LivePreflightError("a live preflight command timed out") from exc
    return CommandResult(completed.returncode, completed.stdout, completed.stderr)


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _validate_identifier(value: str, label: str) -> str:
    if not SAFE_IDENTIFIER_RE.fullmatch(value):
        raise LivePreflightError(
            f"{label} must contain only letters, numbers, '.', '_', ':', or '-'"
        )
    return value


def _format_time(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc).replace(microsecond=0)
    return normalized.isoformat().replace("+00:00", "Z")


def _parse_time(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise LivePreflightError(f"{label} is missing from provider evidence")

    text = value.strip()

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise LivePreflightError(
            f"{label} is not an ISO-8601 timestamp"
        ) from exc

    # POS 2.5.35 returns calendar timestamps without a UTC offset,
    # e.g. "2026-08-15 02:30:00". These are POS-local times.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=POS_TIMEZONE)

    return parsed.astimezone(timezone.utc)


def _parse_json_object(text: str, label: str) -> Mapping[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise LivePreflightError(f"{label} did not return JSON") from exc
    if not isinstance(value, dict):
        raise LivePreflightError(f"{label} must return one JSON object")
    return value


def _parse_json_objects(text: str, label: str) -> tuple[Mapping[str, Any], ...]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise LivePreflightError(f"{label} did not return JSON") from exc
    if not isinstance(value, list):
        raise LivePreflightError(f"{label} must return one JSON array")
    records: list[Mapping[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            raise LivePreflightError(f"{label} contains a non-object record")
        records.append(item)
    return tuple(records)


def _first_text(
    value: Mapping[str, Any], names: Sequence[str], label: str
) -> str:
    for name in names:
        candidate = value.get(name)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    raise LivePreflightError(f"{label} is missing from provider evidence")


def _first_identifier(
    value: Mapping[str, Any], names: Sequence[str], label: str
) -> str:
    for name in names:
        candidate = value.get(name)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
        if (
            isinstance(candidate, int)
            and not isinstance(candidate, bool)
            and candidate >= 0
        ):
            return str(candidate)
    raise LivePreflightError(f"{label} is missing from provider evidence")


def _node_names(value: object) -> set[str]:
    if not isinstance(value, list):
        raise LivePreflightError("reservation nodes are missing from provider evidence")
    names: set[str] = set()
    for item in value:
        if isinstance(item, str) and item.strip():
            names.add(item.strip())
        elif isinstance(item, dict):
            names.add(_first_text(item, ("id", "name", "node"), "reservation node"))
        else:
            raise LivePreflightError("reservation nodes contain an unsupported value")
    return names


def _checked_output(
    runner: Runner,
    command: Sequence[str],
    *,
    timeout_seconds: int,
    label: str,
) -> str:
    result = runner(command, timeout_seconds)
    if result.returncode != 0:
        raise LivePreflightError(f"{label} failed")
    if not result.stdout.strip():
        raise LivePreflightError(f"{label} returned no output")
    return result.stdout


def verify_reservation(
    *,
    runner: Runner,
    reservation_id: str | None,
    owner: str,
    nodes: set[str],
    now: datetime,
    timeout_seconds: int,
) -> str:
    """Verify a current, owned POS calendar record without creating one."""

    records = _parse_json_objects(
        _checked_output(
            runner,
            (
                "pos",
                "calendar",
                "list",
                "--filter",
                f"owner={owner}",
                "--json",
            ),
            timeout_seconds=timeout_seconds,
            label="POS reservation query",
        ),
        "POS reservation query",
    )
    if reservation_id is None:
        matches = tuple(
            record
            for record in records
            if _first_text(record, ("owner",), "reservation owner") == owner
            and _parse_time(record.get("start_date"), "reservation start")
            <= now
            < _parse_time(record.get("end_date"), "reservation end")
            and nodes.issubset(_node_names(record.get("nodes")))
        )
    else:
        matches = tuple(
            record
            for record in records
            if _first_identifier(record, ("id",), "reservation id")
            == reservation_id
        )
    if not matches:
        raise LivePreflightError(
            "no matching active reservation was found in the POS calendar"
            if reservation_id is None
            else "reservation id was not found in the POS calendar"
        )
    if len(matches) != 1:
        raise LivePreflightError(
            "active reservation is ambiguous in the POS calendar"
            if reservation_id is None
            else "reservation id is ambiguous in the POS calendar"
        )
    payload = matches[0]
    observed_id = _first_identifier(payload, ("id",), "reservation id")
    observed_owner = _first_text(payload, ("owner",), "reservation owner")
    observed_nodes = _node_names(payload.get("nodes"))
    starts_at = _parse_time(payload.get("start_date"), "reservation start")
    ends_at = _parse_time(payload.get("end_date"), "reservation end")
    if observed_owner != owner:
        raise LivePreflightError("reservation owner does not match the expected operator")
    if not starts_at <= now < ends_at:
        raise LivePreflightError("reservation is not active at the current UTC time")
    if not nodes.issubset(observed_nodes):
        raise LivePreflightError("reservation does not cover every selected node")
    return observed_id


def verify_allocations(
    *,
    runner: Runner,
    allocation_id: str | None,
    owner: str,
    nodes: set[str],
    timeout_seconds: int,
) -> str:
    """Verify every selected node belongs to one expected, owned allocation."""

    observed_ids: set[str] = set()
    for node in sorted(nodes):
        payload = _parse_json_object(
            _checked_output(
                runner,
                ("pos", "allocations", "show", node),
                timeout_seconds=timeout_seconds,
                label=f"POS allocation query for {node}",
            ),
            f"POS allocation query for {node}",
        )
        observed_id = _first_identifier(payload, ("id",), "allocation id")
        observed_owner = _first_text(payload, ("owner",), "allocation owner")
        if allocation_id is not None and observed_id != allocation_id:
            raise LivePreflightError(
                "a selected node is not in the expected allocation"
            )
        if observed_owner != owner:
            raise LivePreflightError(
                "a selected node allocation is not owned by the expected operator"
            )
        observed_ids.add(observed_id)
    if len(observed_ids) != 1:
        raise LivePreflightError(
            "selected nodes do not belong to one common allocation"
        )
    return next(iter(observed_ids))


def ssh_command(
    host: InventoryHost,
    *remote_command: str,
) -> tuple[str, ...]:
    address = host.variables.get("ansible_host", host.name)
    user = host.variables.get("ansible_user")
    if not user:
        raise LivePreflightError("inventory host is missing ansible_user")

    known_hosts_value = os.environ.get("SYNTHRAN_KNOWN_HOSTS")
    if not known_hosts_value:
        raise LivePreflightError(
            "SYNTHRAN_KNOWN_HOSTS is required for strict SSH probes"
        )

    known_hosts = Path(known_hosts_value).expanduser().resolve()
    if not known_hosts.is_file():
        raise LivePreflightError(
            "SYNTHRAN_KNOWN_HOSTS does not name an existing file"
        )

    target = f"{user}@{address}"
    command: list[str] = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
    ]

    port = host.variables.get("ansible_port")
    if port:
        if not port.isdigit() or not 1 <= int(port) <= 65535:
            raise LivePreflightError("inventory ansible_port is invalid")
        command.extend(("-p", port))

    command.append(target)

    if remote_command:
        command.append(" ".join(shlex.quote(part) for part in remote_command))

    return tuple(command)


def _short_hostname(value: str) -> str:
    return value.strip().lower().split(".", 1)[0]


def verify_ssh_host(
    *, runner: Runner, host: InventoryHost, timeout_seconds: int
) -> None:
    output = _checked_output(
        runner,
        ssh_command(host, "hostname"),
        timeout_seconds=timeout_seconds,
        label=f"SSH identity probe for {host.name}",
    )
    observed = output.splitlines()[0] if output.splitlines() else ""
    if _short_hostname(observed) != _short_hostname(host.name):
        raise LivePreflightError("SSH host identity does not match the inventory alias")


def verify_ansible_controller(
    *, runner: Runner, lock: DependencyLock, timeout_seconds: int
) -> str:
    """Require the exact Ansible controller version recorded by the lock."""

    output = _checked_output(
        runner,
        ("ansible-playbook", "--version"),
        timeout_seconds=timeout_seconds,
        label="Ansible controller probe",
    )
    conda = lock.raw.get("conda")
    packages = conda.get("packages") if isinstance(conda, dict) else None
    entry = packages.get("ansible-core") if isinstance(packages, dict) else None
    expected = entry.get("version") if isinstance(entry, dict) else None
    if not isinstance(expected, str) or not expected:
        raise LivePreflightError("dependency lock does not define ansible-core")
    match = re.search(r"\bcore\s+([0-9]+\.[0-9]+\.[0-9]+)", output)
    if match is None:
        raise LivePreflightError("Ansible controller version is not parseable")
    if match.group(1) != expected:
        raise LivePreflightError(
            f"ansible-core must exactly match locked version {expected}"
        )
    galaxy = runner(("ansible-galaxy", "--version"), timeout_seconds)
    if galaxy.returncode != 0:
        raise LivePreflightError("ansible-galaxy is unavailable")
    return f"ansible-core exactly matches locked version {expected}"


def verify_remote_deployment_tools(
    *,
    runner: Runner,
    core_host: InventoryHost,
    ran_host: InventoryHost,
    lock: DependencyLock,
    timeout_seconds: int,
) -> str:
    """Require the exact preinstalled tools that bypass upstream download paths."""

    tools = lock.raw.get("tools")
    if not isinstance(tools, dict):
        raise LivePreflightError("dependency lock tool mapping is unavailable")
    yq = tools.get("yq_linux_amd64")
    helm = tools.get("helm_linux_amd64")
    if not isinstance(yq, dict):
        raise LivePreflightError("dependency lock does not define the golden-path yq tool")
    version = yq.get("version")
    digest = yq.get("sha256")
    path = yq.get("path")
    if (
        not isinstance(version, str)
        or not isinstance(digest, str)
        or not IMAGE_DIGEST_RE.fullmatch(digest)
        or path != "/usr/local/bin/yq"
    ):
        raise LivePreflightError("golden-path yq tool lock is malformed")
    if not isinstance(helm, dict):
        raise LivePreflightError("dependency lock does not define the golden-path Helm tool")
    helm_version = helm.get("version")
    helm_path = helm.get("path")
    helm_digest = helm.get("sha256")
    if (
        not isinstance(helm_version, str)
        or not re.fullmatch(r"[0-9]+(?:\.[0-9]+){2}", helm_version)
        or helm_path != "/usr/local/bin/helm"
        or not isinstance(helm_digest, str)
        or not IMAGE_DIGEST_RE.fullmatch(helm_digest)
    ):
        raise LivePreflightError("golden-path Helm tool lock is malformed")

    remote_python = lock.raw.get("remote_python")
    packages = remote_python.get("packages") if isinstance(remote_python, dict) else None
    if (
        not isinstance(packages, dict)
        or not packages
        or any(
            not isinstance(name, str) or not isinstance(package_version, str)
            for name, package_version in packages.items()
        )
    ):
        raise LivePreflightError("remote Python package lock is malformed")
    expected_packages = dict(sorted(packages.items()))

    _checked_output(
        runner,
        ssh_command(
            core_host,
            "sh",
            "-c",
            "command -v git && command -v jq && command -v kubectl && "
            "test -x /opt/synthran-venv/bin/python",
        ),
        timeout_seconds=timeout_seconds,
        label="core-node deployment tool probe",
    )
    package_names = json.dumps(list(expected_packages))
    package_probe = (
        "import importlib.metadata as m, json, kubernetes; "
        f"names = {package_names}; "
        "print(json.dumps({name: m.version(name) for name in names}, sort_keys=True))"
    )
    observed_packages = _parse_json_object(
        _checked_output(
            runner,
            ssh_command(
                core_host,
                "/opt/synthran-venv/bin/python",
                "-c",
                package_probe,
            ),
            timeout_seconds=timeout_seconds,
            label="core-node Python package probe",
        ),
        "core-node Python package probe",
    )
    if observed_packages != expected_packages:
        raise LivePreflightError(
            "core-node Python package versions do not match the lock"
        )

    _checked_output(
        runner,
        ssh_command(
            ran_host,
            "sh",
            "-c",
            "command -v git && command -v kubectl && "
            "test -x /opt/synthran-venv/bin/python",
        ),
        timeout_seconds=timeout_seconds,
        label="RAN-node deployment tool probe",
    )
    observed_ran_packages = _parse_json_object(
        _checked_output(
            runner,
            ssh_command(
                ran_host,
                "/opt/synthran-venv/bin/python",
                "-c",
                package_probe,
            ),
            timeout_seconds=timeout_seconds,
            label="RAN-node Python package probe",
        ),
        "RAN-node Python package probe",
    )
    if observed_ran_packages != expected_packages:
        raise LivePreflightError(
            "RAN-node Python package versions do not match the lock"
        )
    helm_output = _checked_output(
        runner,
        ssh_command(ran_host, helm_path, "version", "--short"),
        timeout_seconds=timeout_seconds,
        label="remote Helm probe",
    ).strip()
    if f"v{helm_version}" not in helm_output:
        raise LivePreflightError("remote Helm version does not match the lock")
    helm_archive = f"/opt/synthran-tools/helm-{helm_version}.tar.gz"
    helm_archive_digest = _checked_output(
        runner,
        ssh_command(ran_host, "sha256sum", helm_archive),
        timeout_seconds=timeout_seconds,
        label="remote Helm archive digest probe",
    )
    if (
        helm_archive_digest.split(maxsplit=1)[0]
        != helm_digest.removeprefix("sha256:")
    ):
        raise LivePreflightError("remote Helm archive digest does not match the lock")
    _checked_output(
        runner,
        ssh_command(
            ran_host,
            "sh",
            "-c",
            f"cmp -s {helm_path} "
            f"/opt/synthran-tools/{helm_version}/linux-amd64/helm "
            "&& echo helm-binary-ready",
        ),
        timeout_seconds=timeout_seconds,
        label="remote Helm binary probe",
    )
    yq_output = _checked_output(
        runner,
        ssh_command(ran_host, path, "--version"),
        timeout_seconds=timeout_seconds,
        label="remote yq version probe",
    )
    if f"v{version}" not in yq_output:
        raise LivePreflightError("remote yq version does not match the lock")
    digest_output = _checked_output(
        runner,
        ssh_command(ran_host, "sha256sum", path),
        timeout_seconds=timeout_seconds,
        label="remote yq digest probe",
    )
    if digest_output.split(maxsplit=1)[0] != digest.removeprefix("sha256:"):
        raise LivePreflightError("remote yq digest does not match the lock")
    return (
        "remote Git, kubectl, exact Helm, digest-locked yq, jq, and exact "
        "SynthRAN Python environments are ready"
    )


_KUBERNETES_STATE_COMMAND = (
    "test -r /etc/kubernetes/admin.conf && "
    "KUBECONFIG=/etc/kubernetes/admin.conf kubectl get nodes -o json"
)

_KUBERNETES_NETWORK_COMMAND = (
    "test -x /opt/cni/bin/multus-shim && test -x /opt/cni/bin/ovs && "
    "! KUBECONFIG=/etc/kubernetes/admin.conf kubectl get namespace open5gs "
    ">/dev/null 2>&1 && "
    "KUBECONFIG=/etc/kubernetes/admin.conf kubectl get crd "
    "network-attachment-definitions.k8s.cni.cncf.io -o name"
)


def verify_kubernetes_start_state(
    *,
    runner: Runner,
    core_host: InventoryHost,
    selected_nodes: set[str],
    timeout_seconds: int,
) -> str:
    """Require the selected nodes and 5G networking prerequisites to be Ready."""

    payload = _parse_json_object(
        _checked_output(
            runner,
            ssh_command(core_host, "sh", "-c", _KUBERNETES_STATE_COMMAND),
            timeout_seconds=timeout_seconds,
            label="Kubernetes start-state probe",
        ),
        "Kubernetes start-state probe",
    )
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise LivePreflightError("Kubernetes returned no nodes")
    ready_nodes: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            raise LivePreflightError("Kubernetes node evidence is malformed")
        metadata = item.get("metadata")
        status = item.get("status")
        if not isinstance(metadata, dict) or not isinstance(status, dict):
            raise LivePreflightError("Kubernetes node evidence is incomplete")
        name = metadata.get("name")
        conditions = status.get("conditions")
        if not isinstance(name, str) or not isinstance(conditions, list):
            raise LivePreflightError("Kubernetes node readiness is unavailable")
        is_ready = any(
            isinstance(condition, dict)
            and condition.get("type") == "Ready"
            and condition.get("status") == "True"
            for condition in conditions
        )
        if is_ready:
            ready_nodes.add(_short_hostname(name))
    expected = {_short_hostname(node) for node in selected_nodes}
    if not expected.issubset(ready_nodes):
        raise LivePreflightError("Kubernetes does not report every selected node Ready")
    network_result = runner(
        ssh_command(core_host, "sh", "-c", _KUBERNETES_NETWORK_COMMAND),
        timeout_seconds,
    )
    if network_result.returncode != 0:
        raise LivePreflightError(
            "Kubernetes Multus, OVS CNI, or NetworkAttachmentDefinition support is unavailable"
        )
    if (
        network_result.stdout.strip()
        != "customresourcedefinition.apiextensions.k8s.io/network-attachment-definitions.k8s.cni.cncf.io"
    ):
        raise LivePreflightError("Kubernetes NetworkAttachmentDefinition CRD is unavailable")
    return "selected empty cluster, Multus, OVS CNI, and NAD support are Ready"


def golden_path_image_references(lock: DependencyLock) -> tuple[str, ...]:
    containers = lock.raw.get("containers")
    if not isinstance(containers, dict):
        raise LivePreflightError("dependency lock container mapping is unavailable")
    references: list[str] = []
    for name, raw in containers.items():
        if not isinstance(raw, dict):
            raise LivePreflightError(f"container lock entry {name} is malformed")
        role = raw.get("role", "")
        if not isinstance(role, str) or not role.startswith("Golden path "):
            continue
        image = raw.get("image")
        digest = raw.get("digest")
        platform = raw.get("platform")
        if not isinstance(image, str) or not image:
            raise LivePreflightError(f"container lock entry {name} has no image")
        if not isinstance(digest, str) or not IMAGE_DIGEST_RE.fullmatch(digest):
            raise LivePreflightError(f"container lock entry {name} has no full digest")
        if platform != "linux/amd64":
            raise LivePreflightError(
                f"container lock entry {name} is not pinned for linux/amd64"
            )
        references.append(f"{image}@{digest}")
    if not references:
        raise LivePreflightError("dependency lock defines no golden-path images")
    return tuple(sorted(references))


def _registry_parts(reference: str) -> tuple[str, str, str]:
    try:
        image, digest = reference.rsplit("@", 1)
    except ValueError as exc:
        raise LivePreflightError("image reference is not digest-addressed") from exc
    if not IMAGE_DIGEST_RE.fullmatch(digest):
        raise LivePreflightError("image reference does not use a full sha256 digest")
    pieces = image.split("/")
    if len(pieces) < 2:
        registry = "docker.io"
        repository = f"library/{image}"
    elif "." in pieces[0] or ":" in pieces[0] or pieces[0] == "localhost":
        registry = pieces[0].lower()
        repository = "/".join(pieces[1:])
    else:
        registry = "docker.io"
        repository = image
    if registry == "index.docker.io":
        registry = "docker.io"
    return registry, repository, digest


def registry_image_probe(reference: str, timeout_seconds: int) -> None:
    """Verify a public digest is available without pulling image layers."""

    registry, repository, digest = _registry_parts(reference)
    if registry == "docker.io":
        token_url = "https://auth.docker.io/token?" + urllib.parse.urlencode(
            {
                "service": "registry.docker.io",
                "scope": f"repository:{repository}:pull",
            }
        )
        manifest_url = f"https://registry-1.docker.io/v2/{repository}/manifests/{digest}"
    elif registry == "ghcr.io":
        token_url = "https://ghcr.io/token?" + urllib.parse.urlencode(
            {"scope": f"repository:{repository}:pull"}
        )
        manifest_url = f"https://ghcr.io/v2/{repository}/manifests/{digest}"
    else:
        raise LivePreflightError(f"unsupported public image registry: {registry}")
    try:
        with urllib.request.urlopen(token_url, timeout=timeout_seconds) as response:
            token_payload = json.load(response)
        bearer_value = token_payload.get("token")
        if not isinstance(bearer_value, str) or not bearer_value:
            raise LivePreflightError("container registry did not issue a pull token")
        request = urllib.request.Request(
            manifest_url,
            method="HEAD",
            headers={
                "Authorization": f"Bearer {bearer_value}",
                "Accept": (
                    "application/vnd.oci.image.index.v1+json, "
                    "application/vnd.oci.image.manifest.v1+json, "
                    "application/vnd.docker.distribution.manifest.list.v2+json, "
                    "application/vnd.docker.distribution.manifest.v2+json"
                ),
            },
        )
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            observed = response.headers.get("Docker-Content-Digest")
    except (OSError, ValueError) as exc:
        raise LivePreflightError("container image registry probe failed") from exc
    if observed != digest:
        raise LivePreflightError("container registry digest does not match the lock")


def _record(checks: list[LiveCheck], name: str, operation: Callable[[], str]) -> bool:
    try:
        detail = operation()
    except LivePreflightError as exc:
        checks.append(LiveCheck(name, False, str(exc)))
        return False
    checks.append(LiveCheck(name, True, detail))
    return True


def _verified_detail(operation: Callable[[], object], detail: str) -> str:
    operation()
    return detail


def run_live_preflight(
    *,
    inventory: NetworkInventory,
    lock: DependencyLock,
    owner: str,
    reservation_id: str,
    allocation_id: str,
    slices_project: str,
    slices_experiment: str,
    runner: Runner = subprocess_runner,
    which: Which = shutil.which,
    image_probe: ImageProbe = registry_image_probe,
    now: datetime | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> LivePreflightReport:
    """Run all read-only golden-path readiness probes and never reserve or modify."""

    owner = _validate_identifier(owner, "owner")
    reservation_id = _validate_identifier(reservation_id, "reservation id")
    allocation_id = _validate_identifier(allocation_id, "allocation id")
    if timeout_seconds <= 0 or timeout_seconds > 300:
        raise LivePreflightError("preflight timeout must be between 1 and 300 seconds")
    observed_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    nodes = {inventory.core_node.name, inventory.ran_node.name}
    checks: list[LiveCheck] = []
    controller: SlicesControllerReport | None = None
    try:
        controller = verify_slices_controller(
            lock=lock,
            project=slices_project,
            experiment=slices_experiment,
            runner=runner,
            which=which,
            timeout_seconds=timeout_seconds,
        )
    except SlicesControllerError as exc:
        checks.append(LiveCheck("slices-controller", False, str(exc)))
    else:
        checks.append(
            LiveCheck(
                "slices-controller",
                True,
                "SLICES login, project, experiment, and locked controller verified",
            )
        )


    required_tools = ("slices", "pos", "ssh", "git", "ansible-galaxy", "ansible-playbook")
    missing = tuple(tool for tool in required_tools if which(tool) is None)
    tools_ready = controller is not None and not missing
    checks.append(
        LiveCheck(
            "toolchain",
            tools_ready,
            (
                "required SLICES, POS, SSH, Git, and Ansible commands are available"
                if tools_ready
                else (
                    "SLICES controller verification failed"
                    if controller is None and not missing
                    else "missing required command(s): " + ", ".join(missing)
                )
            ),
        )
    )

    if tools_ready:
        _record(
            checks,
            "ansible-controller",
            lambda: verify_ansible_controller(
                runner=runner,
                lock=lock,
                timeout_seconds=timeout_seconds,
            ),
        )
        _record(
            checks,
            "reservation",
            lambda: _verified_detail(
                lambda: verify_reservation(
                    runner=runner,
                    reservation_id=reservation_id,
                    owner=owner,
                    nodes=nodes,
                    now=observed_at,
                    timeout_seconds=timeout_seconds,
                ),
                f"active owned reservation covers {len(nodes)} selected node(s)",
            ),
        )
        _record(
            checks,
            "allocation",
            lambda: _verified_detail(
                lambda: verify_allocations(
                    runner=runner,
                    allocation_id=allocation_id,
                    owner=owner,
                    nodes=nodes,
                    timeout_seconds=timeout_seconds,
                ),
                f"one owned allocation contains {len(nodes)} selected node(s)",
            ),
        )
        core_ssh_ready = _record(
            checks,
            f"ssh:{inventory.core_node.name}",
            lambda: (
                verify_ssh_host(
                    runner=runner,
                    host=inventory.core_node,
                    timeout_seconds=timeout_seconds,
                )
                or "strict host-key and remote identity checks passed"
            ),
        )
        ran_ssh_ready = core_ssh_ready
        if inventory.ran_node.name != inventory.core_node.name:
            ran_ssh_ready = _record(
                checks,
                f"ssh:{inventory.ran_node.name}",
                lambda: (
                    verify_ssh_host(
                        runner=runner,
                        host=inventory.ran_node,
                        timeout_seconds=timeout_seconds,
                    )
                    or "strict host-key and remote identity checks passed"
                ),
            )
        if ran_ssh_ready:
            _record(
                checks,
                "remote-deployment-tools",
                lambda: verify_remote_deployment_tools(
                    runner=runner,
                    core_host=inventory.core_node,
                    ran_host=inventory.ran_node,
                    lock=lock,
                    timeout_seconds=timeout_seconds,
                ),
            )
        else:
            checks.append(
                LiveCheck(
                    "remote-deployment-tools",
                    False,
                    "not probed because RAN-node SSH identity failed",
                )
            )
        if core_ssh_ready:
            _record(
                checks,
                "kubernetes-start-state",
                lambda: verify_kubernetes_start_state(
                    runner=runner,
                    core_host=inventory.core_node,
                    selected_nodes=nodes,
                    timeout_seconds=timeout_seconds,
                ),
            )
        else:
            checks.append(
                LiveCheck(
                    "kubernetes-start-state",
                    False,
                    "not probed because core-node SSH identity failed",
                )
            )
    else:
        for name in (
            "ansible-controller",
            "reservation",
            "allocation",
            f"ssh:{inventory.core_node.name}",
            *(
                (f"ssh:{inventory.ran_node.name}",)
                if inventory.ran_node.name != inventory.core_node.name
                else ()
            ),
            "remote-deployment-tools",
            "kubernetes-start-state",
        ):
            checks.append(
                LiveCheck(name, False, "not probed because the live toolchain is incomplete")
            )

    if controller is None:
        checks.append(
            LiveCheck(
                "required-images",
                False,
                "not probed because SLICES controller verification failed",
            )
        )
    else:
        try:
            images = golden_path_image_references(lock)
        except LivePreflightError as exc:
            checks.append(LiveCheck("required-images", False, str(exc)))
        else:
            def verify_images() -> str:
                for reference in images:
                    image_probe(reference, timeout_seconds)
                return f"{len(images)} digest-addressed golden-path image(s) are available"

            _record(checks, "required-images", verify_images)

    return LivePreflightReport(
        generated_at_utc=observed_at,
        dependency_lock_sha256=dependency_lock_sha256(lock),
        slices_controller=controller,
        inventory_sha256=inventory.sha256,
        owner_fingerprint=_fingerprint(owner),
        reservation_fingerprint=_fingerprint(reservation_id),
        allocation_fingerprint=_fingerprint(allocation_id),
        checks=tuple(checks),
    )


def save_live_evidence(report: LivePreflightReport, destination: Path) -> None:
    """Atomically write only sanitized readiness facts."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=destination.parent,
        delete=False,
    ) as temporary:
        temporary.write(payload)
        temporary_path = Path(temporary.name)
    temporary_path.replace(destination)


def load_fresh_live_evidence(
    *,
    path: Path,
    inventory: NetworkInventory,
    owner: str,
    reservation_id: str,
    allocation_id: str,
    lock: DependencyLock,
    slices_project: str,
    slices_experiment: str,
    now: datetime | None = None,
    max_age: timedelta = DEFAULT_EVIDENCE_MAX_AGE,
) -> Mapping[str, Any]:
    """Validate that deployment authorization is fresh and matches its exact inputs."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise LivePreflightError("live preflight evidence file was not found") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LivePreflightError("live preflight evidence must be readable JSON") from exc
    if not isinstance(payload, dict) or payload.get("schema") != LIVE_PREFLIGHT_SCHEMA:
        raise LivePreflightError("live preflight evidence schema is unsupported")
    if payload.get("ready") is not True:
        raise LivePreflightError("live preflight evidence is not READY")
    lock_digest = dependency_lock_sha256(lock)
    if payload.get("dependency_lock_sha256") != lock_digest:
        raise LivePreflightError("live preflight evidence does not match the dependency lock")
    try:
        project = validate_context(slices_project, "SLICES project")
        experiment = validate_context(slices_experiment, "SLICES experiment")
    except SlicesControllerError as exc:
        raise LivePreflightError(str(exc)) from exc
    controller = payload.get("slices_controller")
    if not isinstance(controller, dict) or controller.get("ready") is not True:
        raise LivePreflightError("live preflight evidence lacks verified SLICES context")
    controller_expected = {
        "schema": "synthran/slices-controller/v1alpha1",
        "dependency_lock_sha256": lock_digest,
        "project_fingerprint": context_fingerprint(project),
        "experiment_fingerprint": context_fingerprint(experiment),
    }
    for name, value in controller_expected.items():
        if controller.get(name) != value:
            raise LivePreflightError(
                "live preflight evidence does not match the SLICES controller context"
            )
    for name in (
        "python_version",
        "ansible_version",
        "pos_version",
        "slices_cli_version",
    ):
        if not isinstance(controller.get(name), str) or not controller[name]:
            raise LivePreflightError(
                "live preflight evidence has incomplete SLICES controller versions"
            )
    if payload.get("inventory_sha256") != inventory.sha256:
        raise LivePreflightError("live preflight evidence does not match the inventory")
    expected = {
        "owner_fingerprint": _fingerprint(
            _validate_identifier(owner, "owner")
        ),
        "reservation_fingerprint": _fingerprint(
            _validate_identifier(reservation_id, "reservation id")
        ),
        "allocation_fingerprint": _fingerprint(
            _validate_identifier(allocation_id, "allocation id")
        ),
    }
    for name, value in expected.items():
        if payload.get(name) != value:
            raise LivePreflightError(
                "live preflight evidence does not match the deployment authority"
            )
    checks = payload.get("checks")
    if not isinstance(checks, list):
        raise LivePreflightError("live preflight evidence checks are missing")
    expected_names = {
        "slices-controller",
        "toolchain",
        "ansible-controller",
        "reservation",
        "allocation",
        f"ssh:{inventory.core_node.name}",
        "remote-deployment-tools",
        "kubernetes-start-state",
        "required-images",
    }
    if inventory.ran_node.name != inventory.core_node.name:
        expected_names.add(f"ssh:{inventory.ran_node.name}")
    observed_names: set[str] = set()
    for check in checks:
        if not isinstance(check, dict):
            raise LivePreflightError("live preflight evidence contains an invalid check")
        name = check.get("name")
        detail = check.get("detail")
        if (
            not isinstance(name, str)
            or name in observed_names
            or check.get("passed") is not True
            or not isinstance(detail, str)
            or not detail
        ):
            raise LivePreflightError("live preflight evidence check set is not trustworthy")
        observed_names.add(name)
    if observed_names != expected_names:
        raise LivePreflightError("live preflight evidence check set is incomplete")
    generated_at = _parse_time(payload.get("generated_at_utc"), "evidence timestamp")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    age = current - generated_at
    if age < timedelta(0) or age > max_age:
        raise LivePreflightError("live preflight evidence is stale")
    return payload
