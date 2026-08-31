"""Small shared helpers for the active physical gNB lifecycle.

The physical lifecycle itself lives in :mod:`synthran.r2lab.n3xx`. This module
contains only dependency-locked Helm materialization and sanitized Kubernetes
pod-state parsing that are shared by that lifecycle.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import tarfile
import tempfile
import urllib.request

from synthran.dependencies import DependencyLock


GNB_NAMESPACE = "open5gs"
GNB_DEPLOYMENT = "srsran-gnb"
GNB_SELECTOR = "app=srsran,component=gnb"
POD_RUNTIME_STATE_KEY = "phase"
MAX_LOCKED_HELM_ARCHIVE_BYTES = 64 * 1024 * 1024
LOCKED_HELM_ARCHIVE_MEMBER = "linux-amd64/helm"


class R2LabPhysicalHelmError(ValueError):
    """Raised when the dependency-locked Helm executable cannot be proven."""


class R2LabGnbLifecycleError(ValueError):
    """Raised when sanitized gNB pod state is malformed."""


def _locked_helm_metadata(lock: DependencyLock) -> tuple[str, str, str]:
    tools = lock.raw.get("tools")
    entry = tools.get("helm_linux_amd64") if isinstance(tools, dict) else None
    version = entry.get("version") if isinstance(entry, dict) else None
    url = entry.get("url") if isinstance(entry, dict) else None
    digest = entry.get("sha256") if isinstance(entry, dict) else None
    if (
        not isinstance(version, str)
        or not re.fullmatch(r"[0-9]+(?:\.[0-9]+){2}", version)
        or not isinstance(url, str)
        or not url.startswith("https://")
        or not isinstance(digest, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
    ):
        raise R2LabPhysicalHelmError("dependency lock does not define a complete Helm tool")
    return version, url, digest.removeprefix("sha256:")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def materialize_locked_helm(
    *,
    lock: DependencyLock,
    destination: Path,
    timeout_seconds: int = 60,
) -> Path:
    """Materialize the checksum-locked Linux AMD64 Helm executable once."""

    if timeout_seconds < 5 or timeout_seconds > 300:
        raise R2LabPhysicalHelmError(
            "locked Helm download timeout must be between 5 and 300 seconds"
        )
    if platform.system() != "Linux" or platform.machine().lower() not in {"amd64", "x86_64"}:
        raise R2LabPhysicalHelmError(
            "locked Helm executable supports only Linux AMD64 controllers"
        )
    version, url, expected_archive_sha256 = _locked_helm_metadata(lock)
    try:
        root = destination.expanduser().resolve() / version
        root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise R2LabPhysicalHelmError("locked Helm directory could not be prepared") from exc

    archive = root / f"helm-v{version}-linux-amd64.tar.gz"
    if archive.exists():
        if not archive.is_file() or archive.is_symlink():
            raise R2LabPhysicalHelmError("locked Helm archive path is unsafe")
        try:
            observed = _sha256(archive)
        except OSError as exc:
            raise R2LabPhysicalHelmError("locked Helm archive could not be inspected") from exc
        if observed != expected_archive_sha256:
            raise R2LabPhysicalHelmError(
                "existing locked Helm archive does not match the dependency lock"
            )
    else:
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix=".helm-download-", dir=root, delete=False
            ) as temporary:
                temporary_path = Path(temporary.name)
                with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
                    total = 0
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > MAX_LOCKED_HELM_ARCHIVE_BYTES:
                            raise R2LabPhysicalHelmError(
                                "locked Helm archive exceeds the reviewed size limit"
                            )
                        temporary.write(chunk)
            if _sha256(temporary_path) != expected_archive_sha256:
                raise R2LabPhysicalHelmError(
                    "downloaded Helm archive does not match the dependency lock"
                )
            temporary_path.replace(archive)
            temporary_path = None
        except R2LabPhysicalHelmError:
            raise
        except (OSError, ValueError) as exc:
            raise R2LabPhysicalHelmError("locked Helm archive could not be downloaded") from exc
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    executable = root / "helm"
    if executable.is_file() and not executable.is_symlink():
        return executable

    temporary_executable: Path | None = None
    try:
        with tarfile.open(archive, mode="r:gz") as bundle:
            member = bundle.getmember(LOCKED_HELM_ARCHIVE_MEMBER)
            if not member.isfile() or member.size <= 0 or member.size > MAX_LOCKED_HELM_ARCHIVE_BYTES:
                raise R2LabPhysicalHelmError("locked Helm archive member is malformed")
            source = bundle.extractfile(member)
            if source is None:
                raise R2LabPhysicalHelmError(
                    "locked Helm executable is unavailable in the archive"
                )
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix=".helm-extract-", dir=root, delete=False
            ) as temporary:
                temporary_executable = Path(temporary.name)
                remaining = member.size
                while remaining:
                    chunk = source.read(min(1024 * 1024, remaining))
                    if not chunk:
                        raise R2LabPhysicalHelmError("locked Helm executable is truncated")
                    temporary.write(chunk)
                    remaining -= len(chunk)
        os.chmod(temporary_executable, 0o755)
        temporary_executable.replace(executable)
        temporary_executable = None
    except R2LabPhysicalHelmError:
        raise
    except (KeyError, OSError, tarfile.TarError) as exc:
        raise R2LabPhysicalHelmError("locked Helm executable could not be materialized") from exc
    finally:
        if temporary_executable is not None:
            temporary_executable.unlink(missing_ok=True)
    return executable


@dataclass(frozen=True)
class GnbPodObservation:
    total_count: int
    ready_running_count: int
    terminating_count: int

    @property
    def zero(self) -> bool:
        return self.total_count == 0

    @property
    def exactly_one_ready(self) -> bool:
        return (
            self.total_count == 1
            and self.ready_running_count == 1
            and self.terminating_count == 0
        )


def parse_gnb_pods_json(text: str) -> GnbPodObservation:
    """Reduce a Kubernetes pod-list response to the gNB singleton state."""

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise R2LabGnbLifecycleError("gNB pod query did not return JSON") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise R2LabGnbLifecycleError("gNB pod query returned malformed JSON")

    total = 0
    ready_running = 0
    terminating = 0
    for item in payload["items"]:
        if not isinstance(item, dict):
            raise R2LabGnbLifecycleError("gNB pod query returned a malformed pod")
        metadata = item.get("metadata")
        status = item.get("status")
        if not isinstance(metadata, dict) or not isinstance(status, dict):
            raise R2LabGnbLifecycleError("gNB pod query returned incomplete pod state")
        total += 1
        is_terminating = metadata.get("deletionTimestamp") is not None
        if is_terminating:
            terminating += 1
        container_statuses = status.get("containerStatuses")
        containers_ready = (
            isinstance(container_statuses, list)
            and bool(container_statuses)
            and all(
                isinstance(container, dict) and container.get("ready") is True
                for container in container_statuses
            )
        )
        if (
            not is_terminating
            and status.get(POD_RUNTIME_STATE_KEY) == "Running"
            and containers_ready
        ):
            ready_running += 1
    return GnbPodObservation(total, ready_running, terminating)
