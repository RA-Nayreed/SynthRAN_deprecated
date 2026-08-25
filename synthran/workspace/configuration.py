"""Local SynthRAN workspace configuration discovery and updates."""

from __future__ import annotations

from dataclasses import dataclass, replace
import fcntl
import os
from pathlib import Path
from typing import Mapping

from synthran.workspace.model import WorkspaceConfig, WorkspaceError, validate_profile_name
from synthran.workspace.store import (
    PROFILE_DIRECTORY,
    _atomic_text,
    active_path,
    config_home,
    load_active_experiment_id,
    load_experiment_record,
    load_profile,
    load_workspace,
    workspace_directory,
    workspace_file,
    workspace_to_toml,
)


PRIVATE_KEY_BEGIN = b"-----BEGIN "
PRIVATE_KEY_SUFFIX = b" KEY-----"
PRIVATE_KEY_MARKER = b"PRIVATE"
SSH_IDENTITY_PREFIX = "~/.ssh/"


@dataclass(frozen=True)
class ProfileSummary:
    name: str
    slices_username: str | None
    r2lab_slice: str | None
    identity_name: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "slices_username": self.slices_username,
            "r2lab_slice": self.r2lab_slice,
            "identity_name": self.identity_name,
        }


def configuration_root(start: Path | None = None) -> Path:
    """Choose the nearest existing workspace, SynthRAN state, or Git project root."""

    current = (start or Path.cwd()).expanduser().resolve()
    home = Path.home().resolve()
    for candidate in (current, *current.parents):
        if workspace_file(candidate).is_file():
            return candidate
        if candidate != home and workspace_directory(candidate).exists():
            return candidate
        if (candidate / ".git").exists():
            return candidate
    return current


def _environment_home(environment: Mapping[str, str]) -> Path:
    configured = environment.get("HOME")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path.home().resolve()


def _looks_like_private_identity(prefix: bytes) -> bool:
    first_line = prefix.splitlines()[0] if prefix else b""
    return (
        first_line.startswith(PRIVATE_KEY_BEGIN)
        and first_line.endswith(PRIVATE_KEY_SUFFIX)
        and PRIVATE_KEY_MARKER in first_line
    )


def _portable_identity_reference(path: Path) -> str:
    return f"{SSH_IDENTITY_PREFIX}{path.name}"


def resolve_ssh_identity_reference(
    reference: str,
    environment: Mapping[str, str] | None = None,
) -> Path:
    """Resolve one SSH identity reference against the configured home directory."""

    if not reference.startswith(SSH_IDENTITY_PREFIX):
        raise WorkspaceError("SSH identity must use a discovered ~/.ssh reference")
    name = reference.removeprefix(SSH_IDENTITY_PREFIX)
    if not name or name in {".", ".."} or "/" in name or "\\" in name:
        raise WorkspaceError("SSH identity reference is malformed")
    env = dict(os.environ if environment is None else environment)
    return (_environment_home(env) / ".ssh" / name).resolve()


def discover_ssh_identity_references(
    environment: Mapping[str, str] | None = None,
) -> tuple[str, ...]:
    """Return verified private-key references without exposing host paths or key material."""

    env = dict(os.environ if environment is None else environment)
    ssh_directory = _environment_home(env) / ".ssh"
    if not ssh_directory.is_dir():
        return ()

    identities: list[Path] = []
    for candidate in ssh_directory.iterdir():
        if not candidate.is_file() or candidate.name.endswith(".pub"):
            continue
        try:
            with candidate.open("rb") as stream:
                prefix = stream.read(256)
        except OSError:
            continue
        if _looks_like_private_identity(prefix):
            identities.append(candidate.resolve())

    ordered = sorted(
        identities,
        key=lambda path: ("r2lab" not in path.name.lower(), path.name.lower()),
    )
    return tuple(_portable_identity_reference(path) for path in ordered)


def available_profiles(
    environment: Mapping[str, str] | None = None,
) -> tuple[ProfileSummary, ...]:
    """Return sanitized existing profile metadata suitable for operator selection."""

    env = dict(os.environ if environment is None else environment)
    directory = config_home(env) / PROFILE_DIRECTORY
    if not directory.is_dir():
        return ()

    summaries: list[ProfileSummary] = []
    for path in sorted(directory.glob("*.toml"), key=lambda item: item.name.lower()):
        name = path.stem
        try:
            validate_profile_name(name)
            profile = load_profile(name, environment=env)
        except WorkspaceError as exc:
            raise WorkspaceError("configured profile catalog contains invalid state") from exc
        identity_name = (
            Path(profile.r2lab_identity).name
            if profile.r2lab_identity is not None
            else None
        )
        summaries.append(
            ProfileSummary(
                name=profile.name,
                slices_username=profile.slices_username,
                r2lab_slice=profile.r2lab_slice,
                identity_name=identity_name,
            )
        )
    return tuple(summaries)


def first_use_snapshot(
    *,
    start: Path | None = None,
    environment: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Return local-only first-use choices without contacting or mutating providers."""

    env = dict(os.environ if environment is None else environment)
    root = configuration_root(start)
    return {
        "workspace_initialized": workspace_file(root).is_file(),
        "profiles": [item.to_dict() for item in available_profiles(env)],
        "ssh_identities": list(discover_ssh_identity_references(env)),
        "defaults": {
            "profile": env.get("SYNTHRAN_PROFILE", "default"),
            "project": env.get("SYNTHRAN_SLICES_PROJECT", ""),
            "slices_username": env.get("SYNTHRAN_SLICES_USERNAME", ""),
            "r2lab_slice": env.get("SYNTHRAN_R2LAB_SLICE", ""),
            "reservation_minutes": 120,
            "placement": "automatic",
        },
    }


def update_workspace_defaults(
    root: Path,
    *,
    reservation_minutes: int,
    placement: str,
) -> WorkspaceConfig:
    """Atomically replace operator defaults while preserving workspace identity and policy."""

    directory = workspace_directory(root)
    if not workspace_file(root).is_file():
        raise WorkspaceError("SynthRAN workspace is not initialized")
    lock_path = directory / ".workspace.lock"
    try:
        with lock_path.open("a+", encoding="utf-8") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            current = load_workspace(root)
            updated = replace(
                current,
                reservation_minutes=reservation_minutes,
                placement=placement,
            )
            _atomic_text(workspace_file(root), workspace_to_toml(updated), mode=0o600)
            return updated
    except OSError as exc:
        raise WorkspaceError("workspace defaults could not be persisted") from exc


def switch_workspace_profile(root: Path, *, profile_name: str) -> WorkspaceConfig:
    """Switch to an already verified profile while preserving prior experiment history.

    The caller must verify provider access before this local write. An unbound local
    experiment may be deactivated, but it is never deleted or rewritten.
    """

    validate_profile_name(profile_name)
    directory = workspace_directory(root)
    if not workspace_file(root).is_file():
        raise WorkspaceError("SynthRAN workspace is not initialized")
    lock_path = directory / ".workspace.lock"
    try:
        with lock_path.open("a+", encoding="utf-8") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            current = load_workspace(root)
            if current.profile == profile_name:
                return current

            active_id = load_active_experiment_id(root)
            if active_id is not None:
                active = load_experiment_record(root, active_id)
                if active.slices_experiment is not None:
                    raise WorkspaceError(
                        "cannot switch profile while the active configuration is bound to SLICES"
                    )
                active_path(root).unlink(missing_ok=True)

            updated = replace(current, profile=profile_name)
            _atomic_text(workspace_file(root), workspace_to_toml(updated), mode=0o600)
            return updated
    except OSError as exc:
        raise WorkspaceError("workspace profile could not be persisted") from exc
