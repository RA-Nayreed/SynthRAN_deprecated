"""Filesystem storage for persistent SynthRAN profiles and workspaces."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import tomllib
from typing import Mapping

from synthran.workspace.model import (
    ACCESS_SCHEMA,
    ACTIVE_SCHEMA,
    AccessRecord,
    ExperimentRecord,
    Profile,
    WorkspaceConfig,
    WorkspaceError,
    format_utc,
    utc_now,
    validate_experiment_id,
    validate_profile_name,
    validate_safe_name,
)


DEFAULT_PROFILE_NAME = "default"
WORKSPACE_DIRECTORY = ".synthran"
WORKSPACE_FILE = "workspace.toml"
PROFILE_DIRECTORY = "profiles"


def _quote(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def config_home(environment: Mapping[str, str] | None = None) -> Path:
    env = environment or os.environ
    override = env.get("SYNTHRAN_CONFIG_HOME")
    if override:
        return Path(override).expanduser().resolve()
    xdg = env.get("XDG_CONFIG_HOME")
    base = Path(xdg).expanduser() if xdg else Path.home() / ".config"
    return (base / "synthran").resolve()


def profile_path(
    name: str,
    *,
    environment: Mapping[str, str] | None = None,
) -> Path:
    validate_profile_name(name)
    return config_home(environment) / PROFILE_DIRECTORY / f"{name}.toml"


def _atomic_text(path: Path, content: str, *, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=path.parent,
        delete=False,
    ) as temporary:
        temporary.write(content)
        temporary_path = Path(temporary.name)
    os.chmod(temporary_path, mode)
    temporary_path.replace(path)


def _atomic_json(path: Path, value: Mapping[str, object], *, mode: int = 0o600) -> None:
    _atomic_text(
        path,
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        mode=mode,
    )


def normalize_identity_reference(value: str | Path) -> str:
    raw = str(value)
    expanded = Path(raw).expanduser().resolve()
    try:
        relative = expanded.relative_to(Path.home().resolve())
    except ValueError:
        return str(expanded)
    return f"~/{relative.as_posix()}"


def resolve_identity_reference(value: str) -> Path:
    return Path(value).expanduser().resolve()


def _public_blob_from_private(identity: Path) -> bytes:
    try:
        completed = subprocess.run(
            ("ssh-keygen", "-y", "-f", str(identity)),
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=15,
        )
    except FileNotFoundError as exc:
        raise WorkspaceError("ssh-keygen is required to verify an SSH identity") from exc
    except subprocess.TimeoutExpired as exc:
        raise WorkspaceError("SSH identity verification timed out") from exc
    if completed.returncode != 0:
        raise WorkspaceError("SSH identity could not be read by ssh-keygen")
    parts = completed.stdout.strip().split()
    if len(parts) < 2:
        raise WorkspaceError("ssh-keygen returned a malformed public key")
    try:
        return base64.b64decode(parts[1], validate=True)
    except ValueError as exc:
        raise WorkspaceError("ssh-keygen returned malformed public-key data") from exc


def ssh_identity_fingerprint(value: str | Path) -> str:
    identity = Path(value).expanduser().resolve()
    try:
        info = identity.stat()
    except FileNotFoundError as exc:
        raise WorkspaceError("SSH identity file does not exist") from exc
    except OSError as exc:
        raise WorkspaceError("SSH identity file cannot be inspected") from exc
    if not stat.S_ISREG(info.st_mode):
        raise WorkspaceError("SSH identity path must be a regular file")
    if info.st_mode & 0o077:
        raise WorkspaceError("SSH private identity must not be group- or world-accessible")
    digest = hashlib.sha256(_public_blob_from_private(identity)).digest()
    encoded = base64.b64encode(digest).decode("ascii").rstrip("=")
    return f"SHA256:{encoded}"


def profile_to_toml(profile: Profile) -> str:
    lines = [
        f"schema = {_quote(profile.schema)}",
        f"name = {_quote(profile.name)}",
        f"created_at_utc = {_quote(profile.created_at_utc)}",
        f"updated_at_utc = {_quote(profile.updated_at_utc)}",
    ]
    if profile.slices_username is not None:
        lines.extend(("", "[slices]", f"username = {_quote(profile.slices_username)}"))
    if profile.r2lab_slice is not None:
        lines.extend(("", "[r2lab]", f"slice = {_quote(profile.r2lab_slice)}"))
        if profile.r2lab_identity is not None:
            lines.extend(
                (
                    "",
                    "[r2lab.ssh]",
                    f"identity = {_quote(profile.r2lab_identity)}",
                    f"fingerprint = {_quote(profile.r2lab_identity_fingerprint or '')}",
                )
            )
    return "\n".join(lines) + "\n"


def save_profile(profile: Profile, *, environment: Mapping[str, str] | None = None) -> Path:
    path = profile_path(profile.name, environment=environment)
    _atomic_text(path, profile_to_toml(profile), mode=0o600)
    return path


def load_profile(
    name: str = DEFAULT_PROFILE_NAME,
    *,
    environment: Mapping[str, str] | None = None,
) -> Profile:
    path = profile_path(name, environment=environment)
    try:
        value = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise WorkspaceError(f"SynthRAN profile '{name}' was not found") from exc
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise WorkspaceError(f"SynthRAN profile '{name}' is not readable TOML") from exc
    slices = value.get("slices") if isinstance(value.get("slices"), dict) else {}
    r2lab = value.get("r2lab") if isinstance(value.get("r2lab"), dict) else {}
    ssh = r2lab.get("ssh") if isinstance(r2lab.get("ssh"), dict) else {}
    return Profile(
        schema=str(value.get("schema", "")),
        name=str(value.get("name", "")),
        created_at_utc=str(value.get("created_at_utc", "")),
        updated_at_utc=str(value.get("updated_at_utc", "")),
        slices_username=(str(slices["username"]) if "username" in slices else None),
        r2lab_slice=(str(r2lab["slice"]) if "slice" in r2lab else None),
        r2lab_identity=(str(ssh["identity"]) if "identity" in ssh else None),
        r2lab_identity_fingerprint=(
            str(ssh["fingerprint"]) if "fingerprint" in ssh else None
        ),
    )


def create_or_update_profile(
    *,
    name: str,
    slices_username: str | None,
    r2lab_slice: str | None,
    r2lab_identity: Path | None,
    update: bool = False,
    environment: Mapping[str, str] | None = None,
    now: datetime | None = None,
) -> Profile:
    validate_profile_name(name)
    path = profile_path(name, environment=environment)
    current_time = format_utc(now or utc_now())
    existing: Profile | None = None
    if path.exists():
        if not update:
            raise WorkspaceError(
                f"SynthRAN profile '{name}' already exists; use an explicit update"
            )
        existing = load_profile(name, environment=environment)
    identity_reference: str | None = None
    fingerprint: str | None = None
    if r2lab_identity is not None:
        identity_reference = normalize_identity_reference(r2lab_identity)
        fingerprint = ssh_identity_fingerprint(r2lab_identity)
    elif existing is not None:
        identity_reference = existing.r2lab_identity
        fingerprint = existing.r2lab_identity_fingerprint
    profile = Profile(
        name=name,
        created_at_utc=existing.created_at_utc if existing else current_time,
        updated_at_utc=current_time,
        slices_username=(
            slices_username if slices_username is not None else (existing.slices_username if existing else None)
        ),
        r2lab_slice=(
            r2lab_slice if r2lab_slice is not None else (existing.r2lab_slice if existing else None)
        ),
        r2lab_identity=identity_reference,
        r2lab_identity_fingerprint=fingerprint,
    )
    save_profile(profile, environment=environment)
    return profile


def verify_profile_identity(profile: Profile) -> str | None:
    if profile.r2lab_identity is None:
        return None
    observed = ssh_identity_fingerprint(resolve_identity_reference(profile.r2lab_identity))
    if observed != profile.r2lab_identity_fingerprint:
        raise WorkspaceError("R2Lab SSH identity fingerprint no longer matches the profile")
    return observed


def workspace_directory(root: Path) -> Path:
    return root.resolve() / WORKSPACE_DIRECTORY


def workspace_file(root: Path) -> Path:
    return workspace_directory(root) / WORKSPACE_FILE


def workspace_to_toml(workspace: WorkspaceConfig) -> str:
    return "\n".join(
        (
            f"schema = {_quote(workspace.schema)}",
            f"profile = {_quote(workspace.profile)}",
            f"project = {_quote(workspace.project)}",
            f"created_at_utc = {_quote(workspace.created_at_utc)}",
            "",
            "[defaults]",
            f"reservation_minutes = {workspace.reservation_minutes}",
            f"placement = {_quote(workspace.placement)}",
            "",
            "[policy]",
            f"ownership = {_quote(workspace.ownership)}",
            "",
        )
    )


def initialize_workspace(
    *,
    root: Path,
    profile: str,
    project: str,
    reservation_minutes: int = 120,
    placement: str = "automatic",
    now: datetime | None = None,
) -> WorkspaceConfig:
    path = workspace_file(root)
    if path.exists():
        raise WorkspaceError("SynthRAN workspace is already initialized")
    config = WorkspaceConfig(
        profile=profile,
        project=project,
        created_at_utc=format_utc(now or utc_now()),
        reservation_minutes=reservation_minutes,
        placement=placement,
    )
    directory = workspace_directory(root)
    directory.mkdir(parents=True, exist_ok=True)
    for name in ("access", "experiments", "operations"):
        (directory / name).mkdir(exist_ok=True)
    _atomic_text(path, workspace_to_toml(config), mode=0o600)
    return config


def find_workspace_root(
    start: Path | None = None,
    *,
    environment: Mapping[str, str] | None = None,
) -> Path:
    env = environment or os.environ
    override = env.get("SYNTHRAN_WORKSPACE")
    if override:
        candidate = Path(override).expanduser().resolve()
        if candidate.name == WORKSPACE_DIRECTORY:
            candidate = candidate.parent
        if not workspace_file(candidate).is_file():
            raise WorkspaceError("SYNTHRAN_WORKSPACE does not contain a SynthRAN workspace")
        return candidate
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if workspace_file(candidate).is_file():
            return candidate
    raise WorkspaceError("no SynthRAN workspace was found; run synthran init")


def load_workspace(root: Path) -> WorkspaceConfig:
    path = workspace_file(root)
    try:
        value = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise WorkspaceError("SynthRAN workspace is not initialized") from exc
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise WorkspaceError("SynthRAN workspace configuration is not readable TOML") from exc
    defaults = value.get("defaults") if isinstance(value.get("defaults"), dict) else {}
    policy = value.get("policy") if isinstance(value.get("policy"), dict) else {}
    return WorkspaceConfig(
        schema=str(value.get("schema", "")),
        profile=str(value.get("profile", "")),
        project=str(value.get("project", "")),
        created_at_utc=str(value.get("created_at_utc", "")),
        reservation_minutes=int(defaults.get("reservation_minutes", 120)),
        placement=str(defaults.get("placement", "automatic")),
        ownership=str(policy.get("ownership", "strict")),
    )


def access_path(root: Path, provider: str) -> Path:
    return workspace_directory(root) / "access" / f"{provider}.json"


def save_access_record(root: Path, record: AccessRecord) -> Path:
    path = access_path(root, record.provider)
    _atomic_json(path, record.to_dict(), mode=0o600)
    return path


def load_access_record(root: Path, provider: str) -> AccessRecord | None:
    path = access_path(root, provider)
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WorkspaceError(f"cached {provider} access record is not readable JSON") from exc
    if not isinstance(value, dict) or value.get("schema") != ACCESS_SCHEMA:
        raise WorkspaceError(f"cached {provider} access record schema is unsupported")
    return AccessRecord.from_dict(value)


def experiment_directory(root: Path, experiment_id: str) -> Path:
    validate_experiment_id(experiment_id)
    return workspace_directory(root) / "experiments" / experiment_id


def experiment_to_toml(record: ExperimentRecord) -> str:
    lines = [
        f"schema = {_quote(record.schema)}",
        f"id = {_quote(record.experiment_id)}",
        f"created_at_utc = {_quote(record.created_at_utc)}",
        f"profile = {_quote(record.profile)}",
        f"project = {_quote(record.project)}",
    ]
    if record.label is not None:
        lines.append(f"label = {_quote(record.label)}")
    lines.extend(
        (
            "",
            "[network]",
            f"intent = {_quote(record.network_intent)}",
            f"radio = {_quote(record.radio_mode)}",
        )
    )
    if record.slices_experiment is not None:
        lines.extend(
            (
                "",
                "[providers.slices]",
                f"experiment = {_quote(record.slices_experiment)}",
            )
        )
    return "\n".join(lines) + "\n"


def save_experiment_record(root: Path, record: ExperimentRecord) -> Path:
    directory = experiment_directory(root, record.experiment_id)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "experiment.toml"
    _atomic_text(path, experiment_to_toml(record), mode=0o600)
    return path


def load_experiment_record(root: Path, experiment_id: str) -> ExperimentRecord:
    path = experiment_directory(root, experiment_id) / "experiment.toml"
    try:
        value = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise WorkspaceError(f"experiment {experiment_id} was not found") from exc
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise WorkspaceError(f"experiment {experiment_id} is not readable TOML") from exc
    network = value.get("network") if isinstance(value.get("network"), dict) else {}
    providers = value.get("providers") if isinstance(value.get("providers"), dict) else {}
    slices = providers.get("slices") if isinstance(providers.get("slices"), dict) else {}
    return ExperimentRecord(
        schema=str(value.get("schema", "")),
        experiment_id=str(value.get("id", "")),
        created_at_utc=str(value.get("created_at_utc", "")),
        profile=str(value.get("profile", "")),
        project=str(value.get("project", "")),
        label=(str(value["label"]) if "label" in value else None),
        slices_experiment=(str(slices["experiment"]) if "experiment" in slices else None),
        network_intent=str(network.get("intent", "unspecified")),
        radio_mode=str(network.get("radio", "automatic")),
    )


def bind_slices_experiment(
    root: Path,
    experiment_id: str,
    slices_experiment: str,
) -> ExperimentRecord:
    """Bind one verified SLICES experiment exactly once to a local experiment."""

    validate_experiment_id(experiment_id)
    validate_safe_name(slices_experiment, "SLICES experiment")
    directory = experiment_directory(root, experiment_id)
    lock_path = directory / ".experiment.lock"
    try:
        with lock_path.open("a+", encoding="utf-8") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            record = load_experiment_record(root, experiment_id)
            if record.slices_experiment == slices_experiment:
                return record
            if record.slices_experiment is not None:
                raise WorkspaceError(
                    "experiment already has a different SLICES provider binding"
                )
            bound = replace(record, slices_experiment=slices_experiment)
            save_experiment_record(root, bound)
            return bound
    except FileNotFoundError as exc:
        raise WorkspaceError(f"experiment {experiment_id} was not found") from exc
    except OSError as exc:
        raise WorkspaceError("experiment provider binding could not be persisted") from exc


def active_path(root: Path) -> Path:
    return workspace_directory(root) / "active.json"


def set_active_experiment(root: Path, experiment_id: str) -> Path:
    validate_experiment_id(experiment_id)
    if not experiment_directory(root, experiment_id).is_dir():
        raise WorkspaceError(f"experiment {experiment_id} does not exist")
    path = active_path(root)
    _atomic_json(
        path,
        {
            "schema": ACTIVE_SCHEMA,
            "experiment_id": experiment_id,
            "updated_at_utc": format_utc(utc_now()),
        },
        mode=0o600,
    )
    return path


def load_active_experiment_id(root: Path) -> str | None:
    path = active_path(root)
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WorkspaceError("active experiment pointer is not readable JSON") from exc
    if not isinstance(value, dict) or value.get("schema") != ACTIVE_SCHEMA:
        raise WorkspaceError("active experiment pointer schema is unsupported")
    experiment_id = value.get("experiment_id")
    if not isinstance(experiment_id, str):
        raise WorkspaceError("active experiment pointer is malformed")
    return validate_experiment_id(experiment_id)
