"""Immutable synchronization and validation of direct SynthRAN dependencies."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
from typing import Any, Iterable, Mapping, TextIO


LOCK_SCHEMA = "synthran/dependencies/v1alpha1"
FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
EXACT_VERSION_RE = re.compile(r"^[0-9]+(?:\.[0-9]+){1,3}(?:[A-Za-z0-9.-]+)?$")
CONDA_ENVIRONMENT_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class DependencyError(RuntimeError):
    """Raised when a direct dependency lock or checkout is unsafe."""


@dataclass(frozen=True)
class GitDependency:
    name: str
    url: str
    commit: str
    checkout: PurePosixPath
    sync: bool
    role: str


@dataclass(frozen=True)
class DependencyLock:
    path: Path
    git: tuple[GitDependency, ...]
    raw: Mapping[str, Any]


def _require_mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise DependencyError(f"{label} must be an object")
    return value


def _validate_checkout(value: object, label: str) -> PurePosixPath:
    if not isinstance(value, str) or not value.strip():
        raise DependencyError(f"{label} must be a non-empty relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or path == PurePosixPath("."):
        raise DependencyError(f"{label} must stay below the dependency root")
    return path


def _validate_git(raw: Mapping[str, Any]) -> tuple[GitDependency, ...]:
    git_raw = _require_mapping(raw.get("git"), "git")
    dependencies: list[GitDependency] = []
    for name, entry_value in git_raw.items():
        if not isinstance(name, str) or not name:
            raise DependencyError("git dependency names must be non-empty strings")
        entry = _require_mapping(entry_value, f"git.{name}")
        url = entry.get("url")
        commit = entry.get("commit")
        sync = entry.get("sync")
        role = entry.get("role")
        if not isinstance(url, str) or not url.startswith("https://") or not url.endswith(".git"):
            raise DependencyError(f"git.{name}.url must be an HTTPS Git URL ending in .git")
        if not isinstance(commit, str) or not FULL_SHA_RE.fullmatch(commit):
            raise DependencyError(f"git.{name}.commit must be a full lowercase commit SHA")
        if sync is not True:
            raise DependencyError(f"git.{name}.sync must be true for direct dependencies")
        if not isinstance(role, str) or not role.strip():
            raise DependencyError(f"git.{name}.role must be a non-empty string")
        dependencies.append(
            GitDependency(
                name=name,
                url=url,
                commit=commit,
                checkout=_validate_checkout(entry.get("checkout"), f"git.{name}.checkout"),
                sync=True,
                role=role,
            )
        )
    if {item.name for item in dependencies} != {"fiveg_ansible", "amber"}:
        raise DependencyError(
            "git must contain exactly the direct fiveg_ansible and amber dependencies"
        )
    return tuple(dependencies)


def _validate_containers(raw: Mapping[str, Any]) -> None:
    containers = _require_mapping(raw.get("containers"), "containers")
    if set(containers) != {"mosquitto"}:
        raise DependencyError("containers must contain only the direct Mosquitto runtime")
    entry = _require_mapping(containers["mosquitto"], "containers.mosquitto")
    digest = entry.get("digest")
    if not isinstance(digest, str) or not DIGEST_RE.fullmatch(digest):
        raise DependencyError("containers.mosquitto.digest must be a full sha256 digest")
    if not isinstance(entry.get("image"), str) or not str(entry["image"]).strip():
        raise DependencyError("containers.mosquitto.image must be a non-empty string")
    if not isinstance(entry.get("tag"), str) or not str(entry["tag"]).strip():
        raise DependencyError("containers.mosquitto.tag must be a non-empty string")


def _validate_tools(raw: Mapping[str, Any]) -> None:
    tools = _require_mapping(raw.get("tools"), "tools")
    if set(tools) != {"iperf3_linux_amd64_source"}:
        raise DependencyError("tools must contain only the direct iperf3 research toolchain")
    entry = _require_mapping(tools["iperf3_linux_amd64_source"], "tools.iperf3_linux_amd64_source")
    version = entry.get("version")
    digest = entry.get("sha256")
    url = entry.get("url")
    install_path = entry.get("path")
    if not isinstance(version, str) or not EXACT_VERSION_RE.fullmatch(version):
        raise DependencyError("tools.iperf3_linux_amd64_source.version must be one exact version")
    if not isinstance(digest, str) or not DIGEST_RE.fullmatch(digest):
        raise DependencyError("tools.iperf3_linux_amd64_source.sha256 must be a full sha256 digest")
    if not isinstance(url, str) or not url.startswith("https://"):
        raise DependencyError("tools.iperf3_linux_amd64_source.url must be one HTTPS URL")
    if not isinstance(install_path, str) or not PurePosixPath(install_path).is_absolute():
        raise DependencyError("tools.iperf3_linux_amd64_source.path must be absolute")


def _validate_conda(raw: Mapping[str, Any]) -> None:
    conda = _require_mapping(raw.get("conda"), "conda")
    environment_name = conda.get("environment_name")
    if not isinstance(environment_name, str) or not CONDA_ENVIRONMENT_RE.fullmatch(environment_name):
        raise DependencyError("conda.environment_name has unsupported characters")
    if environment_name != "synthran":
        raise DependencyError("conda.environment_name must be 'synthran'")
    if conda.get("platform") != "linux-64":
        raise DependencyError("conda.platform must be 'linux-64'")
    if conda.get("channels") != ["conda-forge", "nodefaults"]:
        raise DependencyError("conda.channels must be exactly ['conda-forge', 'nodefaults']")

    installer = _require_mapping(conda.get("installer"), "conda.installer")
    version = installer.get("version")
    digest = installer.get("linux_x86_64_sha256")
    if not isinstance(version, str) or not EXACT_VERSION_RE.fullmatch(version):
        raise DependencyError("conda.installer.version must be one exact version")
    if not isinstance(digest, str) or not DIGEST_RE.fullmatch(digest):
        raise DependencyError("conda.installer.linux_x86_64_sha256 must be a full sha256 digest")

    packages = _require_mapping(conda.get("packages"), "conda.packages")
    if not packages:
        raise DependencyError("conda.packages must not be empty")
    for name, entry_value in packages.items():
        entry = _require_mapping(entry_value, f"conda.packages.{name}")
        package_version = entry.get("version")
        if not isinstance(package_version, str) or not EXACT_VERSION_RE.fullmatch(package_version):
            raise DependencyError(f"conda.packages.{name}.version must be one exact package version")
    if conda.get("lock_status") != "direct-versions-only":
        raise DependencyError("conda.lock_status must be 'direct-versions-only' until artifact locks exist")


def _validate_actions(raw: Mapping[str, Any]) -> None:
    actions = _require_mapping(raw.get("github_actions"), "github_actions")
    for name, entry_value in actions.items():
        entry = _require_mapping(entry_value, f"github_actions.{name}")
        commit = entry.get("commit")
        if not isinstance(commit, str) or not FULL_SHA_RE.fullmatch(commit):
            raise DependencyError(f"github_actions.{name}.commit must be a full lowercase commit SHA")


def load_lock(path: Path) -> DependencyLock:
    """Load the direct-dependency lock without bootstrap dependencies."""

    try:
        raw_value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise DependencyError(f"dependency lock not found: {path}") from exc
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DependencyError(
            f"dependency lock must use the JSON-compatible YAML subset: {exc}"
        ) from exc
    raw = _require_mapping(raw_value, "lock")
    if raw.get("schema") != LOCK_SCHEMA:
        raise DependencyError(f"unsupported dependency lock schema: {raw.get('schema')!r}")
    allowed = {
        "schema",
        "resolved_at_utc",
        "git",
        "containers",
        "tools",
        "conda",
        "github_actions",
    }
    extra = sorted(set(raw) - allowed)
    if extra:
        raise DependencyError(
            "dependency lock contains upstream-owned sections: " + ", ".join(extra)
        )
    git = _validate_git(raw)
    _validate_containers(raw)
    _validate_tools(raw)
    _validate_conda(raw)
    _validate_actions(raw)
    return DependencyLock(path=path, git=git, raw=raw)


def _run_git(
    args: Iterable[str],
    *,
    cwd: Path | None = None,
    capture: bool = False,
) -> str:
    command = ["git", *args]
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            check=True,
            text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
        )
    except FileNotFoundError as exc:
        raise DependencyError("git is required but was not found") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or "").strip()
        suffix = f": {detail}" if detail else ""
        raise DependencyError(f"git command failed{suffix}") from exc
    return (completed.stdout or "").strip()


def _safe_target(root: Path, relative: PurePosixPath) -> Path:
    root_resolved = root.resolve()
    target = root.joinpath(*relative.parts).resolve(strict=False)
    try:
        target.relative_to(root_resolved)
    except ValueError as exc:
        raise DependencyError(f"dependency target escapes root: {relative}") from exc
    return target


def _print_plan(dependency: GitDependency, output: TextIO) -> None:
    print(
        f"[dry-run] {dependency.name}: clone/fetch {dependency.url} and detach at "
        f"{dependency.commit} -> .deps/{dependency.checkout.as_posix()}",
        file=output,
    )


def _sync_one(dependency: GitDependency, root: Path, output: TextIO) -> None:
    target = _safe_target(root, dependency.checkout)
    if target.exists():
        if not target.is_dir() or not (target / ".git").exists():
            raise DependencyError(
                "managed dependency target exists but is not a Git checkout: "
                f".deps/{dependency.checkout.as_posix()}"
            )
        actual_url = _run_git(["remote", "get-url", "origin"], cwd=target, capture=True)
        if actual_url.rstrip("/") != dependency.url.rstrip("/"):
            raise DependencyError(
                f"origin mismatch for {dependency.name}; refusing to replace an existing checkout"
            )
        dirty = _run_git(["status", "--porcelain"], cwd=target, capture=True)
        if dirty:
            raise DependencyError(
                f"{dependency.name} has local changes; refusing to change its checkout"
            )
        _run_git(["fetch", "--no-tags", "origin", dependency.commit], cwd=target)
    else:
        root.mkdir(parents=True, exist_ok=True)
        _run_git(
            [
                "clone",
                "--filter=blob:none",
                "--no-checkout",
                dependency.url,
                str(target),
            ]
        )
        _run_git(["fetch", "--no-tags", "origin", dependency.commit], cwd=target)
    _run_git(["checkout", "--detach", dependency.commit], cwd=target)
    actual_commit = _run_git(["rev-parse", "HEAD"], cwd=target, capture=True)
    if actual_commit != dependency.commit:
        raise DependencyError(f"verification failed for {dependency.name}")
    print(
        f"synced {dependency.name} at {dependency.commit} "
        f"(.deps/{dependency.checkout.as_posix()})",
        file=output,
    )


def sync_dependencies(
    lock: DependencyLock,
    root: Path,
    *,
    include_transitive: bool = False,
    names: Iterable[str] | None = None,
    dry_run: bool = False,
    output: TextIO,
) -> None:
    """Synchronize only repositories present in the direct dependency lock."""

    del include_transitive  # retained only as a harmless CLI compatibility flag
    requested = tuple(dict.fromkeys(names or ()))
    if requested:
        known = {entry.name for entry in lock.git}
        unknown = sorted(set(requested) - known)
        if unknown:
            raise DependencyError("unknown Git dependencies: " + ", ".join(unknown))
        selected = [entry for entry in lock.git if entry.name in requested]
    else:
        selected = list(lock.git)
    if not selected:
        raise DependencyError("the dependency lock selected no Git repositories")
    for dependency in selected:
        if dry_run:
            _print_plan(dependency, output)
        else:
            _sync_one(dependency, root, output)
