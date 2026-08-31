"""Pinned iperf3 toolchain provisioning for research measurements.

The golden-path srsUE image and measurement host currently ship different,
older iperf3 versions. Research runs therefore build one source-locked,
static iperf3 binary on each prepared host and copy the same version into the
UE container. This keeps calibration, background-load generation, and the
external measurement server on one reproducible iperf3 release.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
import re
import shlex
from typing import Mapping

from synthran.dependencies import DependencyError, load_lock
from synthran.experiment import live as base_runtime
from synthran.experiment import ExperimentError
from synthran.fiveg_ansible import NetworkInventory
from synthran.live_preflight import ssh_command


LOCK_KEY = "iperf3_linux_amd64_source"
CONTROL_KEEPALIVE = "60/10/3"
CONTROL_KEEPALIVE_ARG = f"--cntl-ka={CONTROL_KEEPALIVE}"
UE_IPERF_PATH = "/usr/local/bin/iperf3"
_VERSION_RE = re.compile(r"^iperf\s+([0-9]+(?:\.[0-9]+){1,2})\b", re.MULTILINE)

_BUILD_SCRIPT = r'''
import hashlib
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request

version, url, expected_sha, final_binary = sys.argv[1:5]
expected_sha = expected_sha.removeprefix("sha256:")
final_binary = Path(final_binary)
final_root = final_binary.parent.parent


def run(argv, *, cwd=None, check=True):
    return subprocess.run(
        [str(item) for item in argv],
        cwd=cwd,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def valid(binary):
    try:
        version_out = run([binary, "--version"]).stdout
        help_out = run([binary, "--help"]).stdout
    except Exception:
        return False
    return f"iperf {version}" in version_out and "--cntl-ka" in help_out


if final_binary.is_file() and valid(final_binary):
    raise SystemExit(0)

for tool in ("cc", "make"):
    if shutil.which(tool) is None:
        raise SystemExit(f"required build tool is unavailable: {tool}")

final_root.parent.mkdir(parents=True, exist_ok=True)
work = Path(tempfile.mkdtemp(prefix=f".iperf-{version}-", dir=str(final_root.parent)))
try:
    archive = work / f"iperf-{version}.tar.gz"
    digest = hashlib.sha256()
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "SynthRAN/iperf-source-fetch"},
    )
    with urllib.request.urlopen(request, timeout=60) as response, archive.open("wb") as handle:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            handle.write(chunk)
    actual_sha = digest.hexdigest()
    if actual_sha != expected_sha:
        raise SystemExit(
            f"iperf3 source digest mismatch: expected {expected_sha}, got {actual_sha}"
        )

    extract_root = work / "src"
    extract_root.mkdir()
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar.getmembers():
            member_path = PurePosixPath(member.name)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise SystemExit("iperf3 source archive contains an unsafe path")
        tar.extractall(extract_root)

    candidates = [
        path for path in extract_root.iterdir()
        if path.is_dir() and path.name.startswith("iperf-")
    ]
    if len(candidates) != 1:
        raise SystemExit("iperf3 source archive did not contain one source directory")
    source = candidates[0]
    prefix = work / "install"
    configure = source / "configure"
    if not configure.is_file():
        raise SystemExit("iperf3 source archive is missing configure")

    run(
        [
            configure,
            f"--prefix={prefix}",
            "--enable-static-bin",
            "--disable-shared",
            "--without-openssl",
            "--without-sctp",
        ],
        cwd=source,
    )
    run(["make", "-j2"], cwd=source)
    run(["make", "install"], cwd=source)
    candidate = prefix / "bin" / "iperf3"
    if not candidate.is_file() or not valid(candidate):
        raise SystemExit("built iperf3 does not match the locked version/features")

    ldd = run(["ldd", candidate], check=False).stdout.lower()
    if "not a dynamic executable" not in ldd and "statically linked" not in ldd:
        raise SystemExit("locked iperf3 build is not static; refusing UE injection")

    if final_root.exists():
        shutil.rmtree(final_root)
    prefix.replace(final_root)
    (final_root / ".source-sha256").write_text(expected_sha + "\n", encoding="utf-8")
finally:
    shutil.rmtree(work, ignore_errors=True)

if not valid(final_binary):
    raise SystemExit("locked iperf3 verification failed after installation")
'''


@dataclass(frozen=True)
class LockedIperfSpec:
    version: str
    url: str
    sha256: str
    path: str


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _locked_spec(repository_root: Path | None = None) -> LockedIperfSpec:
    root = (repository_root or _repository_root()).resolve()
    try:
        lock = load_lock(root / "dependencies.lock.yml")
    except DependencyError as exc:
        raise ExperimentError(str(exc)) from exc
    tools = lock.raw.get("tools")
    entry = tools.get(LOCK_KEY) if isinstance(tools, Mapping) else None
    if not isinstance(entry, Mapping):
        raise ExperimentError(f"dependency lock does not define tools.{LOCK_KEY}")
    version = entry.get("version")
    url = entry.get("url")
    sha256 = entry.get("sha256")
    path = entry.get("path")
    platform = entry.get("platform")
    if (
        not isinstance(version, str)
        or not isinstance(url, str)
        or not isinstance(sha256, str)
        or not isinstance(path, str)
        or platform != "linux/amd64"
    ):
        raise ExperimentError("locked iperf3 tool entry is malformed")
    return LockedIperfSpec(version=version, url=url, sha256=sha256, path=path)


def _version_matches(output: str, version: str) -> bool:
    match = _VERSION_RE.search(output)
    return match is not None and match.group(1) == version


def _inventory_for_node(inventory: NetworkInventory, node_name: str) -> NetworkInventory:
    if node_name == inventory.core_node.name:
        return inventory
    if node_name == inventory.ran_node.name:
        return replace(inventory, core_node=inventory.ran_node)
    raise ExperimentError("locked iperf3 target must be a prepared inventory node")


def _host_tool_ready(remote_inventory: NetworkInventory, spec: LockedIperfSpec) -> bool:
    try:
        version = base_runtime._remote(
            remote_inventory,
            spec.path,
            "--version",
            label="locked iperf3 version probe",
            timeout_seconds=10,
        )
        help_text = base_runtime._remote(
            remote_inventory,
            spec.path,
            "--help",
            label="locked iperf3 feature probe",
            timeout_seconds=10,
        )
    except Exception:
        return False
    return _version_matches(version, spec.version) and "--cntl-ka" in help_text


def ensure_locked_iperf_host(
    inventory: NetworkInventory,
    *,
    node_name: str,
    repository_root: Path | None = None,
) -> str:
    """Build and verify the source-locked static iperf3 binary on one host."""

    spec = _locked_spec(repository_root)
    remote_inventory = _inventory_for_node(inventory, node_name)
    if not _host_tool_ready(remote_inventory, spec):
        base_runtime._remote(
            remote_inventory,
            "python3",
            "-c",
            _BUILD_SCRIPT,
            spec.version,
            spec.url,
            spec.sha256,
            spec.path,
            label="locked iperf3 source build",
            timeout_seconds=300,
        )
    if not _host_tool_ready(remote_inventory, spec):
        raise ExperimentError("locked iperf3 host verification failed after build")
    return spec.path


def _ue_exec_command(
    inventory: NetworkInventory,
    ue_pod: str,
    *command: str,
) -> tuple[str, ...]:
    return tuple(
        ssh_command(
            inventory.core_node,
            "sh",
            "-c",
            "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
            f"-n open5gs {shlex.quote(ue_pod)} -c ue -- "
            + " ".join(shlex.quote(part) for part in command),
        )
    )


def _ue_tool_ready(
    inventory: NetworkInventory,
    ue_pod: str,
    spec: LockedIperfSpec,
) -> bool:
    for args, marker in (
        ((UE_IPERF_PATH, "--version"), f"iperf {spec.version}"),
        ((UE_IPERF_PATH, "--help"), "--cntl-ka"),
    ):
        result = base_runtime._run(
            _ue_exec_command(inventory, ue_pod, *args),
            timeout_seconds=10,
        )
        if result.returncode != 0 or marker not in (result.stdout + result.stderr):
            return False

    # Calibration still invokes `iperf3` by command name. Prove that the live
    # UE PATH resolves that name to the exact pinned binary before allowing any
    # measurement to proceed, so calibration and campaign cannot silently use
    # different iperf3 versions.
    resolved = base_runtime._run(
        _ue_exec_command(inventory, ue_pod, "sh", "-c", "command -v iperf3"),
        timeout_seconds=10,
    )
    if resolved.returncode != 0 or resolved.stdout.strip() != UE_IPERF_PATH:
        return False
    bare_version = base_runtime._run(
        _ue_exec_command(inventory, ue_pod, "iperf3", "--version"),
        timeout_seconds=10,
    )
    return (
        bare_version.returncode == 0
        and _version_matches(
            bare_version.stdout + bare_version.stderr,
            spec.version,
        )
    )


def prepare_locked_iperf_client(
    inventory: NetworkInventory,
    ue_pod: str,
    *,
    repository_root: Path | None = None,
) -> str:
    """Install the locked static iperf3 into the live UE container and verify it."""

    spec = _locked_spec(repository_root)
    host_binary = ensure_locked_iperf_host(
        inventory,
        node_name=inventory.core_node.name,
        repository_root=repository_root,
    )
    if _ue_tool_ready(inventory, ue_pod, spec):
        return UE_IPERF_PATH

    destination = PurePosixPath(UE_IPERF_PATH)
    parent = str(destination.parent)
    base_runtime._remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
        f"-n open5gs {shlex.quote(ue_pod)} -c ue -- mkdir -p {shlex.quote(parent)}",
        label="locked iperf3 UE directory preparation",
        timeout_seconds=15,
    )
    base_runtime._remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl cp "
        f"{shlex.quote(host_binary)} open5gs/{shlex.quote(ue_pod)}:{shlex.quote(UE_IPERF_PATH)} -c ue",
        label="locked iperf3 UE installation",
        timeout_seconds=30,
    )
    result = base_runtime._run(
        _ue_exec_command(inventory, ue_pod, "chmod", "0755", UE_IPERF_PATH),
        timeout_seconds=10,
    )
    if result.returncode != 0:
        raise ExperimentError("locked iperf3 UE chmod failed")
    if not _ue_tool_ready(inventory, ue_pod, spec):
        raise ExperimentError(
            "locked iperf3 UE verification failed after installation or PATH resolution did not select the pinned binary"
        )
    return UE_IPERF_PATH


def prepare_locked_iperf_server(
    inventory: NetworkInventory,
    *,
    server_node: str,
    repository_root: Path | None = None,
) -> str:
    return ensure_locked_iperf_host(
        inventory,
        node_name=server_node,
        repository_root=repository_root,
    )
