"""Topology-driven R2Lab resource ownership and live authority.

This module owns claims, lease checks, exact hardware power state, and strict
management reachability.  UE modem/setup mechanics are intentionally delegated
to the pinned 5g-Ansible roles.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Callable, Mapping, Sequence, TextIO

from synthran.live_preflight import CommandResult, verify_allocations, verify_reservation
from synthran.network.runtime import validate_run_id
from synthran.r2lab.controller import gateway_command, subprocess_runner
from synthran.r2lab.hardware import PhysicalTopology, R2LabHardwareError, UeProfile, topology_path
from synthran.r2lab.provider import (
    PowerState,
    execute_verified_pdu_transition,
    execute_verified_qfit_transition,
    execute_verified_qfit_usb_transition,
    observe_qfit_usb_power,
    parse_pdu_status,
    parse_qfit_status,
    qfit_node_number,
)


Runner = Callable[[Sequence[str], int], CommandResult]
Sleeper = Callable[[float], None]
RUN_SCHEMA = "synthran/r2lab-resource/v1alpha2"
CLAIM_SCHEMA = "synthran/r2lab-claim/v1alpha2"
QFIT_IMAGE = "mbim-quectel-any-dnn"
DEFAULT_TIMEOUT_SECONDS = 30


class R2LabTopologyResourceError(RuntimeError):
    """Raised when exact selected R2Lab resource state cannot be proven."""


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _slice_fingerprint(slice_name: str) -> str:
    if not slice_name or len(slice_name) > 64 or any(
        not (character.isalnum() or character in "._-") for character in slice_name
    ):
        raise R2LabTopologyResourceError("R2Lab slice name is malformed")
    return hashlib.sha256(slice_name.encode("utf-8")).hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, object]) -> Path:
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(dict(payload), indent=2, sort_keys=True) + "\n"
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=path.parent,
            delete=False,
        ) as stream:
            stream.write(content)
            temporary = Path(stream.name)
        temporary.replace(path)
    except OSError as exc:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
        raise R2LabTopologyResourceError("R2Lab run state could not be persisted") from exc
    return path


def _read_json(path: Path, label: str) -> dict[str, object]:
    try:
        payload = json.loads(path.expanduser().resolve().read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise R2LabTopologyResourceError(f"{label} is missing") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise R2LabTopologyResourceError(f"{label} is not readable JSON") from exc
    if not isinstance(payload, dict):
        raise R2LabTopologyResourceError(f"{label} must contain one JSON object")
    return payload


def _remote_runner(slice_name: str, runner: Runner) -> Runner:
    return lambda command, timeout: runner(
        gateway_command(slice_name, *tuple(command)), timeout
    )


def _lease(slice_name: str, runner: Runner, timeout_seconds: int) -> None:
    try:
        result = runner(
            gateway_command(slice_name, "rhubarbe", "leases", "--check"),
            timeout_seconds,
        )
    except Exception as exc:
        raise R2LabTopologyResourceError("current R2Lab lease could not be verified") from exc
    if result.returncode != 0:
        raise R2LabTopologyResourceError("current R2Lab lease was not verified")


def ue_host(profile: UeProfile) -> str:
    if profile.kind == "qfit":
        return profile.host
    if profile.kind == "qhat":
        return profile.name
    raise R2LabTopologyResourceError("selected UE has no supported management host")


def _nested_ssh(slice_name: str, profile: UeProfile, *remote: str) -> tuple[str, ...]:
    import shlex

    if not remote:
        raise R2LabTopologyResourceError("UE command requires an explicit remote argv")

    # OpenSSH does not preserve argv boundaries for a remote command: it joins
    # all post-host arguments into shell text.  The R2Lab path has two SSH
    # hops (operator -> Faraday -> UE), so the UE argv must be shell-quoted
    # before it becomes the single remote-command argument of the second hop.
    remote_command = shlex.join(remote)

    return (
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile=/home/{slice_name}/.ssh/known_hosts",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        "--",
        f"root@{ue_host(profile)}",
        remote_command,
    )


def ue_gateway_command(slice_name: str, profile: UeProfile, *remote: str) -> tuple[str, ...]:
    import shlex

    return gateway_command(slice_name, shlex.join(_nested_ssh(slice_name, profile, *remote)))


def _run_ue(
    *,
    slice_name: str,
    profile: UeProfile,
    runner: Runner,
    timeout_seconds: int,
    remote: Sequence[str],
) -> CommandResult:
    try:
        return runner(
            ue_gateway_command(slice_name, profile, *tuple(remote)), timeout_seconds
        )
    except Exception as exc:
        raise R2LabTopologyResourceError("selected UE management command could not complete") from exc


def _require_ue_ssh(
    *, slice_name: str, profile: UeProfile, runner: Runner, timeout_seconds: int
) -> None:
    if _run_ue(
        slice_name=slice_name,
        profile=profile,
        runner=runner,
        timeout_seconds=timeout_seconds,
        remote=("true",),
    ).returncode != 0:
        raise R2LabTopologyResourceError("selected UE is not strict-SSH reachable")


def _wait_ssh(
    *,
    slice_name: str,
    profile: UeProfile,
    runner: Runner,
    sleeper: Sleeper,
    timeout_seconds: int,
    attempts: int = 30,
    interval_seconds: float = 2.0,
) -> None:
    for attempt in range(attempts):
        try:
            _require_ue_ssh(
                slice_name=slice_name,
                profile=profile,
                runner=runner,
                timeout_seconds=timeout_seconds,
            )
            return
        except R2LabTopologyResourceError:
            if attempt + 1 < attempts:
                sleeper(interval_seconds)
    raise R2LabTopologyResourceError("selected UE did not become strict-SSH reachable")


def _management_ready(
    *, slice_name: str, profile: UeProfile, runner: Runner, timeout_seconds: int
) -> bool:
    checks: list[tuple[str, ...]] = [
        ("ip", "link", "show", "dev", "wwan0"),
        ("test", "-c", "/dev/ttyUSB2"),
    ]
    if profile.mode in {"mbim", "qmi"}:
        checks.append(("test", "-c", "/dev/cdc-wdm0"))
    if profile.mode == "qmi":
        checks.append(("command", "-v", "quectel-CM"))
    return all(
        _run_ue(
            slice_name=slice_name,
            profile=profile,
            runner=runner,
            timeout_seconds=timeout_seconds,
            remote=command,
        ).returncode
        == 0
        for command in checks
    )


def _wait_management(
    *,
    slice_name: str,
    profile: UeProfile,
    runner: Runner,
    sleeper: Sleeper,
    timeout_seconds: int,
    attempts: int = 30,
    interval_seconds: float = 2.0,
) -> None:
    for attempt in range(attempts):
        try:
            _require_ue_ssh(
                slice_name=slice_name,
                profile=profile,
                runner=runner,
                timeout_seconds=timeout_seconds,
            )
            if _management_ready(
                slice_name=slice_name,
                profile=profile,
                runner=runner,
                timeout_seconds=timeout_seconds,
            ):
                return
        except R2LabTopologyResourceError:
            pass
        if attempt + 1 < attempts:
            sleeper(interval_seconds)
    raise R2LabTopologyResourceError("selected UE did not become management-ready")


@dataclass(frozen=True)
class PhysicalAuthority:
    run_id: str
    topology: PhysicalTopology
    lease_verified: bool
    radio_state: str
    ue_state: str

    def validate(self) -> "PhysicalAuthority":
        validate_run_id(self.run_id)
        self.topology.validate()
        if self.lease_verified is not True:
            raise R2LabTopologyResourceError("R2Lab lease authority is not current")
        if self.radio_state != "on":
            raise R2LabTopologyResourceError("selected physical radio is not proven on")
        if self.ue_state != "ready":
            raise R2LabTopologyResourceError("selected UE is not proven management-ready")
        return self


@dataclass(frozen=True)
class PreparedPhysicalRun:
    run_id: str
    run_directory: Path
    topology: PhysicalTopology
    manifest_path: Path
    claim_path: Path

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": RUN_SCHEMA,
            "run_id": self.run_id,
            "status": "ready",
            "resource_claim": "held",
            "topology": self.topology.to_dict(),
            "manifest_path": str(self.manifest_path),
        }


def _topology_payload(topology: PhysicalTopology) -> dict[str, object]:
    return {
        "schema": "synthran/r2lab-topology/v1alpha1",
        "core_node": topology.core_node,
        "ran_node": topology.ran_node,
        "radio": topology.radio,
        "ue": topology.ue,
        "dnn": topology.dnn,
    }


def _topology_from_payload(payload: Mapping[str, object]) -> PhysicalTopology:
    if payload.get("schema") != "synthran/r2lab-topology/v1alpha1":
        raise R2LabTopologyResourceError("stored physical topology schema is unsupported")
    fields = ("core_node", "ran_node", "radio", "ue", "dnn")
    if any(not isinstance(payload.get(field), str) for field in fields):
        raise R2LabTopologyResourceError("stored physical topology is malformed")
    try:
        return PhysicalTopology(
            core_node=str(payload["core_node"]),
            ran_node=str(payload["ran_node"]),
            radio=str(payload["radio"]),
            ue=str(payload["ue"]),
            dnn=str(payload["dnn"]),
        ).validate()
    except R2LabHardwareError as exc:
        raise R2LabTopologyResourceError(str(exc)) from exc


def load_topology(*, run_root: Path, run_id: str) -> PhysicalTopology:
    return _topology_from_payload(
        _read_json(topology_path(run_root, run_id), "physical topology")
    )


def _claim_path(run_root: Path) -> Path:
    return run_root.expanduser().resolve() / "active.json"


def _write_claim(
    *, run_root: Path, run_id: str, slice_name: str, topology: PhysicalTopology
) -> Path:
    path = _claim_path(run_root)
    if path.exists():
        raise R2LabTopologyResourceError(
            "another physical resource claim exists in this workspace; release it first"
        )
    payload = {
        "schema": CLAIM_SCHEMA,
        "run_id": run_id,
        "slice_fingerprint": _slice_fingerprint(slice_name),
        "core_node": topology.core_node,
        "ran_node": topology.ran_node,
        "radio": topology.radio,
        "ue": topology.ue,
        "created_at_utc": _now(),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write("\n")
    except FileExistsError as exc:
        raise R2LabTopologyResourceError("another physical claim appeared concurrently") from exc
    except OSError as exc:
        path.unlink(missing_ok=True)
        raise R2LabTopologyResourceError("physical resource claim could not be written") from exc
    return path


def _require_claim(
    *, run_root: Path, run_id: str, slice_name: str, topology: PhysicalTopology
) -> Path:
    path = _claim_path(run_root)
    payload = _read_json(path, "active physical resource claim")
    expected = {
        "schema": CLAIM_SCHEMA,
        "run_id": run_id,
        "slice_fingerprint": _slice_fingerprint(slice_name),
        "core_node": topology.core_node,
        "ran_node": topology.ran_node,
        "radio": topology.radio,
        "ue": topology.ue,
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        raise R2LabTopologyResourceError("active physical claim does not match the requested run")
    return path


def _prove_qhat_on(*, profile: UeProfile, provider: Runner, timeout_seconds: int) -> None:
    result = provider(("rhubarbe", "pdu", "status", profile.name), timeout_seconds)
    observed = parse_pdu_status(
        "\n".join(part for part in (result.stdout, result.stderr) if part),
        resource=profile.name,
    )
    if observed.state is not PowerState.ON:
        raise R2LabTopologyResourceError("selected qhat was not proven on after upstream setup")


def _prepare_qfit(
    *,
    slice_name: str,
    profile: UeProfile,
    provider: Runner,
    runner: Runner,
    require_lease: Callable[[str], None],
    sleeper: Sleeper,
    timeout_seconds: int,
) -> None:
    node = qfit_node_number(profile.name)
    require_lease("lease-before-qfit-state")
    status = provider(("rhubarbe", "status", str(node)), timeout_seconds)
    observed = parse_qfit_status(
        "\n".join(part for part in (status.stdout, status.stderr) if part),
        qfit=profile.name,
    )
    if observed.state is PowerState.UNKNOWN:
        raise R2LabTopologyResourceError("selected qfit power state is unknown")
    if observed.state is PowerState.OFF:
        require_lease("lease-before-qfit-image-load")
        if provider(("rhubarbe", "load", "-i", QFIT_IMAGE, str(node)), 300).returncode != 0:
            raise R2LabTopologyResourceError("selected qfit image load returned nonzero")
        status = provider(("rhubarbe", "status", str(node)), timeout_seconds)
        observed = parse_qfit_status(
            "\n".join(part for part in (status.stdout, status.stderr) if part),
            qfit=profile.name,
        )
        if observed.state is not PowerState.ON:
            raise R2LabTopologyResourceError("selected qfit was not proven on after image load")
    if provider(("rhubarbe", "wait", str(node)), 300).returncode != 0:
        raise R2LabTopologyResourceError("selected qfit did not become SSH-ready")

    usb = observe_qfit_usb_power(
        qfit=profile.name, runner=provider, timeout_seconds=timeout_seconds
    )
    if usb.state is PowerState.OFF:
        require_lease("lease-before-qfit-usb")
        transition = execute_verified_qfit_usb_transition(
            qfit=profile.name,
            requested_state=PowerState.ON,
            runner=provider,
            timeout_seconds=timeout_seconds,
        )
        if not transition.confirmed:
            raise R2LabTopologyResourceError("selected qfit USB rail was not proven on")
    elif usb.state is PowerState.UNKNOWN:
        raise R2LabTopologyResourceError("selected qfit USB power is unknown")

    # The pinned qfit image supplies the UE tooling.  Connection/session
    # mechanics are deferred to r2lab/ue/connect; no local init script is run.
    _wait_management(
        slice_name=slice_name,
        profile=profile,
        runner=runner,
        sleeper=sleeper,
        timeout_seconds=min(timeout_seconds, 60),
    )


def prepare_physical_resources(
    *,
    run_id: str,
    slice_name: str,
    topology: PhysicalTopology,
    lock_path: Path = Path("dependencies.lock.yml"),
    deps_root: Path = Path(".deps"),
    run_root: Path = Path(".synthran/r2lab"),
    runner: Runner = subprocess_runner,
    sleeper: Sleeper = time.sleep,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    progress: TextIO | None = None,
) -> PreparedPhysicalRun:
    """Claim exact hardware and prepare UE management without custom modem actuation."""

    from synthran.r2lab.ue_ansible import R2LabUeAnsibleError, execute_selected_ue_role

    validate_run_id(run_id)
    topology = topology.validate()
    if timeout_seconds < 5 or timeout_seconds > 300:
        raise R2LabTopologyResourceError("R2Lab command timeout must be between 5 and 300 seconds")
    run_root = run_root.expanduser().resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    run_directory = run_root / run_id
    if run_directory.exists():
        raise R2LabTopologyResourceError("physical run directory already exists; choose a new run ID")
    run_directory.mkdir()
    manifest_path = run_directory / "manifest.json"
    topology_file = topology_path(run_root, run_id)
    claim_path: Path | None = None
    provider = _remote_runner(slice_name, runner)

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] {message}", file=progress, flush=True)

    def require_lease(stage: str) -> None:
        report(stage)
        _lease(slice_name, runner, timeout_seconds)

    try:
        _atomic_json(topology_file, _topology_payload(topology))
        claim_path = _write_claim(
            run_root=run_root,
            run_id=run_id,
            slice_name=slice_name,
            topology=topology,
        )

        require_lease("lease-before-radio")
        radio = execute_verified_pdu_transition(
            resource=topology.radio,
            requested_state=PowerState.ON,
            runner=provider,
            timeout_seconds=timeout_seconds,
        )
        if not radio.confirmed:
            raise R2LabTopologyResourceError("selected physical radio was not proven on")

        profile = topology.ue_profile
        if profile.kind == "qfit":
            _prepare_qfit(
                slice_name=slice_name,
                profile=profile,
                provider=provider,
                runner=runner,
                require_lease=require_lease,
                sleeper=sleeper,
                timeout_seconds=timeout_seconds,
            )
        elif profile.kind == "qhat":
            require_lease("lease-before-qhat-setup")
            try:
                execute_selected_ue_role(
                    run_id=run_id,
                    slice_name=slice_name,
                    topology=topology,
                    action="setup",
                    lock_path=lock_path,
                    deps_root=deps_root,
                    run_root=run_root,
                    timeout_seconds=timeout_seconds,
                )
            except R2LabUeAnsibleError as exc:
                raise R2LabTopologyResourceError(str(exc)) from exc
            require_lease("lease-after-qhat-setup")
            _prove_qhat_on(profile=profile, provider=provider, timeout_seconds=timeout_seconds)
            _wait_management(
                slice_name=slice_name,
                profile=profile,
                runner=runner,
                sleeper=sleeper,
                timeout_seconds=min(timeout_seconds, 60),
                attempts=60,
            )
        else:
            raise R2LabTopologyResourceError("selected UE kind is outside the canonical physical path")

        require_lease("lease-final")
        manifest = {
            "schema": RUN_SCHEMA,
            "run_id": run_id,
            "status": "ready",
            "updated_at_utc": _now(),
            "resource_claim": "held",
            "topology": _topology_payload(topology),
            "global_power_off": False,
            "ue_mechanics": "pinned-5g-ansible",
        }
        _atomic_json(manifest_path, manifest)
        report("R2Lab resources READY")
        return PreparedPhysicalRun(
            run_id=run_id,
            run_directory=run_directory,
            topology=topology,
            manifest_path=manifest_path,
            claim_path=claim_path,
        )
    except Exception:
        if claim_path is None:
            try:
                topology_file.unlink(missing_ok=True)
                run_directory.rmdir()
            except OSError:
                pass
        raise


def verify_physical_authority(
    *,
    run_id: str,
    slice_name: str,
    run_root: Path = Path(".synthran/r2lab"),
    runner: Runner = subprocess_runner,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> PhysicalAuthority:
    """Refresh current lease, exact claim, radio, and UE management state."""

    topology = load_topology(run_root=run_root, run_id=run_id)
    _require_claim(
        run_root=run_root,
        run_id=run_id,
        slice_name=slice_name,
        topology=topology,
    )
    _lease(slice_name, runner, timeout_seconds)
    provider = _remote_runner(slice_name, runner)
    radio_result = provider(
        ("rhubarbe", "pdu", "status", topology.radio), timeout_seconds
    )
    radio = parse_pdu_status(
        "\n".join(part for part in (radio_result.stdout, radio_result.stderr) if part),
        resource=topology.radio,
    )
    if radio.state is not PowerState.ON:
        raise R2LabTopologyResourceError("selected physical radio is no longer proven on")
    _wait_management(
        slice_name=slice_name,
        profile=topology.ue_profile,
        runner=runner,
        sleeper=lambda _: None,
        timeout_seconds=min(timeout_seconds, 60),
        attempts=1,
        interval_seconds=0,
    )
    return PhysicalAuthority(
        run_id=run_id,
        topology=topology,
        lease_verified=True,
        radio_state="on",
        ue_state="ready",
    ).validate()


def verify_selected_allocation(
    *,
    topology: PhysicalTopology,
    runner: Runner,
    owner: str,
    allocation_id: str | None,
    timeout_seconds: int,
) -> str:
    try:
        return verify_allocations(
            runner=runner,
            allocation_id=allocation_id,
            owner=owner,
            nodes=set(topology.nodes),
            timeout_seconds=min(timeout_seconds, 60),
        )
    except Exception as exc:
        raise R2LabTopologyResourceError(
            "current SLICES allocation for the selected compute nodes was not proven"
        ) from exc


def claim_selected_allocation(
    *,
    run_id: str,
    slice_name: str,
    topology: PhysicalTopology,
    r2lab_runner: Runner,
    allocation_runner: Runner,
    owner: str,
    allocation_id: str | None,
    timeout_seconds: int,
    run_root: Path = Path(".synthran/r2lab"),
) -> str:
    """Reuse one exact allocation or reclaim only the selected reserved nodes."""

    try:
        return verify_selected_allocation(
            topology=topology,
            runner=allocation_runner,
            owner=owner,
            allocation_id=allocation_id,
            timeout_seconds=timeout_seconds,
        )
    except R2LabTopologyResourceError:
        if allocation_id is not None:
            raise

    verify_physical_authority(
        run_id=run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    try:
        reservation_id = verify_reservation(
            runner=allocation_runner,
            reservation_id=None,
            owner=owner,
            nodes=set(topology.nodes),
            now=datetime.now(timezone.utc),
            timeout_seconds=min(timeout_seconds, 60),
        )
    except Exception as exc:
        raise R2LabTopologyResourceError(
            "active SLICES reservation for the selected compute nodes was not proven"
        ) from exc

    for node in topology.nodes:
        verify_physical_authority(
            run_id=run_id,
            slice_name=slice_name,
            run_root=run_root,
            runner=r2lab_runner,
            timeout_seconds=min(timeout_seconds, 300),
        )
        verify_reservation(
            runner=allocation_runner,
            reservation_id=reservation_id,
            owner=owner,
            nodes=set(topology.nodes),
            now=datetime.now(timezone.utc),
            timeout_seconds=min(timeout_seconds, 60),
        )
        result = allocation_runner(
            ("pos", "allocations", "free", "-k", node), min(timeout_seconds, 60)
        )
        if result.returncode != 0:
            raise R2LabTopologyResourceError(f"allocation release for {node} returned nonzero")

    verify_physical_authority(
        run_id=run_id,
        slice_name=slice_name,
        run_root=run_root,
        runner=r2lab_runner,
        timeout_seconds=min(timeout_seconds, 300),
    )
    result = allocation_runner(
        ("pos", "allocations", "allocate", *topology.nodes), min(timeout_seconds, 60)
    )
    if result.returncode != 0:
        raise R2LabTopologyResourceError("selected compute allocation creation returned nonzero")
    return verify_selected_allocation(
        topology=topology,
        runner=allocation_runner,
        owner=owner,
        allocation_id=None,
        timeout_seconds=timeout_seconds,
    )


def release_physical_resources(
    *,
    run_id: str,
    slice_name: str,
    run_root: Path = Path(".synthran/r2lab"),
    runner: Runner = subprocess_runner,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    stop_gnb: Callable[[], object] | None = None,
    progress: TextIO | None = None,
) -> dict[str, object]:
    """Release only exact selected hardware after proving each resulting state."""

    topology = load_topology(run_root=run_root, run_id=run_id)
    claim = _require_claim(
        run_root=run_root,
        run_id=run_id,
        slice_name=slice_name,
        topology=topology,
    )
    provider = _remote_runner(slice_name, runner)

    def report(message: str) -> None:
        if progress is not None:
            print(f"[synthran] {message}", file=progress, flush=True)

    if stop_gnb is not None:
        report("gNB scale-to-zero")
        stop_gnb()

    _lease(slice_name, runner, timeout_seconds)
    profile = topology.ue_profile
    if profile.kind == "qfit":
        usb = execute_verified_qfit_usb_transition(
            qfit=profile.name,
            requested_state=PowerState.OFF,
            runner=provider,
            timeout_seconds=timeout_seconds,
        )
        if not usb.confirmed:
            raise R2LabTopologyResourceError("selected qfit USB rail was not proven off")
        _lease(slice_name, runner, timeout_seconds)
        host = execute_verified_qfit_transition(
            qfit=profile.name,
            requested_state=PowerState.OFF,
            runner=provider,
            timeout_seconds=timeout_seconds,
        )
        if not host.confirmed:
            raise R2LabTopologyResourceError("selected qfit host was not proven off")
    elif profile.kind == "qhat":
        ue = execute_verified_pdu_transition(
            resource=profile.name,
            requested_state=PowerState.OFF,
            runner=provider,
            timeout_seconds=timeout_seconds,
        )
        if not ue.confirmed:
            raise R2LabTopologyResourceError("selected qhat was not proven off")

    _lease(slice_name, runner, timeout_seconds)
    radio = execute_verified_pdu_transition(
        resource=topology.radio,
        requested_state=PowerState.OFF,
        runner=provider,
        timeout_seconds=timeout_seconds,
    )
    if not radio.confirmed:
        raise R2LabTopologyResourceError("selected physical radio was not proven off")
    try:
        claim.unlink()
    except OSError as exc:
        raise R2LabTopologyResourceError(
            "hardware is off but the local physical claim could not be removed"
        ) from exc

    manifest = run_root.expanduser().resolve() / run_id / "manifest.json"
    payload = _read_json(manifest, "physical run manifest")
    payload.update(
        {
            "status": "released",
            "resource_claim": "released",
            "updated_at_utc": _now(),
            "cleanup": {
                "ue": "proven-off",
                "radio": "proven-off",
                "claim": "released",
            },
        }
    )
    _atomic_json(manifest, payload)
    report("R2Lab resources RELEASED")
    return payload
