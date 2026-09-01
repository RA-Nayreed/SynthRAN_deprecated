"""Canonical SynthRAN experiment orchestration.

5g-Ansible owns reservation, preparation, 5G deployment, physical resources,
and teardown.  SynthRAN submits one upstream deployment request, observes its
result, runs Amber, and persists experiment evidence.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Mapping

from synthran.adapters.fiveg import FIVEG_SPEC_SCHEMA, FiveGAdapter, FiveGAdapterError, write_spec
from synthran.amber_experiment_runtime import execute_amber_experiment
from synthran.dependencies import load_lock
from synthran.errors import SynthRANError
from synthran.experiment.live import DEFAULT_COLLECTION_SECONDS, DEFAULT_MINIMUM_PER_SENSOR
from synthran.experiment.physical import execute_physical_amber_experiment
from synthran.fiveg_ansible import NetworkInventory, load_inventory
from synthran.network.runtime import save_network_evidence, validate_run_id, verify_network_path
from synthran.privacy import repository_root
from synthran.provider import ensure_slices_provider_context
from synthran.run_events import RunProgress
from synthran.utils.environment import scoped_environment


DEFAULT_NETWORK_ROOT = Path(".synthran/runs")
DEFAULT_EXPERIMENT_ROOT = Path(".synthran/experiments")


def _subparsers(parser: argparse.ArgumentParser) -> argparse._SubParsersAction:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action
    raise SynthRANError("SynthRAN parser does not expose top-level commands")


def configure_run_parser(parser: argparse.ArgumentParser) -> None:
    """Install the single full-run command without duplicating upstream capabilities."""

    root = _subparsers(parser)
    if "run" in root.choices:
        return
    run = root.add_parser(
        "run",
        help="execute one complete SynthRAN experiment",
        description=(
            "Ask 5g-Ansible for a deployment, run Amber over the resulting path, "
            "persist scientific evidence, and optionally tear the deployment down."
        ),
    )
    run.add_argument("--radio", required=True, choices=("rfsim", "r2lab"))
    run.add_argument("--run-id", required=True)
    run.add_argument("--core", default="open5gs", help="5g-Ansible core type")
    run.add_argument("--ran", default="srsRAN", help="5g-Ansible RAN type")
    run.add_argument("--core-node", required=True)
    run.add_argument("--ran-node", required=True)
    run.add_argument("--fiveg-profile", default="default")
    run.add_argument("--owner", default=os.environ.get("SYNTHRAN_OWNER"))
    run.add_argument("--slices-project", default=os.environ.get("SYNTHRAN_SLICES_PROJECT"))
    run.add_argument("--slices-experiment", default=os.environ.get("SYNTHRAN_SLICES_EXPERIMENT"))
    run.add_argument("--slices-duration", default="4h")
    run.add_argument("--duration-minutes", type=int, default=120)
    run.add_argument("--lock", type=Path, default=Path("dependencies.lock.yml"))
    run.add_argument("--deps-root", type=Path, default=Path(".deps"))
    run.add_argument("--collection-seconds", type=int, default=DEFAULT_COLLECTION_SECONDS)
    run.add_argument("--minimum-per-sensor", type=int, default=DEFAULT_MINIMUM_PER_SENSOR)
    run.add_argument("--timeout", type=int, default=1800)
    run.add_argument("--json", action="store_true")
    run.add_argument("--quiet", action="store_true")

    # Physical selections are passed through to the upstream deployment spec.
    run.add_argument("--device", help="physical RU selection for 5g-Ansible")
    run.add_argument("--ue", help="physical UE selection for 5g-Ansible")
    run.add_argument(
        "--slice",
        dest="r2lab_slice",
        default=os.environ.get("SYNTHRAN_R2LAB_SLICE"),
        help="R2Lab account used by 5g-Ansible",
    )
    run.add_argument(
        "--known-hosts",
        type=Path,
        default=os.environ.get("SYNTHRAN_SLICES_KNOWN_HOSTS"),
        help="strict SSH known-hosts file",
    )
    run.add_argument(
        "--keep-resources",
        action="store_true",
        help="retain the upstream deployment after experiment acceptance",
    )

    # Controlled experiments reuse these persisted roots.
    run.add_argument(
        "--network-run-root",
        type=Path,
        default=DEFAULT_NETWORK_ROOT,
        help=argparse.SUPPRESS,
    )
    run.add_argument(
        "--experiment-root",
        type=Path,
        default=DEFAULT_EXPERIMENT_ROOT,
        help=argparse.SUPPRESS,
    )


def _component(
    progress: RunProgress,
    stage: str,
    name: str,
    detail: str | None = None,
    *,
    event: str = "detail",
) -> None:
    marker = {
        "started": "→",
        "completed": "✓",
        "resumed": "↻",
        "skipped": "–",
        "failed": "✗",
    }.get(event, "·")
    suffix = f": {detail}" if detail else ""
    progress.stream.emit(
        f"  {marker} {name}{suffix}",
        stage=stage,
        component=name,
        event=event,
    )


def _known_hosts(args: argparse.Namespace) -> Path:
    if args.known_hosts is None:
        raise SynthRANError(
            "run requires --known-hosts or SYNTHRAN_SLICES_KNOWN_HOSTS"
        )
    path = Path(args.known_hosts).expanduser().resolve()
    if not path.is_file():
        raise SynthRANError("strict SSH known-hosts file is missing")
    return path


def _ue_selection(ue: str | None) -> dict[str, list[str]]:
    if ue is None:
        return {"qhats": [], "qfits": [], "phones": []}
    if ue.startswith("qhat"):
        return {"qhats": [ue], "qfits": [], "phones": []}
    if ue.startswith("qfit"):
        return {"qhats": [], "qfits": [ue], "phones": []}
    return {"qhats": [], "qfits": [], "phones": [ue]}


def _deployment_spec(args: argparse.Namespace, *, known_hosts: Path) -> dict[str, Any]:
    physical = args.radio == "r2lab"
    if physical and (not args.device or not args.ue or not args.r2lab_slice):
        raise SynthRANError("physical run requires --device, --ue, and --slice")
    if not 10 <= args.duration_minutes <= 1440:
        raise SynthRANError("reservation duration must be between 10 and 1440 minutes")
    return {
        "schema": FIVEG_SPEC_SCHEMA,
        "id": args.run_id,
        "core": {"type": args.core, "node": args.core_node},
        "ran": {"type": args.ran, "node": args.ran_node},
        "platform": {
            "type": args.radio,
            "ru": args.device if physical else "rfsim",
        },
        "ues": _ue_selection(args.ue if physical else None),
        "monitoring": {"enabled": False},
        "profile": args.fiveg_profile,
        "reservation": {
            "enabled": True,
            "duration_minutes": args.duration_minutes,
            "r2lab_mode": "require-existing" if physical else "none",
        },
        "deployment": {
            "selected_ues": [args.ue] if physical else [],
            "open5gs_webui_enabled": False,
            "open5gs_admin_account_enabled": False,
        },
        "scenario": {"type": "none"},
        "r2lab": {
            "username": str(args.r2lab_slice or ""),
            "known_hosts_file": str(known_hosts) if physical else "",
            "strict_host_key_checking": True,
        },
    }


def _locked_fiveg_commit(lock: Any) -> str:
    dependency = next((item for item in lock.git if item.name == "fiveg_ansible"), None)
    if dependency is None:
        raise SynthRANError("dependency lock does not define fiveg_ansible")
    return dependency.commit


def _validate_manifest(
    manifest: Mapping[str, Any], *, run_id: str, locked_commit: str
) -> None:
    if manifest.get("schema") != "fiveg/deployment-manifest/v1":
        raise SynthRANError("5g-Ansible deployment manifest schema is unsupported")
    if manifest.get("id") != run_id or manifest.get("state") != "ready":
        raise SynthRANError("5g-Ansible deployment is not ready")
    if manifest.get("fiveg_ansible_commit") != locked_commit:
        raise SynthRANError("5g-Ansible deployment provenance does not match the lock")


def _status_ready(adapter: FiveGAdapter, run_id: str) -> Mapping[str, Any]:
    status = adapter.status(run_id)
    if status.get("state") != "ready" or status.get("observation_returncode") != 0:
        raise SynthRANError("current 5g-Ansible deployment status is not ready")
    return status


def _deploy(
    args: argparse.Namespace,
    *,
    known_hosts: Path,
    progress: RunProgress,
) -> tuple[FiveGAdapter, Mapping[str, Any], NetworkInventory, Path]:
    lock = load_lock(args.lock)
    state_root = args.network_run_root.expanduser().resolve()
    state_root.mkdir(parents=True, exist_ok=True)
    spec_path = write_spec(
        state_root / "requests" / f"{args.run_id}.json",
        _deployment_spec(args, known_hosts=known_hosts),
    )
    adapter = FiveGAdapter.from_lock(
        lock=lock,
        dependency_root=args.deps_root,
        state_root=state_root,
        timeout_seconds=args.timeout,
    )
    directory = state_root / args.run_id
    manifest_path = directory / "manifest.json"
    if manifest_path.is_file():
        _component(
            progress,
            "network",
            "5G deployment",
            "upstream deployment retained",
            event="resumed",
        )
        _status_ready(adapter, args.run_id)
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SynthRANError("5g-Ansible deployment manifest is unreadable") from exc
    else:
        _component(progress, "network", "5G deployment", "5g-Ansible plan/up", event="started")
        adapter.plan(spec_path)
        manifest = adapter.up(spec_path, resume=(directory / "state.json").is_file())
        _component(progress, "network", "5G deployment", "upstream deployment ready", event="completed")
    if not isinstance(manifest, dict):
        raise SynthRANError("5g-Ansible deployment manifest is malformed")
    _validate_manifest(
        manifest,
        run_id=args.run_id,
        locked_commit=_locked_fiveg_commit(lock),
    )
    return adapter, manifest, load_inventory(directory / "hosts.ini"), manifest_path


def _read_json(path: Path) -> Mapping[str, object] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _provider_payload(args: argparse.Namespace) -> dict[str, object]:
    project, experiment, created, controller = ensure_slices_provider_context(args)
    assert controller.post5g_network is not None
    return {
        "project": project,
        "experiment": experiment,
        "experiment_created": created,
        "network": controller.post5g_network.to_dict(),
    }


def _accepted_experiment(path: Path) -> bool:
    payload = _read_json(path)
    return payload is not None and payload.get("ready") is True


def _run_rfsim(
    args: argparse.Namespace,
    *,
    progress: RunProgress,
    provider: Mapping[str, object],
    known_hosts: Path,
) -> dict[str, object]:
    progress.start("network", "5g-Ansible deployment and path observation")
    adapter, manifest, inventory, manifest_path = _deploy(
        args, known_hosts=known_hosts, progress=progress
    )
    del adapter
    lock = load_lock(args.lock)
    network_directory = args.network_run_root.expanduser().resolve() / args.run_id
    network_evidence = network_directory / "network-evidence.json"
    with scoped_environment({"SYNTHRAN_KNOWN_HOSTS": str(known_hosts)}):
        verification = verify_network_path(
            inventory=inventory,
            lock=lock,
            run_id=args.run_id,
            timeout_seconds=min(args.timeout, 300),
        )
    save_network_evidence(verification, network_evidence, manifest_path)
    if not verification.ready:
        raise SynthRANError("RFSIM experiment path was not proven")
    progress.done("network", "READY")

    experiment_directory = args.experiment_root.expanduser().resolve() / args.run_id
    evidence = experiment_directory / "experiment-evidence.json"
    progress.start("workload", "deterministic Amber experiment")
    if not evidence.is_file():
        with scoped_environment({"SYNTHRAN_KNOWN_HOSTS": str(known_hosts)}):
            result = execute_amber_experiment(
                inventory=inventory,
                lock=lock,
                dependency_root=args.deps_root,
                network_manifest=manifest_path,
                network_evidence=network_evidence,
                run_id=args.run_id,
                repository_root=repository_root(),
                run_root=args.experiment_root,
                collection_seconds=args.collection_seconds,
                minimum_per_sensor=args.minimum_per_sensor,
                iot_profile=args.iot_profile,
                iot_seed=args.iot_seed,
                sensor_period_seconds=args.sensor_period,
                energy_power_scale=args.energy_power_scale,
                energy_node_variation=args.energy_node_variation,
                progress=progress.child_stream,
            )
        if not result.ready:
            raise SynthRANError("RFSIM Amber experiment was not accepted")
        progress.done("workload", "accepted")
    else:
        progress.resumed("workload", "accepted experiment evidence retained")
    if not _accepted_experiment(evidence):
        raise SynthRANError("RFSIM experiment evidence is not accepted")
    progress.start("acceptance", "verify experiment evidence")
    progress.done("acceptance", str(evidence))
    progress.skipped("cleanup", "virtual deployment retained for controlled measurements")
    return {
        "schema": "synthran/run/v2",
        "run_id": args.run_id,
        "radio": "rfsim",
        "deployment": dict(manifest),
        "provider": dict(provider),
        "accepted": True,
        "released": False,
        "evidence_path": str(evidence),
        "event_path": str(progress.event_path),
    }


def _run_physical(
    args: argparse.Namespace,
    *,
    progress: RunProgress,
    provider: Mapping[str, object],
    known_hosts: Path,
) -> dict[str, object]:
    progress.start("network", "5g-Ansible physical deployment and current status gate")
    adapter, manifest, inventory, _manifest_path = _deploy(
        args, known_hosts=known_hosts, progress=progress
    )
    _status_ready(adapter, args.run_id)
    progress.done("network", "READY")

    experiment_directory = args.experiment_root.expanduser().resolve() / args.run_id
    evidence = experiment_directory / "experiment-evidence.json"
    progress.start("workload", "deterministic Amber experiment through physical UE")
    if not evidence.is_file():
        result = execute_physical_amber_experiment(
            inventory=inventory,
            lock=load_lock(args.lock),
            dependency_root=args.deps_root,
            run_id=args.run_id,
            ue=str(args.ue),
            repository_root=repository_root(),
            run_root=args.experiment_root,
            collection_seconds=args.collection_seconds,
            minimum_per_sensor=args.minimum_per_sensor,
            iot_profile=args.iot_profile,
            iot_seed=args.iot_seed,
            sensor_period_seconds=args.sensor_period,
            progress=progress.child_stream,
        )
        if not result.ready:
            raise SynthRANError("physical Amber experiment was not accepted")
        progress.done("workload", "accepted")
    else:
        progress.resumed("workload", "accepted experiment evidence retained")
    if not _accepted_experiment(evidence):
        raise SynthRANError("physical experiment evidence is not accepted")
    _status_ready(adapter, args.run_id)
    progress.start("acceptance", "verify experiment evidence")
    progress.done("acceptance", str(evidence))

    release_payload: Mapping[str, Any] | None = None
    if args.keep_resources:
        progress.skipped("cleanup", "upstream deployment retained by --keep-resources")
    else:
        progress.start("cleanup", "5g-Ansible down")
        release_payload = adapter.down(args.run_id)
        if release_payload.get("state") != "stopped":
            raise SynthRANError("5g-Ansible cleanup did not reach stopped state")
        progress.done("cleanup", "upstream deployment stopped")
    return {
        "schema": "synthran/run/v2",
        "run_id": args.run_id,
        "radio": "r2lab",
        "device": args.device,
        "ue": args.ue,
        "deployment": dict(manifest),
        "provider": dict(provider),
        "accepted": True,
        "released": release_payload is not None,
        "release": dict(release_payload) if release_payload is not None else None,
        "evidence_path": str(evidence),
        "event_path": str(progress.event_path),
    }


def execute_run(args: argparse.Namespace) -> dict[str, object]:
    """Execute one experiment while leaving all 5G ownership to 5g-Ansible."""

    if args.command != "run":
        raise SynthRANError("unsupported lifecycle command")
    progress = RunProgress(enabled=not args.quiet, run_id=args.run_id, radio=args.radio)
    try:
        validate_run_id(args.run_id)
        if not args.owner:
            raise SynthRANError("run requires --owner or SYNTHRAN_OWNER")
        known_hosts = _known_hosts(args)
        progress.start("provider", "verify SLICES experiment context")
        provider = _provider_payload(args)
        progress.done("provider", f"{provider['project']}/{provider['experiment']}")
        if args.radio == "r2lab":
            return _run_physical(
                args,
                progress=progress,
                provider=provider,
                known_hosts=known_hosts,
            )
        return _run_rfsim(
            args,
            progress=progress,
            provider=provider,
            known_hosts=known_hosts,
        )
    except (FiveGAdapterError, SynthRANError) as exc:
        progress.fail(str(exc))
        raise SynthRANError(str(exc)) from exc
    except Exception as exc:
        progress.fail(str(exc))
        raise SynthRANError(str(exc)) from exc
    finally:
        progress.close()
