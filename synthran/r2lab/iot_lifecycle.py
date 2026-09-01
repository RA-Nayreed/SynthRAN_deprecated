"""Physical Amber workload orchestration on an upstream 5g-Ansible deployment."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from synthran.dependencies import load_lock
from synthran.fiveg_ansible import NetworkInventory
from synthran.privacy import repository_root
from synthran.r2lab.iot_workload import PhysicalIoTConfig, execute_physical_iot_workload
from synthran.r2lab.ue import PhysicalWorkloadContext


DEFAULT_PHYSICAL_RUN_ROOT = Path(".synthran/experiments-r2lab")


class R2LabIoTLifecycleError(RuntimeError):
    """Raised when the physical Amber experiment cannot be executed safely."""


@dataclass(frozen=True)
class PhysicalIoTWorkloadSummary:
    run_id: str
    workload_id: str
    workload_result_path: Path
    accepted: bool
    cleanup_proven: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-workload/v3",
            "run_id": self.run_id,
            "workload_id": self.workload_id,
            "backend": "r2lab",
            "iot_source": "amber",
            "ue_interface": "wwan0",
            "accepted": self.accepted,
            "cleanup_proven": self.cleanup_proven,
            "workload_result_path": str(self.workload_result_path),
        }


def _next_workload_id(base: str, experiment_root: Path) -> str:
    root = experiment_root.expanduser().resolve()
    if not (root / base).exists():
        return base
    for attempt in range(2, 1000):
        candidate = f"{base}-attempt-{attempt}"
        if not (root / candidate).exists():
            return candidate
    raise R2LabIoTLifecycleError("physical workload attempt space is exhausted")


def run_physical_iot_workload(
    *,
    run_id: str,
    workload_id: str,
    slice_name: str,
    ue: str,
    known_hosts: Path,
    inventory: NetworkInventory,
    lock_path: Path = Path("dependencies.lock.yml"),
    deps_root: Path = Path(".deps"),
    experiment_root: Path = DEFAULT_PHYSICAL_RUN_ROOT,
    collection_seconds: int = 180,
    minimum_per_sensor: int = 3,
    iot_profile: str = "transport-v1",
    iot_seed: int = 424242,
    sensor_period_seconds: int = 10,
    progress: TextIO | None = None,
) -> PhysicalIoTWorkloadSummary:
    """Run Amber using only upstream inventory plus experiment-side UE evidence."""

    try:
        effective_workload_id = _next_workload_id(workload_id, experiment_root)
        config = PhysicalIoTConfig(
            slice_name=slice_name,
            inventory=inventory,
            lock=load_lock(lock_path),
            dependency_root=deps_root,
            repository_root=repository_root(),
            known_hosts=known_hosts,
            workload_id=effective_workload_id,
            run_root=experiment_root,
            collection_seconds=collection_seconds,
            minimum_per_sensor=minimum_per_sensor,
            iot_profile=iot_profile,
            iot_seed=iot_seed,
            sensor_period_seconds=sensor_period_seconds,
            progress=progress,
        ).validate()
        result = execute_physical_iot_workload(
            PhysicalWorkloadContext(run_id=run_id, ue=ue, interface="wwan0"),
            config=config,
        )
        result_path = (
            experiment_root.expanduser().resolve()
            / effective_workload_id
            / "physical-workload.json"
        )
        return PhysicalIoTWorkloadSummary(
            run_id=run_id,
            workload_id=effective_workload_id,
            workload_result_path=result_path,
            accepted=result.accepted,
            cleanup_proven=result.cleanup_proven,
        )
    except R2LabIoTLifecycleError:
        raise
    except Exception as exc:
        raise R2LabIoTLifecycleError(str(exc)) from exc
