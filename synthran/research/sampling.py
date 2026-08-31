"""Synchronized network sampling for controlled research measurements."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shlex
import threading
import time
from typing import Any, Mapping

from synthran.experiment import live as base_runtime
from synthran.fiveg_ansible import NetworkInventory
from synthran.ingress import IngressSnapshot
from synthran.research import NETWORK_SAMPLE_SCHEMA, ResearchError, append_jsonl

_NAMESPACE = "open5gs"
_UPF_SELECTOR = "app=open5gs,nf=upf,name=upf1"
_SAFE_KUBERNETES_NAME = re.compile(
    r"^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$"
)
_COUNTERS = (
    "rx_bytes",
    "tx_bytes",
    "rx_packets",
    "tx_packets",
    "rx_dropped",
    "tx_dropped",
)
_MINIMUM_CADENCE_RATIO = 0.80
_MINIMUM_CADENCE_WINDOW_MULTIPLIER = 5.0


def _ready(status: Mapping[str, Any]) -> bool:
    conditions = status.get("conditions")
    return isinstance(conditions, list) and any(
        isinstance(condition, Mapping)
        and condition.get("type") == "Ready"
        and condition.get("status") == "True"
        for condition in conditions
    )


def _active_run_owned_upf(
    inventory: NetworkInventory, network_run_id: str
) -> str:
    output = base_runtime._remote(
        inventory,
        "sh",
        "-c",
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl get pods "
        f"-n {_NAMESPACE} -l {_UPF_SELECTOR} -o json",
        label="research UPF discovery",
        timeout_seconds=15,
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as exc:
        raise ResearchError("research UPF discovery did not return JSON") from exc
    items = payload.get("items") if isinstance(payload, Mapping) else None
    if not isinstance(items, list):
        raise ResearchError(
            "research UPF discovery returned malformed pod evidence"
        )
    active = []
    for item in items:
        if not isinstance(item, Mapping):
            raise ResearchError(
                "research UPF discovery returned malformed pod evidence"
            )
        metadata = item.get("metadata")
        status = item.get("status")
        if (
            not isinstance(metadata, Mapping)
            or not isinstance(status, Mapping)
            or metadata.get("deletionTimestamp") is not None
            or not _ready(status)
        ):
            continue
        labels = metadata.get("labels")
        if (
            not isinstance(labels, Mapping)
            or labels.get("synthran.run/id") != network_run_id
        ):
            continue
        name = metadata.get("name")
        if (
            not isinstance(name, str)
            or not _SAFE_KUBERNETES_NAME.fullmatch(name)
        ):
            raise ResearchError("research UPF pod name is unsafe")
        active.append(name)
    if len(active) != 1:
        raise ResearchError(
            "research sampling requires exactly one Ready run-owned slice-one UPF"
        )
    return active[0]


def _interface_counters(
    inventory: NetworkInventory,
    *,
    pod: str,
    interface: str,
    container: str | None = None,
) -> dict[str, int]:
    if not _SAFE_KUBERNETES_NAME.fullmatch(pod):
        raise ResearchError("research counter pod name is unsafe")
    command = (
        "KUBECONFIG=/etc/kubernetes/admin.conf kubectl exec "
        f"-n {_NAMESPACE} {pod}"
    )
    if container is not None:
        if not _SAFE_KUBERNETES_NAME.fullmatch(container):
            raise ResearchError("research counter container name is unsafe")
        command += f" -c {container}"
    names = " ".join(_COUNTERS)
    command += (
        " -- sh -c 'for counter in "
        + names
        + f"; do cat /sys/class/net/{interface}/statistics/$counter; done'"
    )
    output = base_runtime._remote(
        inventory,
        "sh",
        "-c",
        command,
        label=f"research {interface} counter sample",
        timeout_seconds=10,
    )
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if len(lines) != len(_COUNTERS):
        raise ResearchError(
            f"research {interface} counter sample is incomplete"
        )
    result: dict[str, int] = {}
    for counter, raw in zip(_COUNTERS, lines):
        try:
            value = int(raw)
        except ValueError as exc:
            raise ResearchError(
                f"research {interface} {counter} sample is not an integer"
            ) from exc
        if value < 0:
            raise ResearchError(
                f"research {interface} {counter} sample is negative"
            )
        result[counter] = value
    return result


def _ingress_snapshot(
    inventory: NetworkInventory, run_id: str
) -> IngressSnapshot:
    root = f"/tmp/synthran/{run_id}"
    amber_path = f"{root}/amber-ingress-snapshot.json"
    legacy_path = f"{root}/ingress-snapshot.json"
    command = (
        f"if test -f {shlex.quote(amber_path)}; then cat {shlex.quote(amber_path)}; "
        f"elif test -f {shlex.quote(legacy_path)}; then cat {shlex.quote(legacy_path)}; "
        "else exit 44; fi"
    )
    output = base_runtime._remote(
        inventory,
        "sh",
        "-c",
        command,
        label="research counted ingress sample",
        timeout_seconds=10,
    )
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as exc:
        raise ResearchError(
            "research counted ingress sample did not return JSON"
        ) from exc
    try:
        return IngressSnapshot.from_dict(payload)
    except Exception as exc:
        raise ResearchError(
            "research counted ingress sample is malformed"
        ) from exc


def _future_deadline(deadline: float, interval: float, now: float) -> float:
    """Return the first scheduled deadline strictly after ``now``."""

    if deadline > now:
        return deadline
    missed = int((now - deadline) // interval) + 1
    return deadline + missed * interval


def _collect_sample_inputs(
    inventory: NetworkInventory,
    *,
    experiment_run_id: str,
    ue_pod: str,
    upf_pod: str,
) -> tuple[IngressSnapshot, dict[str, int], dict[str, int]]:
    """Collect independent read-only counter sources concurrently.

    The campaign-06 audit showed that the three previous sequential remote
    queries required about 2.6 seconds, turning a requested 1 Hz cadence into
    roughly 0.33 Hz. These observations are independent and safe to collect in
    parallel. The resulting record is still timestamped only after all three
    inputs have completed.
    """

    with ThreadPoolExecutor(
        max_workers=3,
        thread_name_prefix="synthran-network-sample",
    ) as executor:
        ingress_future = executor.submit(
            _ingress_snapshot,
            inventory,
            experiment_run_id,
        )
        ue_future = executor.submit(
            _interface_counters,
            inventory,
            pod=ue_pod,
            interface="tun_srsue1",
            container="ue",
        )
        upf_future = executor.submit(
            _interface_counters,
            inventory,
            pod=upf_pod,
            interface="ogstun",
        )
        ingress = ingress_future.result()
        ue = ue_future.result()
        upf = upf_future.result()
    return ingress, ue, upf


class ResearchNetworkSampler:
    def __init__(
        self,
        *,
        inventory: NetworkInventory,
        network_run_id: str,
        experiment_run_id: str,
        ue_pod: str,
        interval_seconds: float,
        destination: Path,
    ) -> None:
        if interval_seconds <= 0:
            raise ResearchError(
                "research network sample interval must be positive"
            )
        if not _SAFE_KUBERNETES_NAME.fullmatch(ue_pod):
            raise ResearchError("research UE pod name is unsafe")
        self.inventory = inventory
        self.network_run_id = network_run_id
        self.experiment_run_id = experiment_run_id
        self.ue_pod = ue_pod
        self.interval_seconds = interval_seconds
        self.destination = destination
        self.upf_pod = _active_run_owned_upf(inventory, network_run_id)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None
        self._started = 0.0
        self._next_deadline = 0.0
        self._sample_count = 0

    def _sample(self, *, scheduled_at: float | None = None) -> None:
        sample_started = time.monotonic()
        if scheduled_at is None:
            scheduled_at = sample_started
        ingress, ue, upf = _collect_sample_inputs(
            self.inventory,
            experiment_run_id=self.experiment_run_id,
            ue_pod=self.ue_pod,
            upf_pod=self.upf_pod,
        )
        sample_ended = time.monotonic()
        record: dict[str, Any] = {
            "schema": NETWORK_SAMPLE_SCHEMA,
            "observed_at_utc": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "elapsed_seconds": sample_started - self._started,
            "sample_duration_seconds": sample_ended - sample_started,
            "schedule_lag_seconds": max(0.0, sample_started - scheduled_at),
            "ue_interface": "tun_srsue1",
            "upf_interface": "ogstun",
            "ingress_accepted_connections": ingress.accepted_connections,
            "ingress_upstream_bytes": ingress.upstream_bytes,
            "ingress_downstream_bytes": ingress.downstream_bytes,
        }
        for counter in _COUNTERS:
            record[f"ue_{counter}"] = ue[counter]
            record[f"upf_{counter}"] = upf[counter]
        append_jsonl(self.destination, record)
        self._sample_count += 1

    def _run(self) -> None:
        try:
            deadline = self._next_deadline
            while not self._stop.is_set():
                now = time.monotonic()
                wait_seconds = max(0.0, deadline - now)
                if wait_seconds and self._stop.wait(wait_seconds):
                    break
                if self._stop.is_set():
                    break
                self._sample(scheduled_at=deadline)
                deadline += self.interval_seconds
                deadline = _future_deadline(
                    deadline,
                    self.interval_seconds,
                    time.monotonic(),
                )
                self._next_deadline = deadline
        except BaseException as exc:
            self._error = exc
            self._stop.set()

    def start(self) -> None:
        if self._thread is not None:
            raise ResearchError("research network sampler is already running")
        self._started = time.monotonic()
        self._sample(scheduled_at=self._started)
        self._next_deadline = _future_deadline(
            self._started + self.interval_seconds,
            self.interval_seconds,
            time.monotonic(),
        )
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def check(self) -> None:
        if self._error is not None:
            raise ResearchError(
                "research network sampler failed during measurement"
            ) from self._error
        if (
            self._thread is not None
            and not self._thread.is_alive()
            and not self._stop.is_set()
        ):
            raise ResearchError("research network sampler stopped unexpectedly")

    def _verify_cadence(self, *, stopped_at: float) -> None:
        elapsed = max(0.0, stopped_at - self._started)
        minimum_window = max(
            5.0,
            self.interval_seconds * _MINIMUM_CADENCE_WINDOW_MULTIPLIER,
        )
        if elapsed < minimum_window:
            return
        expected = max(1, int(elapsed / self.interval_seconds))
        ratio = self._sample_count / expected
        if ratio < _MINIMUM_CADENCE_RATIO:
            raise ResearchError(
                "research network sampler cadence fell below the accepted "
                f"{_MINIMUM_CADENCE_RATIO:.0%} coverage of the requested rate "
                f"({self._sample_count}/{expected} samples, ratio={ratio:.3f})"
            )

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(
                timeout=max(5.0, self.interval_seconds + 2.0)
            )
            if self._thread.is_alive():
                raise ResearchError("research network sampler did not stop")
        if self._error is not None:
            raise ResearchError(
                "research network sampler failed during measurement"
            ) from self._error
        self._verify_cadence(stopped_at=time.monotonic())
