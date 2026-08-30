"""Portable IoT transport through the selected physical R2Lab UE."""

from __future__ import annotations

import json
from pathlib import Path
import shlex
import time
from typing import Any, Mapping

from synthran.experiment import ExperimentError, validate_run_id
from synthran.experiment.r2lab import (
    LOCAL_UE_RELAY_PORT,
    ManagedPhysicalUeRelay,
    _prove_ue_route,
    _ue_relay_process_count,
    _validate_ue,
    build_physical_ue_stdio_relay_command,
)
from synthran.experiment.runtime import (
    REMOTE_EDGE_FORWARD_PORT,
    _core_address,
    _remote,
    _remote_path_exists,
    _ssh_reverse_tunnel_command,
    _transfer_file,
)
from synthran.fiveg_ansible import NetworkInventory
from synthran.ingress import IngressSnapshot
from synthran.iot_edge_transport import (
    OwnedProcess,
    _local_forward_command,
    _local_port_is_closed,
    _remote_port_is_closed,
    _start_process,
    _wait_local_tcp,
    _wait_remote_listener,
)
from synthran.iot_source import MQTTEndpoint
from synthran.live_preflight import LivePreflightError, ssh_command


R2LAB_AMBER_INGRESS_PORT = 18886
R2LAB_AMBER_LOCAL_PORT = 18886


class R2LabIoTTransportError(ExperimentError):
    """Raised when the selected physical UE transport cannot be proven."""


class R2LabIoTTransportSession:
    def __init__(
        self,
        *,
        inventory: NetworkInventory,
        slice_name: str,
        ue: str,
        run_id: str,
        endpoint: MQTTEndpoint,
        relay_port: int,
        remote_ingress_port: int,
        snapshot_remote_path: str,
        remote_workspace: str,
        processes: list[OwnedProcess],
        relay: ManagedPhysicalUeRelay,
    ) -> None:
        self.inventory = inventory
        self.slice_name = slice_name
        self.ue = ue
        self.run_id = run_id
        self._endpoint = endpoint
        self.relay_port = relay_port
        self.remote_ingress_port = remote_ingress_port
        self.snapshot_remote_path = snapshot_remote_path
        self.remote_workspace = remote_workspace
        self.processes = processes
        self.relay = relay
        self._cleanup_errors: list[str] = []
        self._last_snapshot: IngressSnapshot | None = None
        self._stopped = False

    @property
    def mqtt_endpoint(self) -> MQTTEndpoint:
        return self._endpoint

    def snapshot(self) -> IngressSnapshot:
        try:
            command = ssh_command(
                self.inventory.core_node,
                "cat",
                self.snapshot_remote_path,
            )
        except LivePreflightError as exc:
            raise R2LabIoTTransportError(str(exc)) from exc
        from synthran.iot_edge_transport import _run

        result = _run(command, timeout_seconds=10)
        if result.returncode != 0:
            raise R2LabIoTTransportError("physical counted-ingress snapshot is unavailable")
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise R2LabIoTTransportError(
                "physical counted-ingress snapshot is invalid JSON"
            ) from exc
        if not isinstance(value, dict):
            raise R2LabIoTTransportError(
                "physical counted-ingress snapshot must be a JSON object"
            )
        snapshot = IngressSnapshot.from_dict(value)
        self._last_snapshot = snapshot
        return snapshot

    def stop(self) -> None:
        if self._stopped:
            return
        try:
            self.snapshot()
        except Exception:
            pass

        for process in reversed(self.processes):
            try:
                process.stop()
            except Exception as exc:
                self._cleanup_errors.append(f"{process.name}: {exc}")
        try:
            self.relay.stop()
        except Exception as exc:
            self._cleanup_errors.append(f"physical UE relay: {exc}")

        try:
            _remote(
                self.inventory,
                "rm",
                "-rf",
                self.remote_workspace,
                label="physical IoT workspace cleanup",
                timeout_seconds=10,
            )
        except Exception as exc:
            self._cleanup_errors.append(f"remote workspace cleanup: {exc}")
        try:
            if _remote_path_exists(
                self.inventory,
                self.remote_workspace,
                timeout_seconds=5,
            ):
                self._cleanup_errors.append("remote workspace still exists after cleanup")
        except Exception as exc:
            self._cleanup_errors.append(f"remote workspace postcondition: {exc}")

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if (
                _local_port_is_closed(self._endpoint.port)
                and _local_port_is_closed(self.relay_port)
                and _remote_port_is_closed(self.inventory, self.remote_ingress_port)
                and _remote_port_is_closed(self.inventory, REMOTE_EDGE_FORWARD_PORT)
            ):
                break
            time.sleep(0.25)

        if not _local_port_is_closed(self._endpoint.port):
            self._cleanup_errors.append("local publisher forward still listens")
        if not _local_port_is_closed(self.relay_port):
            self._cleanup_errors.append("local physical UE relay still listens")
        if not _remote_port_is_closed(self.inventory, self.remote_ingress_port):
            self._cleanup_errors.append("remote counted ingress still listens")
        if not _remote_port_is_closed(self.inventory, REMOTE_EDGE_FORWARD_PORT):
            self._cleanup_errors.append("remote physical UE forward still listens")

        try:
            profile = _validate_ue(self.ue)
            if _ue_relay_process_count(self.slice_name, profile, self.run_id) != 0:
                self._cleanup_errors.append("physical UE relay processes remain after cleanup")
        except Exception as exc:
            self._cleanup_errors.append(f"physical UE relay postcondition: {exc}")
        self._stopped = True

    def evidence(self) -> Mapping[str, Any]:
        snapshot = self._last_snapshot
        if snapshot is None and not self._stopped:
            try:
                snapshot = self.snapshot()
            except Exception:
                snapshot = None
        return {
            "backend": "r2lab",
            "ue": self.ue,
            "ue_interface": "wwan0",
            "publisher_endpoint": {
                "host": self._endpoint.host,
                "port": self._endpoint.port,
            },
            "remote_ingress": {
                "host": "127.0.0.1",
                "port": self.remote_ingress_port,
                "snapshot": snapshot.to_dict() if snapshot is not None else None,
            },
            "relay_port": self.relay_port,
            "stopped": self._stopped,
            "cleanup_errors": list(self._cleanup_errors),
            "cleanup_valid": self._stopped and not self._cleanup_errors,
        }


class R2LabIoTTransportAdapter:
    """Expose the selected UE/wwan0 path through counted loopback ingress."""

    def __init__(
        self,
        *,
        inventory: NetworkInventory,
        slice_name: str,
        ue: str,
        repository_root: Path,
        relay_port: int = LOCAL_UE_RELAY_PORT,
        remote_ingress_port: int = R2LAB_AMBER_INGRESS_PORT,
        local_port: int = R2LAB_AMBER_LOCAL_PORT,
    ) -> None:
        self.inventory = inventory
        self.slice_name = slice_name
        self.ue = ue
        self.repository_root = repository_root.resolve()
        self.relay_port = relay_port
        self.remote_ingress_port = remote_ingress_port
        self.local_port = local_port

    def start(
        self,
        *,
        run_id: str,
        run_directory: Path,
    ) -> R2LabIoTTransportSession:
        validate_run_id(run_id)
        profile = _validate_ue(self.ue)
        central_address = _core_address(self.inventory)
        _prove_ue_route(self.slice_name, profile, central_address)
        if _ue_relay_process_count(self.slice_name, profile, run_id) != 0:
            raise R2LabIoTTransportError(
                "an existing physical UE relay already owns this workload ID"
            )
        if not _local_port_is_closed(self.local_port):
            raise R2LabIoTTransportError("local publisher port is already in use")
        if not _local_port_is_closed(self.relay_port):
            raise R2LabIoTTransportError("local physical UE relay port is already in use")
        if not _remote_port_is_closed(self.inventory, self.remote_ingress_port):
            raise R2LabIoTTransportError("remote counted-ingress port is already in use")
        if not _remote_port_is_closed(self.inventory, REMOTE_EDGE_FORWARD_PORT):
            raise R2LabIoTTransportError("remote physical UE forward port is already in use")

        logs = run_directory / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        remote_workspace = f"/tmp/synthran/{run_id}"
        _remote(
            self.inventory,
            "mkdir",
            "-p",
            remote_workspace,
            label="physical IoT workspace creation",
        )
        try:
            _transfer_file(
                self.inventory,
                self.repository_root / "synthran" / "ingress.py",
                f"{remote_workspace}/ingress.py",
                label="physical counted-ingress helper transfer",
            )
        except Exception:
            _remote(
                self.inventory,
                "rm",
                "-rf",
                remote_workspace,
                label="physical IoT workspace rollback",
            )
            raise

        relay_command = build_physical_ue_stdio_relay_command(
            slice_name=self.slice_name,
            ue=self.ue,
            run_id=run_id,
            central_address=central_address,
        )
        relay = ManagedPhysicalUeRelay(port=self.relay_port, command=relay_command)
        processes: list[OwnedProcess] = []
        relay.start()
        try:
            reverse_forward = _start_process(
                "physical UE reverse forward",
                _ssh_reverse_tunnel_command(
                    self.inventory,
                    remote_port=REMOTE_EDGE_FORWARD_PORT,
                    local_port=relay.port,
                ),
                cwd=self.repository_root,
                log_path=logs / "physical-ue-reverse-forward.log",
            )
            processes.append(reverse_forward)
            _wait_remote_listener(
                self.inventory,
                port=REMOTE_EDGE_FORWARD_PORT,
                process=reverse_forward,
            )

            snapshot_remote = f"{remote_workspace}/amber-ingress-snapshot.json"
            ingress_command = (
                f"exec python3 {shlex.quote(remote_workspace)}/ingress.py "
                "--listen-host 127.0.0.1 "
                f"--listen-port {self.remote_ingress_port} "
                "--target-host 127.0.0.1 "
                f"--target-port {REMOTE_EDGE_FORWARD_PORT} "
                f"--snapshot-path {shlex.quote(snapshot_remote)}"
            )
            try:
                ingress_ssh = ssh_command(
                    self.inventory.core_node,
                    "sh",
                    "-c",
                    ingress_command,
                )
            except LivePreflightError as exc:
                raise R2LabIoTTransportError(str(exc)) from exc
            ingress = _start_process(
                "physical counted ingress",
                ingress_ssh,
                cwd=self.repository_root,
                log_path=logs / "physical-counted-ingress.log",
            )
            processes.append(ingress)
            _wait_remote_listener(
                self.inventory,
                port=self.remote_ingress_port,
                process=ingress,
            )

            publisher_forward = _start_process(
                "physical publisher forward",
                _local_forward_command(
                    self.inventory,
                    local_port=self.local_port,
                    remote_port=self.remote_ingress_port,
                ),
                cwd=self.repository_root,
                log_path=logs / "physical-publisher-forward.log",
            )
            processes.append(publisher_forward)
            _wait_local_tcp(self.local_port, process=publisher_forward)
        except Exception:
            for process in reversed(processes):
                try:
                    process.stop()
                except Exception:
                    pass
            try:
                relay.stop()
            except Exception:
                pass
            try:
                _remote(
                    self.inventory,
                    "rm",
                    "-rf",
                    remote_workspace,
                    label="physical IoT workspace rollback",
                )
            except Exception:
                pass
            raise

        return R2LabIoTTransportSession(
            inventory=self.inventory,
            slice_name=self.slice_name,
            ue=self.ue,
            run_id=run_id,
            endpoint=MQTTEndpoint("127.0.0.1", self.local_port),
            relay_port=self.relay_port,
            remote_ingress_port=self.remote_ingress_port,
            snapshot_remote_path=snapshot_remote,
            remote_workspace=remote_workspace,
            processes=processes,
            relay=relay,
        )
