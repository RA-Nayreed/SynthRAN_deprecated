"""Read-only qfit/FIT readiness proof for physical R2Lab acceptance.

This module deliberately does not provision, reboot, attach, connect, or reset a
UE. It proves that the already-selected qfit has the minimum management surface
needed before modem observation is meaningful: external USB power, strict SSH
to the corresponding FIT host, both reviewed modem devices, and ``wwan0``.

The caller supplies a runner that already executes argv on Faraday. Keeping the
gateway transport outside this module avoids hidden credential/profile lookup
and makes the readiness proof deterministic and testable.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
import re
from typing import Callable, Sequence

from synthran.live_preflight import CommandResult


RemoteRunner = Callable[[Sequence[str], int], CommandResult]
_QFIT_RE = re.compile(r"^qfit(?P<node>07|09|18|29|32|34)$")
_MAX_REMOTE_PATH = 512


class R2LabQfitReadinessError(RuntimeError):
    """Raised when a qfit readiness request crosses the reviewed boundary."""


@dataclass(frozen=True)
class QfitReadinessEvidence:
    qfit: str
    usb_power_on: bool
    strict_ssh: bool
    serial_device_present: bool
    mbim_device_present: bool
    wwan_interface_present: bool
    transport_error: bool

    @property
    def ready(self) -> bool:
        return (
            not self.transport_error
            and self.usb_power_on
            and self.strict_ssh
            and self.serial_device_present
            and self.mbim_device_present
            and self.wwan_interface_present
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "qfit": self.qfit,
            "usb_power_on": self.usb_power_on,
            "strict_ssh": self.strict_ssh,
            "serial_device_present": self.serial_device_present,
            "mbim_device_present": self.mbim_device_present,
            "wwan_interface_present": self.wwan_interface_present,
            "transport_error": self.transport_error,
            "ready": self.ready,
            "status": "management-ready" if self.ready else "management-not-ready",
        }


def _qfit_node(qfit: str) -> tuple[str, int]:
    value = qfit.strip().lower()
    match = _QFIT_RE.fullmatch(value)
    if match is None:
        raise R2LabQfitReadinessError("readiness requires one reviewed qfit resource")
    return value, int(match.group("node"))


def _remote_known_hosts(value: str) -> str:
    if not value or len(value) > _MAX_REMOTE_PATH or any(ch in value for ch in "\r\n\0"):
        raise R2LabQfitReadinessError("qfit known-hosts path is malformed")
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts:
        raise R2LabQfitReadinessError("qfit known-hosts path must be absolute and normalized")
    return str(path)


def _nested_ssh_base(*, node: int, known_hosts: str) -> tuple[str, ...]:
    return (
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        "--",
        f"root@fit{node:02d}",
    )


def qfit_readiness_commands(
    *, qfit: str, remote_known_hosts: str
) -> tuple[tuple[str, ...], ...]:
    """Return the fixed read-only readiness command set for review/tests."""

    _, node = _qfit_node(qfit)
    known_hosts = _remote_known_hosts(remote_known_hosts)
    ssh = _nested_ssh_base(node=node, known_hosts=known_hosts)
    return (
        ("curl", "-fsS", f"http://reboot{node:02d}/usrpstatus"),
        (*ssh, "true"),
        (*ssh, "test", "-c", "/dev/ttyUSB2"),
        (*ssh, "test", "-c", "/dev/cdc-wdm0"),
        (*ssh, "ip", "link", "show", "dev", "wwan0"),
    )


def execute_qfit_readiness(
    *,
    qfit: str,
    remote_known_hosts: str,
    runner: RemoteRunner,
    timeout_seconds: int = 15,
) -> QfitReadinessEvidence:
    """Prove current qfit management readiness without performing mutation."""

    value, _ = _qfit_node(qfit)
    if timeout_seconds < 5 or timeout_seconds > 60:
        raise R2LabQfitReadinessError("qfit readiness timeout must be between 5 and 60 seconds")
    commands = qfit_readiness_commands(
        qfit=value,
        remote_known_hosts=remote_known_hosts,
    )

    results: list[CommandResult | None] = []
    for command in commands:
        try:
            result = runner(command, timeout_seconds)
        except (RuntimeError, OSError):
            results.append(None)
        else:
            results.append(result)

    usb, strict_ssh, serial, mbim, interface = results
    usb_power_on = (
        usb is not None
        and usb.returncode == 0
        and usb.stdout.strip().lower() == "usrpon"
    )
    transport_error = any(result is None for result in results)
    return QfitReadinessEvidence(
        qfit=value,
        usb_power_on=usb_power_on,
        strict_ssh=strict_ssh is not None and strict_ssh.returncode == 0,
        serial_device_present=serial is not None and serial.returncode == 0,
        mbim_device_present=mbim is not None and mbim.returncode == 0,
        wwan_interface_present=interface is not None and interface.returncode == 0,
        transport_error=transport_error,
    )
