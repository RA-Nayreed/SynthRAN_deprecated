"""Sanitized qfit runtime-state classification and user-plane probes."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import ipaddress
import re
from typing import Callable, Sequence

from synthran.live_preflight import CommandResult


class QfitRuntimeStateError(ValueError):
    """Raised when qfit runtime evidence is malformed or contradictory."""


class CellAcquisitionState(str, Enum):
    ACQUIRED_NR_SA = "acquired-nr-sa"
    NO_SERVICE = "no-service"
    OTHER_SERVICE = "other-service"
    UNKNOWN = "unknown"


class RegistrationState(str, Enum):
    REGISTERED = "registered"
    SEARCHING = "searching"
    NOT_REGISTERED = "not-registered"
    UNKNOWN = "unknown"


class PacketServiceState(str, Enum):
    ATTACHED = "attached"
    DETACHED = "detached"
    UNKNOWN = "unknown"


class Ipv4State(str, Enum):
    PRESENT = "present"
    ABSENT = "absent"
    UNKNOWN = "unknown"


_C5GREG_RE = re.compile(r"\+C5GREG:\s*\d+\s*,\s*(\d+)", re.IGNORECASE)
_PACKET_RE = re.compile(
    r"packet\s+service\s+state\s*:\s*['\"]?(attached|detached)['\"]?",
    re.IGNORECASE,
)
_INET_RE = re.compile(r"\binet\s+([^\s/]+)/\d+\b", re.IGNORECASE)


def parse_qnwinfo(output: str) -> CellAcquisitionState:
    normalized = output.upper()
    if "NO SERVICE" in normalized:
        return CellAcquisitionState.NO_SERVICE
    if "NR5G-SA" in normalized or "NR5G_SA" in normalized:
        return CellAcquisitionState.ACQUIRED_NR_SA
    if "+QNWINFO:" in normalized:
        return CellAcquisitionState.OTHER_SERVICE
    return CellAcquisitionState.UNKNOWN


def parse_c5greg(output: str) -> RegistrationState:
    statuses = [int(match.group(1)) for match in _C5GREG_RE.finditer(output)]
    if not statuses or len(set(statuses)) != 1:
        return RegistrationState.UNKNOWN
    status = statuses[-1]
    if status in {1, 5}:
        return RegistrationState.REGISTERED
    if status == 2:
        return RegistrationState.SEARCHING
    if status in {0, 3, 4}:
        return RegistrationState.NOT_REGISTERED
    return RegistrationState.UNKNOWN


def parse_packet_service(output: str) -> PacketServiceState:
    states = {match.group(1).lower() for match in _PACKET_RE.finditer(output)}
    if not states or len(states) != 1:
        return PacketServiceState.UNKNOWN
    return PacketServiceState.ATTACHED if "attached" in states else PacketServiceState.DETACHED


def parse_ipv4_state(output: str, *, interface_present: bool = True) -> Ipv4State:
    if not interface_present:
        return Ipv4State.UNKNOWN
    addresses = []
    for match in _INET_RE.finditer(output):
        try:
            address = ipaddress.ip_address(match.group(1))
        except ValueError:
            continue
        if isinstance(address, ipaddress.IPv4Address):
            addresses.append(address)
    return Ipv4State.PRESENT if addresses else Ipv4State.ABSENT


@dataclass(frozen=True)
class QfitRuntimeEvidence:
    cell: CellAcquisitionState
    registration: RegistrationState
    packet_service: PacketServiceState
    ipv4: Ipv4State

    @property
    def cell_acquired(self) -> bool:
        return self.cell is CellAcquisitionState.ACQUIRED_NR_SA

    @property
    def registered(self) -> bool:
        return self.cell_acquired and self.registration is RegistrationState.REGISTERED

    @property
    def pdu_session_established(self) -> bool:
        return (
            self.registered
            and self.packet_service is PacketServiceState.ATTACHED
            and self.ipv4 is Ipv4State.PRESENT
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "cell": self.cell.value,
            "registration": self.registration.value,
            "packet_service": self.packet_service.value,
            "ipv4": self.ipv4.value,
            "cell_acquired": self.cell_acquired,
            "registered": self.registered,
            "pdu_session_established": self.pdu_session_established,
            "user_plane": "requires-separate-traffic-probe",
        }


def classify_qfit_runtime(
    *,
    qnwinfo_output: str,
    c5greg_output: str,
    packet_service_output: str,
    ipv4_output: str,
    interface_present: bool = True,
) -> QfitRuntimeEvidence:
    """Reduce raw modem/network probes to sanitized states suitable for persistence."""

    return QfitRuntimeEvidence(
        cell=parse_qnwinfo(qnwinfo_output),
        registration=parse_c5greg(c5greg_output),
        packet_service=parse_packet_service(packet_service_output),
        ipv4=parse_ipv4_state(ipv4_output, interface_present=interface_present),
    )


class UserPlaneProbeError(ValueError):
    """Raised when a physical user-plane probe request is unsafe or malformed."""


UserPlaneRunner = Callable[[Sequence[str], int], CommandResult]
_PING_SUMMARY_RE = re.compile(
    r"(?P<tx>\d+)\s+packets transmitted,\s+(?P<rx>\d+)\s+(?:packets\s+)?received",
    re.IGNORECASE,
)


def _peer_fingerprint(peer: str) -> str:
    return hashlib.sha256(peer.encode("ascii")).hexdigest()


def build_user_plane_ping_command(
    peer: str,
    *,
    interface: str = "wwan0",
    count: int = 4,
    reply_timeout_seconds: int = 2,
) -> tuple[str, ...]:
    """Build an argv-only probe that is explicitly bound to the physical UE interface."""

    if interface != "wwan0":
        raise UserPlaneProbeError("current qfit user-plane acceptance requires wwan0")
    if count < 1 or count > 10:
        raise UserPlaneProbeError("user-plane ping count must be between 1 and 10")
    if reply_timeout_seconds < 1 or reply_timeout_seconds > 10:
        raise UserPlaneProbeError("user-plane ping reply timeout must be between 1 and 10 seconds")
    try:
        address = ipaddress.ip_address(peer)
    except ValueError as exc:
        raise UserPlaneProbeError("user-plane peer must be a literal IP address") from exc
    if not isinstance(address, ipaddress.IPv4Address):
        raise UserPlaneProbeError("qfit user-plane verification supports IPv4 only")
    return (
        "ping",
        "-n",
        "-I",
        interface,
        "-c",
        str(count),
        "-W",
        str(reply_timeout_seconds),
        str(address),
    )


@dataclass(frozen=True)
class UserPlaneProbeEvidence:
    interface: str
    peer_sha256: str
    requested_packets: int
    transmitted_packets: int
    received_packets: int
    summary_observed: bool
    returncode: int | None
    transport_error: bool

    def __post_init__(self) -> None:
        if self.interface != "wwan0":
            raise UserPlaneProbeError("user-plane evidence must be bound to wwan0")
        if not re.fullmatch(r"[0-9a-f]{64}", self.peer_sha256):
            raise UserPlaneProbeError("user-plane peer fingerprint must be SHA-256")
        if self.requested_packets < 1:
            raise UserPlaneProbeError("user-plane requested packet count must be positive")
        if min(self.transmitted_packets, self.received_packets) < 0:
            raise UserPlaneProbeError("user-plane packet counters cannot be negative")
        if self.received_packets > self.transmitted_packets:
            raise UserPlaneProbeError("received packet count cannot exceed transmitted count")

    @property
    def proven(self) -> bool:
        return (
            not self.transport_error
            and self.summary_observed
            and self.returncode == 0
            and self.transmitted_packets == self.requested_packets
            and self.received_packets == self.requested_packets
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "interface": self.interface,
            "peer_sha256": self.peer_sha256,
            "requested_packets": self.requested_packets,
            "transmitted_packets": self.transmitted_packets,
            "received_packets": self.received_packets,
            "summary_observed": self.summary_observed,
            "returncode": self.returncode,
            "transport_error": self.transport_error,
            "proven": self.proven,
            "probe": "interface-bound-icmp",
        }


def execute_user_plane_probe(
    *,
    peer: str,
    runner: UserPlaneRunner,
    interface: str = "wwan0",
    count: int = 4,
    reply_timeout_seconds: int = 2,
    command_timeout_seconds: int = 15,
) -> UserPlaneProbeEvidence:
    """Run one bounded interface-bound probe and retain no raw network output."""

    command = build_user_plane_ping_command(
        peer,
        interface=interface,
        count=count,
        reply_timeout_seconds=reply_timeout_seconds,
    )
    if command_timeout_seconds < 1 or command_timeout_seconds > 60:
        raise UserPlaneProbeError("user-plane command timeout must be between 1 and 60 seconds")
    fingerprint = _peer_fingerprint(str(ipaddress.ip_address(peer)))
    try:
        result = runner(command, command_timeout_seconds)
    except (RuntimeError, OSError):
        return UserPlaneProbeEvidence(
            interface=interface,
            peer_sha256=fingerprint,
            requested_packets=count,
            transmitted_packets=0,
            received_packets=0,
            summary_observed=False,
            returncode=None,
            transport_error=True,
        )
    text = "\n".join(part for part in (result.stdout, result.stderr) if part)
    match = _PING_SUMMARY_RE.search(text)
    transmitted = int(match.group("tx")) if match is not None else 0
    received = int(match.group("rx")) if match is not None else 0
    return UserPlaneProbeEvidence(
        interface=interface,
        peer_sha256=fingerprint,
        requested_packets=count,
        transmitted_packets=transmitted,
        received_packets=received,
        summary_observed=match is not None,
        returncode=result.returncode,
        transport_error=False,
    )
