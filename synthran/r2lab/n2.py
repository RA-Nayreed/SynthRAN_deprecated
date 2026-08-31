"""Sanitized N2 evidence classification for physical R2Lab runs.

SynthRAN accepts N2 from either an affirmative gNB-side NGAP/SCTP log or an
exact-peer Open5GS AMF acceptance record. This module owns both classifiers so
N2 evidence has one home and no generic ``runtime`` module is needed.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import ipaddress
import re


class R2LabN2EvidenceError(RuntimeError):
    """Raised when N2 evidence cannot be safely interpreted."""


class N2State(str, Enum):
    ESTABLISHED = "established"
    NOT_OBSERVED = "not-observed"
    UNKNOWN = "unknown"


def parse_n2_log_state(text: str) -> N2State:
    """Accept only affirmative, non-error gNB N2/NGAP connection evidence."""

    if not text.strip():
        return N2State.NOT_OBSERVED
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for line in lines:
        lowered = line.lower()
        if any(
            word in lowered
            for word in ("failed", "failure", "error", "timeout", "disconnected")
        ):
            continue
        if re.search(
            r"\bamf\b.*\b(?:connection|association)\b.*\b(?:established|connected|successful)\b",
            lowered,
        ):
            return N2State.ESTABLISHED
        if re.search(
            r"\b(?:ngap|ng[- ]?setup|n2)\b.*\b(?:established|connected|successful|success|response)\b",
            lowered,
        ):
            return N2State.ESTABLISHED
    all_text = "\n".join(lines).lower()
    if ("amf" in all_text or "ngap" in all_text) and re.search(
        r"\bsctp\b.*\b(?:established|connected)\b", all_text
    ):
        return N2State.ESTABLISHED
    return N2State.NOT_OBSERVED


@dataclass(frozen=True)
class AmfN2Evidence:
    accepted: bool
    log_observed: bool
    transport_error: bool
    peer_fingerprint: str

    @property
    def proven(self) -> bool:
        return self.accepted and self.log_observed and not self.transport_error

    def to_dict(self) -> dict[str, object]:
        return {
            "accepted": self.accepted,
            "log_observed": self.log_observed,
            "transport_error": self.transport_error,
            "peer_fingerprint": self.peer_fingerprint,
            "proven": self.proven,
            "source": "sanitized-amf-exact-gnb-peer",
        }


def _expected_peer(value: str) -> str:
    try:
        address = ipaddress.ip_address(value)
    except ValueError as exc:
        raise R2LabN2EvidenceError("expected gNB N2 peer must be an IP address") from exc
    if address.version != 4:
        raise R2LabN2EvidenceError("current R2Lab N2 evidence requires an IPv4 gNB peer")
    return str(address)


def _peer_fingerprint(peer: str) -> str:
    return hashlib.sha256(peer.encode("ascii")).hexdigest()


def parse_amf_n2_acceptance(text: str, *, expected_peer: str) -> bool:
    """Return true only for an affirmative Open5GS gNB-N2 accept of one exact peer."""

    peer = _expected_peer(expected_peer)
    pattern = re.compile(
        rf"\bgNB-N2\s+accepted\[{re.escape(peer)}\](?::\d+)?(?=$|\s)",
        re.IGNORECASE,
    )
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        lowered = stripped.lower()
        if any(word in lowered for word in ("rejected", "failed", "failure", "error")):
            continue
        if pattern.search(stripped):
            return True
    return False


def build_amf_n2_evidence(
    *,
    text: str,
    expected_peer: str,
    transport_error: bool = False,
) -> AmfN2Evidence:
    """Reduce raw AMF logs immediately to peer-bound sanitized evidence."""

    peer = _expected_peer(expected_peer)
    observed = bool(text.strip())
    accepted = False if transport_error else parse_amf_n2_acceptance(text, expected_peer=peer)
    return AmfN2Evidence(
        accepted=accepted,
        log_observed=observed,
        transport_error=transport_error,
        peer_fingerprint=_peer_fingerprint(peer),
    )
