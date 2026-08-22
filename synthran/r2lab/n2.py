"""Sanitized AMF-side N2 acceptance evidence for physical R2Lab runs.

The gNB runtime can be healthy while its own log omits an affirmative NGAP/N2
message. Open5GS AMF logs provide an independent observation. This module only
parses already-observed AMF text; it performs no Kubernetes or network mutation
and never persists the raw private gNB address.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import ipaddress
import re


class R2LabN2EvidenceError(RuntimeError):
    """Raised when AMF-side N2 evidence cannot be safely interpreted."""


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
