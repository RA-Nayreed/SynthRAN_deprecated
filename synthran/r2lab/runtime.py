"""Sanitized gNB-side N2 log classification used by the active physical path."""

from __future__ import annotations

from enum import Enum
import re


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
