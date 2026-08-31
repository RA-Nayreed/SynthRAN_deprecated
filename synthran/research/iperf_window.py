"""Window-scoped iperf3 metrics for source-clock-aligned Amber research."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from synthran.research import LOAD_RESULT_SCHEMA, ResearchError, append_jsonl


def _interval_bps(interval: Mapping[str, Any], *, protocol: str) -> tuple[float, float, float] | None:
    summary = interval.get("sum")
    if isinstance(summary, Mapping):
        start = summary.get("start")
        end = summary.get("end")
        bps = summary.get("bits_per_second")
        if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in (start, end, bps)):
            return float(start), float(end), float(bps)

    streams = interval.get("streams")
    if not isinstance(streams, list):
        return None
    starts: list[float] = []
    ends: list[float] = []
    rates: list[float] = []
    for stream in streams:
        if not isinstance(stream, Mapping):
            continue
        candidate = stream.get("udp") if protocol == "udp" else stream.get("receiver")
        if not isinstance(candidate, Mapping):
            candidate = stream.get("sender")
        if not isinstance(candidate, Mapping):
            continue
        start = candidate.get("start")
        end = candidate.get("end")
        bps = candidate.get("bits_per_second")
        if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in (start, end, bps)):
            starts.append(float(start))
            ends.append(float(end))
            rates.append(float(bps))
    if not rates:
        return None
    return min(starts), max(ends), sum(rates)


def parse_measurement_load_log(
    source: Path,
    destination: Path,
    *,
    target_bps: int,
    protocol: str,
    measurement_start_offset_seconds: float,
    measurement_duration_seconds: float,
) -> None:
    """Persist one throughput result weighted only over the measurement window."""

    if measurement_start_offset_seconds < 0 or measurement_duration_seconds <= 0:
        raise ResearchError("background-load measurement window is invalid")
    try:
        text = source.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        raise ResearchError("unable to read background load log") from exc
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end < start:
        raise ResearchError("background load did not produce iperf3 JSON")
    try:
        value = json.loads(text[start : end + 1])
    except json.JSONDecodeError as exc:
        raise ResearchError("background load produced invalid iperf3 JSON") from exc
    if not isinstance(value, Mapping):
        raise ResearchError("background load result is malformed")
    error = value.get("error")
    if isinstance(error, str) and error.strip():
        raise ResearchError(f"background load iperf3 failed: {error.strip()}")
    intervals = value.get("intervals")
    if not isinstance(intervals, list) or not intervals:
        raise ResearchError("background load result has no interval measurements")

    window_start = measurement_start_offset_seconds
    window_end = window_start + measurement_duration_seconds
    weighted_bits_per_second = 0.0
    covered_seconds = 0.0
    contributing_intervals = 0
    for interval in intervals:
        if not isinstance(interval, Mapping):
            continue
        parsed = _interval_bps(interval, protocol=protocol)
        if parsed is None:
            continue
        interval_start, interval_end, bps = parsed
        overlap = min(interval_end, window_end) - max(interval_start, window_start)
        if overlap <= 0:
            continue
        weighted_bits_per_second += bps * overlap
        covered_seconds += overlap
        contributing_intervals += 1

    if covered_seconds <= 0 or contributing_intervals <= 0:
        raise ResearchError("background load does not overlap the measurement window")
    # Iperf normally emits contiguous one-second intervals. Require nearly the
    # full requested window so a truncated load cannot pass by contributing a
    # handful of good samples.
    if covered_seconds < measurement_duration_seconds * 0.98:
        raise ResearchError(
            "background load interval coverage is incomplete for the measurement window"
        )
    measured_bps = weighted_bits_per_second / covered_seconds
    if measured_bps <= 0:
        raise ResearchError("background load measurement is not positive")

    append_jsonl(
        destination,
        {
            "schema": LOAD_RESULT_SCHEMA,
            "protocol": protocol,
            "target_bps": target_bps,
            "bits_per_second": measured_bps,
            "measurement_start_offset_seconds": measurement_start_offset_seconds,
            "measurement_duration_seconds": measurement_duration_seconds,
            "covered_seconds": covered_seconds,
            "contributing_intervals": contributing_intervals,
        },
    )
