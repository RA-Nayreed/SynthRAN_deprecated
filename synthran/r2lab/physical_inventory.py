"""Strict adapter from the virtual inventory contract to one physical R2Lab topology."""

from __future__ import annotations

import hashlib
from pathlib import Path

from synthran.fiveg_ansible import FiveGAnsibleError, NetworkInventory, parse_inventory
from synthran.r2lab.hardware import PhysicalTopology


class R2LabPhysicalInventoryError(ValueError):
    """Raised when a workload inventory does not match the persisted physical topology."""


def load_physical_inventory(path: Path, *, topology: PhysicalTopology) -> NetworkInventory:
    """Load one physical workload inventory while keeping the generic RFSIM parser strict.

    The shared inventory parser intentionally validates only the virtual deployment
    contract.  Physical runs generate the same inventory shape with only ``rru``
    changed to the selected N3xx radio.  This adapter first proves that exact radio,
    validates every other field through the shared parser, then restores the truthful
    physical radio and original content digest.
    """

    topology = topology.validate()
    path = path.expanduser().resolve()
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise R2LabPhysicalInventoryError("physical workload inventory was not found") from exc
    except (OSError, UnicodeDecodeError) as exc:
        raise R2LabPhysicalInventoryError(
            "physical workload inventory must be readable UTF-8 text"
        ) from exc

    radio_lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip().startswith("rru=")
    ]
    expected = f'rru="{topology.radio}"'
    if radio_lines != [expected]:
        raise R2LabPhysicalInventoryError(
            "physical workload inventory radio does not match persisted topology"
        )

    normalized = text.replace(expected, 'rru="rfsim"', 1)
    try:
        parsed = parse_inventory(normalized, source=path)
    except FiveGAnsibleError as exc:
        raise R2LabPhysicalInventoryError(str(exc)) from exc

    if parsed.core_node.name != topology.core_node:
        raise R2LabPhysicalInventoryError(
            "physical workload inventory core node does not match persisted topology"
        )
    if parsed.ran_node.name != topology.ran_node:
        raise R2LabPhysicalInventoryError(
            "physical workload inventory RAN node does not match persisted topology"
        )

    all_vars = dict(parsed.all_vars)
    all_vars["rru"] = topology.radio
    return NetworkInventory(
        path=path,
        sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        core_node=parsed.core_node,
        ran_node=parsed.ran_node,
        all_vars=all_vars,
    )
