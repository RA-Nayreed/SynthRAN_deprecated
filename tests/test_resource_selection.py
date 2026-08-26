from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import unittest

from synthran.network.resources import SUPPORTED_NODES
from synthran.r2lab.hardware import RADIOS, UES
from synthran.resources import (
    ProviderResourceSnapshot,
    ResourceDescriptor,
    ResourceInventory,
    ResourceSelectionError,
    ResourceState,
    requirements_from_desired,
    reviewed_resource_descriptors,
    select_resources,
)
from synthran.workspace.desired import (
    ExperimentDesiredState,
    PlacementDesiredState,
    RadioDesiredState,
    RanDesiredState,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 17, 19, 0, tzinfo=UTC)


def snapshot(
    provider: str,
    states: list[ResourceState],
    *,
    complete: bool = True,
    minutes: int = 10,
) -> ProviderResourceSnapshot:
    return ProviderResourceSnapshot(
        provider=provider,
        observed_at_utc="2026-08-17T19:00:00Z",
        fresh_until_utc=(NOW + timedelta(minutes=minutes)).isoformat().replace("+00:00", "Z"),
        complete=complete,
        resources=tuple(states),
    )


def reviewed_ids(provider: str, kind: str | None = None) -> list[str]:
    return [
        item.resource_id
        for item in reviewed_resource_descriptors()
        if item.provider == provider and (kind is None or item.kind == kind)
    ]


def slices_states(
    *,
    unavailable: set[str] | None = None,
    ownership: dict[str, str] | None = None,
) -> list[ResourceState]:
    unavailable = unavailable or set()
    ownership = ownership or {}
    return [
        ResourceState(
            resource_id,
            "unavailable" if resource_id in unavailable else (
                "allocated" if ownership.get(resource_id) in {"synthran", "operator", "other"} else "available"
            ),
            ownership.get(resource_id, "unowned"),
        )
        for resource_id in reviewed_ids("slices", "compute")
    ]


def r2lab_states(
    *,
    available: set[str],
    ownership: dict[str, str] | None = None,
) -> list[ResourceState]:
    ownership = ownership or {}
    return [
        ResourceState(
            resource_id,
            (
                "allocated"
                if resource_id in available
                and ownership.get(resource_id) in {"synthran", "operator", "other"}
                else "available"
                if resource_id in available
                else "unavailable"
            ),
            ownership.get(resource_id, "unowned"),
        )
        for resource_id in reviewed_ids("r2lab")
    ]


def inventory(
    *,
    slices: ProviderResourceSnapshot,
    r2lab: ProviderResourceSnapshot | None = None,
    extra_descriptors: tuple[ResourceDescriptor, ...] = (),
) -> ResourceInventory:
    snapshots = (slices,) + ((r2lab,) if r2lab is not None else ())
    return ResourceInventory(
        descriptors=reviewed_resource_descriptors() + extra_descriptors,
        snapshots=snapshots,
    )


class ResourceSelectionTests(unittest.TestCase):
    def test_catalog_matches_resources_already_reviewed_by_live_providers(self) -> None:
        descriptors = reviewed_resource_descriptors()
        compute = {
            item.resource_id
            for item in descriptors
            if item.provider == "slices" and item.kind == "compute"
        }
        radios = {
            item.resource_id
            for item in descriptors
            if item.provider == "r2lab" and item.kind == "radio"
        }
        ues = {
            item.resource_id
            for item in descriptors
            if item.provider == "r2lab" and item.kind == "ue"
        }
        executable_radios = {name for name, profile in RADIOS.items() if profile.executable}
        executable_ues = {name for name, profile in UES.items() if profile.executable}
        self.assertEqual(compute, set(SUPPORTED_NODES))
        self.assertEqual(radios, executable_radios)
        self.assertEqual(ues, executable_ues)

    def test_virtual_default_prefers_recommended_compute_pair_without_r2lab(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="virtual-5g")
        selected = select_resources(
            desired,
            inventory(slices=snapshot("slices", slices_states())),
            now=NOW,
        )
        self.assertEqual(selected.for_role("core")[0].resource_id, "sopnode-f2")
        self.assertEqual(selected.for_role("ran")[0].resource_id, "sopnode-f3")
        self.assertEqual(selected.for_role("radio")[0].resource_id, "virtual:rfsim")
        self.assertIsNone(
            next(
                (
                    group
                    for group in selected.provider_sets
                    if group.provider == "r2lab"
                ),
                None,
            )
        )

    def test_selector_falls_back_when_recommended_core_is_unavailable(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="virtual-5g")
        selected = select_resources(
            desired,
            inventory(
                slices=snapshot(
                    "slices",
                    slices_states(unavailable={"sopnode-f2"}),
                )
            ),
            now=NOW,
        )
        self.assertEqual(selected.for_role("core")[0].resource_id, "sopnode-f1")
        self.assertEqual(selected.for_role("ran")[0].resource_id, "sopnode-f3")

    def test_already_owned_compatible_resource_wins_over_unowned_preference(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="virtual-5g")
        selected = select_resources(
            desired,
            inventory(
                slices=snapshot(
                    "slices",
                    slices_states(ownership={"sopnode-f1": "synthran"}),
                )
            ),
            now=NOW,
        )
        self.assertEqual(selected.for_role("core")[0].resource_id, "sopnode-f1")
        self.assertEqual(selected.for_role("core")[0].ownership, "synthran")

    def test_foreign_or_unknown_resources_are_not_automatic_candidates(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="virtual-5g")
        states = slices_states(
            unavailable={"sopnode-f1", "sopnode-w3"},
            ownership={"sopnode-f2": "other", "sopnode-f3": "unknown"},
        )
        with self.assertRaises(ResourceSelectionError):
            select_resources(
                desired,
                inventory(slices=snapshot("slices", states)),
                now=NOW,
            )

    def test_stale_or_partial_provider_inventory_fails_closed(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="virtual-5g")
        with self.assertRaises(ResourceSelectionError):
            select_resources(
                desired,
                inventory(
                    slices=snapshot("slices", slices_states(), minutes=1)
                ),
                now=NOW + timedelta(minutes=2),
            )
        with self.assertRaises(ResourceSelectionError):
            select_resources(
                desired,
                inventory(
                    slices=snapshot(
                        "slices", slices_states(), complete=False
                    )
                ),
                now=NOW,
            )

    def test_manual_placement_is_exact_but_still_checks_live_safety(self) -> None:
        desired = replace(
            ExperimentDesiredState.recommended(intent="virtual-5g"),
            placement=PlacementDesiredState(
                mode="manual",
                core_node="sopnode-f1",
                ran_node="sopnode-w3",
            ),
        )
        selected = select_resources(
            desired,
            inventory(slices=snapshot("slices", slices_states())),
            now=NOW,
        )
        self.assertEqual(selected.for_role("core")[0].resource_id, "sopnode-f1")
        self.assertEqual(selected.for_role("ran")[0].resource_id, "sopnode-w3")

        unsafe = slices_states(ownership={"sopnode-f1": "other"})
        with self.assertRaises(ResourceSelectionError):
            select_resources(
                desired,
                inventory(slices=snapshot("slices", unsafe)),
                now=NOW,
            )

    def test_manual_extra_resources_are_included_in_provider_set(self) -> None:
        desired = replace(
            ExperimentDesiredState.recommended(intent="virtual-5g"),
            placement=PlacementDesiredState(
                mode="manual",
                core_node="sopnode-f1",
                ran_node="sopnode-f3",
                extra_resources=("sopnode-w3",),
            ),
        )
        selected = select_resources(
            desired,
            inventory(slices=snapshot("slices", slices_states())),
            now=NOW,
        )
        self.assertEqual(selected.for_role("extra001")[0].resource_id, "sopnode-w3")
        slices_group = next(
            group for group in selected.provider_sets if group.provider == "slices"
        )
        self.assertEqual(
            set(slices_group.resource_ids),
            {"sopnode-f1", "sopnode-f3", "sopnode-w3"},
        )

    def test_physical_selection_requires_fresh_complete_r2lab_inventory(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="physical-5g")
        with self.assertRaises(ResourceSelectionError):
            select_resources(
                desired,
                inventory(slices=snapshot("slices", slices_states())),
                now=NOW,
            )

        available = {"n300", "qhat01", "qhat02"}
        selected = select_resources(
            desired,
            inventory(
                slices=snapshot("slices", slices_states()),
                r2lab=snapshot(
                    "r2lab",
                    r2lab_states(available=available),
                ),
            ),
            now=NOW,
        )
        self.assertEqual(selected.for_role("radio")[0].resource_id, "n300")
        self.assertEqual(selected.for_role("ue")[0].resource_id, "qhat01")

    def test_pinned_radio_hardware_and_ran_compatibility_are_enforced(self) -> None:
        desired = replace(
            ExperimentDesiredState.recommended(intent="physical-5g"),
            radio=RadioDesiredState(
                mode="physical",
                backend="r2lab",
                hardware="n320",
            ),
            ran=RanDesiredState(implementation="srsran"),
        )
        selected = select_resources(
            desired,
            inventory(
                slices=snapshot("slices", slices_states()),
                r2lab=snapshot(
                    "r2lab",
                    r2lab_states(available={"n300", "n320", "qhat01"}),
                ),
            ),
            now=NOW,
        )
        self.assertEqual(selected.for_role("radio")[0].resource_id, "n320")

    def test_multi_ue_selection_is_deterministic_and_non_overlapping(self) -> None:
        desired = replace(
            ExperimentDesiredState.recommended(intent="physical-5g"),
            ue=replace(
                ExperimentDesiredState.recommended(intent="physical-5g").ue,
                count=2,
            ),
        )
        selected = select_resources(
            desired,
            inventory(
                slices=snapshot("slices", slices_states()),
                r2lab=snapshot(
                    "r2lab",
                    r2lab_states(
                        available={"n300", "qhat01", "qhat02", "qhat03"}
                    ),
                ),
            ),
            now=NOW,
        )
        self.assertEqual(
            [item.resource_id for item in selected.for_role("ue")],
            ["qhat01", "qhat02"],
        )

    def test_catalog_can_be_extended_without_changing_selector(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="virtual-5g")
        extra = ResourceDescriptor(
            resource_id="sopnode-new",
            provider="slices",
            kind="compute",
            capabilities=frozenset({"compute", "role:core"}),
            role_priority={"core": 50},
        )
        states = slices_states(ownership={"sopnode-f1": "other"})
        states.append(ResourceState("sopnode-new", "allocated", "synthran"))
        selected = select_resources(
            desired,
            inventory(
                slices=snapshot("slices", states),
                extra_descriptors=(extra,),
            ),
            now=NOW,
        )
        self.assertEqual(selected.for_role("core")[0].resource_id, "sopnode-new")

    def test_ambiguous_automatic_radio_requires_explicit_resolution(self) -> None:
        desired = ExperimentDesiredState(intent="open-ran")
        with self.assertRaises(ResourceSelectionError):
            requirements_from_desired(desired)


if __name__ == "__main__":
    unittest.main()
