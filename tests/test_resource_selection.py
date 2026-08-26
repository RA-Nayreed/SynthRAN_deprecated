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

    def test_r2lab_selection_uses_live_availability(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="physical-ran")
        states = r2lab_states(available={"n320", "qfit09"})
        selected = select_resources(
            desired,
            inventory(
                slices=snapshot("slices", slices_states()),
                r2lab=snapshot("r2lab", states),
            ),
            now=NOW,
        )
        self.assertEqual(selected.for_role("radio")[0].resource_id, "n320")
        self.assertEqual(selected.for_role("ue")[0].resource_id, "qfit09")

    def test_r2lab_owned_compatible_resource_is_preferred(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="physical-ran")
        states = r2lab_states(
            available={"n300", "n320", "qfit07", "qfit09"},
            ownership={"n320": "synthran", "qfit09": "synthran"},
        )
        selected = select_resources(
            desired,
            inventory(
                slices=snapshot("slices", slices_states()),
                r2lab=snapshot("r2lab", states),
            ),
            now=NOW,
        )
        self.assertEqual(selected.for_role("radio")[0].resource_id, "n320")
        self.assertEqual(selected.for_role("ue")[0].resource_id, "qfit09")

    def test_missing_required_physical_provider_blocks_selection(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="physical-ran")
        with self.assertRaises(ResourceSelectionError):
            select_resources(
                desired,
                inventory(slices=snapshot("slices", slices_states())),
                now=NOW,
            )

    def test_incomplete_provider_snapshot_blocks_mutating_selection(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="physical-ran")
        with self.assertRaises(ResourceSelectionError):
            select_resources(
                desired,
                inventory(
                    slices=snapshot("slices", slices_states()),
                    r2lab=snapshot(
                        "r2lab",
                        r2lab_states(available={"n300", "qfit07"}),
                        complete=False,
                    ),
                ),
                now=NOW,
            )

    def test_stale_provider_snapshot_blocks_mutating_selection(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="physical-ran")
        with self.assertRaises(ResourceSelectionError):
            select_resources(
                desired,
                inventory(
                    slices=snapshot("slices", slices_states()),
                    r2lab=snapshot(
                        "r2lab",
                        r2lab_states(available={"n300", "qfit07"}),
                        minutes=-1,
                    ),
                ),
                now=NOW,
            )

    def test_requirements_from_desired_preserve_physical_radio_constraints(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="physical-ran")
        requirements = requirements_from_desired(desired)
        radio = next(item for item in requirements if item.role == "radio")
        ue = next(item for item in requirements if item.role == "ue")
        self.assertIn("backend:r2lab", radio.capabilities)
        self.assertIn("backend:r2lab", ue.capabilities)

    def test_explicit_physical_preferences_are_respected(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="physical-ran")
        desired = replace(
            desired,
            radio=RadioDesiredState(
                backend="r2lab",
                implementation="srsran",
                resource_preferences=("n320",),
            ),
            placement=PlacementDesiredState(
                core_preferences=desired.placement.core_preferences,
                ran_preferences=desired.placement.ran_preferences,
                ue_preferences=("qfit09",),
            ),
        )
        selected = select_resources(
            desired,
            inventory(
                slices=snapshot("slices", slices_states()),
                r2lab=snapshot("r2lab", r2lab_states(available={"n300", "n320", "qfit07", "qfit09"})),
            ),
            now=NOW,
        )
        self.assertEqual(selected.for_role("radio")[0].resource_id, "n320")
        self.assertEqual(selected.for_role("ue")[0].resource_id, "qfit09")

    def test_unsupported_ran_implementation_has_no_compatible_physical_radio(self) -> None:
        desired = ExperimentDesiredState.recommended(intent="physical-ran")
        desired = replace(
            desired,
            ran=RanDesiredState(implementation="ueransim"),
        )
        with self.assertRaises(ResourceSelectionError):
            select_resources(
                desired,
                inventory(
                    slices=snapshot("slices", slices_states()),
                    r2lab=snapshot("r2lab", r2lab_states(available={"n300", "qfit07"})),
                ),
                now=NOW,
            )


if __name__ == "__main__":
    unittest.main()
