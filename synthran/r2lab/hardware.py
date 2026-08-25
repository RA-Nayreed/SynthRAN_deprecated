"""Capability-driven R2Lab hardware and topology model.

The public physical backend selects resources through this module instead of
embedding one validation run (N300/qfit07/sopnode-f2+f3) in runtime code.  The
catalog deliberately distinguishes hardware that exists in R2Lab from hardware
that the pinned SynthRAN srsRAN path can execute safely.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Mapping

from synthran.network.resources import SUPPORTED_NODES
from synthran.network.runtime import validate_run_id


RadioFamily = Literal["n3xx", "ofh", "oai", "placeholder"]
UeKind = Literal["qfit", "qhat", "fr2", "phone"]
UeMode = Literal["mbim", "qmi", "adb", "unknown"]


class R2LabHardwareError(ValueError):
    """Raised when a requested R2Lab hardware topology is unsupported or unsafe."""


@dataclass(frozen=True)
class RadioProfile:
    name: str
    family: RadioFamily
    values_file: str | None
    resource_name: str
    executable: bool
    fixed_ran_node: str | None = None
    image_repository: str | None = None
    image_tag: str | None = None
    container_lock_key: str | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "family": self.family,
            "values_file": self.values_file,
            "resource_name": self.resource_name,
            "executable": self.executable,
            "fixed_ran_node": self.fixed_ran_node,
            "image_repository": self.image_repository,
            "image_tag": self.image_tag,
            "container_lock_key": self.container_lock_key,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class UeProfile:
    name: str
    kind: UeKind
    mode: UeMode
    host: str
    data_interface: str | None
    executable: bool
    reason: str | None = None

    @property
    def is_fr1_quectel(self) -> bool:
        return self.kind in {"qfit", "qhat"} and self.data_interface == "wwan0"

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "kind": self.kind,
            "mode": self.mode,
            "host": self.host,
            "data_interface": self.data_interface,
            "executable": self.executable,
            "reason": self.reason,
        }


_CHART = "charts/srsran-gnb"

RADIOS: Mapping[str, RadioProfile] = {
    "n300": RadioProfile(
        name="n300",
        family="n3xx",
        values_file=f"{_CHART}/values-n300-n78-20MHz.yaml",
        resource_name="n300",
        executable=True,
        image_repository="r2labuser/srsran-gnb-uhd-csi",
        image_tag="v1.0.0.21",
        container_lock_key="srsran_gnb_physical",
    ),
    "n320": RadioProfile(
        name="n320",
        family="n3xx",
        values_file=f"{_CHART}/values-n320-n78-20MHz.yaml",
        resource_name="n320",
        executable=True,
        image_repository="r2labuser/srsran-gnb-uhd",
        image_tag="v1.0",
        # The pinned upstream profile names this tag but the repository lock did
        # not previously contain its digest.  Runtime records the observed
        # imageID as provenance; a future lock entry may tighten this further.
        container_lock_key=None,
    ),
    "benetel1": RadioProfile(
        name="benetel1",
        family="ofh",
        values_file=f"{_CHART}/values-benetel1.yaml",
        resource_name="benetel1",
        executable=False,
        fixed_ran_node="sopnode-f3",
        image_repository="r2labuser/srsran-gnb-dpdk",
        image_tag="v1.0",
        reason="requires the separate SR-IOV/DPDK OFH execution profile",
    ),
    "benetel2": RadioProfile(
        name="benetel2",
        family="ofh",
        values_file=f"{_CHART}/values-benetel2.yaml",
        resource_name="benetel2",
        executable=False,
        fixed_ran_node="sopnode-f3",
        image_repository="r2labuser/srsran-gnb-dpdk",
        image_tag="v1.0",
        reason="requires the separate SR-IOV/DPDK OFH execution profile",
    ),
    "liteon": RadioProfile(
        name="liteon",
        family="placeholder",
        values_file=f"{_CHART}/values-liteon.yaml",
        resource_name="liteon",
        executable=False,
        reason="pinned srsRAN values explicitly mark Liteon as a do-not-use placeholder",
    ),
    "jaguar": RadioProfile(
        name="jaguar",
        family="oai",
        values_file=None,
        resource_name="jaguar",
        executable=False,
        reason="pinned srsRAN adapter has no Jaguar profile; R2Lab documents Jaguar through OAI",
    ),
    "panther": RadioProfile(
        name="panther",
        family="oai",
        values_file=None,
        resource_name="panther",
        executable=False,
        reason="pinned srsRAN adapter has no Panther profile; R2Lab documents Panther through OAI",
    ),
}


def _qfit(name: str) -> UeProfile:
    return UeProfile(name, "qfit", "mbim", f"fit{name[-2:]}", "wwan0", True)


def _qhat(name: str, mode: UeMode) -> UeProfile:
    return UeProfile(name, "qhat", mode, name, "wwan0", True)


UES: Mapping[str, UeProfile] = {
    **{name: _qfit(name) for name in ("qfit07", "qfit09", "qfit18", "qfit29", "qfit32", "qfit34")},
    **{name: _qhat(name, "mbim") for name in ("qhat01", "qhat02", "qhat03", "qhat10", "qhat11")},
    **{name: _qhat(name, "qmi") for name in ("qhat20", "qhat21", "qhat22", "qhat23")},
    "rg530f-01": UeProfile(
        "rg530f-01",
        "fr2",
        "unknown",
        "pc03",
        None,
        False,
        "FR2 UE belongs to the Liteon path, whose pinned srsRAN profile is a placeholder",
    ),
    "rg530f-02": UeProfile(
        "rg530f-02",
        "fr2",
        "unknown",
        "pc04",
        None,
        False,
        "FR2 UE belongs to the Liteon path, whose pinned srsRAN profile is a placeholder",
    ),
    "phone1": UeProfile(
        "phone1",
        "phone",
        "adb",
        "macphone1",
        None,
        False,
        "the canonical SynthRAN data path requires a host network interface bound to the PDU session",
    ),
    "phone2": UeProfile(
        "phone2",
        "phone",
        "adb",
        "macphone2",
        None,
        False,
        "the canonical SynthRAN data path requires a host network interface bound to the PDU session",
    ),
}


@dataclass(frozen=True)
class PhysicalTopology:
    """One exact topology for a physical SynthRAN run."""

    core_node: str
    ran_node: str
    radio: str
    ue: str
    dnn: str = "internet"

    def validate(self, *, require_executable: bool = True) -> "PhysicalTopology":
        if self.core_node not in SUPPORTED_NODES:
            raise R2LabHardwareError(
                "unsupported core node; choose one of: " + ", ".join(sorted(SUPPORTED_NODES))
            )
        if self.ran_node not in SUPPORTED_NODES:
            raise R2LabHardwareError(
                "unsupported RAN node; choose one of: " + ", ".join(sorted(SUPPORTED_NODES))
            )
        if self.core_node == self.ran_node:
            raise R2LabHardwareError("physical core and RAN nodes must differ")
        try:
            radio = RADIOS[self.radio]
        except KeyError as exc:
            raise R2LabHardwareError(
                "unknown R2Lab radio; choose one of: " + ", ".join(sorted(RADIOS))
            ) from exc
        try:
            ue = UES[self.ue]
        except KeyError as exc:
            raise R2LabHardwareError(
                "unknown R2Lab UE; choose one of: " + ", ".join(sorted(UES))
            ) from exc
        if radio.fixed_ran_node is not None and self.ran_node != radio.fixed_ran_node:
            raise R2LabHardwareError(
                f"{radio.name} requires RAN node {radio.fixed_ran_node}"
            )
        if require_executable and not radio.executable:
            raise R2LabHardwareError(f"radio {radio.name} is not executable: {radio.reason}")
        if require_executable and not ue.executable:
            raise R2LabHardwareError(f"UE {ue.name} is not executable: {ue.reason}")
        if require_executable and radio.family != "n3xx":
            raise R2LabHardwareError(
                f"radio {radio.name} requires a different physical radio adapter ({radio.family})"
            )
        if require_executable and not ue.is_fr1_quectel:
            raise R2LabHardwareError("current canonical physical experiment requires an FR1 Quectel UE")
        if self.dnn != "internet":
            raise R2LabHardwareError("the pinned Open5GS physical profile requires DNN 'internet'")
        return self

    @property
    def radio_profile(self) -> RadioProfile:
        return RADIOS[self.radio]

    @property
    def ue_profile(self) -> UeProfile:
        return UES[self.ue]

    @property
    def nodes(self) -> tuple[str, str]:
        return self.core_node, self.ran_node

    def to_dict(self) -> dict[str, object]:
        self.validate(require_executable=False)
        return {
            "schema": "synthran/r2lab-topology/v1alpha1",
            "core_node": self.core_node,
            "ran_node": self.ran_node,
            "radio": self.radio_profile.to_dict(),
            "ue": self.ue_profile.to_dict(),
            "dnn": self.dnn,
        }


def topology_path(run_root: Path, run_id: str) -> Path:
    validate_run_id(run_id)
    return run_root.expanduser().resolve() / run_id / "topology.json"


def capabilities() -> dict[str, object]:
    return {
        "schema": "synthran/r2lab-capabilities/v1alpha1",
        "compute_nodes": sorted(SUPPORTED_NODES),
        "radios": {name: profile.to_dict() for name, profile in sorted(RADIOS.items())},
        "ues": {name: profile.to_dict() for name, profile in sorted(UES.items())},
        "canonical_executable_radios": sorted(
            name for name, profile in RADIOS.items() if profile.executable and profile.family == "n3xx"
        ),
        "canonical_executable_ues": sorted(name for name, profile in UES.items() if profile.executable),
    }
