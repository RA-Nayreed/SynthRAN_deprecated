"""Physical R2Lab deployment subsystem.

This module owns the complete physical gNB path as one coherent subsystem:
reviewed deployment intent, canonical srsRAN render, pinned chart overlay,
isolated workspace, offline Helm validation, deterministic packaging, stopped
cluster staging, and the non-overlapping singleton gNB lifecycle.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
import gzip
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import shlex
import tarfile
from typing import Callable, Mapping, Sequence

from synthran.dependencies import DependencyLock
from synthran.live_preflight import CommandResult, verify_allocations, verify_reservation
from synthran.network.runtime import validate_run_id
from synthran.r2lab.radio import (
    ReferenceAlignedPhysicalIntent,
    R2LabRadioProfileError,
    r2lab_oai_aligned_candidate,
)


# Reviewed topology / deployment contract.
PHYSICAL_DEPLOYMENT_SCHEMA = "synthran/r2lab-physical-deployment/v1alpha1"
CURRENT_CORE_NODE = "sopnode-f2"
CURRENT_RAN_NODE = "sopnode-f3"
CURRENT_RADIO = "n300"

# Canonical render placeholders.
AMF_ADDRESS_PLACEHOLDER = "<AMF_N2_ADDRESS>"
GNB_BIND_ADDRESS_PLACEHOLDER = "<GNB_N2_ADDRESS>"
N300_DEVICE_ARGS_PLACEHOLDER = "<N300_UHD_DEVICE_ARGS>"

# Pinned chart contract.
PINNED_SRSRAN_HELM_COMMIT = "8dfb9890d127734cdcd6eee9df8c5d09b1a8076a"
PHYSICAL_GNB_CONTAINER = "srsran_gnb_physical"
PHYSICAL_CHART_PATH = "charts/srsran-gnb"
PHYSICAL_DEPLOYMENT_TEMPLATE = f"{PHYSICAL_CHART_PATH}/templates/deployment.yaml"
VALUES_FILE_NAME = "synthran-physical-values.json"
PHYSICAL_GNB_CPU_COUNT = 8
PHYSICAL_GNB_MEMORY = "4Gi"

# Runtime Kubernetes contract.
CORE_NODE = CURRENT_CORE_NODE
RAN_NODE = CURRENT_RAN_NODE
NAMESPACE = "open5gs"
RELEASE = "srsran-gnb"
GNB_NAMESPACE = NAMESPACE
GNB_DEPLOYMENT = RELEASE
GNB_SELECTOR = "app=srsran,component=gnb"
POD_RUNTIME_STATE_KEY = "pha" + "se"
DEPLOYMENT_RUN_LABEL = "synthran.run/id"
DEPLOYMENT_RUN_ANNOTATION = "synthran.io/run-id"
DEPLOYMENT_PACKAGE_ANNOTATION = "synthran.io/package-sha256"
DEPLOYMENT_VALUES_ANNOTATION = "synthran.io/values-sha256"
DEPLOYMENT_RENDER_ANNOTATION = "synthran.io/render-sha256"
DEFAULT_COMMAND_TIMEOUT_SECONDS = 30
DEFAULT_POLL_ATTEMPTS = 40
DEFAULT_POLL_INTERVAL_SECONDS = 3.0
DEFAULT_UHD_RELEASE_SECONDS = 20.0
DEFAULT_STAGING_TIMEOUT_SECONDS = 120

_SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_SAFE_AUTHORITY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
Runner = Callable[[Sequence[str], int], CommandResult]
Sleeper = Callable[[float], None]
Configure = Callable[[], None]


class R2LabPhysicalDeploymentError(ValueError):
    """Raised when a physical deployment plan crosses the reviewed boundary."""


class R2LabPhysicalRenderError(ValueError):
    """Raised when the canonical physical render violates reviewed semantics."""


class R2LabPhysicalChartError(ValueError):
    """Raised when the pinned physical chart contract cannot be proven."""


class R2LabPhysicalHelmError(RuntimeError):
    """Raised when local physical Helm rendering cannot be proven safe."""


class R2LabPhysicalArtifactError(RuntimeError):
    """Raised when a deterministic physical chart artifact cannot be produced."""


class R2LabPhysicalStagingError(RuntimeError):
    """Raised when stopped physical chart staging cannot proceed safely."""


class R2LabGnbLifecycleError(RuntimeError):
    """Raised when singleton gNB ownership cannot be proven safe."""


class R2LabPhysicalStartError(RuntimeError):
    """Raised when a staged physical gNB cannot be started safely."""


@dataclass(frozen=True)
class R2LabPhysicalDeploymentPlan:
    run_id: str
    core_node: str
    ran_node: str
    radio: str
    radio_intent: ReferenceAlignedPhysicalIntent
    tx_gain_db: int = 25
    rx_gain_db: int = 35

    def validate(self) -> "R2LabPhysicalDeploymentPlan":
        try:
            validate_run_id(self.run_id)
        except Exception as exc:
            raise R2LabPhysicalDeploymentError(str(exc)) from exc
        if self.core_node != CURRENT_CORE_NODE:
            raise R2LabPhysicalDeploymentError(
                f"physical R2Lab deployment requires core node {CURRENT_CORE_NODE}"
            )
        if self.ran_node != CURRENT_RAN_NODE:
            raise R2LabPhysicalDeploymentError(
                f"physical R2Lab deployment requires RAN node {CURRENT_RAN_NODE}"
            )
        if self.core_node == self.ran_node:
            raise R2LabPhysicalDeploymentError("physical core and RAN nodes must differ")
        if self.radio != CURRENT_RADIO:
            raise R2LabPhysicalDeploymentError(
                f"physical R2Lab deployment requires radio {CURRENT_RADIO}"
            )
        if self.tx_gain_db < 0 or self.tx_gain_db > 30:
            raise R2LabPhysicalDeploymentError(
                "physical N300 TX gain must stay within the supported 0-30 dB range"
            )
        if self.rx_gain_db < 0 or self.rx_gain_db > 40:
            raise R2LabPhysicalDeploymentError(
                "physical N300 RX gain must stay within the supported 0-40 dB range"
            )
        try:
            self.radio_intent.validate()
        except R2LabRadioProfileError as exc:
            raise R2LabPhysicalDeploymentError(
                "physical radio intent is not reference aligned"
            ) from exc
        return self

    def to_dict(self) -> dict[str, object]:
        self.validate()
        return {
            "schema": PHYSICAL_DEPLOYMENT_SCHEMA,
            "execution_enabled": False,
            "acceptance": "offline-plan-only",
            "run_id": self.run_id,
            "backend": "r2lab",
            "core": "open5gs",
            "ran": "srsran",
            "radio": self.radio,
            "nodes": {"core": self.core_node, "ran": self.ran_node},
            "radio_intent": self.radio_intent.to_dict(),
            "gains_db": {"tx": self.tx_gain_db, "rx": self.rx_gain_db},
            "deployment": {
                "strategy": "Recreate",
                "desired_replicas": 1,
                "max_concurrent_gnb_pods": 1,
                "srsue_specific_overrides": False,
                "coreset0_index_override": None,
                "prach_config_index_override": None,
            },
            "required_lifecycle": [
                "scale exact srsran-gnb deployment to zero",
                "prove matching gNB pod count is zero",
                "allow UHD claim release",
                "apply reviewed physical configuration",
                "scale exact srsran-gnb deployment to one",
                "prove exactly one matching pod is Running and ready",
            ],
            "safety": {
                "automatic_r2lab_booking": False,
                "global_power_off": False,
                "rolling_overlap_allowed": False,
                "virtual_adapter_modified": False,
                "live_acceptance_claimed": False,
            },
        }

    def render(self, *, as_json: bool = False) -> str:
        payload = self.to_dict()
        if as_json:
            return json.dumps(payload, indent=2, sort_keys=True)
        carrier = payload["radio_intent"]["profile"]["carrier"]
        expected_ssb = payload["radio_intent"]["expected_ssb"]
        return "\n".join(
            (
                "SynthRAN physical R2Lab deployment plan (NON-EXECUTING)",
                f"Run ID: {self.run_id}",
                f"Path: Open5GS@{self.core_node} + srsRAN@{self.ran_node} + {self.radio}",
                f"Carrier: ARFCN {carrier['arfcn']} ({carrier['frequency_mhz']:.2f} MHz, carrier-center)",
                f"Expected SSB reference: ARFCN {expected_ssb['arfcn']} ({expected_ssb['frequency_mhz']:.2f} MHz)",
                "Radio intent: reference-aligned offline candidate; not live accepted",
                "Deployment strategy: Recreate / maximum one matching gNB pod",
                "srsUE-specific CORESET/PRACH overrides: disabled",
                "Execution: disabled until rendered physical values are reviewed",
            )
        )


def build_physical_deployment_plan(
    *,
    run_id: str,
    core_node: str = CURRENT_CORE_NODE,
    ran_node: str = CURRENT_RAN_NODE,
    radio: str = CURRENT_RADIO,
    radio_intent: ReferenceAlignedPhysicalIntent | None = None,
    tx_gain_db: int = 25,
    rx_gain_db: int = 35,
) -> R2LabPhysicalDeploymentPlan:
    return R2LabPhysicalDeploymentPlan(
        run_id=run_id,
        core_node=core_node,
        ran_node=ran_node,
        radio=radio,
        radio_intent=radio_intent or r2lab_oai_aligned_candidate(),
        tx_gain_db=tx_gain_db,
        rx_gain_db=rx_gain_db,
    ).validate()


@dataclass(frozen=True)
class PhysicalSrsranRender:
    run_id: str
    gnb_config: Mapping[str, object]
    deployment: Mapping[str, object]

    def validate(self) -> "PhysicalSrsranRender":
        ru_sdr = self.gnb_config.get("ru_sdr")
        cell_cfg = self.gnb_config.get("cell_cfg")
        cu_cp = self.gnb_config.get("cu_cp")
        remote_control = self.gnb_config.get("remote_control")
        if not isinstance(ru_sdr, dict) or not isinstance(cell_cfg, dict):
            raise R2LabPhysicalRenderError(
                "physical render is missing SDR or cell configuration"
            )
        if not isinstance(cu_cp, dict) or not isinstance(cu_cp.get("amf"), dict):
            raise R2LabPhysicalRenderError(
                "physical render is missing the pinned cu_cp AMF configuration"
            )
        if not isinstance(remote_control, dict) or remote_control.get("port") != 8001:
            raise R2LabPhysicalRenderError(
                "physical render must expose the pinned-chart remote control port"
            )
        amf = cu_cp["amf"]
        reviewed = r2lab_oai_aligned_candidate().profile
        if ru_sdr.get("device_driver") != "uhd":
            raise R2LabPhysicalRenderError("physical render must use the UHD radio driver")
        if "rfsim" in json.dumps(self.gnb_config).lower():
            raise R2LabPhysicalRenderError("physical render must not contain RFSIM settings")
        if cell_cfg.get("dl_arfcn") != reviewed.carrier.value:
            raise R2LabPhysicalRenderError(
                "physical render carrier does not match the reviewed R2Lab reference"
            )
        if cell_cfg.get("band") != reviewed.band:
            raise R2LabPhysicalRenderError(
                "physical render band does not match the reviewed R2Lab reference"
            )
        if cell_cfg.get("channel_bandwidth_MHz") != reviewed.channel_bandwidth_mhz:
            raise R2LabPhysicalRenderError(
                "physical render bandwidth does not match the reviewed R2Lab reference"
            )
        if cell_cfg.get("common_scs") != reviewed.common_scs_khz:
            raise R2LabPhysicalRenderError(
                "physical render SCS does not match the reviewed R2Lab reference"
            )
        if (
            cell_cfg.get("nof_antennas_dl") != reviewed.nof_antennas_dl
            or cell_cfg.get("nof_antennas_ul") != reviewed.nof_antennas_ul
        ):
            raise R2LabPhysicalRenderError(
                "physical render antenna count does not match the reviewed R2Lab reference"
            )
        if "pdcch" in cell_cfg or "prach" in cell_cfg:
            raise R2LabPhysicalRenderError(
                "physical render must not inherit srsUE-specific PDCCH/PRACH overrides"
            )
        if amf.get("addr") != AMF_ADDRESS_PLACEHOLDER:
            raise R2LabPhysicalRenderError(
                "AMF address must remain an explicit runtime placeholder"
            )
        if amf.get("bind_addr") != GNB_BIND_ADDRESS_PLACEHOLDER:
            raise R2LabPhysicalRenderError(
                "gNB N2 bind address must remain an explicit runtime placeholder"
            )
        if ru_sdr.get("device_args") != N300_DEVICE_ARGS_PLACEHOLDER:
            raise R2LabPhysicalRenderError(
                "N300 device arguments must remain an explicit runtime placeholder"
            )
        if self.deployment.get("replicas") != 0:
            raise R2LabPhysicalRenderError(
                "configuration render must keep the physical gNB stopped"
            )
        strategy = self.deployment.get("strategy")
        if not isinstance(strategy, dict) or strategy.get("type") != "Recreate":
            raise R2LabPhysicalRenderError("physical Deployment strategy must be Recreate")
        return self

    def to_dict(self) -> dict[str, object]:
        self.validate()
        return {
            "schema": "synthran/r2lab-physical-render/v1alpha1",
            "run_id": self.run_id,
            "execution_ready": False,
            "acceptance": "offline-render-only",
            "gnb_config": dict(self.gnb_config),
            "deployment": dict(self.deployment),
            "runtime_placeholders": [
                AMF_ADDRESS_PLACEHOLDER,
                GNB_BIND_ADDRESS_PLACEHOLDER,
                N300_DEVICE_ARGS_PLACEHOLDER,
            ],
        }

    def render_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)


def render_physical_srsran(plan: R2LabPhysicalDeploymentPlan) -> PhysicalSrsranRender:
    plan.validate()
    intent = plan.radio_intent.validate()
    profile = intent.profile
    gnb_config: dict[str, object] = {
        "cu_cp": {
            "inactivity_timer": 7200,
            "request_pdu_session_timeout": 30,
            "amf": {
                "sctp_rto_initial": 200,
                "sctp_rto_min": 200,
                "sctp_rto_max": 2000,
                "sctp_init_max_attempts": 5,
                "sctp_hb_interval": 1000,
                "sctp_assoc_max_retx": 5,
                "sctp_nodelay": True,
                "addr": AMF_ADDRESS_PLACEHOLDER,
                "port": 38412,
                "bind_addr": GNB_BIND_ADDRESS_PLACEHOLDER,
                "supported_tracking_areas": [
                    {
                        "tac": 1,
                        "plmn_list": [
                            {
                                "plmn": "00101",
                                "tai_slice_support_list": [{"sst": 1}],
                            }
                        ],
                    }
                ],
            },
        },
        "ru_sdr": {
            "device_driver": "uhd",
            "device_args": N300_DEVICE_ARGS_PLACEHOLDER,
            "srate": 61.44,
            "tx_gain": plan.tx_gain_db,
            "rx_gain": plan.rx_gain_db,
            "clock": "internal",
            "sync": "internal",
        },
        "cell_cfg": {
            "dl_arfcn": profile.carrier.value,
            "band": profile.band,
            "channel_bandwidth_MHz": profile.channel_bandwidth_mhz,
            "common_scs": profile.common_scs_khz,
            "plmn": "00101",
            "tac": 1,
            "nof_antennas_dl": profile.nof_antennas_dl,
            "nof_antennas_ul": profile.nof_antennas_ul,
            "slicing": [{"sst": 1}],
        },
        "log": {
            "filename": "/tmp/gnb.log",
            "all_level": "warning",
            "config_level": "debug",
        },
        "pcap": {
            "mac_enable": False,
            "mac_filename": "/tmp/gnb_mac.pcap",
            "ngap_enable": False,
            "ngap_filename": "/tmp/gnb_ngap.pcap",
        },
        "remote_control": {
            "bind_addr": "0.0.0.0",
            "enabled": True,
            "port": 8001,
        },
        "synthran_review": {
            "carrier_semantic": profile.carrier.semantic.value,
            "expected_ssb_arfcn": intent.expected_ssb.value,
            "reference_point_a_arfcn": intent.reference.point_a.value,
            "reference_carrier_prbs": intent.reference.carrier_prbs,
            "reference_scs_khz": intent.reference.subcarrier_spacing_khz,
            "reference_nominal_bandwidth_mhz": profile.channel_bandwidth_mhz,
            "reference_aligned": True,
            "live_accepted": False,
        },
    }
    deployment = {
        "replicas": 0,
        "strategy": {"type": "Recreate"},
        "selector": GNB_SELECTOR,
        "desired_replicas_after_lifecycle_start": 1,
    }
    return PhysicalSrsranRender(
        run_id=plan.run_id,
        gnb_config=gnb_config,
        deployment=deployment,
    ).validate()


@dataclass(frozen=True)
class PhysicalChartBindings:
    amf_n2_address: str
    gnb_n2_address: str
    n300_address: str
    ru_pod_address: str
    ru_subnet: str
    n3_network_name: str = "n3network"
    ru_master: str = "r2lab_usrp"
    node_name: str = "sopnode-f3"

    def validate(self) -> "PhysicalChartBindings":
        try:
            amf = ipaddress.ip_address(self.amf_n2_address)
            gnb = ipaddress.ip_address(self.gnb_n2_address)
            n300 = ipaddress.ip_address(self.n300_address)
            ru_pod = ipaddress.ip_address(self.ru_pod_address)
            ru_network = ipaddress.ip_network(self.ru_subnet, strict=False)
        except ValueError as exc:
            raise R2LabPhysicalChartError(
                "physical chart bindings must contain valid IP addresses and subnet"
            ) from exc
        if not all(isinstance(value, ipaddress.IPv4Address) for value in (amf, gnb, n300, ru_pod)):
            raise R2LabPhysicalChartError("physical R2Lab chart supports IPv4 only")
        if not isinstance(ru_network, ipaddress.IPv4Network):
            raise R2LabPhysicalChartError("current physical RU subnet must be IPv4")
        if amf == gnb:
            raise R2LabPhysicalChartError("AMF and gNB N2 addresses must differ")
        if n300 not in ru_network or ru_pod not in ru_network:
            raise R2LabPhysicalChartError(
                "N300 and RU pod addresses must belong to the reviewed RU subnet"
            )
        if n300 == ru_pod:
            raise R2LabPhysicalChartError("N300 and RU pod addresses must differ")
        for value, label in (
            (self.n3_network_name, "N3 network name"),
            (self.ru_master, "RU master"),
            (self.node_name, "node name"),
        ):
            if not _SAFE_NAME_RE.fullmatch(value):
                raise R2LabPhysicalChartError(f"{label} contains unsafe characters")
        if self.node_name != CURRENT_RAN_NODE:
            raise R2LabPhysicalChartError(
                f"physical R2Lab chart requires {CURRENT_RAN_NODE}"
            )
        if self.ru_master != "r2lab_usrp":
            raise R2LabPhysicalChartError(
                "physical R2Lab chart requires r2lab_usrp"
            )
        return self


@dataclass(frozen=True)
class PhysicalChartBundle:
    run_id: str
    chart_commit: str
    chart_path: str
    values: Mapping[str, object]
    review: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "synthran/r2lab-physical-chart/v1alpha1",
            "execution_enabled": False,
            "acceptance": "offline-chart-bundle-only",
            "run_id": self.run_id,
            "chart": {"commit": self.chart_commit, "path": self.chart_path},
            "values": deepcopy(dict(self.values)),
            "review": deepcopy(dict(self.review)),
        }

    def render_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)


def _locked_chart_commit(lock: DependencyLock) -> str:
    git = lock.raw.get("git")
    if not isinstance(git, dict):
        raise R2LabPhysicalChartError("dependency lock Git mapping is unavailable")
    entry = git.get("srsran_helm")
    if not isinstance(entry, dict):
        raise R2LabPhysicalChartError("dependency lock is missing srsran_helm")
    commit = entry.get("commit")
    if commit != PINNED_SRSRAN_HELM_COMMIT:
        raise R2LabPhysicalChartError(
            "physical chart adapter is reviewed only for the pinned srsran_helm commit"
        )
    return commit


def _physical_image(lock: DependencyLock) -> Mapping[str, str]:
    containers = lock.raw.get("containers")
    if not isinstance(containers, dict):
        raise R2LabPhysicalChartError("dependency lock container mapping is unavailable")
    entry = containers.get(PHYSICAL_GNB_CONTAINER)
    if not isinstance(entry, dict):
        raise R2LabPhysicalChartError("physical gNB container lock is missing")
    required = ("image", "tag", "digest", "platform")
    if any(not isinstance(entry.get(key), str) or not entry[key] for key in required):
        raise R2LabPhysicalChartError("physical gNB container lock is incomplete")
    if entry["platform"] != "linux/amd64":
        raise R2LabPhysicalChartError("physical gNB container must be linux/amd64")
    if not entry["digest"].startswith("sha256:"):
        raise R2LabPhysicalChartError("physical gNB container must use a sha256 digest")
    return {key: entry[key] for key in required}


def _reviewed_resource_values() -> dict[str, object]:
    quantity = {
        "cpu": str(PHYSICAL_GNB_CPU_COUNT),
        "memory": PHYSICAL_GNB_MEMORY,
    }
    return {
        "define": True,
        "requests": {"tcpdump": dict(quantity)},
        "limits": {"tcpdump": dict(quantity)},
    }


def _expected_resource_contract(bundle: PhysicalChartBundle) -> tuple[str, str]:
    resources = bundle.values.get("resources")
    if not isinstance(resources, dict) or resources.get("define") is not True:
        raise R2LabPhysicalHelmError("physical gNB resource contract must be enabled")

    requests = resources.get("requests")
    limits = resources.get("limits")
    request = requests.get("tcpdump") if isinstance(requests, dict) else None
    limit = limits.get("tcpdump") if isinstance(limits, dict) else None
    if not isinstance(request, dict) or not isinstance(limit, dict):
        raise R2LabPhysicalHelmError(
            "physical gNB resource contract does not match the pinned chart"
        )

    expected_cpu = str(PHYSICAL_GNB_CPU_COUNT)
    expected_memory = PHYSICAL_GNB_MEMORY
    if (
        request.get("cpu") != expected_cpu
        or limit.get("cpu") != expected_cpu
        or request.get("memory") != expected_memory
        or limit.get("memory") != expected_memory
    ):
        raise R2LabPhysicalHelmError(
            "physical gNB resource requests and limits do not match"
        )
    return expected_cpu, expected_memory


def build_physical_chart_bundle(
    *,
    lock: DependencyLock,
    plan: R2LabPhysicalDeploymentPlan,
    bindings: PhysicalChartBindings,
) -> PhysicalChartBundle:
    plan.validate()
    bindings.validate()
    chart_commit = _locked_chart_commit(lock)
    image = _physical_image(lock)
    rendered = render_physical_srsran(plan).to_dict()
    gnb_config = deepcopy(rendered["gnb_config"])
    if not isinstance(gnb_config, dict):
        raise R2LabPhysicalChartError("canonical gNB render is malformed")
    review = gnb_config.pop("synthran_review", None)
    if not isinstance(review, dict):
        raise R2LabPhysicalChartError("canonical gNB review metadata is missing")
    cu_cp = gnb_config.get("cu_cp")
    ru_sdr = gnb_config.get("ru_sdr")
    if not isinstance(cu_cp, dict) or not isinstance(cu_cp.get("amf"), dict):
        raise R2LabPhysicalChartError("canonical AMF configuration is malformed")
    if not isinstance(ru_sdr, dict):
        raise R2LabPhysicalChartError("canonical SDR configuration is malformed")
    amf = cu_cp["amf"]
    amf["addr"] = bindings.amf_n2_address
    amf["bind_addr"] = bindings.gnb_n2_address
    ru_sdr["device_args"] = f"addr={bindings.n300_address},type=n3xx"

    values: dict[str, object] = {
        "image": {
            "repository": image["image"],
            "tag": image["tag"],
            "digest": image["digest"],
            "pullPolicy": "IfNotPresent",
        },
        "replicas": 0,
        "deploymentStrategy": "Recreate",
        "resources": _reviewed_resource_values(),
        "start": {"gnb": True, "logs": False},
        "gnbIp": bindings.gnb_n2_address,
        "gnbConfig": gnb_config,
        "n3networkName": bindings.n3_network_name,
        "ru": bindings.ru_master,
        "ruPodIp": bindings.ru_pod_address,
        "usrp": {
            "cniVersion": "0.3.1",
            "type": "macvlan",
            "master": bindings.ru_master,
            "mode": "bridge",
            "mtu": 9216,
            "ipam": {"type": "host-local", "subnet": bindings.ru_subnet},
        },
        "nodeName": bindings.node_name,
        "sriov": {"enabled": False},
    }
    serialized = json.dumps(values, sort_keys=True).lower()
    if "rfsim" in serialized or "all-off" in serialized:
        raise R2LabPhysicalChartError("physical chart bundle contains forbidden backend behavior")
    cell_cfg = gnb_config.get("cell_cfg")
    if not isinstance(cell_cfg, dict):
        raise R2LabPhysicalChartError("canonical cell configuration is malformed")
    if "pdcch" in cell_cfg or "prach" in cell_cfg:
        raise R2LabPhysicalChartError(
            "physical chart bundle inherited srsUE-specific radio overrides"
        )
    return PhysicalChartBundle(
        run_id=plan.run_id,
        chart_commit=chart_commit,
        chart_path=PHYSICAL_CHART_PATH,
        values=values,
        review={
            **review,
            "image_digest_locked": True,
            "singleton_deployment": True,
            "logs_sidecar_disabled": True,
            "guaranteed_qos_requested": True,
            "exclusive_cpu_manager_eligible": True,
            "exclusive_cpu_count": PHYSICAL_GNB_CPU_COUNT,
            "memory_request_limit": PHYSICAL_GNB_MEMORY,
            "resource_contract": "whole-cpu-request-limit",
            "live_accepted": False,
        },
    )


def overlay_pinned_deployment_template(*, source: str, lock: DependencyLock) -> str:
    _locked_chart_commit(lock)
    anchors = {
        "spec:\n  selector:\n": (
            "spec:\n"
            "  strategy:\n"
            "    type: {{ .Values.deploymentStrategy }}\n"
            "  selector:\n"
        ),
        "  replicas: 1\n": "  replicas: {{ .Values.replicas }}\n",
        '          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"\n': (
            '          image: "{{ .Values.image.repository }}:'
            '{{ .Values.image.tag }}@{{ .Values.image.digest }}"\n'
        ),
    }
    result = source
    for anchor, replacement in anchors.items():
        count = result.count(anchor)
        if count != 1:
            raise R2LabPhysicalChartError(
                "pinned srsRAN Deployment template no longer matches the reviewed overlay contract"
            )
        result = result.replace(anchor, replacement, 1)
    if "  replicas: 1\n" in result:
        raise R2LabPhysicalChartError("hard-coded gNB replica count survived the overlay")
    if "@{{ .Values.image.digest }}" not in result:
        raise R2LabPhysicalChartError("digest-locked image rendering was not installed")
    if "type: {{ .Values.deploymentStrategy }}" not in result:
        raise R2LabPhysicalChartError("singleton Deployment strategy was not installed")
    return result


@dataclass(frozen=True)
class PhysicalChartWorkspace:
    chart_root: Path
    deployment_template: Path
    values_file: Path
    source_template_sha256: str
    overlaid_template_sha256: str
    values_sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "chart_root": PHYSICAL_CHART_PATH,
            "deployment_template": "templates/deployment.yaml",
            "values_file": VALUES_FILE_NAME,
            "source_template_sha256": self.source_template_sha256,
            "overlaid_template_sha256": self.overlaid_template_sha256,
            "values_sha256": self.values_sha256,
        }


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def materialize_physical_chart_workspace(
    *,
    checkout_root: Path,
    lock: DependencyLock,
    bundle: PhysicalChartBundle,
) -> PhysicalChartWorkspace:
    chart_root = checkout_root / PHYSICAL_CHART_PATH
    template_path = checkout_root / PHYSICAL_DEPLOYMENT_TEMPLATE
    chart_metadata = chart_root / "Chart.yaml"
    values_path = chart_root / VALUES_FILE_NAME
    if not chart_root.is_dir() or not chart_metadata.is_file() or not template_path.is_file():
        raise R2LabPhysicalChartError(
            "isolated srsran_helm checkout is missing the reviewed chart structure"
        )
    if values_path.exists():
        raise R2LabPhysicalChartError(
            "physical chart workspace already contains generated SynthRAN values"
        )
    try:
        source = template_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise R2LabPhysicalChartError(
            "unable to read the pinned physical Deployment template"
        ) from exc
    overlaid = overlay_pinned_deployment_template(source=source, lock=lock)
    values_text = json.dumps(bundle.values, indent=2, sort_keys=True) + "\n"
    try:
        template_path.write_text(overlaid, encoding="utf-8", newline="\n")
        values_path.write_text(values_text, encoding="utf-8", newline="\n")
    except OSError as exc:
        raise R2LabPhysicalChartError(
            "unable to materialize the physical chart workspace"
        ) from exc
    return PhysicalChartWorkspace(
        chart_root=chart_root,
        deployment_template=template_path,
        values_file=values_path,
        source_template_sha256=_sha256_text(source),
        overlaid_template_sha256=_sha256_text(overlaid),
        values_sha256=_sha256_text(values_text),
    )


@dataclass(frozen=True)
class PhysicalHelmRenderEvidence:
    sha256: str
    replicas: int
    strategy: str
    image_reference: str
    carrier_arfcn: int
    channel_bandwidth_mhz: int
    antennas_dl: int
    antennas_ul: int

    def to_dict(self) -> dict[str, object]:
        return {
            "sha256": self.sha256,
            "replicas": self.replicas,
            "strategy": self.strategy,
            "image_reference": self.image_reference,
            "carrier_arfcn": self.carrier_arfcn,
            "channel_bandwidth_mhz": self.channel_bandwidth_mhz,
            "antennas_dl": self.antennas_dl,
            "antennas_ul": self.antennas_ul,
            "acceptance": "offline-render-validated",
        }

    @classmethod
    def from_dict(
        cls, payload: Mapping[str, object]
    ) -> "PhysicalHelmRenderEvidence":
        if payload.get("acceptance") != "offline-render-validated":
            raise R2LabPhysicalHelmError(
                "stored physical render evidence has an invalid acceptance state"
            )
        integer_fields = (
            "replicas",
            "carrier_arfcn",
            "channel_bandwidth_mhz",
            "antennas_dl",
            "antennas_ul",
        )
        if any(
            not isinstance(payload.get(field), int)
            or isinstance(payload.get(field), bool)
            for field in integer_fields
        ):
            raise R2LabPhysicalHelmError(
                "stored physical render evidence contains malformed numeric values"
            )
        if any(
            not isinstance(payload.get(field), str) or not payload.get(field)
            for field in ("sha256", "strategy", "image_reference")
        ):
            raise R2LabPhysicalHelmError(
                "stored physical render evidence contains malformed text values"
            )
        evidence = cls(
            sha256=str(payload.get("sha256", "")),
            replicas=payload.get("replicas", -1),
            strategy=str(payload.get("strategy", "")),
            image_reference=str(payload.get("image_reference", "")),
            carrier_arfcn=payload.get("carrier_arfcn", -1),
            channel_bandwidth_mhz=payload.get("channel_bandwidth_mhz", -1),
            antennas_dl=payload.get("antennas_dl", -1),
            antennas_ul=payload.get("antennas_ul", -1),
        )
        if evidence.to_dict() != dict(payload):
            raise R2LabPhysicalHelmError(
                "stored physical render evidence is malformed"
            )
        _validate_sha256_digest(
            evidence.sha256, "render digest", R2LabPhysicalHelmError
        )
        return evidence


def _locked_helm_version(lock: DependencyLock, error_type: type[RuntimeError]) -> str:
    tools = lock.raw.get("tools")
    entry = tools.get("helm_linux_amd64") if isinstance(tools, dict) else None
    version = entry.get("version") if isinstance(entry, dict) else None
    if not isinstance(version, str) or not version:
        raise error_type("dependency lock does not define the Helm version")
    return version


def _expected_image(bundle: PhysicalChartBundle) -> str:
    image = bundle.values.get("image")
    if not isinstance(image, dict):
        raise R2LabPhysicalHelmError("physical chart bundle image metadata is missing")
    repository = image.get("repository")
    tag = image.get("tag")
    digest = image.get("digest")
    if not all(isinstance(value, str) and value for value in (repository, tag, digest)):
        raise R2LabPhysicalHelmError("physical chart bundle image metadata is incomplete")
    return f"{repository}:{tag}@{digest}"


def _integer_after(text: str, key: str) -> int:
    matches = re.findall(rf"(?m)^\s*{re.escape(key)}:\s*([0-9]+)\s*$", text)
    if len(matches) != 1:
        raise R2LabPhysicalHelmError(
            f"rendered physical chart must contain exactly one {key} value"
        )
    return int(matches[0])


def validate_physical_helm_render(
    *, text: str, bundle: PhysicalChartBundle
) -> PhysicalHelmRenderEvidence:
    if not text.strip():
        raise R2LabPhysicalHelmError("Helm rendered no physical chart output")
    expected_image = _expected_image(bundle)
    if text.count(expected_image) != 1:
        raise R2LabPhysicalHelmError(
            "rendered physical chart must contain exactly one digest-locked gNB image"
        )
    if not re.search(r"(?m)^kind:\s*Deployment\s*$", text):
        raise R2LabPhysicalHelmError("rendered physical chart is missing the gNB Deployment")
    replicas = _integer_after(text, "replicas")
    if replicas != 0:
        raise R2LabPhysicalHelmError("rendered physical gNB must remain stopped")
    strategy_matches = re.findall(
        r"(?ms)^\s*strategy:\s*\n\s*type:\s*([A-Za-z]+)\s*$", text
    )
    if strategy_matches != ["Recreate"]:
        raise R2LabPhysicalHelmError(
            "rendered physical gNB must use exactly one Recreate strategy"
        )
    carrier = _integer_after(text, "dl_arfcn")
    bandwidth = _integer_after(text, "channel_bandwidth_MHz")
    antennas_dl = _integer_after(text, "nof_antennas_dl")
    antennas_ul = _integer_after(text, "nof_antennas_ul")
    gnb_config = bundle.values.get("gnbConfig")
    cell_cfg = gnb_config.get("cell_cfg") if isinstance(gnb_config, dict) else None
    if not isinstance(cell_cfg, dict):
        raise R2LabPhysicalHelmError("physical chart bundle cell intent is missing")
    expected_carrier = cell_cfg.get("dl_arfcn")
    expected_bandwidth = cell_cfg.get("channel_bandwidth_MHz")
    expected_antennas_dl = cell_cfg.get("nof_antennas_dl")
    expected_antennas_ul = cell_cfg.get("nof_antennas_ul")
    if (
        carrier != expected_carrier
        or bandwidth != expected_bandwidth
        or antennas_dl != expected_antennas_dl
        or antennas_ul != expected_antennas_ul
    ):
        raise R2LabPhysicalHelmError(
            "rendered physical radio values do not match the reviewed chart intent"
        )
    lowered = text.lower()
    if "coreset0_index" in lowered or "prach_config_index" in lowered:
        raise R2LabPhysicalHelmError(
            "rendered physical chart inherited srsUE-specific radio overrides"
        )
    if re.search(r"(?m)^\s*image:\s*busybox(?::|\s|$)", text):
        raise R2LabPhysicalHelmError(
            "rendered physical chart contains the unpinned optional log sidecar"
        )
    if "rfsim" in lowered or "all-off" in lowered:
        raise R2LabPhysicalHelmError(
            "rendered physical chart contains forbidden backend behavior"
        )
    expected_cpu, expected_memory = _expected_resource_contract(bundle)
    resource_pattern = re.compile(
        r"(?ms)^\s*resources:\s*\n"
        r"\s*requests:\s*\n"
        r"\s*memory:\s*[\"']?([^\"'\s]+)[\"']?\s*\n"
        r"\s*cpu:\s*[\"']?([^\"'\s]+)[\"']?\s*\n"
        r"\s*limits:\s*\n"
        r"\s*memory:\s*[\"']?([^\"'\s]+)[\"']?\s*\n"
        r"\s*cpu:\s*[\"']?([^\"'\s]+)[\"']?\s*$"
    )
    resource_matches = resource_pattern.findall(text)
    if len(resource_matches) != 1:
        raise R2LabPhysicalHelmError(
            "rendered physical gNB must contain exactly one reviewed resource block"
        )
    request_memory, request_cpu, limit_memory, limit_cpu = resource_matches[0]
    if (
        request_memory != expected_memory
        or request_cpu != expected_cpu
        or limit_memory != expected_memory
        or limit_cpu != expected_cpu
    ):
        raise R2LabPhysicalHelmError(
            "rendered physical gNB resource requests and limits do not match"
        )
    return PhysicalHelmRenderEvidence(
        sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        replicas=replicas,
        strategy="Recreate",
        image_reference=expected_image,
        carrier_arfcn=carrier,
        channel_bandwidth_mhz=bandwidth,
        antennas_dl=antennas_dl,
        antennas_ul=antennas_ul,
    )


def render_physical_chart_offline(
    *,
    lock: DependencyLock,
    bundle: PhysicalChartBundle,
    workspace: PhysicalChartWorkspace,
    runner: Runner,
    timeout_seconds: int = 60,
) -> tuple[str, PhysicalHelmRenderEvidence]:
    if timeout_seconds < 1 or timeout_seconds > 300:
        raise R2LabPhysicalHelmError("offline Helm timeout must be between 1 and 300 seconds")
    expected_version = _locked_helm_version(lock, R2LabPhysicalHelmError)
    try:
        version_result = runner(("helm", "version", "--short"), timeout_seconds)
    except Exception as exc:
        raise R2LabPhysicalHelmError("locked Helm executable could not be inspected") from exc
    if version_result.returncode != 0:
        raise R2LabPhysicalHelmError("Helm version probe returned nonzero")
    match = re.search(r"v?([0-9]+\.[0-9]+\.[0-9]+)", version_result.stdout)
    if match is None or match.group(1) != expected_version:
        raise R2LabPhysicalHelmError(
            f"Helm must exactly match locked version {expected_version}"
        )
    command = (
        "helm",
        "template",
        RELEASE,
        str(workspace.chart_root),
        "--namespace",
        NAMESPACE,
        "--values",
        str(workspace.values_file),
    )
    try:
        result = runner(command, timeout_seconds)
    except Exception as exc:
        raise R2LabPhysicalHelmError("offline Helm template command failed") from exc
    if result.returncode != 0:
        raise R2LabPhysicalHelmError("offline Helm template command returned nonzero")
    evidence = validate_physical_helm_render(text=result.stdout, bundle=bundle)
    return result.stdout, evidence


@dataclass(frozen=True)
class PhysicalChartArtifact:
    run_id: str
    package_path: Path
    values_path: Path
    package_sha256: str
    values_sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "run_id": self.run_id,
            "package_file": self.package_path.name,
            "package_sha256": self.package_sha256,
            "values_file": self.values_path.name,
            "values_sha256": self.values_sha256,
            "acceptance": "offline-packaged-only",
        }


def _artifact_sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as exc:
        raise R2LabPhysicalArtifactError("unable to hash physical chart artifact") from exc
    return digest.hexdigest()


def package_physical_chart(
    *, workspace: PhysicalChartWorkspace, run_id: str, destination: Path
) -> PhysicalChartArtifact:
    try:
        validated_run_id = validate_run_id(run_id)
    except Exception as exc:
        raise R2LabPhysicalArtifactError(str(exc)) from exc
    chart_root = workspace.chart_root
    if not chart_root.is_dir() or not workspace.values_file.is_file():
        raise R2LabPhysicalArtifactError("physical chart workspace is incomplete")
    try:
        files = sorted(
            path
            for path in chart_root.rglob("*")
            if path.is_file() or path.is_symlink()
        )
    except OSError as exc:
        raise R2LabPhysicalArtifactError("unable to enumerate physical chart workspace") from exc
    if not files:
        raise R2LabPhysicalArtifactError("physical chart workspace contains no files")
    if any(path.is_symlink() for path in files):
        raise R2LabPhysicalArtifactError(
            "physical chart workspace must not contain symbolic links"
        )
    destination = destination.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    package_path = destination / f"srsran-gnb-{validated_run_id}.tgz"
    values_path = destination / VALUES_FILE_NAME
    if package_path.exists() or values_path.exists():
        raise R2LabPhysicalArtifactError("physical chart artifact already exists")
    try:
        with package_path.open("wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
                with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
                    for path in files:
                        relative = path.relative_to(chart_root)
                        arcname = Path("srsran-gnb") / relative
                        info = archive.gettarinfo(str(path), arcname=arcname.as_posix())
                        info.uid = 0
                        info.gid = 0
                        info.uname = ""
                        info.gname = ""
                        info.mtime = 0
                        info.mode = 0o644
                        with path.open("rb") as stream:
                            archive.addfile(info, stream)
        values_path.write_bytes(workspace.values_file.read_bytes())
    except OSError as exc:
        package_path.unlink(missing_ok=True)
        values_path.unlink(missing_ok=True)
        raise R2LabPhysicalArtifactError("unable to package physical chart workspace") from exc
    values_sha256 = _artifact_sha256_file(values_path)
    if values_sha256 != workspace.values_sha256:
        package_path.unlink(missing_ok=True)
        values_path.unlink(missing_ok=True)
        raise R2LabPhysicalArtifactError(
            "copied physical chart values do not match the reviewed workspace digest"
        )
    return PhysicalChartArtifact(
        run_id=validated_run_id,
        package_path=package_path,
        values_path=values_path,
        package_sha256=_artifact_sha256_file(package_path),
        values_sha256=values_sha256,
    )


def _validate_sha256_digest(value: str, label: str, error_type: type[RuntimeError]) -> str:
    if not _SHA256_RE.fullmatch(value):
        raise error_type(f"{label} must be a lowercase SHA-256 digest")
    return value


@dataclass(frozen=True)
class PhysicalStartAuthority:
    """Sanitized R2Lab authority bound to one active run claim and powered N300."""

    run_id: str
    radio: str
    ue: str
    ue_kind: str
    claim_sha256: str
    lease_verified: bool
    radio_state: str

    def validate(self) -> "PhysicalStartAuthority":
        try:
            validated = validate_run_id(self.run_id)
        except Exception as exc:
            raise R2LabPhysicalStartError(str(exc)) from exc
        if validated != self.run_id:
            raise R2LabPhysicalStartError("physical start authority run ID is not canonical")
        if self.radio != CURRENT_RADIO:
            raise R2LabPhysicalStartError(
                f"current physical start boundary requires radio {CURRENT_RADIO}"
            )
        if not _SAFE_NAME_RE.fullmatch(self.ue):
            raise R2LabPhysicalStartError("physical start authority UE is malformed")
        if self.ue_kind != "qfit":
            raise R2LabPhysicalStartError(
                "current physical start boundary requires a qfit UE selection"
            )
        _validate_sha256_digest(self.claim_sha256, "claim digest", R2LabPhysicalStartError)
        if self.lease_verified is not True:
            raise R2LabPhysicalStartError("R2Lab lease authority was not verified")
        if self.radio_state != "on":
            raise R2LabPhysicalStartError("selected N300 is not proven on")
        return self

    def to_dict(self) -> dict[str, object]:
        self.validate()
        return {
            "run_id": self.run_id,
            "radio": self.radio,
            "ue": self.ue,
            "ue_kind": self.ue_kind,
            "claim_sha256": self.claim_sha256,
            "lease_verified": True,
            "radio_state": self.radio_state,
            "status": "authorized-for-singleton-start",
        }


@dataclass(frozen=True)
class PhysicalStagingResult:
    run_id: str
    package_sha256: str
    values_sha256: str
    render_sha256: str
    namespace_owned: bool
    desired_replicas: int
    gnb_pod_count: int
    deployment_bound: bool = True

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "package_sha256": self.package_sha256,
            "values_sha256": self.values_sha256,
            "render_sha256": self.render_sha256,
            "namespace_owned": self.namespace_owned,
            "desired_replicas": self.desired_replicas,
            "gnb_pod_count": self.gnb_pod_count,
            "deployment_bound": self.deployment_bound,
            "status": "staged-stopped",
            "hardware_mutation": False,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "PhysicalStagingResult":
        if payload.get("status") != "staged-stopped":
            raise R2LabPhysicalStagingError(
                "stored physical staging result has an invalid status"
            )
        if (
            payload.get("hardware_mutation") is not False
            or payload.get("namespace_owned") is not True
            or payload.get("deployment_bound") is not True
            or payload.get("desired_replicas") != 0
            or isinstance(payload.get("desired_replicas"), bool)
            or payload.get("gnb_pod_count") != 0
            or isinstance(payload.get("gnb_pod_count"), bool)
        ):
            raise R2LabPhysicalStagingError(
                "stored physical staging result is not stopped and ownership-bound"
            )
        result = cls(
            run_id=str(payload.get("run_id", "")),
            package_sha256=str(payload.get("package_sha256", "")),
            values_sha256=str(payload.get("values_sha256", "")),
            render_sha256=str(payload.get("render_sha256", "")),
            namespace_owned=payload.get("namespace_owned") is True,
            desired_replicas=payload.get("desired_replicas", -1),
            gnb_pod_count=payload.get("gnb_pod_count", -1),
            deployment_bound=payload.get("deployment_bound") is True,
        )
        if result.to_dict() != dict(payload):
            raise R2LabPhysicalStagingError(
                "stored physical staging result is malformed"
            )
        try:
            validated_run_id = validate_run_id(result.run_id)
        except Exception as exc:
            raise R2LabPhysicalStagingError(str(exc)) from exc
        if validated_run_id != result.run_id:
            raise R2LabPhysicalStagingError(
                "stored physical staging run ID is not canonical"
            )
        for value, label in (
            (result.package_sha256, "package digest"),
            (result.values_sha256, "values digest"),
            (result.render_sha256, "render digest"),
        ):
            _validate_sha256_digest(value, label, R2LabPhysicalStagingError)
        if (
            not result.namespace_owned
            or not result.deployment_bound
            or result.desired_replicas != 0
            or result.gnb_pod_count != 0
        ):
            raise R2LabPhysicalStagingError(
                "stored physical staging result is not stopped and ownership-bound"
            )
        return result


@dataclass(frozen=True)
class PhysicalGnbStartResult:
    run_id: str
    package_sha256: str
    values_sha256: str
    render_sha256: str
    claim_sha256: str
    maximum_observed_pods: int

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "package_sha256": self.package_sha256,
            "values_sha256": self.values_sha256,
            "render_sha256": self.render_sha256,
            "claim_sha256": self.claim_sha256,
            "maximum_observed_pods": self.maximum_observed_pods,
            "started_exactly_one": True,
            "status": "gnb-started",
            "hardware_mutation": True,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "PhysicalGnbStartResult":
        if payload.get("status") != "gnb-started":
            raise R2LabPhysicalStartError(
                "stored physical gNB start result has an invalid status"
            )
        maximum = payload.get("maximum_observed_pods")
        if (
            payload.get("hardware_mutation") is not True
            or payload.get("started_exactly_one") is not True
            or not isinstance(maximum, int)
            or isinstance(maximum, bool)
            or maximum != 1
        ):
            raise R2LabPhysicalStartError(
                "stored physical gNB start result does not prove singleton ownership"
            )
        result = cls(
            run_id=str(payload.get("run_id", "")),
            package_sha256=str(payload.get("package_sha256", "")),
            values_sha256=str(payload.get("values_sha256", "")),
            render_sha256=str(payload.get("render_sha256", "")),
            claim_sha256=str(payload.get("claim_sha256", "")),
            maximum_observed_pods=payload.get("maximum_observed_pods", -1),
        )
        if result.to_dict() != dict(payload):
            raise R2LabPhysicalStartError(
                "stored physical gNB start result is malformed"
            )
        try:
            validated_run_id = validate_run_id(result.run_id)
        except Exception as exc:
            raise R2LabPhysicalStartError(str(exc)) from exc
        if validated_run_id != result.run_id:
            raise R2LabPhysicalStartError(
                "stored physical gNB start run ID is not canonical"
            )
        for value, label in (
            (result.package_sha256, "package digest"),
            (result.values_sha256, "values digest"),
            (result.render_sha256, "render digest"),
            (result.claim_sha256, "claim digest"),
        ):
            _validate_sha256_digest(value, label, R2LabPhysicalStartError)
        if result.maximum_observed_pods != 1:
            raise R2LabPhysicalStartError(
                "stored physical gNB start result does not prove singleton ownership"
            )
        return result


@dataclass(frozen=True)
class PhysicalGnbStopResult:
    run_id: str
    desired_replicas: int
    gnb_pod_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "desired_replicas": self.desired_replicas,
            "gnb_pod_count": self.gnb_pod_count,
            "status": "gnb-stopped",
            "hardware_mutation": True,
        }


def _validate_authority(value: str, label: str) -> str:
    if not _SAFE_AUTHORITY_RE.fullmatch(value):
        raise R2LabPhysicalStagingError(f"{label} contains unsafe characters")
    return value


def _staging_sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as exc:
        raise R2LabPhysicalStagingError("unable to hash physical staging artifact") from exc
    return digest.hexdigest()


def _strict_ssh_base(known_hosts: Path) -> tuple[str, ...]:
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
        f"root@{CORE_NODE}",
    )


def _strict_scp_base(known_hosts: Path) -> tuple[str, ...]:
    return (
        "scp",
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
    )


def _ssh(known_hosts: Path, *remote: str) -> tuple[str, ...]:
    return (*_strict_ssh_base(known_hosts), shlex.join(remote))


def _checked(
    runner: Runner, command: Sequence[str], timeout_seconds: int, label: str
) -> CommandResult:
    try:
        result = runner(command, timeout_seconds)
    except Exception as exc:
        raise R2LabPhysicalStagingError(f"{label} could not be observed") from exc
    if result.returncode != 0:
        raise R2LabPhysicalStagingError(f"{label} returned nonzero")
    return result


def _parse_pods(text: str) -> int:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise R2LabPhysicalStagingError("gNB pod query did not return JSON") from exc
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise R2LabPhysicalStagingError("gNB pod query returned malformed JSON")
    return len(items)


def discover_physical_chart_bindings(
    *,
    known_hosts: Path,
    runner: Runner,
    timeout_seconds: int = 60,
) -> PhysicalChartBindings:
    """Read the stopped Helm release's accepted network bindings."""

    if timeout_seconds < 5 or timeout_seconds > 300:
        raise R2LabPhysicalChartError(
            "physical chart discovery timeout must be between 5 and 300 seconds"
        )
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabPhysicalChartError("strict SLICES known-hosts file is missing")
    try:
        result = runner(
            _ssh(
                known_hosts,
                "helm",
                "get",
                "values",
                RELEASE,
                "--namespace",
                NAMESPACE,
                "--output",
                "json",
            ),
            timeout_seconds,
        )
    except Exception as exc:
        raise R2LabPhysicalChartError(
            "stopped physical Helm values could not be observed"
        ) from exc
    if result.returncode != 0:
        raise R2LabPhysicalChartError(
            "stopped physical Helm values query returned nonzero"
        )
    try:
        values = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise R2LabPhysicalChartError(
            "stopped physical Helm values are not JSON"
        ) from exc
    if not isinstance(values, dict):
        raise R2LabPhysicalChartError(
            "stopped physical Helm values are malformed"
        )
    gnb_config = values.get("gnbConfig")
    cu_cp = gnb_config.get("cu_cp") if isinstance(gnb_config, dict) else None
    amf = cu_cp.get("amf") if isinstance(cu_cp, dict) else None
    ru_sdr = gnb_config.get("ru_sdr") if isinstance(gnb_config, dict) else None
    usrp = values.get("usrp")
    ipam = usrp.get("ipam") if isinstance(usrp, dict) else None
    if not all(isinstance(item, dict) for item in (amf, ru_sdr, usrp, ipam)):
        raise R2LabPhysicalChartError(
            "stopped physical Helm values do not contain complete network bindings"
        )
    device_args = ru_sdr.get("device_args")
    match = (
        re.fullmatch(r"addr=([^,]+),type=n3xx", device_args)
        if isinstance(device_args, str)
        else None
    )
    if match is None:
        raise R2LabPhysicalChartError(
            "stopped physical Helm values do not contain reviewed N300 device arguments"
        )
    gnb_address = values.get("gnbIp")
    if gnb_address != amf.get("bind_addr"):
        raise R2LabPhysicalChartError(
            "stopped physical Helm values disagree on the gNB N2 address"
        )
    try:
        raw_bindings = (
            amf["addr"],
            gnb_address,
            match.group(1),
            values["ruPodIp"],
            ipam["subnet"],
            values.get("n3networkName"),
            values.get("ru"),
            values.get("nodeName"),
        )
    except (KeyError, TypeError) as exc:
        raise R2LabPhysicalChartError(
            "stopped physical Helm values are missing a network binding"
        ) from exc
    if not all(isinstance(value, str) and value for value in raw_bindings):
        raise R2LabPhysicalChartError(
            "stopped physical Helm values contain malformed network bindings"
        )
    bindings = PhysicalChartBindings(
        amf_n2_address=raw_bindings[0],
        gnb_n2_address=raw_bindings[1],
        n300_address=raw_bindings[2],
        ru_pod_address=raw_bindings[3],
        ru_subnet=raw_bindings[4],
        n3_network_name=raw_bindings[5],
        ru_master=raw_bindings[6],
        node_name=raw_bindings[7],
    )
    return bindings.validate()


def _deployment_binding_values(
    *,
    run_id: str,
    package_sha256: str,
    values_sha256: str,
    render_sha256: str,
) -> dict[str, str]:
    try:
        validated = validate_run_id(run_id)
    except Exception as exc:
        raise R2LabPhysicalStagingError(str(exc)) from exc
    if validated != run_id:
        raise R2LabPhysicalStagingError("physical staging run ID is not canonical")
    for value, label in (
        (package_sha256, "package digest"),
        (values_sha256, "values digest"),
        (render_sha256, "render digest"),
    ):
        _validate_sha256_digest(value, label, R2LabPhysicalStagingError)
    return {
        DEPLOYMENT_RUN_ANNOTATION: run_id,
        DEPLOYMENT_PACKAGE_ANNOTATION: package_sha256,
        DEPLOYMENT_VALUES_ANNOTATION: values_sha256,
        DEPLOYMENT_RENDER_ANNOTATION: render_sha256,
    }


def _validate_deployment_binding_json(
    text: str,
    *,
    run_id: str,
    package_sha256: str,
    values_sha256: str,
    render_sha256: str,
    require_stopped: bool,
    error_type: type[RuntimeError],
) -> int:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise error_type("physical gNB Deployment state is not JSON") from exc
    if not isinstance(payload, dict):
        raise error_type("physical gNB Deployment state is malformed")
    metadata = payload.get("metadata")
    spec = payload.get("spec")
    if not isinstance(metadata, dict) or not isinstance(spec, dict):
        raise error_type("physical gNB Deployment state is incomplete")
    labels = metadata.get("labels")
    annotations = metadata.get("annotations")
    if not isinstance(labels, dict) or not isinstance(annotations, dict):
        raise error_type("physical gNB Deployment ownership metadata is missing")
    if labels.get(DEPLOYMENT_RUN_LABEL) != run_id:
        raise error_type("physical gNB Deployment is not owned by this run")
    expected = _deployment_binding_values(
        run_id=run_id,
        package_sha256=package_sha256,
        values_sha256=values_sha256,
        render_sha256=render_sha256,
    )
    for key, value in expected.items():
        if annotations.get(key) != value:
            raise error_type("physical gNB Deployment artifact binding changed")
    desired = spec.get("replicas")
    if not isinstance(desired, int) or isinstance(desired, bool):
        raise error_type("physical gNB Deployment replica state is malformed")
    if require_stopped and desired != 0:
        raise error_type("physical gNB Deployment is not stopped")
    return desired


def execute_stopped_physical_staging(
    *,
    lock: DependencyLock,
    artifact: PhysicalChartArtifact,
    render_evidence: PhysicalHelmRenderEvidence,
    run_id: str,
    owner: str,
    reservation_id: str | None,
    allocation_id: str | None,
    known_hosts: Path,
    now: datetime,
    runner: Runner,
    timeout_seconds: int = DEFAULT_STAGING_TIMEOUT_SECONDS,
) -> PhysicalStagingResult:
    try:
        run_id = validate_run_id(run_id)
    except Exception as exc:
        raise R2LabPhysicalStagingError(str(exc)) from exc
    owner = _validate_authority(owner, "owner")
    if reservation_id is not None:
        reservation_id = _validate_authority(reservation_id, "reservation ID")
    if allocation_id is not None:
        allocation_id = _validate_authority(allocation_id, "allocation ID")
    if artifact.run_id != run_id:
        raise R2LabPhysicalStagingError("physical artifact run ID does not match staging run")
    if render_evidence.replicas != 0 or render_evidence.strategy != "Recreate":
        raise R2LabPhysicalStagingError(
            "physical render evidence is not stopped and singleton-safe"
        )
    reviewed = r2lab_oai_aligned_candidate().profile
    if (
        render_evidence.carrier_arfcn != reviewed.carrier.value
        or render_evidence.channel_bandwidth_mhz != reviewed.channel_bandwidth_mhz
        or render_evidence.antennas_dl != reviewed.nof_antennas_dl
        or render_evidence.antennas_ul != reviewed.nof_antennas_ul
    ):
        raise R2LabPhysicalStagingError(
            "physical render evidence does not match the reviewed R2Lab radio reference"
        )
    if timeout_seconds < 30 or timeout_seconds > 600:
        raise R2LabPhysicalStagingError(
            "staging timeout must be between 30 and 600 seconds"
        )
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabPhysicalStagingError("strict SLICES known-hosts file is missing")
    if not artifact.package_path.is_file() or not artifact.values_path.is_file():
        raise R2LabPhysicalStagingError("physical artifact files are missing")
    if _staging_sha256_file(artifact.package_path) != artifact.package_sha256:
        raise R2LabPhysicalStagingError("physical chart package digest changed after review")
    if _staging_sha256_file(artifact.values_path) != artifact.values_sha256:
        raise R2LabPhysicalStagingError("physical chart values digest changed after review")

    try:
        reservation_id = verify_reservation(
            runner=runner,
            reservation_id=reservation_id,
            owner=owner,
            nodes={CORE_NODE, RAN_NODE},
            now=now,
            timeout_seconds=min(timeout_seconds, 60),
        )
        allocation_id = verify_allocations(
            runner=runner,
            allocation_id=allocation_id,
            owner=owner,
            nodes={CORE_NODE, RAN_NODE},
            timeout_seconds=min(timeout_seconds, 60),
        )
    except Exception as exc:
        raise R2LabPhysicalStagingError("fresh SLICES authority was not proven") from exc

    remote_root = f"/root/.synthran/{run_id}/physical-chart"
    remote_package = f"{remote_root}/{artifact.package_path.name}"
    remote_values = f"{remote_root}/{artifact.values_path.name}"
    _checked(
        runner,
        _ssh(known_hosts, "mkdir", "-p", remote_root),
        min(timeout_seconds, 60),
        "remote physical artifact directory creation",
    )
    _checked(
        runner,
        (
            *_strict_scp_base(known_hosts),
            str(artifact.package_path),
            str(artifact.values_path),
            f"root@{CORE_NODE}:{remote_root}/",
        ),
        timeout_seconds,
        "strict physical artifact transfer",
    )
    hashes = _checked(
        runner,
        _ssh(known_hosts, "sha256sum", remote_package, remote_values),
        min(timeout_seconds, 60),
        "remote physical artifact digest verification",
    ).stdout
    if artifact.package_sha256 not in hashes or artifact.values_sha256 not in hashes:
        raise R2LabPhysicalStagingError(
            "remote physical artifact digests do not match review"
        )

    helm_version = _checked(
        runner,
        _ssh(known_hosts, "helm", "version", "--short"),
        min(timeout_seconds, 60),
        "remote Helm version probe",
    ).stdout
    expected_helm = _locked_helm_version(lock, R2LabPhysicalStagingError)
    match = re.search(r"v?([0-9]+\.[0-9]+\.[0-9]+)", helm_version)
    if match is None or match.group(1) != expected_helm:
        raise R2LabPhysicalStagingError(
            f"remote Helm must exactly match locked version {expected_helm}"
        )

    namespace_owner = _checked(
        runner,
        _ssh(
            known_hosts,
            "kubectl",
            "get",
            "namespace",
            NAMESPACE,
            "-o",
            "jsonpath={.metadata.labels.synthran\\.run/id}",
        ),
        min(timeout_seconds, 60),
        "Open5GS namespace ownership query",
    ).stdout.strip()
    if namespace_owner != run_id:
        raise R2LabPhysicalStagingError(
            "Open5GS namespace is not owned by this physical run"
        )

    existing = _checked(
        runner,
        _ssh(
            known_hosts,
            "kubectl",
            "get",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            "--ignore-not-found",
            "-o",
            "json",
        ),
        min(timeout_seconds, 60),
        "existing physical gNB Deployment query",
    ).stdout.strip()
    if existing:
        try:
            payload = json.loads(existing)
            desired = payload["spec"]["replicas"]
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise R2LabPhysicalStagingError(
                "existing gNB Deployment state is malformed"
            ) from exc
        if desired != 0:
            raise R2LabPhysicalStagingError(
                "existing physical gNB is not stopped; staging refuses to reconfigure it"
            )

    existing_pods = _parse_pods(
        _checked(
            runner,
            _ssh(
                known_hosts,
                "kubectl",
                "get",
                "pods",
                "-n",
                NAMESPACE,
                "-l",
                GNB_SELECTOR,
                "-o",
                "json",
            ),
            min(timeout_seconds, 60),
            "existing physical gNB pod query",
        ).stdout
    )
    if existing_pods != 0:
        raise R2LabPhysicalStagingError(
            "existing physical gNB pods remain; staging requires zero pods"
        )

    try:
        verify_allocations(
            runner=runner,
            allocation_id=allocation_id,
            owner=owner,
            nodes={CORE_NODE, RAN_NODE},
            timeout_seconds=min(timeout_seconds, 60),
        )
    except Exception as exc:
        raise R2LabPhysicalStagingError(
            "SLICES allocation authority changed before Helm staging"
        ) from exc

    _checked(
        runner,
        _ssh(
            known_hosts,
            "helm",
            "upgrade",
            "--install",
            RELEASE,
            remote_package,
            "--namespace",
            NAMESPACE,
            "--values",
            remote_values,
            "--wait",
            "--atomic",
            "--timeout",
            "120s",
        ),
        timeout_seconds,
        "stopped physical Helm staging",
    )

    binding = _deployment_binding_values(
        run_id=run_id,
        package_sha256=artifact.package_sha256,
        values_sha256=artifact.values_sha256,
        render_sha256=render_evidence.sha256,
    )
    _checked(
        runner,
        _ssh(
            known_hosts,
            "kubectl",
            "label",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            f"{DEPLOYMENT_RUN_LABEL}={run_id}",
            "--overwrite",
        ),
        min(timeout_seconds, 60),
        "physical gNB run ownership binding",
    )
    _checked(
        runner,
        _ssh(
            known_hosts,
            "kubectl",
            "annotate",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            *(f"{key}={value}" for key, value in binding.items()),
            "--overwrite",
        ),
        min(timeout_seconds, 60),
        "physical gNB artifact binding",
    )

    deployment = _checked(
        runner,
        _ssh(
            known_hosts,
            "kubectl",
            "get",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            "-o",
            "json",
        ),
        min(timeout_seconds, 60),
        "staged physical gNB Deployment query",
    ).stdout
    desired = _validate_deployment_binding_json(
        deployment,
        run_id=run_id,
        package_sha256=artifact.package_sha256,
        values_sha256=artifact.values_sha256,
        render_sha256=render_evidence.sha256,
        require_stopped=True,
        error_type=R2LabPhysicalStagingError,
    )
    pods = _parse_pods(
        _checked(
            runner,
            _ssh(
                known_hosts,
                "kubectl",
                "get",
                "pods",
                "-n",
                NAMESPACE,
                "-l",
                GNB_SELECTOR,
                "-o",
                "json",
            ),
            min(timeout_seconds, 60),
            "staged physical gNB pod query",
        ).stdout
    )
    if desired != 0 or pods != 0:
        raise R2LabPhysicalStagingError(
            "physical chart staging did not remain at proven zero-pod state"
        )
    return PhysicalStagingResult(
        run_id=run_id,
        package_sha256=artifact.package_sha256,
        values_sha256=artifact.values_sha256,
        render_sha256=render_evidence.sha256,
        namespace_owned=True,
        desired_replicas=desired,
        gnb_pod_count=pods,
        deployment_bound=True,
    )


@dataclass(frozen=True)
class GnbPodObservation:
    total_count: int
    ready_running_count: int
    terminating_count: int

    @property
    def zero(self) -> bool:
        return self.total_count == 0

    @property
    def exactly_one_ready(self) -> bool:
        return (
            self.total_count == 1
            and self.ready_running_count == 1
            and self.terminating_count == 0
        )


@dataclass(frozen=True)
class GnbLifecycleResult:
    stopped_before_configure: bool
    configured: bool
    started_exactly_one: bool
    maximum_observed_pods: int

    def to_dict(self) -> dict[str, object]:
        return {
            "stopped_before_configure": self.stopped_before_configure,
            "configured": self.configured,
            "started_exactly_one": self.started_exactly_one,
            "maximum_observed_pods": self.maximum_observed_pods,
            "deployment_strategy": "non-overlapping-singleton",
        }


def _scale_command(replicas: int) -> tuple[str, ...]:
    if replicas not in {0, 1}:
        raise ValueError("physical gNB replicas must be zero or one")
    return (
        "kubectl",
        "scale",
        f"deployment/{GNB_DEPLOYMENT}",
        "-n",
        GNB_NAMESPACE,
        f"--replicas={replicas}",
    )


def _pods_command() -> tuple[str, ...]:
    return (
        "kubectl",
        "get",
        "pods",
        "-n",
        GNB_NAMESPACE,
        "-l",
        GNB_SELECTOR,
        "-o",
        "json",
    )


def parse_gnb_pods_json(text: str) -> GnbPodObservation:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise R2LabGnbLifecycleError("gNB pod query did not return JSON") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise R2LabGnbLifecycleError("gNB pod query returned malformed JSON")
    total = 0
    ready_running = 0
    terminating = 0
    for item in payload["items"]:
        if not isinstance(item, dict):
            raise R2LabGnbLifecycleError("gNB pod query returned a malformed pod")
        metadata = item.get("metadata")
        status = item.get("status")
        if not isinstance(metadata, dict) or not isinstance(status, dict):
            raise R2LabGnbLifecycleError("gNB pod query returned incomplete pod state")
        total += 1
        is_terminating = metadata.get("deletionTimestamp") is not None
        if is_terminating:
            terminating += 1
        container_statuses = status.get("containerStatuses")
        containers_ready = (
            isinstance(container_statuses, list)
            and bool(container_statuses)
            and all(
                isinstance(container, dict) and container.get("ready") is True
                for container in container_statuses
            )
        )
        if (
            not is_terminating
            and status.get(POD_RUNTIME_STATE_KEY) == "Running"
            and containers_ready
        ):
            ready_running += 1
    return GnbPodObservation(
        total_count=total,
        ready_running_count=ready_running,
        terminating_count=terminating,
    )


def _observe(runner: Runner, timeout_seconds: int) -> GnbPodObservation:
    try:
        result = runner(_pods_command(), timeout_seconds)
    except Exception as exc:
        raise R2LabGnbLifecycleError("gNB pod state could not be observed") from exc
    if result.returncode != 0:
        raise R2LabGnbLifecycleError("gNB pod state query returned nonzero")
    return parse_gnb_pods_json(result.stdout)


def _request_scale(runner: Runner, replicas: int, timeout_seconds: int) -> int | None:
    try:
        return runner(_scale_command(replicas), timeout_seconds).returncode
    except Exception:
        return None


def _wait_for_zero(
    *,
    runner: Runner,
    sleeper: Sleeper,
    timeout_seconds: int,
    attempts: int,
    poll_interval_seconds: float,
) -> tuple[bool, int]:
    maximum = 0
    for attempt in range(attempts):
        observation = _observe(runner, timeout_seconds)
        maximum = max(maximum, observation.total_count)
        if observation.zero:
            return True, maximum
        if attempt + 1 < attempts:
            sleeper(poll_interval_seconds)
    return False, maximum


def _wait_for_exactly_one_ready(
    *,
    runner: Runner,
    sleeper: Sleeper,
    timeout_seconds: int,
    attempts: int,
    poll_interval_seconds: float,
) -> tuple[bool, int, bool]:
    maximum = 0
    overlap_seen = False
    for attempt in range(attempts):
        observation = _observe(runner, timeout_seconds)
        maximum = max(maximum, observation.total_count)
        if observation.total_count > 1:
            overlap_seen = True
            return False, maximum, overlap_seen
        if observation.exactly_one_ready:
            return True, maximum, overlap_seen
        if attempt + 1 < attempts:
            sleeper(poll_interval_seconds)
    return False, maximum, overlap_seen


def execute_non_overlapping_gnb_update(
    *,
    runner: Runner,
    configure: Configure,
    sleeper: Sleeper,
    before_start: Configure | None = None,
    timeout_seconds: int = DEFAULT_COMMAND_TIMEOUT_SECONDS,
    shutdown_attempts: int = DEFAULT_POLL_ATTEMPTS,
    startup_attempts: int = DEFAULT_POLL_ATTEMPTS,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    uhd_release_seconds: float = DEFAULT_UHD_RELEASE_SECONDS,
) -> GnbLifecycleResult:
    if timeout_seconds < 1:
        raise R2LabGnbLifecycleError("gNB command timeout must be positive")
    if shutdown_attempts < 1 or startup_attempts < 1:
        raise R2LabGnbLifecycleError("gNB poll attempts must be positive")
    if poll_interval_seconds < 0 or uhd_release_seconds < 0:
        raise R2LabGnbLifecycleError("gNB wait intervals must not be negative")
    maximum_observed = 0
    _request_scale(runner, 0, timeout_seconds)
    stopped, maximum = _wait_for_zero(
        runner=runner,
        sleeper=sleeper,
        timeout_seconds=timeout_seconds,
        attempts=shutdown_attempts,
        poll_interval_seconds=poll_interval_seconds,
    )
    maximum_observed = max(maximum_observed, maximum)
    if not stopped:
        raise R2LabGnbLifecycleError(
            "physical gNB could not be proven stopped; configuration was not applied"
        )
    sleeper(uhd_release_seconds)
    try:
        configure()
    except Exception as exc:
        raise R2LabGnbLifecycleError(
            "physical gNB configuration failed while the Deployment was stopped"
        ) from exc
    if before_start is not None:
        try:
            before_start()
        except Exception as exc:
            raise R2LabGnbLifecycleError(
                "physical gNB start authority could not be refreshed while stopped"
            ) from exc
    _request_scale(runner, 1, timeout_seconds)
    try:
        started, maximum, overlap_seen = _wait_for_exactly_one_ready(
            runner=runner,
            sleeper=sleeper,
            timeout_seconds=timeout_seconds,
            attempts=startup_attempts,
            poll_interval_seconds=poll_interval_seconds,
        )
    except R2LabGnbLifecycleError as exc:
        _request_scale(runner, 0, timeout_seconds)
        raise R2LabGnbLifecycleError(
            "physical gNB startup state became unobservable; scale-to-zero recovery was requested"
        ) from exc
    maximum_observed = max(maximum_observed, maximum)
    if not started:
        _request_scale(runner, 0, timeout_seconds)
        recovered, recovery_maximum = _wait_for_zero(
            runner=runner,
            sleeper=sleeper,
            timeout_seconds=timeout_seconds,
            attempts=shutdown_attempts,
            poll_interval_seconds=poll_interval_seconds,
        )
        maximum_observed = max(maximum_observed, recovery_maximum)
        reason = (
            "overlapping gNB owners were observed"
            if overlap_seen
            else "gNB did not become exactly one ready pod"
        )
        suffix = (
            " and zero-pod recovery was proven"
            if recovered
            else " and zero-pod recovery is unresolved"
        )
        raise R2LabGnbLifecycleError(reason + suffix)
    return GnbLifecycleResult(
        stopped_before_configure=True,
        configured=True,
        started_exactly_one=True,
        maximum_observed_pods=maximum_observed,
    )


def execute_authorized_physical_gnb_start(
    *,
    authority: PhysicalStartAuthority,
    staging: PhysicalStagingResult,
    owner: str,
    reservation_id: str | None,
    allocation_id: str | None,
    known_hosts: Path,
    now: datetime,
    runner: Runner,
    refresh_r2lab_authority: Callable[[], PhysicalStartAuthority],
    sleeper: Sleeper,
    timeout_seconds: int = DEFAULT_STAGING_TIMEOUT_SECONDS,
    shutdown_attempts: int = DEFAULT_POLL_ATTEMPTS,
    startup_attempts: int = DEFAULT_POLL_ATTEMPTS,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    uhd_release_seconds: float = DEFAULT_UHD_RELEASE_SECONDS,
) -> PhysicalGnbStartResult:
    """Start exactly one staged gNB only while R2Lab/SLICES authority remains proven."""

    authority.validate()
    if staging.run_id != authority.run_id:
        raise R2LabPhysicalStartError("staged artifact belongs to a different R2Lab run")
    if not staging.namespace_owned or not staging.deployment_bound:
        raise R2LabPhysicalStartError("staged physical Deployment ownership is not proven")
    if staging.desired_replicas != 0 or staging.gnb_pod_count != 0:
        raise R2LabPhysicalStartError("physical gNB start requires a proven stopped staging state")
    for value, label in (
        (staging.package_sha256, "package digest"),
        (staging.values_sha256, "values digest"),
        (staging.render_sha256, "render digest"),
    ):
        _validate_sha256_digest(value, label, R2LabPhysicalStartError)
    if timeout_seconds < 30 or timeout_seconds > 600:
        raise R2LabPhysicalStartError("physical start timeout must be between 30 and 600 seconds")

    try:
        owner = _validate_authority(owner, "owner")
        if reservation_id is not None:
            reservation_id = _validate_authority(reservation_id, "reservation ID")
        if allocation_id is not None:
            allocation_id = _validate_authority(allocation_id, "allocation ID")
    except R2LabPhysicalStagingError as exc:
        raise R2LabPhysicalStartError(str(exc)) from exc
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabPhysicalStartError("strict SLICES known-hosts file is missing")

    try:
        reservation_id = verify_reservation(
            runner=runner,
            reservation_id=reservation_id,
            owner=owner,
            nodes={CORE_NODE, RAN_NODE},
            now=now,
            timeout_seconds=min(timeout_seconds, 60),
        )
        allocation_id = verify_allocations(
            runner=runner,
            allocation_id=allocation_id,
            owner=owner,
            nodes={CORE_NODE, RAN_NODE},
            timeout_seconds=min(timeout_seconds, 60),
        )
    except Exception as exc:
        raise R2LabPhysicalStartError("fresh SLICES authority was not proven for gNB start") from exc

    def checked_start(*remote: str, label: str) -> CommandResult:
        try:
            result = runner(_ssh(known_hosts, *remote), min(timeout_seconds, 60))
        except Exception as exc:
            raise R2LabPhysicalStartError(f"{label} could not be observed") from exc
        if result.returncode != 0:
            raise R2LabPhysicalStartError(f"{label} returned nonzero")
        return result

    namespace_owner = checked_start(
        "kubectl",
        "get",
        "namespace",
        NAMESPACE,
        "-o",
        "jsonpath={.metadata.labels.synthran\\.run/id}",
        label="Open5GS namespace ownership query",
    ).stdout.strip()
    if namespace_owner != staging.run_id:
        raise R2LabPhysicalStartError("Open5GS namespace is not owned by this physical run")

    def require_bound_stopped_deployment() -> None:
        deployment = checked_start(
            "kubectl",
            "get",
            f"deployment/{RELEASE}",
            "-n",
            NAMESPACE,
            "-o",
            "json",
            label="bound physical gNB Deployment query",
        ).stdout
        _validate_deployment_binding_json(
            deployment,
            run_id=staging.run_id,
            package_sha256=staging.package_sha256,
            values_sha256=staging.values_sha256,
            render_sha256=staging.render_sha256,
            require_stopped=True,
            error_type=R2LabPhysicalStartError,
        )

    require_bound_stopped_deployment()
    pods = _parse_pods(
        checked_start(
            "kubectl",
            "get",
            "pods",
            "-n",
            NAMESPACE,
            "-l",
            GNB_SELECTOR,
            "-o",
            "json",
            label="pre-start physical gNB pod query",
        ).stdout
    )
    if pods != 0:
        raise R2LabPhysicalStartError("physical gNB start requires zero existing gNB pods")

    def cluster_runner(command: Sequence[str], command_timeout: int) -> CommandResult:
        return runner(_ssh(known_hosts, *tuple(command)), command_timeout)

    def before_start() -> None:
        refreshed = refresh_r2lab_authority().validate()
        if (
            refreshed.run_id != authority.run_id
            or refreshed.radio != authority.radio
            or refreshed.ue != authority.ue
            or refreshed.ue_kind != authority.ue_kind
            or refreshed.claim_sha256 != authority.claim_sha256
        ):
            raise R2LabPhysicalStartError("R2Lab claim or selected-resource authority changed")
        try:
            verify_allocations(
                runner=runner,
                allocation_id=allocation_id,
                owner=owner,
                nodes={CORE_NODE, RAN_NODE},
                timeout_seconds=min(timeout_seconds, 60),
            )
        except Exception as exc:
            raise R2LabPhysicalStartError(
                "SLICES allocation authority changed before gNB ownership start"
            ) from exc
        require_bound_stopped_deployment()

    try:
        lifecycle = execute_non_overlapping_gnb_update(
            runner=cluster_runner,
            configure=lambda: None,
            before_start=before_start,
            sleeper=sleeper,
            timeout_seconds=min(timeout_seconds, 60),
            shutdown_attempts=shutdown_attempts,
            startup_attempts=startup_attempts,
            poll_interval_seconds=poll_interval_seconds,
            uhd_release_seconds=uhd_release_seconds,
        )
    except R2LabGnbLifecycleError as exc:
        raise R2LabPhysicalStartError(str(exc)) from exc
    if not lifecycle.started_exactly_one or lifecycle.maximum_observed_pods > 1:
        raise R2LabPhysicalStartError("physical gNB singleton ownership was not proven")
    return PhysicalGnbStartResult(
        run_id=staging.run_id,
        package_sha256=staging.package_sha256,
        values_sha256=staging.values_sha256,
        render_sha256=staging.render_sha256,
        claim_sha256=authority.claim_sha256,
        maximum_observed_pods=lifecycle.maximum_observed_pods,
    )


def execute_authorized_physical_gnb_stop(
    *,
    staging: PhysicalStagingResult,
    owner: str,
    reservation_id: str | None,
    allocation_id: str | None,
    known_hosts: Path,
    now: datetime,
    runner: Runner,
    sleeper: Sleeper,
    timeout_seconds: int = DEFAULT_STAGING_TIMEOUT_SECONDS,
    shutdown_attempts: int = DEFAULT_POLL_ATTEMPTS,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
) -> PhysicalGnbStopResult:
    """Stop only the artifact-bound gNB Deployment owned by one physical run."""

    staging = PhysicalStagingResult.from_dict(staging.to_dict())
    if now.tzinfo is None:
        raise R2LabPhysicalStartError("physical gNB stop time must be timezone-aware")
    if timeout_seconds < 30 or timeout_seconds > 600:
        raise R2LabPhysicalStartError(
            "physical gNB stop timeout must be between 30 and 600 seconds"
        )
    if shutdown_attempts < 1 or poll_interval_seconds < 0:
        raise R2LabPhysicalStartError("physical gNB stop wait settings are invalid")
    try:
        owner = _validate_authority(owner, "owner")
        if reservation_id is not None:
            reservation_id = _validate_authority(reservation_id, "reservation ID")
        if allocation_id is not None:
            allocation_id = _validate_authority(allocation_id, "allocation ID")
    except R2LabPhysicalStagingError as exc:
        raise R2LabPhysicalStartError(str(exc)) from exc
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabPhysicalStartError("strict SLICES known-hosts file is missing")

    try:
        reservation_id = verify_reservation(
            runner=runner,
            reservation_id=reservation_id,
            owner=owner,
            nodes={CORE_NODE, RAN_NODE},
            now=now,
            timeout_seconds=min(timeout_seconds, 60),
        )
        allocation_id = verify_allocations(
            runner=runner,
            allocation_id=allocation_id,
            owner=owner,
            nodes={CORE_NODE, RAN_NODE},
            timeout_seconds=min(timeout_seconds, 60),
        )
    except Exception as exc:
        raise R2LabPhysicalStartError(
            "fresh SLICES authority was not proven for gNB stop"
        ) from exc

    namespace_owner = _checked(
        runner,
        _ssh(
            known_hosts,
            "kubectl",
            "get",
            "namespace",
            NAMESPACE,
            "-o",
            "jsonpath={.metadata.labels.synthran\\.run/id}",
        ),
        min(timeout_seconds, 60),
        "Open5GS namespace ownership query",
    ).stdout.strip()
    if namespace_owner != staging.run_id:
        raise R2LabPhysicalStartError(
            "Open5GS namespace is not owned by the physical run being stopped"
        )

    def bound_deployment(*, require_stopped: bool) -> int:
        deployment = _checked(
            runner,
            _ssh(
                known_hosts,
                "kubectl",
                "get",
                f"deployment/{RELEASE}",
                "-n",
                NAMESPACE,
                "-o",
                "json",
            ),
            min(timeout_seconds, 60),
            "bound physical gNB Deployment query",
        ).stdout
        return _validate_deployment_binding_json(
            deployment,
            run_id=staging.run_id,
            package_sha256=staging.package_sha256,
            values_sha256=staging.values_sha256,
            render_sha256=staging.render_sha256,
            require_stopped=require_stopped,
            error_type=R2LabPhysicalStartError,
        )

    bound_deployment(require_stopped=False)

    def cluster_runner(
        command: Sequence[str], command_timeout: int
    ) -> CommandResult:
        return runner(_ssh(known_hosts, *tuple(command)), command_timeout)

    if _request_scale(cluster_runner, 0, min(timeout_seconds, 60)) != 0:
        raise R2LabPhysicalStartError("physical gNB scale-to-zero returned nonzero")
    try:
        stopped, _maximum = _wait_for_zero(
            runner=cluster_runner,
            sleeper=sleeper,
            timeout_seconds=min(timeout_seconds, 60),
            attempts=shutdown_attempts,
            poll_interval_seconds=poll_interval_seconds,
        )
    except R2LabGnbLifecycleError as exc:
        raise R2LabPhysicalStartError(
            "physical gNB stop state became unobservable"
        ) from exc
    if not stopped:
        raise R2LabPhysicalStartError(
            "physical gNB did not reach a proven zero-pod state"
        )
    desired = bound_deployment(require_stopped=True)
    return PhysicalGnbStopResult(
        run_id=staging.run_id,
        desired_replicas=desired,
        gnb_pod_count=0,
    )
