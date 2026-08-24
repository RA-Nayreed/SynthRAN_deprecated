"""Physical R2Lab deployment subsystem.

This module owns the complete physical gNB path as one coherent subsystem:
pinned R2Lab configuration, chart overlay, isolated workspace, offline Helm
validation, deterministic packaging, stopped cluster staging, and the
non-overlapping singleton gNB lifecycle.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import gzip
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import shlex
import tarfile
import tempfile
from typing import Callable, Mapping, Sequence
import urllib.request

from synthran.dependencies import DependencyLock
from synthran.live_preflight import CommandResult
from synthran.network.runtime import validate_run_id
from synthran.r2lab.authority import (
    CORE_NODE,
    RADIO as CURRENT_RADIO,
    RAN_NODE,
    verify_physical_allocation,
)
# Reviewed topology / deployment contract.
PHYSICAL_DEPLOYMENT_SCHEMA = "synthran/r2lab-physical-deployment/v1alpha1"
CURRENT_CORE_NODE = CORE_NODE
CURRENT_RAN_NODE = RAN_NODE

# Pinned chart contract.
PINNED_FIVEG_ANSIBLE_COMMIT = "a0149fc0dde39e2872945a0f3c91e804ece52d4f"
PINNED_SRSRAN_HELM_COMMIT = "8dfb9890d127734cdcd6eee9df8c5d09b1a8076a"
FIVEG_R2LAB_PROFILE_TASK = "roles/5g/srsRAN/config/tasks/main.yml"
PHYSICAL_GNB_CONTAINER = "srsran_gnb_physical"
PHYSICAL_CHART_PATH = "charts/srsran-gnb"
PHYSICAL_DEPLOYMENT_TEMPLATE = f"{PHYSICAL_CHART_PATH}/templates/deployment.yaml"
PHYSICAL_VALUES_SOURCE = f"{PHYSICAL_CHART_PATH}/values-n300-n78-20MHz.yaml"
SOURCE_VALUES_FILE_NAME = "r2lab-n300-values.yaml"
VALUES_FILE_NAME = "synthran-physical-values.json"
PHYSICAL_GNB_CPU_COUNT = 8
PHYSICAL_GNB_MEMORY = "4Gi"

# Runtime Kubernetes contract.
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
MAX_LOCKED_HELM_ARCHIVE_BYTES = 64 * 1024 * 1024
LOCKED_HELM_ARCHIVE_MEMBER = "linux-amd64/helm"

_SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_SAFE_AUTHORITY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
Runner = Callable[[Sequence[str], int], CommandResult]
Sleeper = Callable[[float], None]
Configure = Callable[[], None]


class R2LabPhysicalDeploymentError(ValueError):
    """Raised when a physical deployment plan crosses the reviewed boundary."""


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
            "configuration": {
                "adapter_commit": PINNED_FIVEG_ANSIBLE_COMMIT,
                "adapter_task": FIVEG_R2LAB_PROFILE_TASK,
                "chart_commit": PINNED_SRSRAN_HELM_COMMIT,
                "values_file": PHYSICAL_VALUES_SOURCE,
                "radio_overrides": False,
            },
            "deployment": {
                "strategy": "Recreate",
                "desired_replicas": 1,
                "max_concurrent_gnb_pods": 1,
            },
            "required_lifecycle": [
                "scale exact srsran-gnb deployment to zero",
                "prove matching gNB pod count is zero",
                "allow UHD claim release",
                "apply pinned R2Lab configuration",
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
        return "\n".join(
            (
                "SynthRAN physical R2Lab deployment plan (NON-EXECUTING)",
                f"Run ID: {self.run_id}",
                f"Path: Open5GS@{self.core_node} + srsRAN@{self.ran_node} + {self.radio}",
                f"R2Lab adapter: {PINNED_FIVEG_ANSIBLE_COMMIT}",
                f"N300 values: {PHYSICAL_VALUES_SOURCE}",
                "Radio overrides: none",
                "Deployment strategy: Recreate / maximum one matching gNB pod",
                "Execution: disabled until the pinned values render is validated",
            )
        )


def build_physical_deployment_plan(
    *,
    run_id: str,
    core_node: str = CURRENT_CORE_NODE,
    ran_node: str = CURRENT_RAN_NODE,
    radio: str = CURRENT_RADIO,
) -> R2LabPhysicalDeploymentPlan:
    return R2LabPhysicalDeploymentPlan(
        run_id=run_id,
        core_node=core_node,
        ran_node=ran_node,
        radio=radio,
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
            "chart": {
                "commit": self.chart_commit,
                "path": self.chart_path,
                "values_source": PHYSICAL_VALUES_SOURCE,
            },
            "values": deepcopy(dict(self.values)),
            "review": deepcopy(dict(self.review)),
        }

def _locked_git_commit(lock: DependencyLock, name: str) -> str:
    git = lock.raw.get("git")
    if not isinstance(git, dict):
        raise R2LabPhysicalChartError("dependency lock Git mapping is unavailable")
    entry = git.get(name)
    if not isinstance(entry, dict):
        raise R2LabPhysicalChartError(f"dependency lock is missing {name}")
    commit = entry.get("commit")
    if not isinstance(commit, str):
        raise R2LabPhysicalChartError(f"dependency lock commit is missing for {name}")
    return commit


def _locked_source_contract(lock: DependencyLock) -> tuple[str, str]:
    adapter_commit = _locked_git_commit(lock, "fiveg_ansible")
    chart_commit = _locked_git_commit(lock, "srsran_helm")
    if adapter_commit != PINNED_FIVEG_ANSIBLE_COMMIT:
        raise R2LabPhysicalChartError(
            "physical adapter is reviewed only for the pinned fiveg_ansible commit"
        )
    commit = chart_commit
    if commit != PINNED_SRSRAN_HELM_COMMIT:
        raise R2LabPhysicalChartError(
            "physical chart adapter is reviewed only for the pinned srsran_helm commit"
        )
    return adapter_commit, chart_commit


def _locked_chart_commit(lock: DependencyLock) -> str:
    return _locked_source_contract(lock)[1]


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
    adapter_commit, chart_commit = _locked_source_contract(lock)
    image = _physical_image(lock)

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
        "gnbConfig": {
            "cu_cp": {
                "amf": {
                    "addr": bindings.amf_n2_address,
                    "bind_addr": bindings.gnb_n2_address,
                }
            },
            "cu_up": {
                "ngu": {
                    "socket": [{"bind_addr": bindings.gnb_n2_address}],
                }
            },
        },
        "n3networkName": bindings.n3_network_name,
        "namespace": NAMESPACE,
        "ru": True,
        "ruSubnet": bindings.ru_subnet,
        "ruPodIp": bindings.ru_pod_address,
        "nodeName": bindings.node_name,
        "sriov": {"enabled": False},
    }
    serialized = json.dumps(values, sort_keys=True).lower()
    if "rfsim" in serialized or "all-off" in serialized:
        raise R2LabPhysicalChartError("physical chart bundle contains forbidden backend behavior")
    return PhysicalChartBundle(
        run_id=plan.run_id,
        chart_commit=chart_commit,
        chart_path=PHYSICAL_CHART_PATH,
        values=values,
        review={
            "adapter_commit": adapter_commit,
            "adapter_task": FIVEG_R2LAB_PROFILE_TASK,
            "configuration_source": PHYSICAL_VALUES_SOURCE,
            "chart_commit": chart_commit,
            "radio_values_overridden": False,
            "n300_address": bindings.n300_address,
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
    source_values_file: Path
    values_file: Path
    source_template_sha256: str
    overlaid_template_sha256: str
    source_values_sha256: str
    values_sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "chart_root": PHYSICAL_CHART_PATH,
            "deployment_template": "templates/deployment.yaml",
            "source_values_file": PHYSICAL_VALUES_SOURCE.removeprefix(
                f"{PHYSICAL_CHART_PATH}/"
            ),
            "values_file": VALUES_FILE_NAME,
            "source_template_sha256": self.source_template_sha256,
            "overlaid_template_sha256": self.overlaid_template_sha256,
            "source_values_sha256": self.source_values_sha256,
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
    source_values_path = checkout_root / PHYSICAL_VALUES_SOURCE
    chart_metadata = chart_root / "Chart.yaml"
    values_path = chart_root / VALUES_FILE_NAME
    if (
        not chart_root.is_dir()
        or not chart_metadata.is_file()
        or not template_path.is_file()
        or not source_values_path.is_file()
    ):
        raise R2LabPhysicalChartError(
            "isolated srsran_helm checkout is missing the reviewed chart structure"
        )
    if values_path.exists():
        raise R2LabPhysicalChartError(
            "physical chart workspace already contains generated SynthRAN values"
        )
    try:
        source = template_path.read_text(encoding="utf-8")
        source_values_bytes = source_values_path.read_bytes()
        source_values_bytes.decode("utf-8")
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
        source_values_file=source_values_path,
        values_file=values_path,
        source_template_sha256=_sha256_text(source),
        overlaid_template_sha256=_sha256_text(overlaid),
        source_values_sha256=hashlib.sha256(source_values_bytes).hexdigest(),
        values_sha256=_sha256_text(values_text),
    )


@dataclass(frozen=True)
class PhysicalHelmRenderEvidence:
    sha256: str
    source_values_sha256: str
    replicas: int
    strategy: str
    image_reference: str
    carrier_arfcn: int
    band: int
    channel_bandwidth_mhz: int
    common_scs_khz: int
    sample_rate_mhz: float
    tx_gain_db: int
    rx_gain_db: int
    ss0_index: int
    coreset0_index: int
    prach_config_index: int
    device_args_sha256: str

    def to_dict(self) -> dict[str, object]:
        return {
            "sha256": self.sha256,
            "source_values_sha256": self.source_values_sha256,
            "replicas": self.replicas,
            "strategy": self.strategy,
            "image_reference": self.image_reference,
            "carrier_arfcn": self.carrier_arfcn,
            "band": self.band,
            "channel_bandwidth_mhz": self.channel_bandwidth_mhz,
            "common_scs_khz": self.common_scs_khz,
            "sample_rate_mhz": self.sample_rate_mhz,
            "tx_gain_db": self.tx_gain_db,
            "rx_gain_db": self.rx_gain_db,
            "ss0_index": self.ss0_index,
            "coreset0_index": self.coreset0_index,
            "prach_config_index": self.prach_config_index,
            "device_args_sha256": self.device_args_sha256,
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
            "band",
            "channel_bandwidth_mhz",
            "common_scs_khz",
            "tx_gain_db",
            "rx_gain_db",
            "ss0_index",
            "coreset0_index",
            "prach_config_index",
        )
        if any(
            not isinstance(payload.get(field), int)
            or isinstance(payload.get(field), bool)
            for field in integer_fields
        ):
            raise R2LabPhysicalHelmError(
                "stored physical render evidence contains malformed numeric values"
            )
        sample_rate = payload.get("sample_rate_mhz")
        if not isinstance(sample_rate, (int, float)) or isinstance(sample_rate, bool):
            raise R2LabPhysicalHelmError(
                "stored physical render evidence contains a malformed sample rate"
            )
        text_fields = (
            "sha256",
            "source_values_sha256",
            "strategy",
            "image_reference",
            "device_args_sha256",
        )
        if any(
            not isinstance(payload.get(field), str) or not payload.get(field)
            for field in text_fields
        ):
            raise R2LabPhysicalHelmError(
                "stored physical render evidence contains malformed text values"
            )
        evidence = cls(
            sha256=str(payload.get("sha256", "")),
            source_values_sha256=str(payload.get("source_values_sha256", "")),
            replicas=payload.get("replicas", -1),
            strategy=str(payload.get("strategy", "")),
            image_reference=str(payload.get("image_reference", "")),
            carrier_arfcn=payload.get("carrier_arfcn", -1),
            band=payload.get("band", -1),
            channel_bandwidth_mhz=payload.get("channel_bandwidth_mhz", -1),
            common_scs_khz=payload.get("common_scs_khz", -1),
            sample_rate_mhz=float(sample_rate),
            tx_gain_db=payload.get("tx_gain_db", -1),
            rx_gain_db=payload.get("rx_gain_db", -1),
            ss0_index=payload.get("ss0_index", -1),
            coreset0_index=payload.get("coreset0_index", -1),
            prach_config_index=payload.get("prach_config_index", -1),
            device_args_sha256=str(payload.get("device_args_sha256", "")),
        )
        if evidence.to_dict() != dict(payload):
            raise R2LabPhysicalHelmError(
                "stored physical render evidence is malformed"
            )
        for value, label in (
            (evidence.sha256, "render digest"),
            (evidence.source_values_sha256, "source values digest"),
            (evidence.device_args_sha256, "device arguments digest"),
        ):
            _validate_sha256_digest(value, label, R2LabPhysicalHelmError)
        return evidence


@dataclass(frozen=True)
class _PinnedRadioValues:
    carrier_arfcn: int
    band: int
    channel_bandwidth_mhz: int
    common_scs_khz: int
    sample_rate_mhz: float
    tx_gain_db: int
    rx_gain_db: int
    ss0_index: int
    coreset0_index: int
    prach_config_index: int
    device_args: str

    def comparable(self) -> tuple[object, ...]:
        return (
            self.carrier_arfcn,
            self.band,
            self.channel_bandwidth_mhz,
            self.common_scs_khz,
            self.sample_rate_mhz,
            self.tx_gain_db,
            self.rx_gain_db,
            self.ss0_index,
            self.coreset0_index,
            self.prach_config_index,
            self.device_args,
        )


def _locked_helm_metadata(
    lock: DependencyLock, error_type: type[RuntimeError]
) -> tuple[str, str, str]:
    tools = lock.raw.get("tools")
    entry = tools.get("helm_linux_amd64") if isinstance(tools, dict) else None
    version = entry.get("version") if isinstance(entry, dict) else None
    url = entry.get("url") if isinstance(entry, dict) else None
    digest = entry.get("sha256") if isinstance(entry, dict) else None
    if (
        not isinstance(version, str)
        or not re.fullmatch(r"[0-9]+(?:\.[0-9]+){2}", version)
        or not isinstance(url, str)
        or not url.startswith("https://")
        or not isinstance(digest, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
    ):
        raise error_type("dependency lock does not define a complete Helm tool")
    return version, url, digest.removeprefix("sha256:")


def _locked_helm_version(lock: DependencyLock, error_type: type[RuntimeError]) -> str:
    return _locked_helm_metadata(lock, error_type)[0]


def _locked_tool_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def materialize_locked_helm(
    *,
    lock: DependencyLock,
    destination: Path,
    timeout_seconds: int = 60,
) -> Path:
    """Materialize the checksum-locked Linux AMD64 Helm without system install."""

    if timeout_seconds < 5 or timeout_seconds > 300:
        raise R2LabPhysicalHelmError(
            "locked Helm download timeout must be between 5 and 300 seconds"
        )
    if platform.system() != "Linux" or platform.machine().lower() not in {
        "amd64",
        "x86_64",
    }:
        raise R2LabPhysicalHelmError(
            "locked Helm executable supports only Linux AMD64 controllers"
        )
    version, url, expected_archive_sha256 = _locked_helm_metadata(
        lock, R2LabPhysicalHelmError
    )
    try:
        destination = destination.expanduser().resolve()
        destination.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise R2LabPhysicalHelmError(
            "locked Helm directory could not be prepared"
        ) from exc

    archive = destination / f"helm-v{version}-linux-amd64.tar.gz"
    if archive.exists():
        if not archive.is_file() or archive.is_symlink():
            raise R2LabPhysicalHelmError("locked Helm archive path is unsafe")
        try:
            observed_archive_sha256 = _locked_tool_sha256(archive)
        except OSError as exc:
            raise R2LabPhysicalHelmError(
                "locked Helm archive could not be inspected"
            ) from exc
        if observed_archive_sha256 != expected_archive_sha256:
            raise R2LabPhysicalHelmError(
                "existing locked Helm archive does not match the dependency lock"
            )
    else:
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=".helm-download-",
                dir=destination,
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
                    total = 0
                    while True:
                        chunk = response.read(1024 * 1024)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > MAX_LOCKED_HELM_ARCHIVE_BYTES:
                            raise R2LabPhysicalHelmError(
                                "locked Helm archive exceeds the reviewed size limit"
                            )
                        temporary.write(chunk)
            if _locked_tool_sha256(temporary_path) != expected_archive_sha256:
                raise R2LabPhysicalHelmError(
                    "downloaded Helm archive does not match the dependency lock"
                )
            temporary_path.replace(archive)
            temporary_path = None
        except R2LabPhysicalHelmError:
            raise
        except (OSError, ValueError) as exc:
            raise R2LabPhysicalHelmError(
                "locked Helm archive could not be downloaded"
            ) from exc
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    executable = destination / "helm"
    temporary_executable: Path | None = None
    try:
        with tarfile.open(archive, mode="r:gz") as bundle:
            member = bundle.getmember(LOCKED_HELM_ARCHIVE_MEMBER)
            if (
                not member.isfile()
                or member.size <= 0
                or member.size > MAX_LOCKED_HELM_ARCHIVE_BYTES
            ):
                raise R2LabPhysicalHelmError(
                    "locked Helm archive member is malformed"
                )
            source = bundle.extractfile(member)
            if source is None:
                raise R2LabPhysicalHelmError(
                    "locked Helm executable is unavailable in the archive"
                )
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=".helm-extract-",
                dir=destination,
                delete=False,
            ) as temporary:
                temporary_executable = Path(temporary.name)
                remaining = member.size
                while remaining:
                    chunk = source.read(min(1024 * 1024, remaining))
                    if not chunk:
                        raise R2LabPhysicalHelmError(
                            "locked Helm executable is truncated"
                        )
                    temporary.write(chunk)
                    remaining -= len(chunk)
        os.chmod(temporary_executable, 0o755)
        temporary_executable.replace(executable)
        temporary_executable = None
    except R2LabPhysicalHelmError:
        raise
    except (KeyError, OSError, tarfile.TarError) as exc:
        raise R2LabPhysicalHelmError(
            "locked Helm executable could not be materialized"
        ) from exc
    finally:
        if temporary_executable is not None:
            temporary_executable.unlink(missing_ok=True)
    return executable


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
    matches = re.findall(
        rf"(?m)^\s*{re.escape(key)}:\s*([0-9]+)(?:\s+#.*)?\s*$",
        text,
    )
    if len(matches) != 1:
        raise R2LabPhysicalHelmError(
            f"physical configuration must contain exactly one {key} value"
        )
    return int(matches[0])


def _number_after(text: str, key: str) -> float:
    matches = re.findall(
        rf"(?m)^\s*{re.escape(key)}:\s*([0-9]+(?:\.[0-9]+)?)(?:\s+#.*)?\s*$",
        text,
    )
    if len(matches) != 1:
        raise R2LabPhysicalHelmError(
            f"physical configuration must contain exactly one {key} value"
        )
    return float(matches[0])


def _text_after(text: str, key: str) -> str:
    matches = re.findall(
        rf"(?m)^\s*{re.escape(key)}:\s*([^#\r\n]+?)(?:\s+#.*)?\s*$",
        text,
    )
    if len(matches) != 1:
        raise R2LabPhysicalHelmError(
            f"physical configuration must contain exactly one {key} value"
        )
    return matches[0].strip().strip("\"'")


def _pinned_radio_values(text: str) -> _PinnedRadioValues:
    if _text_after(text, "device_driver") != "uhd":
        raise R2LabPhysicalHelmError(
            "pinned R2Lab configuration must use the UHD radio driver"
        )
    return _PinnedRadioValues(
        carrier_arfcn=_integer_after(text, "dl_arfcn"),
        band=_integer_after(text, "band"),
        channel_bandwidth_mhz=_integer_after(text, "channel_bandwidth_MHz"),
        common_scs_khz=_integer_after(text, "common_scs"),
        sample_rate_mhz=_number_after(text, "srate"),
        tx_gain_db=_integer_after(text, "tx_gain"),
        rx_gain_db=_integer_after(text, "rx_gain"),
        ss0_index=_integer_after(text, "ss0_index"),
        coreset0_index=_integer_after(text, "coreset0_index"),
        prach_config_index=_integer_after(text, "prach_config_index"),
        device_args=_text_after(text, "device_args"),
    )


def _device_address(device_args: str) -> str:
    match = re.search(r"(?:^|,)addr=([^,]+)", device_args)
    if match is None:
        raise R2LabPhysicalHelmError(
            "pinned R2Lab device arguments do not contain an N300 address"
        )
    return match.group(1)


def validate_physical_helm_render(
    *,
    text: str,
    bundle: PhysicalChartBundle,
    source_values_text: str,
    source_values_sha256: str,
) -> PhysicalHelmRenderEvidence:
    if not text.strip():
        raise R2LabPhysicalHelmError("Helm rendered no physical chart output")
    _validate_sha256_digest(
        source_values_sha256,
        "source values digest",
        R2LabPhysicalHelmError,
    )
    if hashlib.sha256(source_values_text.encode("utf-8")).hexdigest() != source_values_sha256:
        raise R2LabPhysicalHelmError(
            "pinned R2Lab source values changed after workspace review"
        )
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

    source_radio = _pinned_radio_values(source_values_text)
    rendered_radio = _pinned_radio_values(text)
    if rendered_radio.comparable() != source_radio.comparable():
        raise R2LabPhysicalHelmError(
            "rendered physical radio values differ from the pinned R2Lab source"
        )
    expected_n300 = bundle.review.get("n300_address")
    if (
        not isinstance(expected_n300, str)
        or _device_address(rendered_radio.device_args) != expected_n300
    ):
        raise R2LabPhysicalHelmError(
            "pinned R2Lab N300 address does not match the authorized binding"
        )

    lowered = text.lower()
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
        source_values_sha256=source_values_sha256,
        replicas=replicas,
        strategy="Recreate",
        image_reference=expected_image,
        carrier_arfcn=rendered_radio.carrier_arfcn,
        band=rendered_radio.band,
        channel_bandwidth_mhz=rendered_radio.channel_bandwidth_mhz,
        common_scs_khz=rendered_radio.common_scs_khz,
        sample_rate_mhz=rendered_radio.sample_rate_mhz,
        tx_gain_db=rendered_radio.tx_gain_db,
        rx_gain_db=rendered_radio.rx_gain_db,
        ss0_index=rendered_radio.ss0_index,
        coreset0_index=rendered_radio.coreset0_index,
        prach_config_index=rendered_radio.prach_config_index,
        device_args_sha256=hashlib.sha256(
            rendered_radio.device_args.encode("utf-8")
        ).hexdigest(),
    )


def render_physical_chart_offline(
    *,
    lock: DependencyLock,
    bundle: PhysicalChartBundle,
    workspace: PhysicalChartWorkspace,
    runner: Runner,
    helm_executable: str | Path = "helm",
    timeout_seconds: int = 60,
) -> tuple[str, PhysicalHelmRenderEvidence]:
    if timeout_seconds < 1 or timeout_seconds > 300:
        raise R2LabPhysicalHelmError("offline Helm timeout must be between 1 and 300 seconds")
    expected_version = _locked_helm_version(lock, R2LabPhysicalHelmError)
    helm_command = str(helm_executable)
    try:
        version_result = runner((helm_command, "version", "--short"), timeout_seconds)
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
        helm_command,
        "template",
        RELEASE,
        str(workspace.chart_root),
        "--namespace",
        NAMESPACE,
        "--values",
        str(workspace.source_values_file),
        "--values",
        str(workspace.values_file),
    )
    try:
        result = runner(command, timeout_seconds)
    except Exception as exc:
        raise R2LabPhysicalHelmError("offline Helm template command failed") from exc
    if result.returncode != 0:
        raise R2LabPhysicalHelmError("offline Helm template command returned nonzero")
    try:
        source_values_bytes = workspace.source_values_file.read_bytes()
        source_values_text = source_values_bytes.decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise R2LabPhysicalHelmError(
            "pinned R2Lab source values could not be read"
        ) from exc
    evidence = validate_physical_helm_render(
        text=result.stdout,
        bundle=bundle,
        source_values_text=source_values_text,
        source_values_sha256=workspace.source_values_sha256,
    )
    return result.stdout, evidence


@dataclass(frozen=True)
class PhysicalChartArtifact:
    run_id: str
    package_path: Path
    source_values_path: Path
    values_path: Path
    package_sha256: str
    source_values_sha256: str
    values_sha256: str

    def to_dict(self) -> dict[str, str]:
        return {
            "run_id": self.run_id,
            "package_file": self.package_path.name,
            "package_sha256": self.package_sha256,
            "source_values_file": self.source_values_path.name,
            "source_values_sha256": self.source_values_sha256,
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
    if (
        not chart_root.is_dir()
        or not workspace.source_values_file.is_file()
        or not workspace.values_file.is_file()
    ):
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
    source_values_path = destination / SOURCE_VALUES_FILE_NAME
    values_path = destination / VALUES_FILE_NAME
    if package_path.exists() or source_values_path.exists() or values_path.exists():
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
        source_values_path.write_bytes(workspace.source_values_file.read_bytes())
        values_path.write_bytes(workspace.values_file.read_bytes())
    except OSError as exc:
        package_path.unlink(missing_ok=True)
        source_values_path.unlink(missing_ok=True)
        values_path.unlink(missing_ok=True)
        raise R2LabPhysicalArtifactError("unable to package physical chart workspace") from exc
    source_values_sha256 = _artifact_sha256_file(source_values_path)
    if source_values_sha256 != workspace.source_values_sha256:
        package_path.unlink(missing_ok=True)
        source_values_path.unlink(missing_ok=True)
        values_path.unlink(missing_ok=True)
        raise R2LabPhysicalArtifactError(
            "copied R2Lab source values do not match the pinned workspace digest"
        )
    values_sha256 = _artifact_sha256_file(values_path)
    if values_sha256 != workspace.values_sha256:
        package_path.unlink(missing_ok=True)
        source_values_path.unlink(missing_ok=True)
        values_path.unlink(missing_ok=True)
        raise R2LabPhysicalArtifactError(
            "copied physical chart values do not match the reviewed workspace digest"
        )
    return PhysicalChartArtifact(
        run_id=validated_run_id,
        package_path=package_path,
        source_values_path=source_values_path,
        values_path=values_path,
        package_sha256=_artifact_sha256_file(package_path),
        source_values_sha256=source_values_sha256,
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
    if not isinstance(amf, dict):
        raise R2LabPhysicalChartError(
            "stopped physical Helm values do not contain complete network bindings"
        )
    n300_address = usrp.get("address") if isinstance(usrp, dict) else None
    if not isinstance(n300_address, str) and isinstance(ru_sdr, dict):
        device_args = ru_sdr.get("device_args")
        match = (
            re.search(r"(?:^|,)addr=([^,]+)", device_args)
            if isinstance(device_args, str)
            else None
        )
        n300_address = match.group(1) if match is not None else None
    if not isinstance(n300_address, str) or not n300_address:
        raise R2LabPhysicalChartError(
            "stopped physical Helm values do not contain the N300 binding"
        )
    gnb_address = values.get("gnbIp")
    if gnb_address != amf.get("bind_addr"):
        raise R2LabPhysicalChartError(
            "stopped physical Helm values disagree on the gNB N2 address"
        )
    ru_subnet = ipam.get("subnet") if isinstance(ipam, dict) else None
    if not isinstance(ru_subnet, str) or not ru_subnet:
        ru_subnet = values.get("ruSubnet")
    ru_master = usrp.get("master") if isinstance(usrp, dict) else None
    if not isinstance(ru_master, str) or not ru_master:
        ru_master = "r2lab_usrp"
    try:
        raw_bindings = (
            amf["addr"],
            gnb_address,
            n300_address,
            values["ruPodIp"],
            ru_subnet,
            values.get("n3networkName"),
            ru_master,
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
        node_name=CURRENT_RAN_NODE,
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
    helm_executable: Path,
    run_id: str,
    known_hosts: Path,
    runner: Runner,
    authority_verifier: Callable[[], object],
    timeout_seconds: int = DEFAULT_STAGING_TIMEOUT_SECONDS,
) -> PhysicalStagingResult:
    try:
        run_id = validate_run_id(run_id)
    except Exception as exc:
        raise R2LabPhysicalStagingError(str(exc)) from exc
    if artifact.run_id != run_id:
        raise R2LabPhysicalStagingError("physical artifact run ID does not match staging run")
    if render_evidence.replicas != 0 or render_evidence.strategy != "Recreate":
        raise R2LabPhysicalStagingError(
            "physical render evidence is not stopped and singleton-safe"
        )
    if render_evidence.source_values_sha256 != artifact.source_values_sha256:
        raise R2LabPhysicalStagingError(
            "physical render evidence does not match the pinned R2Lab source values"
        )
    if timeout_seconds < 30 or timeout_seconds > 600:
        raise R2LabPhysicalStagingError(
            "staging timeout must be between 30 and 600 seconds"
        )
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabPhysicalStagingError("strict SLICES known-hosts file is missing")
    if (
        not artifact.package_path.is_file()
        or not artifact.source_values_path.is_file()
        or not artifact.values_path.is_file()
    ):
        raise R2LabPhysicalStagingError("physical artifact files are missing")
    helm_executable = helm_executable.expanduser().resolve()
    if (
        not helm_executable.is_file()
        or helm_executable.is_symlink()
        or not os.access(helm_executable, os.X_OK)
    ):
        raise R2LabPhysicalStagingError(
            "locked Helm executable is missing or unsafe"
        )
    if _staging_sha256_file(artifact.package_path) != artifact.package_sha256:
        raise R2LabPhysicalStagingError("physical chart package digest changed after review")
    if (
        _staging_sha256_file(artifact.source_values_path)
        != artifact.source_values_sha256
    ):
        raise R2LabPhysicalStagingError(
            "pinned R2Lab source values digest changed after review"
        )
    if _staging_sha256_file(artifact.values_path) != artifact.values_sha256:
        raise R2LabPhysicalStagingError("physical chart values digest changed after review")

    try:
        authority_verifier()
    except Exception as exc:
        raise R2LabPhysicalStagingError("fresh physical authority was not proven") from exc

    remote_root = f"/root/.synthran/{run_id}/physical-chart"
    remote_package = f"{remote_root}/{artifact.package_path.name}"
    remote_source_values = f"{remote_root}/{artifact.source_values_path.name}"
    remote_values = f"{remote_root}/{artifact.values_path.name}"
    remote_helm = f"{remote_root}/helm"
    helm_sha256 = _staging_sha256_file(helm_executable)
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
            str(artifact.source_values_path),
            str(artifact.values_path),
            str(helm_executable),
            f"root@{CORE_NODE}:{remote_root}/",
        ),
        timeout_seconds,
        "strict physical artifact transfer",
    )
    hashes = _checked(
        runner,
        _ssh(
            known_hosts,
            "sha256sum",
            remote_package,
            remote_source_values,
            remote_values,
            remote_helm,
        ),
        min(timeout_seconds, 60),
        "remote physical artifact digest verification",
    ).stdout
    if (
        artifact.package_sha256 not in hashes
        or artifact.source_values_sha256 not in hashes
        or artifact.values_sha256 not in hashes
        or helm_sha256 not in hashes
    ):
        raise R2LabPhysicalStagingError(
            "remote physical artifact digests do not match review"
        )

    _checked(
        runner,
        _ssh(known_hosts, "chmod", "0755", remote_helm),
        min(timeout_seconds, 60),
        "remote locked Helm permission preparation",
    )
    helm_version = _checked(
        runner,
        _ssh(known_hosts, remote_helm, "version", "--short"),
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
        authority_verifier()
    except Exception as exc:
        raise R2LabPhysicalStagingError(
            "physical authority changed before Helm staging"
        ) from exc

    _checked(
        runner,
        _ssh(
            known_hosts,
            remote_helm,
            "upgrade",
            "--install",
            RELEASE,
            remote_package,
            "--namespace",
            NAMESPACE,
            "--values",
            remote_source_values,
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
    known_hosts: Path,
    runner: Runner,
    authority_verifier: Callable[[], object],
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

    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabPhysicalStartError("strict SLICES known-hosts file is missing")

    try:
        authority_verifier()
    except Exception as exc:
        raise R2LabPhysicalStartError(
            "fresh physical authority was not proven for gNB start"
        ) from exc

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
        try:
            authority_verifier()
        except Exception as exc:
            raise R2LabPhysicalStartError(
                "physical authority changed before gNB ownership start"
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
    allocation_id: str | None,
    known_hosts: Path,
    runner: Runner,
    sleeper: Sleeper,
    timeout_seconds: int = DEFAULT_STAGING_TIMEOUT_SECONDS,
    shutdown_attempts: int = DEFAULT_POLL_ATTEMPTS,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
) -> PhysicalGnbStopResult:
    """Stop only the artifact-bound gNB Deployment owned by one physical run."""

    staging = PhysicalStagingResult.from_dict(staging.to_dict())
    if timeout_seconds < 30 or timeout_seconds > 600:
        raise R2LabPhysicalStartError(
            "physical gNB stop timeout must be between 30 and 600 seconds"
        )
    if shutdown_attempts < 1 or poll_interval_seconds < 0:
        raise R2LabPhysicalStartError("physical gNB stop wait settings are invalid")
    try:
        owner = _validate_authority(owner, "owner")
        if allocation_id is not None:
            allocation_id = _validate_authority(allocation_id, "allocation ID")
    except R2LabPhysicalStagingError as exc:
        raise R2LabPhysicalStartError(str(exc)) from exc
    known_hosts = known_hosts.expanduser().resolve()
    if not known_hosts.is_file():
        raise R2LabPhysicalStartError("strict SLICES known-hosts file is missing")

    try:
        allocation_id = verify_physical_allocation(
            runner=runner,
            allocation_id=allocation_id,
            owner=owner,
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:
        raise R2LabPhysicalStartError(
            "fresh SLICES allocation authority was not proven for gNB stop"
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
