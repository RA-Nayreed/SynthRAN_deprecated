"""Public physical R2Lab deployment API with the live-proven CPU contract.

The pre-existing physical deployment implementation remains in
``_deployment_impl`` unchanged.  This module wraps only the two chart-rendering
boundaries implicated by r2lab-smoke-004: the physical gNB must request whole,
exclusive-CPU-manager-eligible resources, and offline Helm validation must
prove that contract before an artifact can be staged.

No radio, PRACH, TDD, authority, singleton-lifecycle, or cleanup semantics are
changed here.
"""

from __future__ import annotations

from copy import deepcopy
import re

from . import _deployment_impl as _impl
from ._deployment_impl import *  # noqa: F401,F403 - preserve the established public API


# Live-proven r2lab-smoke-004 contract.
# sopnode-f3 uses kubelet static CPU management with full-pcpus-only and
# reservedSystemCPUs=0-15.  The old physical gNB rendered resources.define=false,
# producing a BestEffort pod with an unrestricted 0-63 cpuset while RF late and
# underflow events accumulated.  Equal integer CPU requests/limits make the
# single-container gNB eligible for Guaranteed QoS and exclusive CPU allocation.
PHYSICAL_GNB_CPU_COUNT = 8
PHYSICAL_GNB_MEMORY = "4Gi"


def _reviewed_resource_values() -> dict[str, object]:
    """Return the exact value shape required by the pinned srsran-helm template."""

    quantity = {
        "cpu": str(PHYSICAL_GNB_CPU_COUNT),
        "memory": PHYSICAL_GNB_MEMORY,
    }
    # The pinned 8dfb989 template names this nested key ``tcpdump`` even for the
    # gNB container.  Preserve that reviewed upstream shape instead of silently
    # changing the dependency.
    return {
        "define": True,
        "requests": {"tcpdump": dict(quantity)},
        "limits": {"tcpdump": dict(quantity)},
    }


def _expected_resource_contract(
    bundle: PhysicalChartBundle,
) -> tuple[str, str]:
    resources = bundle.values.get("resources")
    if not isinstance(resources, dict) or resources.get("define") is not True:
        raise R2LabPhysicalHelmError(
            "physical gNB resource contract must be enabled"
        )

    requests = resources.get("requests")
    limits = resources.get("limits")
    request = requests.get("tcpdump") if isinstance(requests, dict) else None
    limit = limits.get("tcpdump") if isinstance(limits, dict) else None
    if not isinstance(request, dict) or not isinstance(limit, dict):
        raise R2LabPhysicalHelmError(
            "physical gNB resource contract does not match the pinned chart shape"
        )

    request_cpu = request.get("cpu")
    limit_cpu = limit.get("cpu")
    request_memory = request.get("memory")
    limit_memory = limit.get("memory")
    expected_cpu = str(PHYSICAL_GNB_CPU_COUNT)
    if (
        request_cpu != expected_cpu
        or limit_cpu != expected_cpu
        or request_memory != PHYSICAL_GNB_MEMORY
        or limit_memory != PHYSICAL_GNB_MEMORY
    ):
        raise R2LabPhysicalHelmError(
            "physical gNB must request and limit the reviewed whole-CPU/memory quantities"
        )
    if not expected_cpu.isdigit() or int(expected_cpu) < 1:
        raise R2LabPhysicalHelmError(
            "physical gNB CPU quantity must be a positive whole CPU count"
        )
    return expected_cpu, PHYSICAL_GNB_MEMORY


def build_physical_chart_bundle(
    *,
    lock: DependencyLock,
    plan: R2LabPhysicalDeploymentPlan,
    bindings: PhysicalChartBindings,
) -> PhysicalChartBundle:
    """Build the established physical chart plus the smoke-004 CPU contract."""

    base = _impl.build_physical_chart_bundle(
        lock=lock,
        plan=plan,
        bindings=bindings,
    )
    values = deepcopy(dict(base.values))
    review = deepcopy(dict(base.review))
    values["resources"] = _reviewed_resource_values()
    review.update(
        {
            "guaranteed_qos_requested": True,
            "exclusive_cpu_manager_eligible": True,
            "exclusive_cpu_count": PHYSICAL_GNB_CPU_COUNT,
            "memory_request_limit": PHYSICAL_GNB_MEMORY,
            "resource_contract_evidence": "r2lab-smoke-004",
        }
    )
    return _impl.PhysicalChartBundle(
        run_id=base.run_id,
        chart_commit=base.chart_commit,
        chart_path=base.chart_path,
        values=values,
        review=review,
    )


def validate_physical_helm_render(
    *, text: str, bundle: PhysicalChartBundle
) -> PhysicalHelmRenderEvidence:
    """Validate the established render and prove the Guaranteed-QoS contract."""

    evidence = _impl.validate_physical_helm_render(text=text, bundle=bundle)
    expected_cpu, expected_memory = _expected_resource_contract(bundle)

    # The pinned chart emits exactly one gNB container resource block in this
    # physical path (the optional logs sidecar is disabled).  Match that block as
    # a unit so unrelated YAML fields cannot satisfy the proof accidentally.
    resource_pattern = re.compile(
        r"(?ms)^\s*resources:\s*\n"
        r"\s*requests:\s*\n"
        r"\s*memory:\s*[\"']?([^\"'\s]+)[\"']?\s*\n"
        r"\s*cpu:\s*[\"']?([^\"'\s]+)[\"']?\s*\n"
        r"\s*limits:\s*\n"
        r"\s*memory:\s*[\"']?([^\"'\s]+)[\"']?\s*\n"
        r"\s*cpu:\s*[\"']?([^\"'\s]+)[\"']?\s*$"
    )
    matches = resource_pattern.findall(text)
    if len(matches) != 1:
        raise R2LabPhysicalHelmError(
            "rendered physical gNB must contain exactly one reviewed resource block"
        )

    request_memory, request_cpu, limit_memory, limit_cpu = matches[0]
    if (
        request_cpu != expected_cpu
        or limit_cpu != expected_cpu
        or request_memory != expected_memory
        or limit_memory != expected_memory
    ):
        raise R2LabPhysicalHelmError(
            "rendered physical gNB resource requests and limits do not match"
        )
    return evidence


def render_physical_chart_offline(
    *,
    lock: DependencyLock,
    bundle: PhysicalChartBundle,
    workspace: PhysicalChartWorkspace,
    runner: Runner,
    timeout_seconds: int = 60,
) -> tuple[str, PhysicalHelmRenderEvidence]:
    """Run the established offline render, then enforce the CPU resource proof."""

    text, evidence = _impl.render_physical_chart_offline(
        lock=lock,
        bundle=bundle,
        workspace=workspace,
        runner=runner,
        timeout_seconds=timeout_seconds,
    )
    validate_physical_helm_render(text=text, bundle=bundle)
    return text, evidence
