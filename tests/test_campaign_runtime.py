from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from synthran.research import CampaignCondition, ResearchError, build_campaign
from synthran.research.amber_campaign import execute_amber_campaign
from synthran.research.campaign_runtime import CampaignRuntimeSession, StableRuntime
from synthran.research.v2 import RESEARCH_SUMMARY_SCHEMA_V2
from synthran.rfsim_runtime import RfsimRuntimeState


class _RuntimeContext:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc, tb):
        self.exited = True
        return False


def _campaign():
    return build_campaign(
        campaign_id="campaign-test-01",
        network_run_id="network-test-01",
        seeds=(424242,),
        conditions=(
            CampaignCondition("baseline"),
            CampaignCondition("load50", load_fraction=0.5),
        ),
        campaign_seed=7,
    )


class CampaignRuntimeRenderingTests(unittest.TestCase):
    def test_edge_patch_is_campaign_stable(self) -> None:
        campaign = _campaign()
        with tempfile.TemporaryDirectory() as temporary:
            session = CampaignRuntimeSession(
                campaign=campaign,
                inventory=object(),
                lock=object(),
                run_root=Path(temporary),
            )
            session._original_amber_edge_patch = (
                lambda scenario, *, lock, core_address: {
                    "spec": {
                        "template": {
                            "metadata": {
                                "annotations": {
                                    "synthran.experiment/run": scenario.run_id,
                                }
                            },
                            "spec": {
                                "volumes": [
                                    {
                                        "name": "synthran-experiment-edge-config",
                                        "configMap": {"name": "run-specific"},
                                    }
                                ]
                            },
                        }
                    }
                }
            )
            patches = []
            for scheduled in campaign.runs:
                scenario = SimpleNamespace(run_id=scheduled.run_id)
                patches.append(
                    session._render_edge_patch(
                        scenario,
                        lock=object(),
                        core_address="192.0.2.1",
                    )
                )

        for value in patches:
            template = value["spec"]["template"]
            self.assertEqual(
                template["metadata"]["annotations"]["synthran.experiment/run"],
                campaign.campaign_id,
            )
            self.assertEqual(
                template["spec"]["volumes"][0]["configMap"]["name"],
                session.campaign_edge_config_name,
            )
        self.assertTrue(session.sidecar_patch_requested)

    def test_only_edge_configmap_becomes_campaign_scoped(self) -> None:
        campaign = _campaign()
        scheduled = campaign.runs[0]
        scenario = SimpleNamespace(run_id=scheduled.run_id)
        with tempfile.TemporaryDirectory() as temporary:
            session = CampaignRuntimeSession(
                campaign=campaign,
                inventory=object(),
                lock=object(),
                run_root=Path(temporary),
            )
            expected_edge = f"synthran-exp-edge-placeholder"
            session._original_amber_objects = (
                lambda scenario, *, lock, core_node, core_address: (
                    {
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": expected_edge,
                            "labels": {"synthran.experiment/run": scenario.run_id},
                        },
                    },
                    {
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": "central-config",
                            "labels": {"synthran.experiment/run": scenario.run_id},
                        },
                    },
                    {
                        "kind": "Deployment",
                        "metadata": {
                            "name": "central",
                            "labels": {"synthran.experiment/run": scenario.run_id},
                        },
                    },
                )
            )
            with patch(
                "synthran.research.campaign_runtime.names",
                return_value={"edge_config": expected_edge},
            ):
                objects = session._render_experiment_objects(
                    scenario,
                    lock=object(),
                    core_node="core",
                    core_address="192.0.2.1",
                )

        self.assertEqual(objects[0]["metadata"]["name"], session.campaign_edge_config_name)
        self.assertEqual(
            objects[0]["metadata"]["labels"]["synthran.experiment/run"],
            campaign.campaign_id,
        )
        self.assertEqual(
            objects[1]["metadata"]["labels"]["synthran.experiment/run"],
            scheduled.run_id,
        )
        self.assertEqual(
            objects[2]["metadata"]["labels"]["synthran.experiment/run"],
            scheduled.run_id,
        )


class CampaignRuntimeIdentityTests(unittest.TestCase):
    def test_reconcile_happens_once_then_observes_identity(self) -> None:
        campaign = _campaign()
        with tempfile.TemporaryDirectory() as temporary:
            session = CampaignRuntimeSession(
                campaign=campaign,
                inventory=object(),
                lock=object(),
                run_root=Path(temporary),
            )
            session._original_amber_reconcile = MagicMock(
                return_value=RfsimRuntimeState(
                    ue_pod="ue-a",
                    gnb_pod="gnb-a",
                    gnb_deployment="gnb-deployment",
                    pdu_address="12.1.0.2",
                )
            )
            inventory = object()
            first = session._reconcile_runtime(
                inventory,
                network_run_id=campaign.network_run_id,
            )
            with (
                patch(
                    "synthran.research.campaign_runtime._discover_pod",
                    side_effect=["ue-a", "gnb-a"],
                ),
                patch(
                    "synthran.research.campaign_runtime._deployment_owner_for_pod",
                    return_value="gnb-deployment",
                ),
                patch(
                    "synthran.research.campaign_runtime._current_pdu_address",
                    return_value="12.1.0.2",
                ),
            ):
                second = session._reconcile_runtime(
                    inventory,
                    network_run_id=campaign.network_run_id,
                )

        self.assertEqual(first, second)
        session._original_amber_reconcile.assert_called_once_with(
            inventory,
            network_run_id=campaign.network_run_id,
        )

    def test_identity_drift_fails_closed(self) -> None:
        campaign = _campaign()
        with tempfile.TemporaryDirectory() as temporary:
            session = CampaignRuntimeSession(
                campaign=campaign,
                inventory=object(),
                lock=object(),
                run_root=Path(temporary),
            )
            session.stable = StableRuntime(
                ue_pod="ue-a",
                gnb_pod="gnb-a",
                gnb_deployment="gnb-deployment",
                pdu_address="12.1.0.2",
            )
            with (
                patch(
                    "synthran.research.campaign_runtime._discover_pod",
                    side_effect=["ue-b", "gnb-a"],
                ),
                patch(
                    "synthran.research.campaign_runtime._deployment_owner_for_pod",
                    return_value="gnb-deployment",
                ),
                patch(
                    "synthran.research.campaign_runtime._current_pdu_address",
                    return_value="12.1.0.4",
                ),
                self.assertRaisesRegex(ResearchError, "identity drift"),
            ):
                session._reconcile_runtime(
                    object(),
                    network_run_id=campaign.network_run_id,
                )


class CampaignMqttReloadTests(unittest.TestCase):
    def test_reload_uses_sighup_without_container_restart(self) -> None:
        campaign = _campaign()
        with tempfile.TemporaryDirectory() as temporary:
            session = CampaignRuntimeSession(
                campaign=campaign,
                inventory=object(),
                lock=object(),
                run_root=Path(temporary),
            )
            session.current_run_id = campaign.runs[0].run_id
            session.stable = StableRuntime(
                ue_pod="ue-a",
                gnb_pod="gnb-a",
                gnb_deployment="gnb-deployment",
                pdu_address="12.1.0.2",
            )
            with (
                patch(
                    "synthran.research.campaign_runtime.amber_runtime._edge_sidecar_status",
                    side_effect=[
                        (4, True, True, True),
                        (4, True, True, True),
                    ],
                ),
                patch("synthran.research.campaign_runtime.base_runtime._remote") as remote,
            ):
                session._reload_edge_sidecar_and_wait(object(), "ue-a", timeout_seconds=1)

        command = " ".join(str(part) for part in remote.call_args.args[1:])
        self.assertIn("kill -HUP 1", command)
        self.assertNotIn("kill -TERM 1", command)
        self.assertEqual(session.reloads[0]["restart_count"], 4)
        self.assertEqual(session.reloads[0]["method"], "SIGHUP")

    def test_reload_fails_if_container_restarts(self) -> None:
        campaign = _campaign()
        with tempfile.TemporaryDirectory() as temporary:
            session = CampaignRuntimeSession(
                campaign=campaign,
                inventory=object(),
                lock=object(),
                run_root=Path(temporary),
            )
            with (
                patch(
                    "synthran.research.campaign_runtime.amber_runtime._edge_sidecar_status",
                    side_effect=[
                        (4, True, True, True),
                        (5, True, True, True),
                    ],
                ),
                patch("synthran.research.campaign_runtime.base_runtime._remote"),
                self.assertRaisesRegex(ResearchError, "restarted during in-place"),
            ):
                session._reload_edge_sidecar_and_wait(object(), "ue-a", timeout_seconds=1)


class CampaignRuntimeCleanupTests(unittest.TestCase):
    def test_final_cleanup_restores_sidecar_once_and_reproves_network(self) -> None:
        campaign = _campaign()
        inventory = object()
        lock = object()
        with tempfile.TemporaryDirectory() as temporary:
            session = CampaignRuntimeSession(
                campaign=campaign,
                inventory=inventory,
                lock=lock,
                run_root=Path(temporary),
            )
            session.sidecar_patch_requested = True
            session.campaign_resources_requested = True
            session._original_base_reconcile = MagicMock()
            report = SimpleNamespace(ready=True, checks=())

            with (
                patch(
                    "synthran.research.campaign_runtime.base_runtime._discover_ue_deployment",
                    return_value="ue-deployment",
                ),
                patch(
                    "synthran.research.campaign_runtime.base_runtime._kubectl_patch_deployment"
                ) as patch_deployment,
                patch(
                    "synthran.research.campaign_runtime.base_runtime._wait_rollout"
                ) as wait_rollout,
                patch(
                    "synthran.research.campaign_runtime.base_runtime._delete_experiment_objects"
                ) as delete_objects,
                patch(
                    "synthran.research.campaign_runtime.base_runtime.verify_network_path",
                    return_value=report,
                ) as verify,
            ):
                session._restore_base_runtime()

        patch_deployment.assert_called_once()
        wait_rollout.assert_called_once_with(
            inventory,
            "ue-deployment",
            label="campaign srsUE cleanup rollout",
        )
        session._original_base_reconcile.assert_called_once_with(
            inventory,
            network_run_id=campaign.network_run_id,
        )
        delete_objects.assert_called_once_with(inventory, campaign.campaign_id)
        verify.assert_called_once_with(
            inventory=inventory,
            lock=lock,
            run_id=campaign.network_run_id,
            timeout_seconds=120,
        )


class AmberCampaignScopeTests(unittest.TestCase):
    def test_campaign_execution_owns_runtime_scope(self) -> None:
        campaign = build_campaign(
            campaign_id="campaign-scope-01",
            network_run_id="network-test-01",
            seeds=(424242,),
            conditions=(CampaignCondition("baseline"),),
            campaign_seed=1,
        )
        context = _RuntimeContext()
        scheduled = campaign.runs[0]

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)

            def execute(**kwargs):
                path = root / scheduled.run_id / "research-summary-v2.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(
                    json.dumps(
                        {
                            "schema": RESEARCH_SUMMARY_SCHEMA_V2,
                            "iot_source": "amber",
                            "iot_profile": "ambient-v1",
                            "profile_digest": "0" * 64,
                            "iot_seed": scheduled.seed,
                            "condition": scheduled.condition,
                            "energy_treatment": {
                                "external_power_scale": 1.0,
                                "node_variation_fraction": 0.0,
                            },
                        }
                    ),
                    encoding="utf-8",
                )
                return path

            with (
                patch(
                    "synthran.research.amber_campaign.CampaignRuntimeSession",
                    return_value=context,
                ) as runtime,
                patch(
                    "synthran.research.amber_campaign.execute_amber_research_experiment",
                    side_effect=execute,
                ),
            ):
                result = execute_amber_campaign(
                    campaign=campaign,
                    iot_profile="ambient-v1",
                    inventory=object(),
                    lock=object(),
                    dependency_root=root,
                    network_manifest=root / "manifest.json",
                    network_evidence=root / "evidence.json",
                    repository_root=root,
                    run_root=root,
                    target="192.0.2.10",
                    reference_capacity_bps=None,
                    sensor_period_seconds=10,
                    measurement=SimpleNamespace(),
                    parallel_flows=1,
                    load_port=5201,
                )

        self.assertTrue(context.entered)
        self.assertTrue(context.exited)
        runtime.assert_called_once()
        self.assertEqual(result.name, "campaign-scope-01-amber-campaign-v2.json")


if __name__ == "__main__":
    unittest.main()
