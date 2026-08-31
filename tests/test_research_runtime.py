from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from synthran.live_preflight import CommandResult
from synthran.research import ResearchError, load_jsonl
from synthran.research.instrumentation import (
    _base_cleanup_reproved,
    _extract_iperf_bps,
    _install_target_route,
    _parse_load_log,
    _parse_probe_log,
    _prove_target_route,
    _remove_target_route,
)
from synthran.research.runtime import calibrate_capacity


class IperfParsingTests(unittest.TestCase):
    def test_sum_received_is_preferred(self) -> None:
        value = {
            "end": {
                "sum_received": {"bits_per_second": 8_100_000.0},
                "sum_sent": {"bits_per_second": 8_000_000.0},
            }
        }
        self.assertEqual(_extract_iperf_bps(value), 8_100_000.0)

    def test_streams_are_summed_when_aggregate_is_absent(self) -> None:
        value = {
            "end": {
                "streams": [
                    {"receiver": {"bits_per_second": 2_000_000.0}},
                    {"receiver": {"bits_per_second": 3_000_000.0}},
                ]
            }
        }
        self.assertEqual(_extract_iperf_bps(value), 5_000_000.0)

    def test_load_log_becomes_structured_jsonl_with_aggregate_target(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            log = root / "load.log"
            destination = root / "load.jsonl"
            log.write_text(
                "prefix\n"
                + json.dumps(
                    {
                        "end": {
                            "sum_received": {
                                "bits_per_second": 7_950_000
                            }
                        }
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            _parse_load_log(
                log,
                destination,
                target_bps=8_000_000,
                protocol="udp",
            )
            records = load_jsonl(
                destination,
                schema="synthran/research-load-result/v1alpha1",
            )
            self.assertEqual(records[0]["bits_per_second"], 7_950_000.0)
            self.assertEqual(records[0]["target_bps"], 8_000_000)


class ProbeParsingTests(unittest.TestCase):
    def test_probe_log_records_internal_sequence_timeouts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            log = root / "ping.log"
            destination = root / "probe.jsonl"
            log.write_text(
                "[1000.000000] 64 bytes from 192.0.2.1: icmp_seq=1 ttl=64 time=11.2 ms\n"
                "[1002.000000] 64 bytes from 192.0.2.1: icmp_seq=3 ttl=64 time=13.4 ms\n",
                encoding="utf-8",
            )
            _parse_probe_log(log, destination, interval_seconds=1.0)
            records = load_jsonl(
                destination,
                schema="synthran/research-probe/v1alpha1",
            )
            self.assertEqual(len(records), 3)
            self.assertFalse(records[0]["timeout"])
            self.assertTrue(records[1]["timeout"])
            self.assertEqual(records[2]["rtt_ms"], 13.4)

    def test_probe_window_infers_leading_and_trailing_timeouts(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            log = root / "ping.log"
            destination = root / "probe.jsonl"
            log.write_text(
                "[1001.000000] 64 bytes from 192.0.2.1: icmp_seq=2 ttl=64 time=10.0 ms\n"
                "[1003.000000] 64 bytes from 192.0.2.1: icmp_seq=4 ttl=64 time=12.0 ms\n",
                encoding="utf-8",
            )
            _parse_probe_log(
                log,
                destination,
                interval_seconds=1.0,
                window_started_at_utc=datetime.fromtimestamp(
                    1000.0, timezone.utc
                ),
                window_ended_at_utc=datetime.fromtimestamp(
                    1004.0, timezone.utc
                ),
            )
            records = load_jsonl(
                destination,
                schema="synthran/research-probe/v1alpha1",
            )
            self.assertEqual(
                [record["sequence"] for record in records],
                [1, 2, 3, 4, 5],
            )
            self.assertEqual(
                [record["timeout"] for record in records],
                [True, False, True, False, True],
            )


class RuntimeSafetyTests(unittest.TestCase):
    def test_route_proof_rejects_non_ue_path(self) -> None:
        inventory = object()
        with (
            patch(
                "synthran.research.instrumentation._kubectl_exec_command",
                return_value=("ssh",),
            ),
            patch(
                "synthran.research.instrumentation.base_runtime._run",
                return_value=CommandResult(
                    0,
                    "192.0.2.1 dev eth0 src 10.0.0.2\n",
                    "",
                ),
            ),
        ):
            with self.assertRaisesRegex(Exception, "tun_srsue1"):
                _prove_target_route(
                    inventory,
                    "ue-pod",
                    pdu_address="12.1.0.8",
                    target="192.0.2.1",
                )

    def test_route_proof_accepts_exact_ue_tunnel(self) -> None:
        inventory = object()
        with (
            patch(
                "synthran.research.instrumentation._kubectl_exec_command",
                return_value=("ssh",),
            ),
            patch(
                "synthran.research.instrumentation.base_runtime._run",
                return_value=CommandResult(
                    0,
                    "192.0.2.1 from 12.1.0.8 dev tun_srsue1 src 12.1.0.8\n",
                    "",
                ),
            ),
        ):
            _prove_target_route(
                inventory,
                "ue-pod",
                pdu_address="12.1.0.8",
                target="192.0.2.1",
            )

    def test_route_install_reuses_already_proven_tunnel_without_claiming_it(self) -> None:
        with (
            patch(
                "synthran.research.instrumentation._kubectl_exec_command",
                side_effect=lambda _inventory, _pod, *command: tuple(command),
            ),
            patch(
                "synthran.research.instrumentation.base_runtime._run",
                return_value=CommandResult(
                    0,
                    "192.0.2.1 from 12.1.0.8 dev tun_srsue1 src 12.1.0.8\n",
                    "",
                ),
            ) as run,
        ):
            installed = _install_target_route(
                object(),
                "ue-pod",
                pdu_address="12.1.0.8",
                target="192.0.2.1",
            )
        self.assertFalse(installed)
        self.assertEqual(run.call_count, 1)

    def test_route_install_adds_exact_prefix_and_proves_tunnel(self) -> None:
        with (
            patch(
                "synthran.research.instrumentation._kubectl_exec_command",
                side_effect=lambda _inventory, _pod, *command: tuple(command),
            ),
            patch(
                "synthran.research.instrumentation.base_runtime._run",
                side_effect=(
                    CommandResult(
                        0,
                        "192.0.2.1 from 12.1.0.8 via 10.0.0.1 dev eth0\n",
                        "",
                    ),
                    CommandResult(0, "", ""),
                    CommandResult(
                        0,
                        "192.0.2.1 from 12.1.0.8 dev tun_srsue1 src 12.1.0.8\n",
                        "",
                    ),
                ),
            ) as run,
        ):
            installed = _install_target_route(
                object(),
                "ue-pod",
                pdu_address="12.1.0.8",
                target="192.0.2.1",
            )
        self.assertTrue(installed)
        self.assertEqual(
            run.call_args_list[1].args[0],
            ("ip", "route", "add", "192.0.2.1/32", "dev", "tun_srsue1"),
        )
        self.assertNotIn("replace", run.call_args_list[1].args[0])

    def test_route_install_never_replaces_conflicting_exact_state(self) -> None:
        with (
            patch(
                "synthran.research.instrumentation._kubectl_exec_command",
                side_effect=lambda _inventory, _pod, *command: tuple(command),
            ),
            patch(
                "synthran.research.instrumentation.base_runtime._run",
                side_effect=(
                    CommandResult(
                        0,
                        "192.0.2.1 from 12.1.0.8 via 10.0.0.1 dev eth0\n",
                        "",
                    ),
                    CommandResult(2, "", "RTNETLINK answers: File exists"),
                ),
            ) as run,
        ):
            with self.assertRaisesRegex(ResearchError, "without replacing"):
                _install_target_route(
                    object(),
                    "ue-pod",
                    pdu_address="12.1.0.8",
                    target="192.0.2.1",
                )
        self.assertNotIn("replace", run.call_args_list[1].args[0])

    def test_failed_route_proof_removes_the_route_it_created(self) -> None:
        with (
            patch(
                "synthran.research.instrumentation._kubectl_exec_command",
                side_effect=lambda _inventory, _pod, *command: tuple(command),
            ),
            patch(
                "synthran.research.instrumentation.base_runtime._run",
                side_effect=(
                    CommandResult(0, "192.0.2.1 dev eth0\n", ""),
                    CommandResult(0, "", ""),
                    CommandResult(0, "192.0.2.1 dev eth0\n", ""),
                    CommandResult(0, "", ""),
                    CommandResult(0, "192.0.2.1 dev eth0\n", ""),
                ),
            ) as run,
        ):
            with self.assertRaisesRegex(ResearchError, "tun_srsue1"):
                _install_target_route(
                    object(),
                    "ue-pod",
                    pdu_address="12.1.0.8",
                    target="192.0.2.1",
                )
        self.assertEqual(
            run.call_args_list[3].args[0],
            ("ip", "route", "del", "192.0.2.1/32", "dev", "tun_srsue1"),
        )

    def test_owned_route_cleanup_restores_non_tunnel_path(self) -> None:
        with (
            patch(
                "synthran.research.instrumentation._kubectl_exec_command",
                side_effect=lambda _inventory, _pod, *command: tuple(command),
            ),
            patch(
                "synthran.research.instrumentation.base_runtime._run",
                side_effect=(
                    CommandResult(0, "", ""),
                    CommandResult(
                        0,
                        "192.0.2.1 from 12.1.0.8 via 10.0.0.1 dev eth0\n",
                        "",
                    ),
                ),
            ) as run,
        ):
            _remove_target_route(
                object(),
                "ue-pod",
                pdu_address="12.1.0.8",
                target="192.0.2.1",
            )
        self.assertEqual(
            run.call_args_list[0].args[0],
            ("ip", "route", "del", "192.0.2.1/32", "dev", "tun_srsue1"),
        )

    def test_capacity_calibration_releases_route_when_server_start_fails(self) -> None:
        inventory = object()
        lock = object()
        state = SimpleNamespace(ue_pod="ue-pod", pdu_address="12.1.0.8")
        with tempfile.TemporaryDirectory() as temporary:
            with (
                patch(
                    "synthran.research.runtime.verify_network_path",
                    return_value=SimpleNamespace(ready=True),
                ),
                patch(
                    "synthran.research.runtime.reconcile_rfsim_runtime",
                    return_value=state,
                ),
                patch("synthran.research.runtime._check_research_tools"),
                patch(
                    "synthran.research.runtime._install_target_route",
                    return_value=True,
                ),
                patch(
                    "synthran.research.runtime.start_owned_iperf_server",
                    side_effect=ResearchError("server start failed"),
                ),
                patch(
                    "synthran.research.runtime._remove_target_route"
                ) as remove_route,
                self.assertRaisesRegex(ResearchError, "server start failed"),
            ):
                calibrate_capacity(
                    inventory=inventory,
                    lock=lock,
                    network_run_id="network-accepted",
                    target="192.0.2.1",
                    repository_root=Path(temporary),
                    output_path=Path(temporary) / "capacity.json",
                )
        remove_route.assert_called_once_with(
            inventory,
            "ue-pod",
            pdu_address="12.1.0.8",
            target="192.0.2.1",
        )

    def test_cleanup_reproof_requires_persisted_cleanup_pass(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence = root / "experiment-evidence.json"
            evidence.write_text(
                json.dumps(
                    {
                        "checks": [
                            {
                                "name": "cleanup-base-network",
                                "passed": True,
                                "detail": "restored",
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            self.assertTrue(_base_cleanup_reproved(root))
            evidence.write_text(
                json.dumps({"checks": []}), encoding="utf-8"
            )
            self.assertFalse(_base_cleanup_reproved(root))


if __name__ == "__main__":
    unittest.main()
