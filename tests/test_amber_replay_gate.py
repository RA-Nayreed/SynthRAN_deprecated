from __future__ import annotations

import threading
import time
from types import SimpleNamespace
import unittest

from synthran.iot_publisher import (
    AmberReplaySession,
    install_replay_start_gate,
    release_replay_start_gate,
    remove_replay_start_gate,
    wait_replay_start_origin,
)
from synthran.iot_source import MQTTEndpoint


class FixedClock:
    def __init__(self, value: float) -> None:
        self.value = value

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += seconds


class AmberReplayGateTests(unittest.TestCase):
    def test_replay_origin_is_not_chosen_until_gate_release(self) -> None:
        run_id = "amber-gate-test"
        plan = SimpleNamespace(
            spec=SimpleNamespace(run_id=run_id),
            scenario_path=SimpleNamespace(parent=None),
            evidence_path=None,
        )
        session = AmberReplaySession(
            plan=plan,
            endpoint=MQTTEndpoint("127.0.0.1", 1883),
            collector_barrier=object(),
            publisher_events_path=SimpleNamespace(),
            evidence_path=SimpleNamespace(),
            client_factory=lambda client_id: None,
            clock=FixedClock(123.5),
        )

        install_replay_start_gate(run_id)
        result: list[bool] = []
        worker = threading.Thread(target=lambda: result.append(session._await_replay_start()))
        worker.start()
        try:
            time.sleep(0.02)
            self.assertTrue(worker.is_alive())
            self.assertIsNone(session._replay_origin)

            release_replay_start_gate(run_id)
            worker.join(timeout=1.0)
            self.assertFalse(worker.is_alive())
            self.assertEqual([True], result)
            self.assertEqual(123.5, session._replay_origin)
            self.assertEqual(123.5, wait_replay_start_origin(run_id, timeout=1.0))
        finally:
            session._cancel.set()
            remove_replay_start_gate(run_id)


if __name__ == "__main__":
    unittest.main()
