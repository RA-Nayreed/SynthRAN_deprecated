from __future__ import annotations

import unittest

from synthran.live_preflight import CommandResult
from synthran.r2lab.provider import PowerState, execute_verified_qfit_transition


class DelayedQfitRunner:
    def __init__(self, *, on_observations_before_off: int) -> None:
        self.on_observations_before_off = on_observations_before_off
        self.commands: list[tuple[str, ...]] = []
        self.status_calls = 0

    def __call__(self, command, timeout_seconds: int) -> CommandResult:
        value = tuple(command)
        self.commands.append(value)
        if value == ("qfit", "off", "qfit07"):
            return CommandResult(0, "reboot07:ok\n", "")
        if value == ("rhubarbe", "status", "7"):
            self.status_calls += 1
            state = (
                "on"
                if self.status_calls <= self.on_observations_before_off
                else "off"
            )
            return CommandResult(0, f"reboot07:{state}\n", "")
        raise AssertionError(f"unexpected command: {value}")


class R2LabAsyncQfitTests(unittest.TestCase):
    def test_off_polls_until_exact_provider_state_changes(self) -> None:
        runner = DelayedQfitRunner(on_observations_before_off=2)
        sleeps: list[float] = []

        result = execute_verified_qfit_transition(
            qfit="qfit07",
            requested_state=PowerState.OFF,
            runner=runner,
            timeout_seconds=30,
            sleeper=sleeps.append,
            status_attempts=5,
            status_delay_seconds=2.0,
        )

        self.assertTrue(result.confirmed)
        self.assertEqual(PowerState.OFF, result.observed_state)
        self.assertEqual(3, runner.status_calls)
        self.assertEqual([2.0, 2.0], sleeps)
        self.assertEqual(("qfit", "off", "qfit07"), runner.commands[0])
        self.assertEqual(
            [("rhubarbe", "status", "7")] * 3,
            runner.commands[1:],
        )

    def test_off_remains_unresolved_after_bounded_attempts(self) -> None:
        runner = DelayedQfitRunner(on_observations_before_off=10)
        sleeps: list[float] = []

        result = execute_verified_qfit_transition(
            qfit="qfit07",
            requested_state=PowerState.OFF,
            runner=runner,
            timeout_seconds=30,
            sleeper=sleeps.append,
            status_attempts=3,
            status_delay_seconds=1.5,
        )

        self.assertFalse(result.confirmed)
        self.assertEqual(PowerState.ON, result.observed_state)
        self.assertEqual(3, runner.status_calls)
        self.assertEqual([1.5, 1.5], sleeps)

    def test_unknown_observation_is_not_polled_into_success(self) -> None:
        class UnknownRunner:
            def __init__(self) -> None:
                self.commands: list[tuple[str, ...]] = []

            def __call__(self, command, timeout_seconds: int) -> CommandResult:
                value = tuple(command)
                self.commands.append(value)
                if value == ("qfit", "off", "qfit07"):
                    return CommandResult(0, "reboot07:ok\n", "")
                if value == ("rhubarbe", "status", "7"):
                    return CommandResult(0, "", "")
                raise AssertionError(f"unexpected command: {value}")

        runner = UnknownRunner()
        result = execute_verified_qfit_transition(
            qfit="qfit07",
            requested_state=PowerState.OFF,
            runner=runner,
            timeout_seconds=30,
            sleeper=lambda _: None,
            status_attempts=5,
        )

        self.assertFalse(result.confirmed)
        self.assertEqual(PowerState.UNKNOWN, result.observed_state)
        self.assertEqual(2, len(runner.commands))


if __name__ == "__main__":
    unittest.main()
