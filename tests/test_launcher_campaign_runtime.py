from __future__ import annotations

import unittest
from unittest.mock import patch

from synthran.launcher import main


class _Session:
    def __init__(self, *, cleanup_error: str | None = None) -> None:
        self.cleanup_error = cleanup_error
        self.command_exit_code = None
        self.entered = False
        self.exited = False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc, tb):
        self.exited = True
        return False


class LauncherCampaignRuntimeTests(unittest.TestCase):
    def test_campaign_run_is_wrapped_in_runtime_session(self) -> None:
        session = _Session()
        arguments = [
            "research",
            "campaign-run",
            "--campaign",
            "campaign.json",
            "--inventory",
            "hosts.ini",
            "--target",
            "192.0.2.10",
        ]
        with (
            patch("synthran.cli.main", return_value=0) as cli_main,
            patch(
                "synthran.research.campaign_runtime.campaign_runtime_session",
                return_value=session,
            ) as campaign_session,
        ):
            result = main(arguments)

        self.assertEqual(result, 0)
        self.assertTrue(session.entered)
        self.assertTrue(session.exited)
        self.assertEqual(session.command_exit_code, 0)
        campaign_session.assert_called_once_with(arguments)
        cli_main.assert_called_once_with(arguments)

    def test_campaign_cleanup_failure_forces_nonzero_exit(self) -> None:
        session = _Session(cleanup_error="restore failed")
        arguments = [
            "research",
            "campaign-run",
            "--campaign",
            "campaign.json",
            "--inventory",
            "hosts.ini",
            "--target",
            "192.0.2.10",
        ]
        with (
            patch("synthran.cli.main", return_value=0),
            patch(
                "synthran.research.campaign_runtime.campaign_runtime_session",
                return_value=session,
            ),
        ):
            result = main(arguments)

        self.assertEqual(result, 2)

    def test_non_campaign_cli_is_unchanged(self) -> None:
        arguments = ["inspect", "--run-id", "run-01"]
        with patch("synthran.cli.main", return_value=0) as cli_main:
            result = main(arguments)
        self.assertEqual(result, 0)
        cli_main.assert_called_once_with(arguments)


if __name__ == "__main__":
    unittest.main()
