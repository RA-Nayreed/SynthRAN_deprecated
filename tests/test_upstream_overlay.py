from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from synthran.upstream_overlay import (
    UpstreamOverlayError,
    _replace_once,
    apply_network_overlay,
)


class UpstreamOverlayTests(unittest.TestCase):
    def test_exact_anchor_is_replaced_once(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "source.yml"
            path.write_text("before\nanchor\nafter\n", encoding="utf-8")
            _replace_once(root, "source.yml", "anchor", "replacement")
            self.assertEqual(
                path.read_text(encoding="utf-8"),
                "before\nreplacement\nafter\n",
            )

    def test_missing_or_duplicated_anchor_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "source.yml"
            path.write_text("other\n", encoding="utf-8")
            with self.assertRaisesRegex(UpstreamOverlayError, "found 0"):
                _replace_once(root, "source.yml", "anchor", "replacement")
            path.write_text("anchor\nanchor\n", encoding="utf-8")
            with self.assertRaisesRegex(UpstreamOverlayError, "found 2"):
                _replace_once(root, "source.yml", "anchor", "replacement")

    def test_missing_source_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(UpstreamOverlayError, "unable to read"):
                _replace_once(Path(temporary), "missing.yml", "a", "b")

    def test_physical_overlay_selects_only_the_reviewed_qfit(self) -> None:
        with patch("synthran.upstream_overlay._replace_once") as replace:
            apply_network_overlay(Path("worktree"), subscriber_name="qfit07")

        replacements = [call.args[3] for call in replace.call_args_list]
        self.assertIn(
            "    {% for ue_name, ue in profile.ues.items() if ue_name == 'qfit07' %}",
            replacements,
        )

    def test_overlay_rejects_an_unsafe_subscriber_name(self) -> None:
        with self.assertRaisesRegex(UpstreamOverlayError, "subscriber name"):
            apply_network_overlay(Path("worktree"), subscriber_name="qfit07;all")


if __name__ == "__main__":
    unittest.main()
