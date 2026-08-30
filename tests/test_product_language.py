from __future__ import annotations

import os
from pathlib import Path
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class ProductLanguageTests(unittest.TestCase):
    def test_internal_workflow_markers_do_not_appear_in_tracked_text(self) -> None:
        excluded_parts = {".git", ".deps", ".synthran", "__pycache__", "node_modules"}
        forbidden = ("ag" + "ent", "co" + "dex")
        text_suffixes = {
            ".ini",
            ".json",
            ".md",
            ".py",
            ".sh",
            ".toml",
            ".txt",
            ".yaml",
            ".yml",
        }
        for root, dirs, files in os.walk(REPOSITORY_ROOT):
            dirs[:] = [directory for directory in dirs if directory not in excluded_parts]
            for filename in files:
                if filename == "decision.md":
                    continue
                path = Path(root) / filename
                relative = path.relative_to(REPOSITORY_ROOT).as_posix()
                lowered_path = relative.lower()
                for marker in forbidden:
                    self.assertNotIn(marker, lowered_path, relative)
                if path.suffix.lower() not in text_suffixes and path.name not in {
                    "LICENSE",
                    "README.md",
                    "THIRD_PARTY.md",
                }:
                    continue
                content = path.read_text(encoding="utf-8").lower()
                actor_marker = forbidden[0]
                for protocol_form in (
                    "user-" + actor_marker,
                    "user_" + actor_marker,
                    "user " + actor_marker,
                ):
                    content = content.replace(protocol_form, "")
                for marker in forbidden:
                    self.assertNotIn(marker, content, relative)


if __name__ == "__main__":
    unittest.main()
