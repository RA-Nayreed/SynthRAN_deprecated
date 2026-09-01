from __future__ import annotations

from io import StringIO
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from synthran.dependencies import DependencyError, load_lock, sync_dependencies


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class DependencyLockTests(unittest.TestCase):
    def test_repository_lock_contains_only_direct_git_dependencies(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        self.assertEqual(2, len(lock.git))
        self.assertTrue(all(len(item.commit) == 40 for item in lock.git))
        self.assertTrue(all(item.sync for item in lock.git))
        self.assertEqual({"amber", "fiveg_ansible"}, {item.name for item in lock.git})
        self.assertEqual(
            "08dd6bd445e607ad3accf4e9a2dff51a499ebdf9",
            next(item.commit for item in lock.git if item.name == "amber"),
        )
        self.assertEqual(
            "627b190e66aafadf618b0fb9ab2511a07ada535e",
            next(item.commit for item in lock.git if item.name == "fiveg_ansible"),
        )
        self.assertEqual("3.12.13", lock.raw["conda"]["packages"]["python"]["version"])
        self.assertEqual("4.1.1", lock.raw["conda"]["packages"]["simpy"]["version"])

    def test_lock_has_no_upstream_owned_dependency_sections(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        self.assertEqual(
            {"schema", "resolved_at_utc", "git", "containers", "tools", "conda", "github_actions"},
            set(lock.raw),
        )
        self.assertEqual({"mosquitto"}, set(lock.raw["containers"]))
        self.assertEqual({"iperf3_linux_amd64_source"}, set(lock.raw["tools"]))
        for retired in (
            "ansible_collections",
            "remote_python",
            "resource_bootstrap",
            "open5gs",
            "open5gs_smf",
            "helm_linux_amd64",
            "yq_linux_amd64",
        ):
            self.assertNotIn(retired, json.dumps(lock.raw))

    def test_dry_run_syncs_both_direct_dependencies_without_writing(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        output = StringIO()
        root = REPOSITORY_ROOT / ".dry-run-deps-must-not-exist"
        self.assertFalse(root.exists())
        sync_dependencies(lock, root, dry_run=True, output=output)
        self.assertFalse(root.exists())
        rendered = output.getvalue()
        self.assertIn("fiveg_ansible", rendered)
        self.assertIn("amber", rendered)
        self.assertNotIn("open5gs_k8s", rendered)
        self.assertNotIn("srsran_helm", rendered)

    def test_include_transitive_cannot_reintroduce_upstream_internal_repositories(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        output = StringIO()
        with patch.object(Path, "mkdir") as mkdir:
            sync_dependencies(
                lock,
                REPOSITORY_ROOT / ".dry-run-deps-must-not-exist",
                include_transitive=True,
                dry_run=True,
                output=output,
            )
            mkdir.assert_not_called()
        rendered = output.getvalue()
        self.assertIn("fiveg_ansible", rendered)
        self.assertIn("amber", rendered)
        self.assertNotIn("open5gs_k8s", rendered)
        self.assertNotIn("srsran_helm", rendered)

    def test_dry_run_can_select_only_fiveg_ansible(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        output = StringIO()
        sync_dependencies(
            lock,
            REPOSITORY_ROOT / ".dry-run-deps-must-not-exist",
            names=("fiveg_ansible",),
            dry_run=True,
            output=output,
        )
        rendered = output.getvalue()
        self.assertIn("fiveg_ansible", rendered)
        self.assertNotIn("amber", rendered)

    def test_unknown_selected_dependency_is_rejected(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        output = StringIO()
        with self.assertRaisesRegex(DependencyError, "unknown Git dependencies"):
            sync_dependencies(
                lock,
                REPOSITORY_ROOT / ".dry-run-deps-must-not-exist",
                names=("missing",),
                dry_run=True,
                output=output,
            )

    def test_mutable_git_ref_is_rejected_as_commit(self) -> None:
        lock_data = json.loads((REPOSITORY_ROOT / "dependencies.lock.yml").read_text())
        lock_data["git"]["fiveg_ansible"]["commit"] = "main"
        with patch.object(Path, "read_text", return_value=json.dumps(lock_data)):
            with self.assertRaisesRegex(DependencyError, "full lowercase commit SHA"):
                load_lock(Path("virtual-lock.yml"))

    def test_checkout_path_cannot_escape_dependency_root(self) -> None:
        lock_data = json.loads((REPOSITORY_ROOT / "dependencies.lock.yml").read_text())
        lock_data["git"]["fiveg_ansible"]["checkout"] = "../outside"
        with patch.object(Path, "read_text", return_value=json.dumps(lock_data)):
            with self.assertRaisesRegex(DependencyError, "stay below"):
                load_lock(Path("virtual-lock.yml"))

    def test_conda_version_range_is_rejected(self) -> None:
        lock_data = json.loads((REPOSITORY_ROOT / "dependencies.lock.yml").read_text())
        lock_data["conda"]["packages"]["paho-mqtt"]["version"] = ">=2.1"
        with patch.object(Path, "read_text", return_value=json.dumps(lock_data)):
            with self.assertRaisesRegex(DependencyError, "one exact package version"):
                load_lock(Path("virtual-lock.yml"))

    def test_conda_platform_is_linux_only(self) -> None:
        lock_data = json.loads((REPOSITORY_ROOT / "dependencies.lock.yml").read_text())
        lock_data["conda"]["platform"] = "osx-64"
        with patch.object(Path, "read_text", return_value=json.dumps(lock_data)):
            with self.assertRaisesRegex(DependencyError, "must be 'linux-64'"):
                load_lock(Path("virtual-lock.yml"))

    def test_ansible_core_version_range_is_rejected(self) -> None:
        lock_data = json.loads((REPOSITORY_ROOT / "dependencies.lock.yml").read_text())
        lock_data["conda"]["packages"]["ansible-core"]["version"] = ">=2.20"
        with patch.object(Path, "read_text", return_value=json.dumps(lock_data)):
            with self.assertRaisesRegex(DependencyError, "one exact package version"):
                load_lock(Path("virtual-lock.yml"))

    def test_conda_channels_cannot_fall_back_to_defaults(self) -> None:
        lock_data = json.loads((REPOSITORY_ROOT / "dependencies.lock.yml").read_text())
        lock_data["conda"]["channels"] = ["conda-forge", "defaults"]
        with patch.object(Path, "read_text", return_value=json.dumps(lock_data)):
            with self.assertRaisesRegex(DependencyError, "exactly"):
                load_lock(Path("virtual-lock.yml"))

    def test_conda_environment_name_is_fixed(self) -> None:
        lock_data = json.loads((REPOSITORY_ROOT / "dependencies.lock.yml").read_text())
        lock_data["conda"]["environment_name"] = "something-else"
        with patch.object(Path, "read_text", return_value=json.dumps(lock_data)):
            with self.assertRaisesRegex(DependencyError, "must be 'synthran'"):
                load_lock(Path("virtual-lock.yml"))

    def test_iperf_tool_requires_a_full_digest(self) -> None:
        lock_data = json.loads((REPOSITORY_ROOT / "dependencies.lock.yml").read_text())
        lock_data["tools"]["iperf3_linux_amd64_source"]["sha256"] = "latest"
        with patch.object(Path, "read_text", return_value=json.dumps(lock_data)):
            with self.assertRaisesRegex(DependencyError, "full sha256 digest"):
                load_lock(Path("virtual-lock.yml"))


if __name__ == "__main__":
    unittest.main()
