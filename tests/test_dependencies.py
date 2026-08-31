from __future__ import annotations

from io import StringIO
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from synthran.dependencies import DependencyError, load_lock, sync_dependencies


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class DependencyLockTests(unittest.TestCase):
    def test_repository_lock_is_valid_and_immutable(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        self.assertEqual(5, len(lock.git))
        self.assertTrue(all(len(item.commit) == 40 for item in lock.git))
        self.assertEqual(3, sum(item.sync for item in lock.git))
        self.assertIn("amber", {item.name for item in lock.git})
        self.assertEqual(
            "08dd6bd445e607ad3accf4e9a2dff51a499ebdf9",
            next(item.commit for item in lock.git if item.name == "amber"),
        )
        self.assertEqual("3.12.13", lock.raw["conda"]["packages"]["python"]["version"])
        self.assertEqual("21.0.9", lock.raw["conda"]["packages"]["openjdk"]["version"])
        self.assertEqual("4.1.1", lock.raw["conda"]["packages"]["simpy"]["version"])

    def test_dry_run_selects_direct_dependencies_without_writing(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        output = StringIO()
        root = REPOSITORY_ROOT / ".dry-run-deps-must-not-exist"
        self.assertFalse(root.exists())
        sync_dependencies(lock, root, dry_run=True, output=output)
        self.assertFalse(root.exists())
        rendered = output.getvalue()
        self.assertIn("fiveg_ansible", rendered)
        self.assertIn("contiki_ng", rendered)
        self.assertIn("amber", rendered)
        self.assertNotIn("open5gs_k8s", rendered)

    def test_dry_run_all_includes_transitive_dependencies(self) -> None:
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
        self.assertIn("open5gs_k8s", output.getvalue())
        self.assertIn("srsran_helm", output.getvalue())

    def test_dry_run_can_select_only_the_physical_dependencies(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        output = StringIO()
        sync_dependencies(
            lock,
            REPOSITORY_ROOT / ".dry-run-deps-must-not-exist",
            names=("fiveg_ansible", "srsran_helm"),
            dry_run=True,
            output=output,
        )
        rendered = output.getvalue()
        self.assertIn("fiveg_ansible", rendered)
        self.assertIn("srsran_helm", rendered)
        self.assertNotIn("contiki_ng", rendered)
        self.assertNotIn("amber", rendered)
        self.assertNotIn("open5gs_k8s", rendered)

    def test_unknown_selected_dependency_is_rejected(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        with self.assertRaisesRegex(DependencyError, "unknown Git dependencies"):
            sync_dependencies(
                lock,
                REPOSITORY_ROOT / ".dry-run-deps-must-not-exist",
                names=("missing",),
                dry_run=True,
                output=StringIO(),
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

    def test_ansible_collection_version_range_is_rejected(self) -> None:
        lock_data = json.loads((REPOSITORY_ROOT / "dependencies.lock.yml").read_text())
        lock_data["ansible_collections"]["kubernetes_core"]["version"] = ">=6.5"
        with patch.object(Path, "read_text", return_value=json.dumps(lock_data)):
            with self.assertRaisesRegex(DependencyError, "one exact version"):
                load_lock(Path("virtual-lock.yml"))

    def test_ansible_requirements_match_collection_lock(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        requirements = (
            REPOSITORY_ROOT / "deploy" / "ansible" / "preparation-requirements.yml"
        ).read_text(encoding="utf-8")
        collections = lock.raw["ansible_collections"]
        self.assertEqual(
            {
                "kubernetes.core": "6.5.0",
                "community.general": "13.0.1",
                "ansible.posix": "2.2.2",
            },
            {entry["name"]: entry["version"] for entry in collections.values()},
        )
        self.assertEqual(len(collections), requirements.count("  - name: "))
        for entry in collections.values():
            block = (
                f"  - name: {entry['name']}\n"
                f"    version: \"{entry['version']}\"\n"
            )
            self.assertIn(block, requirements)

    def test_golden_path_tool_requires_a_full_digest(self) -> None:
        lock_data = json.loads((REPOSITORY_ROOT / "dependencies.lock.yml").read_text())
        lock_data["tools"]["yq_linux_amd64"]["sha256"] = "latest"
        with patch.object(Path, "read_text", return_value=json.dumps(lock_data)):
            with self.assertRaisesRegex(DependencyError, "full sha256 digest"):
                load_lock(Path("virtual-lock.yml"))

    def test_locked_remote_tools_have_immutable_linux_sources(self) -> None:
        lock = load_lock(REPOSITORY_ROOT / "dependencies.lock.yml")
        tools = lock.raw["tools"]
        self.assertEqual(
            "sha256:f8180838c23d7c7d797b208861fecb591d9ce1690d8704ed1e4cb8e2add966c1",
            tools["helm_linux_amd64"]["sha256"],
        )
        self.assertEqual("/usr/local/bin/helm", tools["helm_linux_amd64"]["path"])
        self.assertEqual(
            "https://github.com/mikefarah/yq/releases/download/v4.45.1/yq_linux_amd64",
            tools["yq_linux_amd64"]["url"],
        )
        self.assertEqual(
            "32.0.1",
            lock.raw["remote_python"]["packages"]["kubernetes"],
        )

    def test_remote_python_package_version_range_is_rejected(self) -> None:
        lock_data = json.loads((REPOSITORY_ROOT / "dependencies.lock.yml").read_text())
        lock_data["remote_python"]["packages"]["pymongo"] = ">=4"
        with patch.object(Path, "read_text", return_value=json.dumps(lock_data)):
            with self.assertRaisesRegex(DependencyError, "one exact version"):
                load_lock(Path("virtual-lock.yml"))


if __name__ == "__main__":
    unittest.main()
