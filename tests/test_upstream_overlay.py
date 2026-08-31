from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from synthran.upstream_overlay import (
    UpstreamOverlayError,
    apply_network_overlay,
    apply_preparation_overlay,
    validate_upstream_interface,
)


class UpstreamOverlayTests(unittest.TestCase):
    def _make_checkout(self, root: Path, *, global_cleanup: bool = False) -> None:
        files = {
            "bin/fiveg": "#!/usr/bin/env bash\n",
            "playbooks/deploy.yml": "---\n",
            "playbooks/deploy_r2lab.yml": "---\n",
            "playbooks/down.yml": "---\n",
            "group_vars/all/all.yml": "\n".join(
                (
                    "fiveg_prepare_only: false",
                    "fiveg_allow_live_installs: true",
                    "fiveg_manage_os_dependencies: true",
                    "fiveg_manage_python_dependencies: true",
                    "fiveg_disruptive_cluster_ops_enabled: true",
                    "fiveg_k8s_env_enabled: true",
                    'fiveg_python_interpreter: ""',
                    "fiveg_selected_slices: []",
                    "fiveg_selected_ues: []",
                    "fiveg_cleanup_namespaces: []",
                    "open5gs_webui_enabled: true",
                    "open5gs_admin_account_enabled: true",
                    "pos_manage_allocation: true",
                    "r2lab_strict_host_key_checking: false",
                )
            )
            + "\n",
            "roles/r2lab/cleanup/tasks/main.yml": (
                "all-off\n"
                if global_cleanup
                else "r2lab_selected_ues: []\nrhubarbe pdu off \"{{ rru }}\"\n"
            ),
        }
        for relative, content in files.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

    def test_interface_validation_is_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._make_checkout(root)
            before = {
                path.relative_to(root): path.read_bytes()
                for path in root.rglob("*")
                if path.is_file()
            }

            apply_network_overlay(root, subscriber_name="qfit07")
            apply_preparation_overlay(root)

            after = {
                path.relative_to(root): path.read_bytes()
                for path in root.rglob("*")
                if path.is_file()
            }
            self.assertEqual(before, after)

    def test_missing_machine_interface_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._make_checkout(root)
            (root / "bin" / "fiveg").unlink()
            with self.assertRaisesRegex(UpstreamOverlayError, "missing required interfaces"):
                validate_upstream_interface(root)

    def test_missing_policy_variable_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._make_checkout(root)
            defaults = root / "group_vars" / "all" / "all.yml"
            defaults.write_text(
                defaults.read_text(encoding="utf-8").replace(
                    "pos_manage_allocation: true\n", ""
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(UpstreamOverlayError, "policy contract"):
                validate_upstream_interface(root)

    def test_global_r2lab_cleanup_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self._make_checkout(root, global_cleanup=True)
            with self.assertRaisesRegex(UpstreamOverlayError, "global R2Lab cleanup"):
                validate_upstream_interface(root)


if __name__ == "__main__":
    unittest.main()
