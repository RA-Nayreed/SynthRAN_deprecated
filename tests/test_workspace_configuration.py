from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from synthran.workspace.configuration import (
    discover_ssh_identity_references,
    first_use_snapshot,
    resolve_ssh_identity_reference,
    switch_workspace_profile,
    update_workspace_defaults,
)
from synthran.workspace.model import Profile, WorkspaceError, format_utc, utc_now
from synthran.workspace.store import (
    initialize_workspace,
    load_workspace,
    save_profile,
    workspace_file,
)


class WorkspaceConfigurationTests(unittest.TestCase):
    def test_identity_discovery_returns_private_references_with_r2lab_first(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            ssh = home / ".ssh"
            ssh.mkdir()
            marker = b"-----BEGIN " + b"OPENSSH " + b"PRIVATE" + b" KEY-----\n"
            (ssh / "id_ed25519").write_bytes(marker + b"fixture\n")
            (ssh / "id_r2lab").write_bytes(marker + b"fixture\n")
            (ssh / "id_r2lab.pub").write_text("public\n", encoding="utf-8")
            (ssh / "known_hosts").write_text("host\n", encoding="utf-8")

            identities = discover_ssh_identity_references({"HOME": str(home)})

            self.assertEqual(identities, ("~/.ssh/id_r2lab", "~/.ssh/id_ed25519"))

    def test_identity_reference_resolves_against_configured_home(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            expected = (home / ".ssh" / "id_r2lab").resolve()

            resolved = resolve_ssh_identity_reference(
                "~/.ssh/id_r2lab",
                {"HOME": str(home)},
            )

            self.assertEqual(resolved, expected)
            with self.assertRaises(WorkspaceError):
                resolve_ssh_identity_reference("/tmp/id_r2lab", {"HOME": str(home)})
            with self.assertRaises(WorkspaceError):
                resolve_ssh_identity_reference("~/.ssh/../other", {"HOME": str(home)})

    def test_first_use_snapshot_is_local_only_and_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            root = base / "repo"
            root.mkdir()
            (root / ".git").mkdir()
            config_home = base / "config"
            environment = {
                "HOME": str(base / "home"),
                "SYNTHRAN_CONFIG_HOME": str(config_home),
                "SYNTHRAN_SLICES_PROJECT": "research-project",
            }
            now = utc_now()
            save_profile(
                Profile(
                    name="default",
                    created_at_utc=format_utc(now),
                    updated_at_utc=format_utc(now),
                    slices_username="operator",
                ),
                environment=environment,
            )

            result = first_use_snapshot(start=root / "nested", environment=environment)

            self.assertFalse(result["workspace_initialized"])
            self.assertEqual(result["defaults"]["project"], "research-project")
            self.assertEqual(result["profiles"][0]["name"], "default")
            self.assertEqual(result["profiles"][0]["slices_username"], "operator")
            self.assertNotIn("fingerprint", str(result).lower())

    def test_workspace_defaults_are_replaced_without_changing_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            original = initialize_workspace(
                root=root,
                profile="default",
                project="research-project",
                reservation_minutes=120,
                placement="automatic",
            )

            updated = update_workspace_defaults(
                root,
                reservation_minutes=180,
                placement="manual",
            )

            self.assertEqual(updated.profile, original.profile)
            self.assertEqual(updated.project, original.project)
            self.assertEqual(updated.created_at_utc, original.created_at_utc)
            self.assertEqual(updated.reservation_minutes, 180)
            self.assertEqual(updated.placement, "manual")
            self.assertEqual(load_workspace(root), updated)

    def test_profile_switch_preserves_project_and_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            original = initialize_workspace(
                root=root,
                profile="default",
                project="research-project",
                reservation_minutes=180,
                placement="manual",
            )

            updated = switch_workspace_profile(root, profile_name="second")

            self.assertEqual(updated.profile, "second")
            self.assertEqual(updated.project, original.project)
            self.assertEqual(updated.created_at_utc, original.created_at_utc)
            self.assertEqual(updated.reservation_minutes, 180)
            self.assertEqual(updated.placement, "manual")
            self.assertEqual(load_workspace(root), updated)

    def test_invalid_workspace_defaults_do_not_replace_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            initialize_workspace(root=root, profile="default", project="research-project")
            before = workspace_file(root).read_text(encoding="utf-8")

            with self.assertRaises(WorkspaceError):
                update_workspace_defaults(
                    root,
                    reservation_minutes=1,
                    placement="automatic",
                )

            self.assertEqual(workspace_file(root).read_text(encoding="utf-8"), before)


if __name__ == "__main__":
    unittest.main()
