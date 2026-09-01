from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from synthran.fiveg_ansible import InventoryHost
from synthran.live_preflight import LivePreflightError, ssh_command


class StrictSshBoundaryTests(unittest.TestCase):
    def _host(self, **variables: str) -> InventoryHost:
        values = {
            "ansible_host": "core.example",
            "ansible_user": "root",
            **variables,
        }
        return InventoryHost("core", values)

    def test_default_openssh_trust_store_remains_strict(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            command = ssh_command(self._host(), "hostname")
        self.assertIn("StrictHostKeyChecking=yes", command)
        self.assertFalse(any(item.startswith("UserKnownHostsFile=") for item in command))

    def test_command_enforces_strict_host_key_checking(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            known_hosts = Path(temporary) / "known_hosts"
            known_hosts.write_text("core.example ssh-ed25519 AAAA\n", encoding="utf-8")
            with patch.dict(os.environ, {"SYNTHRAN_KNOWN_HOSTS": str(known_hosts)}):
                command = ssh_command(self._host(), "sh", "-c", "printf '%s' hello world")
        self.assertEqual(command[0], "ssh")
        self.assertIn("BatchMode=yes", command)
        self.assertIn("StrictHostKeyChecking=yes", command)
        self.assertTrue(any(item.startswith("UserKnownHostsFile=") for item in command))
        self.assertEqual(command[-2], "root@core.example")
        self.assertIn("printf", command[-1])

    def test_missing_explicit_known_hosts_override_fails_closed(self) -> None:
        with patch.dict(os.environ, {"SYNTHRAN_KNOWN_HOSTS": "/definitely/missing/known_hosts"}):
            with self.assertRaisesRegex(LivePreflightError, "does not name an existing file"):
                ssh_command(self._host(), "hostname")

    def test_inventory_port_is_validated(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            known_hosts = Path(temporary) / "known_hosts"
            known_hosts.write_text("core.example ssh-ed25519 AAAA\n", encoding="utf-8")
            with patch.dict(os.environ, {"SYNTHRAN_KNOWN_HOSTS": str(known_hosts)}):
                command = ssh_command(self._host(ansible_port="2222"), "hostname")
                self.assertIn("2222", command)
                with self.assertRaisesRegex(LivePreflightError, "ansible_port"):
                    ssh_command(self._host(ansible_port="70000"), "hostname")

    def test_remote_argv_is_shell_quoted_once(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            known_hosts = Path(temporary) / "known_hosts"
            known_hosts.write_text("core.example ssh-ed25519 AAAA\n", encoding="utf-8")
            with patch.dict(os.environ, {"SYNTHRAN_KNOWN_HOSTS": str(known_hosts)}):
                command = ssh_command(
                    self._host(),
                    "python3",
                    "-c",
                    "print('a b')",
                )
        self.assertEqual(command[-1], "python3 -c 'print('\"'\"'a b'\"'\"')'")


if __name__ == "__main__":
    unittest.main()
