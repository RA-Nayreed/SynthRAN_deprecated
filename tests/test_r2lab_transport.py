from __future__ import annotations

import os
from pathlib import Path
import shlex
import tempfile
import unittest
from unittest.mock import patch

from synthran.r2lab.controller import gateway_command, physical_qfit_host, qfit_gateway_command
from synthran.utils.environment import scoped_environment
from synthran.utils.ssh import ansible_ssh_common_args, strict_scp_command, strict_ssh_command


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class StrictSshUtilityTests(unittest.TestCase):
    def test_cluster_ssh_is_isolated_and_strict(self) -> None:
        command = strict_ssh_command(
            "root@sopnode-f2",
            "kubectl",
            "get",
            "nodes",
            known_hosts=Path("/tmp/known_hosts"),
            quote_remote=True,
        )
        self.assertEqual("ssh", command[0])
        self.assertIn("-F", command)
        self.assertIn("/dev/null", command)
        self.assertIn("StrictHostKeyChecking=yes", command)
        self.assertIn("UserKnownHostsFile=/tmp/known_hosts", command)
        self.assertNotIn("GlobalKnownHostsFile=/dev/null", command)
        self.assertNotIn("StrictHostKeyChecking=no", command)
        self.assertEqual(["kubectl", "get", "nodes"], shlex.split(command[-1]))

    def test_scp_uses_same_trust_policy(self) -> None:
        command = strict_scp_command(
            ("one", "two"),
            "root@sopnode-f2:/tmp/",
            known_hosts="/tmp/known_hosts",
        )
        self.assertEqual("scp", command[0])
        self.assertIn("-F", command)
        self.assertIn("StrictHostKeyChecking=yes", command)
        self.assertIn("UserKnownHostsFile=/tmp/known_hosts", command)
        self.assertNotIn("GlobalKnownHostsFile=/dev/null", command)
        self.assertEqual(("one", "two", "root@sopnode-f2:/tmp/"), command[-3:])

    def test_ansible_common_args_match_direct_policy(self) -> None:
        rendered = ansible_ssh_common_args(known_hosts="/tmp/known_hosts")
        self.assertIn("-F /dev/null", rendered)
        self.assertIn("StrictHostKeyChecking=yes", rendered)
        self.assertIn("UserKnownHostsFile=/tmp/known_hosts", rendered)


class R2LabTransportTests(unittest.TestCase):
    @patch("synthran.r2lab.controller._configured_identity", return_value=Path("/tmp/id_rsa"))
    def test_gateway_ignores_ambient_proxy_configuration(self, _identity) -> None:
        command = gateway_command("oulu_user", "rhubarbe", "leases", "--check")
        self.assertEqual("ssh", command[0])
        self.assertIn("-F", command)
        self.assertIn("/dev/null", command)
        self.assertIn("IdentitiesOnly=yes", command)
        self.assertIn("/tmp/id_rsa", command)
        self.assertEqual("oulu_user@faraday.inria.fr", command[command.index("--") + 1])

    def test_qfit_host_mapping_and_nested_argv(self) -> None:
        self.assertEqual("fit07", physical_qfit_host("qfit07"))
        script = "import socket; print('ok')"
        command = qfit_gateway_command("oulu_user", "qfit07", "python3", "-c", script)
        outer_remote = command[-1]
        nested = shlex.split(outer_remote)
        expected_known_hosts = "UserKnownHostsFile=" + str(
            Path("/", "home", "oulu_user", ".ssh", "known_hosts")
        )
        self.assertEqual("ssh", nested[0])
        self.assertIn("StrictHostKeyChecking=yes", nested)
        self.assertIn(expected_known_hosts, nested)
        self.assertNotIn("GlobalKnownHostsFile=/dev/null", nested)
        self.assertEqual(["python3", "-c", script], shlex.split(nested[-1]))


class ScopedEnvironmentTests(unittest.TestCase):
    def test_restores_present_and_absent_values(self) -> None:
        with patch.dict(os.environ, {"SYNTHRAN_KEEP": "before"}, clear=False):
            os.environ.pop("SYNTHRAN_TEMP", None)
            with scoped_environment(
                {"SYNTHRAN_KEEP": "during", "SYNTHRAN_TEMP": "temporary"}
            ):
                self.assertEqual("during", os.environ["SYNTHRAN_KEEP"])
                self.assertEqual("temporary", os.environ["SYNTHRAN_TEMP"])
            self.assertEqual("before", os.environ["SYNTHRAN_KEEP"])
            self.assertNotIn("SYNTHRAN_TEMP", os.environ)


class PurgeLayoutTests(unittest.TestCase):
    def test_removed_transport_shims_do_not_return(self) -> None:
        r2lab = REPOSITORY_ROOT / "synthran" / "r2lab"
        self.assertFalse((r2lab / "cluster_ssh.py").exists())
        self.assertFalse((r2lab / "ue_overlay.py").exists())
        self.assertTrue((REPOSITORY_ROOT / "synthran" / "utils" / "ssh.py").is_file())
        self.assertTrue((REPOSITORY_ROOT / "docs" / "fiveg-ansible-boundary.md").is_file())

    def test_physical_ue_observation_uses_shared_ssh_policy(self) -> None:
        source = (REPOSITORY_ROOT / "synthran" / "r2lab" / "ue.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("from synthran.utils.ssh import strict_ssh_command", source)
        self.assertNotIn("GlobalKnownHostsFile=/dev/null", source)
        self.assertNotIn("shlex.join(remote)", source)


if __name__ == "__main__":
    unittest.main()
