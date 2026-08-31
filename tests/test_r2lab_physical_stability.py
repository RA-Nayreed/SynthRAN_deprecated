from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from synthran.r2lab.foundation_topology import REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS
from synthran.r2lab.ue_ansible import (
    R2LabUeAnsibleError,
    _apply_connect_convergence,
    _harden_role_tree,
)


UPSTREAM_MBIM_BLOCK = '''        - name: "MBIM: stop.sh + start.sh on {{ ue_item }} if wwan0 not reachable"
          shell: >
            ssh -o ConnectTimeout=5 -o BatchMode=yes -o StrictHostKeyChecking=no
            root@{{ ue_item }}
            'stop.sh; start.sh -F {{ current_dnn }}'
          when:
            - ue_mode == 'mbim'
            - current_dnn is defined
            - not wwan0_up
          ignore_errors: "{{ ignore_task_errors | default(true) }}"
'''


class PhysicalFoundationOwnershipTests(unittest.TestCase):
    def test_open5gs_foundation_owns_only_n3network(self) -> None:
        self.assertEqual(("n3network",), REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS)
        self.assertNotIn("ru-network", REQUIRED_PHYSICAL_NETWORK_ATTACHMENTS)

    def test_physical_gnb_deployment_is_delegated_to_upstream_roles(self) -> None:
        playbook = Path("deploy/ansible/r2lab-srsran-gnb.yml").read_text(encoding="utf-8")

        self.assertIn("name: 5g/srsRAN/config", playbook)
        self.assertIn("name: 5g/srsRAN/deploy", playbook)
        self.assertIn("tasks_from: deploy_gnb.yml", playbook)
        self.assertIn("synthran.run/id={{ synthran_run_id }}", playbook)
        self.assertIn(
            "synthran.io/deployment-authority=fiveg_ansible:{{ synthran_fiveg_ansible_commit }}",
            playbook,
        )
        self.assertNotIn("helm upgrade", playbook)


class MbimConvergenceHardeningTests(unittest.TestCase):
    def test_upstream_connect_copy_is_convergent_and_strict(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            role = Path(directory) / "connect"
            path = role / "tasks" / "main.yml"
            path.parent.mkdir(parents=True)
            path.write_text("---\n" + UPSTREAM_MBIM_BLOCK, encoding="utf-8")

            _apply_connect_convergence(path)
            _harden_role_tree(role, slice_name="oulu_user")
            rendered = path.read_text(encoding="utf-8")

        expected_known_hosts = "UserKnownHostsFile=" + str(
            Path("/", "home", "oulu_user", ".ssh", "known_hosts")
        )
        self.assertNotIn("stop.sh; start.sh", rendered)
        self.assertEqual(1, rendered.count("'stop.sh'"))
        self.assertEqual(1, rendered.count("start.sh -F {{ current_dnn }} -q"))
        self.assertIn("until: mbim_start.rc == 0", rendered)
        self.assertIn("retries: 10", rendered)
        self.assertIn("delay: 3", rendered)
        self.assertIn("StrictHostKeyChecking=yes", rendered)
        self.assertIn(expected_known_hosts, rendered)
        self.assertNotIn("StrictHostKeyChecking=no", rendered)
        self.assertNotIn("UserKnownHostsFile=/dev/null", rendered)

    def test_connect_convergence_fails_closed_on_upstream_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "main.yml"
            path.write_text("---\n# changed upstream\n", encoding="utf-8")
            with self.assertRaisesRegex(R2LabUeAnsibleError, "drifted"):
                _apply_connect_convergence(path)


if __name__ == "__main__":
    unittest.main()
