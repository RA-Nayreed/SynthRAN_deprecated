from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from synthran.r2lab.ue_ansible import _apply_connect_convergence


UPSTREAM_QFIT_MBIM = '''---
- name: "MBIM: initialize QFIT modem {{ ue_item }} after switch-on"
  shell: >
    ssh root@{{ ue_item }} 'init.sh'

- name: "MBIM: connect initialized QFIT {{ ue_item }}"
  shell: >
    ssh root@{{ ue_item }} 'start.sh -F {{ current_dnn }} -q'
'''


class UpstreamOwnedMbimContractTests(unittest.TestCase):
    def test_corrected_upstream_role_is_not_rewritten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "main.yml"
            path.write_text(UPSTREAM_QFIT_MBIM, encoding="utf-8")

            _apply_connect_convergence(path)

            rendered = path.read_text(encoding="utf-8")

        self.assertEqual(UPSTREAM_QFIT_MBIM, rendered)
        self.assertIn("'init.sh'", rendered)
        self.assertIn("'start.sh -F {{ current_dnn }} -q'", rendered)
        self.assertNotIn("until: mbim_start.rc == 0", rendered)


if __name__ == "__main__":
    unittest.main()
