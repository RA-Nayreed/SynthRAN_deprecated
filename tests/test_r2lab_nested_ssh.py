from __future__ import annotations

import shlex
import unittest

from synthran.r2lab.hardware import UES
from synthran.r2lab.resources import _nested_ssh


class R2LabNestedSshTests(unittest.TestCase):
    def test_preserves_python_c_argument_as_one_remote_argv_item(self) -> None:
        script = "import socket; print('ok')"

        command = _nested_ssh(
            "oulu_user",
            UES["qfit07"],
            "python3",
            "-c",
            script,
        )

        self.assertEqual(
            ["python3", "-c", script],
            shlex.split(command[-1]),
        )

    def test_preserves_shell_script_with_spaces_quotes_and_newlines(self) -> None:
        script = """set -eu
value="hello world"
printf '%s\\n' "$value"
"""

        command = _nested_ssh(
            "oulu_user",
            UES["qfit07"],
            "sh",
            "-lc",
            script,
        )

        self.assertEqual(
            ["sh", "-lc", script],
            shlex.split(command[-1]),
        )

    def test_simple_ue_commands_keep_the_same_semantics(self) -> None:
        remote = (
            "ip",
            "-j",
            "route",
            "get",
            "172.28.2.77",
            "oif",
            "wwan0",
        )

        command = _nested_ssh(
            "oulu_user",
            UES["qfit07"],
            *remote,
        )

        self.assertEqual(list(remote), shlex.split(command[-1]))


if __name__ == "__main__":
    unittest.main()
