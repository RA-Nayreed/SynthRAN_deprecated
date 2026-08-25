"""Backend-aware command dispatch for SynthRAN."""

from __future__ import annotations

import sys
from typing import Sequence

from synthran.backends import BackendError, backend_for_argv
from synthran.commands import runtime as command_runtime


_parser = command_runtime._parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    backend = backend_for_argv(arguments)
    if backend is None:
        return command_runtime.main(arguments)

    args = _parser().parse_args(arguments)
    try:
        return backend.dispatch(args)
    except BackendError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
