"""Single public command surface for SynthRAN."""

from __future__ import annotations

import argparse
import sys
from typing import Sequence

from synthran.backends.base import BackendError
from synthran.operator import configure_operator_parser, dispatch


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="synthran",
        description="Run and inspect reproducible SynthRAN experiments across virtual and physical radio backends.",
    )
    parser.add_subparsers(dest="command", required=True)
    configure_operator_parser(parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    args = _parser().parse_args(arguments)
    try:
        return dispatch(args)
    except BackendError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
