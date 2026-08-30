"""Canonical executable entrypoint for SynthRAN."""

from __future__ import annotations

import sys
from typing import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Run the installed SynthRAN command interface."""

    arguments = list(sys.argv[1:] if argv is None else argv)

    from synthran.cli import main as cli_main
    from synthran.experiment import ExperimentError

    try:
        return cli_main(arguments)
    except ExperimentError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
