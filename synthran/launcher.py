"""Canonical executable entrypoint for SynthRAN."""

from __future__ import annotations

import sys
from typing import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Run the installed SynthRAN command interface."""

    arguments = list(sys.argv[1:] if argv is None else argv)

    from synthran.cli import main as cli_main

    return cli_main(arguments)
