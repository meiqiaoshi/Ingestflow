"""Central logging setup for CLI runs."""

from __future__ import annotations

import logging
import sys


def configure_logging(*, verbose: bool = False, quiet: bool = False) -> None:
    if verbose and quiet:
        raise ValueError("Cannot use both --verbose and --quiet")

    if quiet:
        level = logging.WARNING
    elif verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
        force=True,
    )
