"""Resolve ${VAR_NAME} placeholders using process environment variables."""

from __future__ import annotations

import os
import re
from typing import Any

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def resolve_env_placeholders(value: str) -> str:
    """Replace every ``${VAR}`` segment with ``os.environ[VAR]``.

    Raises:
        ValueError: if a referenced variable is missing from the environment.
    """

    def repl(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in os.environ:
            raise ValueError(
                f"Environment variable {name!r} is not set (needed for config placeholder)"
            )
        return os.environ[name]

    return _ENV_PATTERN.sub(repl, value)


def resolve_env_in_obj(obj: Any) -> Any:
    """Recursively resolve placeholders in string values inside dicts and lists."""
    if isinstance(obj, str):
        return resolve_env_placeholders(obj)
    if isinstance(obj, dict):
        return {k: resolve_env_in_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [resolve_env_in_obj(item) for item in obj]
    return obj
