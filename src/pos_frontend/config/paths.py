"""
Path resolution and secrets loading.
Trust-but-verify: cwd is only used if it looks like repo root (sentinel check).
"""

from __future__ import annotations

import os
from pathlib import Path

_SENTINELS = ("sucursales.json", "pyproject.toml", "secrets.env.example")


def _cwd_looks_like_root(cwd: Path) -> bool:
    """True if cwd contains at least one sentinel file."""
    return any((cwd / s).exists() for s in _SENTINELS)


def _find_root_from_file() -> Path | None:
    """Walk up from pos_frontend __file__ to find repo root (has sentinel)."""
    current = Path(__file__).resolve().parent
    for _ in range(10):  # avoid infinite loop
        if _cwd_looks_like_root(current):
            return current
        parent = current.parent
        if parent == current:
            break
        current = parent
    return None


def get_project_root() -> Path:
    """
    Resolve project root. Order:
    1. Path.cwd() only if it looks like repo root (has sentinel)
    2. POS_PIPELINE_ROOT env var if set
    3. __file__ walk from this module
    """
    cwd = Path.cwd()
    if _cwd_looks_like_root(cwd):
        return cwd

    env_root = os.environ.get("POS_PIPELINE_ROOT")
    if env_root:
        p = Path(env_root).resolve()
        if _cwd_looks_like_root(p):
            return p

    found = _find_root_from_file()
    if found:
        return found

    # Fallback: assume cwd (caller may have wrong expectations)
    return cwd


def resolve_path(relative: str) -> Path:
    """Combine project root with a relative path (e.g. 'data', 'sucursales.json')."""
    root = get_project_root()
    return (root / relative).resolve()


def load_secrets_env() -> None:
    """Load secrets.env from project root (and optionally utils/secrets.env) into os.environ."""
    root = get_project_root()
    for name in ("secrets.env", "utils/secrets.env"):
        path = root / name
        if path.exists():
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line and not line.startswith("export"):
                        key, value = line.split("=", 1)
                        value = value.strip('"\'')
                        os.environ.setdefault(key.strip(), value)
