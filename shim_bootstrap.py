"""
Bootstrap helper for shims. Ensures pos_frontend is importable without pip install.
Used by testing/*.py and reporting/*.py shims before delegating to pos_frontend.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _get_project_root() -> Path:
    """Project root = parent of this file (shim_bootstrap.py at repo root)."""
    return Path(__file__).resolve().parent


def add_src_to_syspath() -> None:
    """Prepend src/ to sys.path so pos_frontend is importable."""
    root = _get_project_root()
    src = root / "src"
    if src.exists() and str(src) not in sys.path:
        sys.path.insert(0, str(src))


def ensure_project_cwd() -> None:
    """Optionally chdir to project root for extra safety when cwd might be wrong."""
    root = _get_project_root()
    if os.getcwd() != str(root):
        os.chdir(root)
