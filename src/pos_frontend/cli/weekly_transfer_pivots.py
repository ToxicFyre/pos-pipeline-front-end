"""CLI for weekly transfer pivots."""

from __future__ import annotations

import sys

from pos_frontend.transfers.pivots import main as _main


def main(argv: list[str] | None = None) -> int:
    return _main(sys.argv[1:] if argv is None else argv)


if __name__ == "__main__":
    raise SystemExit(main())
