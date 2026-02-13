"""Shim: delegates to pos_frontend. For standalone investigation (not a .cmd entrypoint)."""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
from shim_bootstrap import add_src_to_syspath
add_src_to_syspath()
from pos_frontend.transfers.gold_investigation import main
sys.exit(main(sys.argv[1:]))
