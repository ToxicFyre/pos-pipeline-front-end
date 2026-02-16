"""Shim: run PT-W vs PT-R comparison from golden Excel."""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
from shim_bootstrap import add_src_to_syspath
add_src_to_syspath()
from pos_frontend.transfers.pt_w_vs_pt_r_comparison import main
sys.exit(main(sys.argv[1:]))
