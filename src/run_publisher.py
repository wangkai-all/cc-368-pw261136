#!/usr/bin/env python3
"""报表发布端入口（隔离模式），路径说明见 run_submit.py。"""
import os
import sys

_SRC = os.environ.get("RECALC_SRC") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"
)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from recalc.publisher import main  # noqa: E402

if __name__ == "__main__":
    main()
