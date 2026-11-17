#!/usr/bin/env python3
"""进程入口（隔离模式 python3 -I）。

``-I`` 会忽略 PYTHONPATH、当前目录甚至脚本目录，因此入口处显式把
``src/`` 加入 sys.path；之后的全部业务代码仍运行在隔离模式下。
路径支持 RECALC_SRC 环境变量覆盖，默认取仓库内的 src/。
"""
import os
import sys

_SRC = os.environ.get("RECALC_SRC") or os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"
)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from recalc.submit_service import main  # noqa: E402

if __name__ == "__main__":
    main()
