#!/usr/bin/env python3
"""自测专用：带“提交前停顿”的导出驱动，便于在停顿时从外部注入 SIGKILL。

停顿通过环境变量 SHOREPOWER_EXPORT_STALL_BEFORE_COMMIT（秒）控制：导出进程已把
两张表 fsync 进不可变版本目录、但还没原子切换 latest 指针时会 sleep，此时被
SIGKILL 等价于真实的“导出中途强杀”，上一版 latest 必须保持可读且成对。

用法：
    SHOREPOWER_EXPORT_STALL_BEFORE_COMMIT=3 \
    python3 -I selftest_support/export_kill_driver.py \
        --state-dir ... --out-prefix ... --format jsonl
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shorepower.export_parquet import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
