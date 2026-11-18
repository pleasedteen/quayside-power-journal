#!/usr/bin/env python3
"""模拟日志生成入口。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from shorepower.generator import main

if __name__ == "__main__":
    sys.exit(main())
