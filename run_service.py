#!/usr/bin/env python3
"""服务启动入口（支持 python -I 隔离模式运行）。"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from shorepower.service import main

if __name__ == "__main__":
    sys.exit(main())
