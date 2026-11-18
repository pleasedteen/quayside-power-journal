"""日志回放器：把固定的模拟日志按可配置速度逐行追加到服务跟读的目标日志。

``--delay 0`` 为瞬时回放；加大 ``--delay`` 可模拟慢速到达。回放速度只改变
墙钟时刻，不改变记录的到达顺序与内容——这是“变速回放结果一致”测试的基础。
"""

from __future__ import annotations

import argparse
import os
import time


def replay(source: str, target: str, delay: float, max_lines: int | None = None) -> int:
    os.makedirs(os.path.dirname(os.path.abspath(target)) or ".", exist_ok=True)
    n = 0
    with open(source, "r", encoding="utf-8") as src, open(target, "a", encoding="utf-8") as dst:
        for line in src:
            if max_lines is not None and n >= max_lines:
                break
            dst.write(line)
            dst.flush()
            os.fsync(dst.fileno())
            n += 1
            if delay > 0:
                time.sleep(delay)
    return n


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="shorepower-replay", description="按速度回放 JSON Lines 日志")
    p.add_argument("--source", required=True)
    p.add_argument("--target", required=True, help="服务正在跟读的日志路径")
    p.add_argument("--delay", type=float, default=0.0, help="每行追加后的间隔秒数（默认 0）")
    p.add_argument("--max-lines", type=int, default=None, help="只回放前 N 行（用于崩溃注入）")
    args = p.parse_args(argv)
    n = replay(args.source, args.target, args.delay, args.max_lines)
    print(f"已回放 {n} 行 ({args.source} -> {args.target}, delay={args.delay}s)")
    return 0
