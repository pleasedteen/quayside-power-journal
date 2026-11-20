"""日志回放器：把固定的模拟日志按可配置速度逐行追加到服务跟读的目标日志。

``--delay 0`` 为瞬时回放；加大 ``--delay`` 可模拟慢速到达。回放速度只改变
墙钟时刻，不改变记录的到达顺序与内容——这是“变速回放结果一致”测试的基础。

两种目标布局：

* ``--target`` 为单个 JSONL 文件（默认）：逐行追加，行为与历史版本一致。
* ``--target`` 为段前缀且给出 ``--segment-lines N``：每 N 行滚出一段
  ``<前缀>.000001``、``<前缀>.000002`` ……，换段时旧段写 ``.sealed`` 封存标记。
  加 ``--seal-lag K`` 可让最近 K 个已写完的段暂不封存，用来构造
  「新段已到达、旧段仍有积压」的场景：服务必须先把旧段读尽并等到它封存，
  才能越过它继续读新段。
"""

from __future__ import annotations

import argparse
import os
import time
from collections import deque

from .storage import SegmentWriter


def replay(
    source: str,
    target: str,
    delay: float,
    max_lines: int | None = None,
    segment_lines: int = 0,
    seal_lag: int = 0,
) -> int:
    os.makedirs(os.path.dirname(os.path.abspath(target)) or ".", exist_ok=True)
    n = 0
    if segment_lines and segment_lines > 0:
        writer = SegmentWriter(target)
        seal_lag = max(0, seal_lag)
        pending: deque[int] = deque()  # 已写完但尚未封存的段号
        try:
            with open(source, "r", encoding="utf-8") as src:
                for line in src:
                    if max_lines is not None and n >= max_lines:
                        break
                    if writer.lines >= segment_lines:
                        writer.open_next_segment()
                        pending.append(writer.segment - 1)
                        while len(pending) > seal_lag:
                            writer.seal_segment(pending.popleft())
                    writer.append(line)
                    n += 1
                    if delay > 0:
                        time.sleep(delay)
            # 收尾：仍按 seal_lag 保留末尾若干段未封存；lag=0 时全部封存
            if seal_lag == 0:
                if writer.lines > 0:
                    pending.append(writer.segment)
                while pending:
                    writer.seal_segment(pending.popleft())
            else:
                while len(pending) > seal_lag:
                    writer.seal_segment(pending.popleft())
        finally:
            writer.detach()
        return n

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
    p.add_argument("--target", required=True, help="服务正在跟读的日志路径（或分段前缀）")
    p.add_argument("--delay", type=float, default=0.0, help="每行追加后的间隔秒数（默认 0）")
    p.add_argument("--max-lines", type=int, default=None, help="只回放前 N 行（用于崩溃注入）")
    p.add_argument(
        "--segment-lines", type=int, default=0,
        help="按编号分段写出时每段行数（0=写单文件，默认 0）",
    )
    p.add_argument(
        "--seal-lag", type=int, default=0,
        help="最近多少个已写完的段暂不封存（模拟旧段积压，默认 0=写完即封存）",
    )
    args = p.parse_args(argv)
    n = replay(args.source, args.target, args.delay, args.max_lines,
               args.segment_lines, args.seal_lag)
    layout = (f"分段每 {args.segment_lines} 行，封存滞后 {args.seal_lag} 段"
              if args.segment_lines else "单文件")
    print(f"已回放 {n} 行 ({args.source} -> {args.target}, delay={args.delay}s, {layout})")
    return 0
