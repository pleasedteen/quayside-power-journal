"""确定性本地模拟日志生成器。

时间线固定（2026-11-18 UTC，四个刻钟窗口），输出顺序固定、无随机源，
因此同一份日志以任意速度回放，发布的批次与窗口状态都必须完全一致。

覆盖场景：
  1. 正常增量读数、迟到读数（到达晚但计量窗口仍开放）
  2. 重复 reading_id 驳回
  3. 更正：改电量 / 改泊位 / 改计量时间（跨窗口成对 remove+add，同批可见）
  4. 版本不高于当前生效版本 -> 驳回，原账不变
  5. 更正目标不存在 -> 驳回
  6. 全网关水位过窗口结束 10 分钟 -> 封账；封账后读数/更正整笔驳回
  7. 坏 JSON 行、未知 type、进度水位回退 -> 驳回留痕，账务不受影响
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone

from .storage import dumps_canonical

BASE = datetime(2026, 11, 18, 8, 0, 0, tzinfo=timezone.utc)
GATEWAYS = ("GW1", "GW2", "GW3")


def ts(minute: int, second: int = 0) -> str:
    return (BASE + timedelta(minutes=minute, seconds=second)).strftime("%Y-%m-%dT%H:%M:%SZ")


# (到达顺序固定) 每条是一行日志的 dict
def build_records() -> list[dict]:
    R: list[dict] = []

    def reading(rid, gw, berth, minute, energy):
        R.append(
            {"type": "reading", "reading_id": rid, "gateway": gw,
             "berth": berth, "meter_ts": ts(minute), "energy": energy}
        )

    def correction(rid, version, berth, minute, energy):
        R.append(
            {"type": "correction", "reading_id": rid, "version": version,
             "berth": berth, "meter_ts": ts(minute), "energy": energy}
        )

    def progress(gw, minute):
        R.append({"type": "progress", "gateway": gw, "progress_ts": ts(minute)})

    # ---- 窗口 08:00（计量分钟 0..14），到达顺序交错 ----
    reading("r1", "GW1", "B1", 2, 10.0)
    reading("r2", "GW1", "B2", 5, 20.0)
    reading("r3", "GW2", "B3", 8, 30.0)          # 稍后才送达的迟到读数
    reading("r1", "GW1", "B1", 2, 10.0)          # 重复 id -> 驳回
    correction("r1", 1, "B1", 2, 12.0)           # v1 不高于生效 v1 -> 驳回
    correction("r1", 2, "B1", 2, 12.0)           # 改电量 10 -> 12，同窗口
    correction("rX", 1, "B1", 3, 5.0)            # 目标不存在 -> 驳回
    reading("r4", "GW3", "B4", 12, 40.0)
    progress("GW1", 14)
    progress("GW2", 14)
    progress("GW3", 14)                           # 最慢 08:14，尚不足 +10min，不封账

    # ---- 窗口 08:15 ----
    reading("r5", "GW1", "B1", 16, 15.0)
    reading("r6", "GW2", "B3", 18, 25.0)
    correction("r2", 2, "B2", 17, 22.0)          # 改电量 20 -> 22
    correction("r3", 2, "B3", 17, 30.0)          # 改计量时间 08:08 -> 08:17：跨窗口
                                                  # 08:00 窗口 remove 30，08:15 窗口 add 30
    reading("r7", "GW3", "B4", 22, 35.0)
    progress("GW1", 25)                           # 08:25 即 08:00 窗口结束 +10min
    progress("GW2", 25)
    progress("GW3", 25)                           # 最慢 08:25 -> 封账 08:00 窗口

    # 封账后入账尝试（08:00 窗口已冻结）
    reading("r8", "GW1", "B1", 10, 99.0)         # 落入已封账窗口 -> 驳回
    correction("r1", 3, "B1", 9, 50.0)           # 涉及已封账窗口 -> 整笔驳回，原账不变

    # ---- 窗口 08:30 ----
    reading("r9", "GW1", "B1", 31, 11.0)
    reading("r10", "GW2", "B3", 33, 13.0)
    correction("r4", 2, "B9", 13, 40.0)          # 改泊位 B4 -> B9，仍在 08:00 窗口 -> 封账驳回
    correction("r5", 2, "B2", 32, 15.0)          # 改泊位 B1 -> B2，08:15 窗口（仍开放）
    correction("r5", 2, "B2", 32, 15.0)          # 同版本重放 -> 驳回，原账不变
    reading("r11", "GW3", "B4", 38, 17.0)
    progress("GW1", 40)
    progress("GW2", 40)
    progress("GW3", 40)                           # 最慢 08:40 -> 追封 08:15 窗口

    # ---- 窗口 08:45 ----
    reading("r12", "GW1", "B1", 46, 0.0)         # 零增量读数
    correction("r12", 2, "B1", 47, 6.0)          # 0 -> 6
    correction("r9", 2, "B2", 48, 11.0)          # 改泊位，仍在 08:30 窗口（开放）
    progress("GW1", 55)
    progress("GW2", 55)
    progress("GW3", 55)                           # 最慢 08:55 -> 追封 08:30 窗口

    # ---- 鲁棒性：坏行 / 未知类型 / 进度回退 ----
    R.append({"__raw__": '{"type": "reading", "reading_id": "rBAD", '
                         '"gateway": "GW1", "berth": "B1", "meter_ts": "not-a-time", '
                         '"energy": 1.0}'})
    R.append({"__raw__": "{not valid json"})
    R.append({"__raw__": '{"type": "telemetry", "temp": 42}'})
    progress("GW2", 30)                           # 水位 08:40 -> 08:30 回退 -> 驳回

    # 最终把水位推过最后一个窗口，封账 08:45
    progress("GW1", 70)
    progress("GW2", 70)
    progress("GW3", 70)                           # 最慢 09:10 -> 封账 08:45（08:30 已封）
    return R


def write_log(path: str) -> int:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    n = 0
    with open(path, "w", encoding="utf-8") as fh:
        for rec in build_records():
            raw = rec.pop("__raw__", None)
            line = raw if raw is not None else dumps_canonical(rec)
            fh.write(line + "\n")
            n += 1
        fh.flush()
        os.fsync(fh.fileno())
    return n


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="shorepower-mock-log", description="生成确定性模拟日志")
    p.add_argument("--out", required=True, help="输出 JSON Lines 路径")
    args = p.parse_args(argv)
    n = write_log(args.out)
    print(f"已写入 {n} 行模拟日志 -> {args.out}")
    return 0
