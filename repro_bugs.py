#!/usr/bin/env python3
"""修复后行为验证（对照 repro_bugs.py 的两个缺陷）。

协议：prepare(私有暂存) -> journal.append_group(整组 fsync) -> commit(原子换入)。
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from shorepower.engine import Engine
from shorepower.model import parse_line
from shorepower.storage import JournalWriter, SnapshotStore

work = Path(tempfile.mkdtemp(prefix="shorepower-fixed-"))
state_dir = work / "state"
state_dir.mkdir()

eng = Engine(snapshot_store=SnapshotStore(str(state_dir)))
journal = JournalWriter(str(state_dir / "batches.jsonl"))


def feed(line: str, offset: int):
    rec = parse_line(line, offset=offset)
    batches = eng.prepare(rec)
    journal.append_group(batches)
    eng.commit(batches)


# 读数正常提交
feed('{"type":"reading","reading_id":"r1","gateway":"GW1","berth":"B1",'
     '"meter_ts":"2026-11-18T08:02:00Z","energy":10.0}\n', 0)

# 更正：prepare 后、fsync 前 —— 查询必须仍读到旧账 10.0
rec = parse_line('{"type":"correction","reading_id":"r1","version":2,"berth":"B1",'
                 '"meter_ts":"2026-11-18T08:02:00Z","energy":12.0}\n', offset=100)
batches = eng.prepare(rec)
win = eng.query_windows()["windows"][0]
assert win["totals"].get("B1") == 10.0, win
print("[修复 A] prepare 后未 fsync，查询仍读完整旧账 B1 =", win["totals"]["B1"], "✅")

# 模拟强杀：丢弃进程，仅按批次日志恢复（更正组从未落盘）
eng2 = Engine(snapshot_store=SnapshotStore(str(state_dir)))
win2 = eng2.query_windows()["windows"][0]
assert win2["totals"].get("B1") == 10.0, win2
print("[修复 A] 强杀重建后 B1 =", win2["totals"]["B1"], "（旧账完整，无幽灵更正）✅")

# 正常把更正提交掉
journal.append_group(batches)
eng.commit(batches)
assert eng.query_windows()["windows"][0]["totals"]["B1"] == 12.0
print("[修复 A] fsync+commit 后查询才读到新账 B1 = 12.0 ✅")

# 缺陷 B：progress 同组携带 decision+seal，整组一次落盘
feed('{"type":"reading","reading_id":"r2","gateway":"GW2","berth":"B2",'
     '"meter_ts":"2026-11-18T08:03:00Z","energy":5.0}\n', 200)
sealed_before = eng.query_windows()["windows"][0]["sealed"]
for gw in ("GW1", "GW2"):
    line = ('{"type":"progress","gateway":"' + gw
            + '","progress_ts":"2026-11-18T08:30:00Z"}\n')
    rec = parse_line(line, offset=300)
    out = eng.prepare(rec)
    kinds = [b["kind"] for b in out]
    if len(out) == 2:
        assert kinds == ["decision", "seal"], kinds
        assert out[-1].get("group_end") is True
        print("[修复 B] 触发封账的 progress 整组批次：", kinds,
              "组标记 group_end =", out[-1]["group_end"], "✅")
    journal.append_group(out)
    eng.commit(out)

assert eng.query_windows()["windows"][0]["sealed"] is True
# 模拟“组内强杀”后恢复：手工只写 decision（无 group_end），恢复时整组回滚
raw = (state_dir / "batches.jsonl").read_text(encoding="utf-8").splitlines()
torn = state_dir / "torn.jsonl"
import json
last_group = [json.loads(x) for x in raw[-2:]]
decision_only = dict(last_group[0])
decision_only.pop("group_end", None)
torn.write_text(
    "\n".join(raw[:-2] + [json.dumps(decision_only, sort_keys=True)]) + "\n",
    encoding="utf-8",
)
from shorepower.storage import recover_committed_batches
kept = recover_committed_batches(str(torn))
assert all(b["seq"] <= len(raw) - 2 for b in kept), "未完成组必须整组截断"
print("[修复 B] 强杀落在组内时，未完成的 decision+seal 整组回滚，保留",
      len(kept), "个已提交批次，源记录将重放补封 ✅")
print("工作目录:", work)
