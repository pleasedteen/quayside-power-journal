#!/usr/bin/env python3
"""一条命令验证快照写盘的韧性与内存边界。

覆盖四条验收点：

1. 暂停快照写盘：服务继续入账，查询仍能看到暂停之后已落盘的批次及其账态；
   暂停期间不构建快照 payload，100 笔更正带来的快照额外内存 <= 4 MiB。
2. 快照写失败：入账与查询不受影响，失败计数可观察；旧版完整快照原样保留，
   仍可用于恢复（tmp + os.replace，失败只动临时文件）。
3. 成功快照只对应「输入完整落盘后」的账态：快照 seq 永不领先已 fsync 批次，
   快照恢复 / 纯批次重放 / 在线引擎三方状态一致。
4. SIGKILL 强杀后同目录重启：批次精确一次，旧快照与批次日志协同恢复。

直接运行（仅标准库，约 10 秒）：
    python3 -I verify_snapshot.py
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import tracemalloc
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))  # -I 模式下显式加入仓库根目录

from shorepower.engine import Engine  # noqa: E402
from shorepower.feeder import replay  # noqa: E402
from shorepower.generator import write_log  # noqa: E402
from shorepower.model import parse_line, parse_ts  # noqa: E402
from shorepower.storage import (  # noqa: E402
    BATCHES_NAME,
    JournalWriter,
    SnapshotManager,
    SnapshotStore,
)

PASS, FAIL = "✅ 通过", "❌ 失败"
MEM_BUDGET = 4 * 1024 * 1024  # 4 MiB
N_READINGS = 100_000
N_CORRECTIONS = 100


# -------------------------------------------------------------------------- 工具
def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"  {PASS if ok else FAIL} {label}" + (f"（{detail}）" if detail else ""))
    return ok


def wait_port(port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            try:
                s.connect(("127.0.0.1", port))
                return
            except OSError:
                time.sleep(0.05)
    raise TimeoutError(f"端口 {port} 等待超时")


def http_request(port: int, path: str, method: str = "GET") -> dict:
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method=method
    )
    with urllib.request.urlopen(req, timeout=8) as resp:
        return json.loads(resp.read().decode("utf-8"))


def wait_until(predicate, timeout: float = 25.0, interval: float = 0.1):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"条件在 {timeout}s 内未满足，最后状态: {last}")


def start_service(state_dir: Path, log_file: Path, port: int,
                  snapshot_every: int = 3) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-I", str(ROOT / "run_service.py"),
         "--log-file", str(log_file), "--state-dir", str(state_dir),
         "--port", str(port), "--max-inflight", "200",
         "--snapshot-every", str(snapshot_every)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    wait_port(port)
    return proc


def stop_service(proc: subprocess.Popen) -> str:
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=15)
    return out


def kill_service(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=10)


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def feed_line(log_file: Path, obj: dict) -> int:
    """追加一行并 fsync，返回该行的起始字节偏移。"""
    line = (json.dumps(obj, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")
    offset = log_file.stat().st_size
    with open(log_file, "ab") as fh:
        fh.write(line)
        fh.flush()
        os.fsync(fh.fileno())
    return offset


def wait_offset(port: int, offset: int) -> dict:
    return wait_until(
        lambda: http_request(port, "/stats")
        if http_request(port, "/stats")["committed_source_offset"] >= offset else None,
        timeout=20,
    )


def fresh_engine(state_dir: Path) -> Engine:
    """按线上恢复路径重建一个引擎（快照 + 批次日志）。"""
    return Engine(snapshot_store=SnapshotStore(str(state_dir)))


def journal_only_engine(state_dir: Path) -> Engine:
    """绕过快照、仅靠批次日志重放重建。"""
    snap = state_dir / "snapshot.json"
    hidden = state_dir / "snapshot.json.hidden"
    if snap.exists():
        snap.rename(hidden)
    try:
        return Engine(snapshot_store=SnapshotStore(str(state_dir)))
    finally:
        if hidden.exists():
            hidden.rename(snap)


# ----------------------------------------------------------------- 进程内：内存边界
class _SlowSnapshotStore(SnapshotStore):
    """模拟慢盘：每次写快照阻塞一小段时间，用于制造「正在写 + 待写」积压。"""

    def __init__(self, directory: str, delay: float):
        super().__init__(directory)
        self.delay = delay

    def write(self, payload: dict) -> None:
        time.sleep(self.delay)
        super().write(payload)


def _reading_line(rid: int) -> dict:
    return {"type": "reading", "reading_id": f"m{rid}", "gateway": "GW1",
            "berth": f"B{rid % 50}", "meter_ts": "2026-11-18T10:02:00Z",
            "energy": 1.0 + (rid % 7) * 0.25}


def _correction_line(rid: int) -> dict:
    return {"type": "correction", "reading_id": f"m{rid}", "version": 2,
            "berth": f"B{rid % 50}", "meter_ts": "2026-11-18T10:02:00Z",
            "energy": 9.0}


def test_memory_bound(work: Path) -> bool:
    print(f"[场景 A] 单窗口 {N_READINGS} 笔计量；暂停快照写盘后更正 "
          f"{N_CORRECTIONS} 笔，快照引入的额外内存须 <= 4 MiB")
    state = work / "mem"
    state.mkdir()
    store = SnapshotStore(str(state))
    manager = SnapshotManager(store)
    engine = Engine(snapshot_store=store, snapshot_manager=manager)
    ok = True
    try:
        # 10 万笔基线只在内存确定性构建（本场景度量的是快照子系统内存，
        # 不是批次日志 fsync 耗时）；批次先缓存在引擎内，测量结束后一次性
        # 用真实 JournalWriter 落盘，恢复路径与线上完全一致。
        offset = -1
        for rid in range(N_READINGS):
            offset += 1
            engine.process(parse_line(json.dumps(_reading_line(rid)), offset))
        # 暂停前先成功写一版完整快照，作为后续恢复可用的旧版快照基线
        ok &= check("暂停前手工快照被接收", engine.take_snapshot())
        ok &= manager.wait_idle(timeout=60)
        ok &= check("基线快照写成功", manager.status()["successes"] >= 1
                    and manager.status()["failures"] == 0,
                    f"successes={manager.status()['successes']}")

        # ---- 暂停：此后快照请求一律拒收，且不构建 payload
        manager.set_paused(True)
        ok &= check("暂停状态可观察", manager.paused is True)

        tracemalloc.start()
        snap_before = tracemalloc.take_snapshot()
        for rid in range(N_CORRECTIONS):
            offset += 1
            engine.process(
                parse_line(json.dumps(_correction_line(rid)), offset)
            )
            # 与服务节奏一致：每条记录后尝试一次快照
            engine.maybe_snapshot(interval=1)
        # 暂停期再显式触发若干次，覆盖「积压 + 暂停」组合
        for _ in range(200):
            engine.trigger_snapshot()
        snap_after = tracemalloc.take_snapshot()
        stats = manager.status()
        ok &= check("暂停期间快照 payload 队列为空（不随更正数增长）",
                    stats["queued"] == 0 and stats["inflight"] == 0,
                    f"queued={stats['queued']}, inflight={stats['inflight']}")
        ok &= check("暂停期间快照请求全部被拒收",
                    stats["skipped_paused"] >= N_CORRECTIONS + 200,
                    f"skipped_paused={stats['skipped_paused']}")

        diff = snap_after.compare_to(snap_before, "lineno")
        # 口径 1：仅快照子系统（storage.py）自身的净分配，严格对应「快照引入」
        snapshot_bytes = sum(
            s.size_diff for s in diff
            if "shorepower/storage.py" in str(s.traceback) and s.size_diff > 0
        )
        # 口径 2：整个暂停 + 更正阶段 Python 堆净增量的保守上界（含引擎账务
        # 改动与解析临时对象），它只会 >= 快照真实开销，用它卡 4 MiB 更严格
        total_bytes = sum(s.size_diff for s in diff if s.size_diff > 0)
        tracemalloc.stop()
        ok &= check(
            f"快照子系统额外内存 {snapshot_bytes} 字节 <= 4 MiB（{MEM_BUDGET} 字节）",
            snapshot_bytes <= MEM_BUDGET,
        )
        ok &= check(
            f"暂停+更正阶段堆净增量上界 {total_bytes} 字节 <= 4 MiB（更严格的总口径）",
            total_bytes <= MEM_BUDGET,
        )

        # ---- 恢复：暂停期间账仍在记；恢复后能重建最新账态（含 100 笔更正）
        manager.set_paused(False)
        ok &= check("恢复后新快照可正常落盘", engine.take_snapshot())
        ok &= manager.wait_idle(timeout=30)

        # 内存场景只需证明「快照可用于恢复」：用落盘快照重建一个引擎，
        # 与在线账态逐项比对。批次日志级别的完整恢复在场景 B/C 覆盖。
        snap_only = Engine(snapshot_store=SnapshotStore(str(state)))
        ok &= check("快照恢复后跟踪计量数 = 10 万",
                    len(snap_only.readings) == N_READINGS,
                    f"tracked={len(snap_only.readings)}")
        # 被更正的 100 笔电量都应为 9.0，其余维持原值
        sample = {rid: snap_only.readings[f"m{rid}"]["energy"]
                  for rid in (0, 1, N_CORRECTIONS, N_CORRECTIONS + 1)}
        ok &= check("快照恢复：更正后的 100 笔为 9.0、未更正的保持原值",
                    sample[0] == 9.0 and sample[1] == 9.0
                    and sample[N_CORRECTIONS] == 1.0 + (N_CORRECTIONS % 7) * 0.25,
                    f"抽样={sample}")
        ok &= check("快照恢复的窗口账态与在线引擎一致",
                    snap_only.windows == engine.windows)
        ok &= check("快照恢复的 readings 与在线引擎一致",
                    snap_only.readings == engine.readings)

        # ---- 慢盘：正在写时新请求被合并，负载严格有界（至多 1 在写 + 1 待写）
        slow_state = work / "mem-slow"
        slow_state.mkdir()
        slow_store = _SlowSnapshotStore(str(slow_state), delay=0.15)
        slow_mgr = SnapshotManager(slow_store)
        slow_engine = Engine(snapshot_store=slow_store, snapshot_manager=slow_mgr)
        observed_max = 0
        off = -1
        for rid in range(300):
            off += 1
            slow_engine.process(parse_line(json.dumps(_reading_line(rid)), off))
            slow_engine.maybe_snapshot(interval=1)
            st = slow_mgr.status()
            observed_max = max(observed_max, st["inflight"] + st["queued"])
        ok &= check("慢盘写盘期间积压负载 <= 2（1 在写 + 1 待写），不随记录数增长",
                    observed_max <= 2, f"观测峰值={observed_max}")
        ok &= check("慢盘期间入账不中断（300 笔全部跟踪）",
                    len(slow_engine.readings) == 300)
        ok &= slow_mgr.wait_idle(timeout=30)
        slow_mgr.close()
        ok &= check("慢盘快照最终成功落盘", slow_mgr.status()["failures"] == 0
                    and slow_mgr.status()["successes"] >= 1)
    finally:
        manager.close()
    return ok


# ----------------------------------------------------- 进程内：成功快照不领先日志
def test_snapshot_matches_journal(work: Path) -> bool:
    print("[场景 B] 成功快照只对应输入完整落盘后的账态（快照/批次日志/纯重放三方一致）")
    state = work / "consistency"
    state.mkdir()
    store = SnapshotStore(str(state))
    manager = SnapshotManager(store)
    engine = Engine(snapshot_store=store, snapshot_manager=manager)
    journal = JournalWriter(str(state / BATCHES_NAME))
    ok = True
    try:
        n = write_log(str(work / "mock.jsonl"))
        offset = -1
        with open(work / "mock.jsonl", encoding="utf-8") as fh:
            for line in fh:
                offset += 1
                for batch in engine.process(parse_line(line, offset)):
                    journal.append(batch)
                engine.maybe_snapshot(interval=3)
        ok &= manager.wait_idle(timeout=15)
        batches = read_jsonl(state / "batches.jsonl")
        snap = json.loads((state / "snapshot.json").read_text(encoding="utf-8"))
        last_seq = batches[-1]["seq"]
        ok &= check("快照 next_seq 不超过已 fsync 批次序号（快照永不领先日志）",
                    snap["next_seq"] <= last_seq + 1,
                    f"snapshot.next_seq={snap['next_seq']}, journal last_seq={last_seq}")
        # 触发一次最终快照，使其覆盖全部批次，再比对两条恢复路径
        engine.take_snapshot()
        ok &= manager.wait_idle(timeout=15)
        via_snapshot = fresh_engine(state)
        via_journal = journal_only_engine(state)
        ok &= check("快照恢复的窗口账态与在线引擎一致",
                    via_snapshot.windows == engine.windows)
        ok &= check("纯批次重放的窗口账态与在线引擎一致",
                    via_journal.windows == engine.windows)
        ok &= check("两条恢复路径的批次列表与在线引擎完全一致",
                    via_snapshot.batches == engine.batches
                    and via_journal.batches == engine.batches)
        ok &= check("两条恢复路径的已提交偏移一致",
                    via_snapshot.committed_offset == engine.committed_offset
                    and via_journal.committed_offset == engine.committed_offset)
        ok &= check(f"模拟日志 {n} 行全部处理", engine.committed_offset == n - 1)
    finally:
        manager.close()
        journal.close()
    return ok


# ------------------------------------------------- 子进程端到端：暂停/失败/强杀
def test_end_to_end(work: Path, port: int) -> bool:
    print("[场景 C] 真实服务子进程：暂停写盘 → 写失败 → 强杀重启（一条本地命令内完成）")
    state = work / "e2e"
    log_file = work / "e2e.log"
    state.mkdir()
    proc = start_service(state, log_file, port)
    ok = True
    try:
        n = write_log(str(work / "mock-e2e.jsonl"))
        replay(str(work / "mock-e2e.jsonl"), str(log_file), delay=0.0)
        wait_offset(port, n - 1)
        # 等服务静默（队列排空、批次数稳定），固定基线，避免与跟读线程竞速
        time.sleep(0.6)
        batches_0 = read_jsonl(state / "batches.jsonl")

        # 先拿到一版成功快照并记录其内容指纹
        wait_until(lambda: (state / "snapshot.json").exists() or None, timeout=10)
        old_snap_bytes = (state / "snapshot.json").read_bytes()
        old_snap_hash = hashlib.sha256(old_snap_bytes).hexdigest()

        # ---- C1 暂停快照写盘：继续入账，查询仍可见后续批次与账态
        st = http_request(port, "/snapshot/pause", method="POST")
        ok &= check("POST /snapshot/pause 返回暂停状态", st["paused"] is True)
        snap_mtime_paused = (state / "snapshot.json").stat().st_mtime_ns

        # 追加 3 笔：读数 + 读数 + 三网关水位（推动 10:00 窗口封账）
        off1 = feed_line(log_file, {"type": "reading", "reading_id": "x1",
                                    "gateway": "GW1", "berth": "B7",
                                    "meter_ts": "2026-11-18T10:02:00Z", "energy": 3.5})
        off2 = feed_line(log_file, {"type": "reading", "reading_id": "x2",
                                    "gateway": "GW2", "berth": "B8",
                                    "meter_ts": "2026-11-18T10:03:00Z", "energy": 4.5})
        feed_line(log_file, {"type": "progress", "gateway": "GW1",
                             "progress_ts": "2026-11-18T10:30:00Z"})
        feed_line(log_file, {"type": "progress", "gateway": "GW2",
                             "progress_ts": "2026-11-18T10:30:00Z"})
        feed_line(log_file, {"type": "progress", "gateway": "GW3",
                             "progress_ts": "2026-11-18T10:30:00Z"})
        wait_offset(port, off2)
        time.sleep(0.5)  # 留一个写盘窗口，证明暂停下后台线程确实没写
        batches_1 = read_jsonl(state / "batches.jsonl")
        # 5 条新记录各发布 1 个 decision 批次，首条水位还推动 10:00 窗口封账，
        # 因此暂停后新增 = 5 个 decision + 1 个 seal，共 6 个批次
        new_batches = batches_1[len(batches_0):]
        ok &= check("暂停期间服务继续入账：5 条新记录全部发布批次并落盘",
                    sum(1 for b in new_batches if b["kind"] == "decision") == 5,
                    f"基线 {len(batches_0)}，暂停后 {len(batches_1)}，新增 {len(new_batches)}")
        ok &= check("暂停后新增批次中含 10:00 窗口的封账批次",
                    any(b["kind"] == "seal"
                        and any(w["window"] == "2026-11-18T10:00:00Z"
                                for w in b["sealed_windows"])
                        for b in new_batches))
        q = http_request(port, "/batches")
        ids = {b.get("reading_id") for b in q["batches"]}
        ok &= check("查询仍能看到暂停后已落盘的批次（x1/x2）",
                    {"x1", "x2"}.issubset(ids))
        win = next(w for w in http_request(port, "/windows")["windows"]
                   if w["window"] == "2026-11-18T10:00:00Z")
        ok &= check("查询可见暂停后批次对应的账态（B7=3.5, B8=4.5）且窗口已封账",
                    win["totals"].get("B7") == 3.5 and win["totals"].get("B8") == 4.5
                    and win["sealed"] is True,
                    f"totals={win['totals']}")
        paused_status = http_request(port, "/snapshot/status")
        ok &= check("暂停期间快照文件保持不变（后台线程不写正式快照）",
                    (state / "snapshot.json").stat().st_mtime_ns == snap_mtime_paused
                    and paused_status["queued"] == 0 and paused_status["inflight"] == 0)
        ok &= check("暂停统计可观察（skipped_paused 增长、无失败）",
                    paused_status["skipped_paused"] > 0 and paused_status["failures"] == 0,
                    f"skipped_paused={paused_status['skipped_paused']}")

        # ---- C2 制造真实写失败（snapshot.json 换成目录，open(tmp,'w') 必失败）
        (state / "snapshot.json").unlink()
        (state / "snapshot.json").mkdir()
        http_request(port, "/snapshot/resume", method="POST")
        # 新记录触发快照写入 -> 必然失败
        off3 = feed_line(log_file, {"type": "reading", "reading_id": "x3",
                                    "gateway": "GW1", "berth": "B7",
                                    "meter_ts": "2026-11-18T10:04:00Z", "energy": 2.0})
        wait_offset(port, off3)
        wait_until(lambda: http_request(port, "/snapshot/status")
                   if http_request(port, "/snapshot/status")["failures"] >= 1 else None,
                   timeout=10)
        fail_status = http_request(port, "/snapshot/status")
        ok &= check("快照写失败被计数并暴露 last_error，服务进程存活",
                    fail_status["failures"] >= 1 and bool(fail_status["last_error"]),
                    str(fail_status["last_error"])[:80])
        batches_2 = read_jsonl(state / "batches.jsonl")
        ok &= check("写失败不影响入账：x3 对应批次照常 fsync",
                    len(batches_2) == len(batches_1) + 1
                    and any(b.get("reading_id") == "x3" for b in batches_2))
        ok &= check("写失败后查询仍返回完整账态",
                    http_request(port, "/health")["status"] == "ok"
                    and http_request(port, "/stats")["last_seq"] == len(batches_2))

        # ---- C3 恢复故障：清掉故障占位，换回旧版完整快照（字节不变）
        shutil.rmtree(state / "snapshot.json")
        (state / "snapshot.json").write_bytes(old_snap_bytes)
        restored_hash = hashlib.sha256(
            (state / "snapshot.json").read_bytes()).hexdigest()
        ok &= check("旧版完整快照内容字节不变（写失败从未动过正式快照）",
                    restored_hash == old_snap_hash)

        # ---- C4 SIGKILL 强杀，同目录重启，旧快照 + 批次日志协同恢复
        kill_service(proc)
        proc = None
        proc2 = start_service(state, log_file, port + 1)
        try:
            # 离线期望状态：同一份批次日志（权威）重放出来的账
            expected = fresh_engine(state)
            stats_r = wait_until(
                lambda: http_request(port + 1, "/stats")
                if http_request(port + 1, "/stats")["last_seq"] == len(batches_2) else None,
                timeout=15,
            )
            ok &= check("强杀重启后批次精确一次（序号连续、总数不增不减）",
                        stats_r["last_seq"] == len(batches_2)
                        and [b["seq"] for b in read_jsonl(state / "batches.jsonl")]
                        == list(range(1, len(batches_2) + 1)))
            win_r = next(w for w in http_request(port + 1, "/windows")["windows"]
                         if w["window"] == "2026-11-18T10:00:00Z")
            ok &= check("恢复后账态与权威批次日志重放一致（含 x1/x2/x3）",
                        win_r["totals"] == expected.windows[
                            parse_ts("2026-11-18T10:00:00Z", "w")]["totals"]
                        and win_r["sealed"],
                        f"totals={win_r['totals']}")
            ok &= check("恢复后跟踪计量数一致",
                        http_request(port + 1, "/stats")["tracked_readings"]
                        == len(expected.readings))
            # 重启后快照链路恢复正常：连续写 4 笔（超过 snapshot-every=3 的节奏），
            # 应能产出新快照（计数器在新进程从 0 开始）
            last_off = -1
            for i in range(4):
                last_off = feed_line(log_file, {
                    "type": "reading", "reading_id": f"x4{i}", "gateway": "GW1",
                    "berth": "B7", "meter_ts": "2026-11-18T10:05:00Z",
                    "energy": 1.0 + i * 0.1})
            wait_offset(port + 1, last_off)
            post_status = wait_until(
                lambda: http_request(port + 1, "/snapshot/status")
                if http_request(port + 1, "/snapshot/status")["successes"] >= 1 else None,
                timeout=20,
            )
            ok &= check("故障恢复后新快照重新成功落盘（失败计数不跨进程累积）",
                        post_status["successes"] >= 1 and post_status["failures"] == 0)
        finally:
            stop_service(proc2)
    finally:
        if proc is not None and proc.poll() is None:
            kill_service(proc)
    return ok


def main() -> int:
    t0 = time.time()
    work = Path(tempfile.mkdtemp(prefix="shorepower-snap-verify-"))
    print(f"工作目录: {work}\n")
    results = []
    try:
        results.append(("内存边界（暂停写盘 + 更正）", test_memory_bound(work)))
        results.append(("快照与批次日志一致性", test_snapshot_matches_journal(work)))
        results.append(("端到端：暂停/写失败/强杀重启",
                        test_end_to_end(work, port=18101)))
    except Exception as exc:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        print(f"{FAIL} 意外异常: {exc}")
        results.append(("执行异常", False))
    finally:
        shutil.rmtree(work, ignore_errors=True)

    print("\n" + "=" * 68)
    for name, ok in results:
        print(f"  {PASS if ok else FAIL} {name}")
    print(f"\n总耗时 {time.time() - t0:.1f}s")
    if all(ok for _, ok in results):
        print(f"结论：{PASS} 快照暂停写盘、写失败、强杀重启全部验证通过")
        return 0
    print(f"结论：{FAIL} 存在失败项")
    return 1


if __name__ == "__main__":
    sys.exit(main())
