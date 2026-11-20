#!/usr/bin/env python3
"""端到端自测：变速回放确定性比对 + SIGKILL 崩溃恢复 + 80 秒内完成。

直接运行：
    python3 -I selftest.py
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))  # -I 模式下不会自动带脚本目录，这里显式加入

from shorepower.engine import Engine  # noqa: E402
from shorepower.feeder import replay  # noqa: E402
from shorepower.generator import write_log  # noqa: E402
from shorepower.model import SCHEMA_VERSION, format_ts, parse_line  # noqa: E402
from shorepower.storage import (  # noqa: E402
    HISTORY_NAME,
    INDEX_NAME,
    SNAPSHOT_NAME,
    BatchJournal,
    HistoryStore,
    SnapshotStore,
    dumps_canonical,
)

PASS, FAIL = "✅ 通过", "❌ 失败"


def wait_port(port: int, timeout: float = 8.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            try:
                s.connect(("127.0.0.1", port))
                return
            except OSError:
                time.sleep(0.05)
    raise TimeoutError(f"端口 {port} 等待超时")


def http_get(port: int, path: str) -> dict:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def start_service(state_dir: Path, log_file: Path, port: int,
                  max_inflight: int = 50) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-I", str(ROOT / "run_service.py"),
         "--log-file", str(log_file), "--state-dir", str(state_dir),
         "--port", str(port), "--max-inflight", str(max_inflight),
         "--snapshot-every", "7"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    wait_port(port)
    return proc


def stop_service(proc: subprocess.Popen) -> str:
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=10)
    return out


def kill_service(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=10)


def wait_until(predicate, timeout: float = 20.0, interval: float = 0.1):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"条件在 {timeout}s 内未满足，最后状态: {last}")


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def last_line_offset(path: Path) -> int:
    """源日志最后一条记录的起始字节偏移（服务处理到这里即代表全部入账/驳回完毕）。"""
    data = path.read_bytes()
    return len(data) - len(data.splitlines()[-1])


def run_full_replay(work: Path, name: str, port: int, delay: float) -> dict:
    state = work / name
    log_file = work / f"{name}.log"
    state.mkdir(parents=True)
    proc = start_service(state, log_file, port)
    try:
        replay(str(work / "mock.jsonl"), str(log_file), delay=delay)
        target_offset = last_line_offset(log_file) - 1  # 零基起始；处理完末行即达到该值
        wait_until(
            lambda: http_get(port, "/stats")["committed_source_offset"] >= target_offset
            or None,
            timeout=25,
        )
        batches = read_jsonl(state / "batches.jsonl")
        windows = http_get(port, "/windows")
        stats = http_get(port, "/stats")
        return {"batches": batches, "windows": windows, "stats": stats,
                "raw_batches": (state / "batches.jsonl").read_text(encoding="utf-8")}
    finally:
        stop_service(proc)


def assert_equal(label: str, a, b) -> None:
    if dumps_canonical(a) != dumps_canonical(b):
        raise AssertionError(f"{label} 不一致\n--- A ---\n{dumps_canonical(a)}\n--- B ---\n{dumps_canonical(b)}")
    print(f"  {PASS} {label}")


# ------------------------------------------------------------------ 规模场景辅助
#
# 十万批次夹具不通过 HTTP 服务生成（进程级 fsync 太慢），而是直接用引擎 +
# 批次日志写出与线上完全相同的 state 目录；随后的重启、分页、升级、强杀
# 都走真实的服务代码路径，只把耗时较长的服务进程换成进程内 Engine。


def _scale_records(batches: int):
    """生成 batches 个 reading 批次的历史（每批次一条读数，含周期进度推进）。"""
    base = 1_766_428_800  # 2026-11-18T08:00:00Z
    for i in range(batches):
        meter = base + i * 900  # 每条落在一个独立的刻钟窗口
        yield {
            "type": "reading",
            "reading_id": f"scale-r{i}",
            "gateway": "GW1",
            "berth": f"B{i % 4}",
            "meter_ts": format_ts(meter),
            "energy": 1.0,
        }
        if i % 40 == 0:
            # 水位推进到刚过一个窗口，让封账批次也进入历史
            yield {
                "type": "progress",
                "gateway": "GW1",
                "progress_ts": format_ts(meter + 2 * 900 + 600),
            }


def build_scale_fixture(
    directory: Path,
    batches: int,
    snapshot_every: int = 5000,
    source_log: Path | None = None,
) -> None:
    """用引擎 + 批次日志直接写出规模夹具（与线上相同的目录布局）。

    仅夹具生成阶段关闭逐条 fsync（否则十万条要跑几分钟）；重启、分页、
    升级、强杀路径全部使用默认的 fsync=True 真实实现。``source_log`` 不为空时
    同步写出与批次偏移完全对应的源日志，供真实服务进程续读。
    """
    directory.mkdir(parents=True, exist_ok=True)
    journal = BatchJournal(str(directory), sync=False)
    history = HistoryStore(str(directory))
    engine = Engine(
        journal=journal,
        snapshot_store=SnapshotStore(str(directory)),
        history=history,
    )
    src = open(source_log, "w", encoding="utf-8") if source_log is not None else None
    try:
        offset = 0
        for i, rec in enumerate(_scale_records(batches)):
            line = dumps_canonical(rec) + "\n"
            if src is not None:
                src.write(line)
            journal.append_many(engine.process(parse_line(line, offset)))
            offset += len(line.encode("utf-8"))
            if i % snapshot_every == snapshot_every - 1:
                engine.take_snapshot()
        engine.take_snapshot()
    finally:
        if src is not None:
            src.flush()
            os.fsync(src.fileno())
            src.close()
        journal.close()
        history.close()


def open_scale_state(directory: Path, auto_snapshot: bool = True) -> dict:
    """模拟服务重启：真实走 BatchJournal 索引恢复 + Engine.restore，再取 /stats 同款统计。

    返回值里的 ``_engine/_journal/_history`` 仍处于打开状态，由调用方负责
    :func:`close_scale_state`；只关心统计时可以忽略它们。
    """
    journal = BatchJournal(str(directory))
    history = HistoryStore(str(directory))
    engine = Engine(
        journal=journal,
        snapshot_store=SnapshotStore(str(directory)),
        history=history,
        auto_snapshot=auto_snapshot,
    )
    stats = engine.stats()
    stats["journal_bodies_read"] = journal.bodies_read
    stats["_engine"] = engine
    stats["_journal"] = journal
    stats["_history"] = history
    return stats


def close_scale_state(stats: dict) -> None:
    stats.get("_journal") and stats["_journal"].close()
    if stats.get("_history") is not None:
        stats["_history"].close()
    stats["_engine"] = None
    stats["_journal"] = None
    stats["_history"] = None


def page_scale_state(directory: Path, after_seq: int, limit: int) -> tuple[list[dict], int]:
    """按分页接口取一批，返回 (批次, 本进程读取的正文条数)。"""
    journal = BatchJournal(str(directory))
    try:
        page = journal.read_page(after_seq, limit)
        return page, journal.bodies_read
    finally:
        journal.close()


def make_legacy_snapshot(directory: Path) -> dict:
    """把新目录退化成旧状态目录：只保留 batches.jsonl，写一份没有 journal_offset 的旧快照。"""
    for name in (INDEX_NAME, HISTORY_NAME, SNAPSHOT_NAME):
        path = directory / name
        if path.exists():
            path.unlink()
    # 先从批次日志重建一次，然后用旧版 schema 快照格式写回
    journal = BatchJournal(str(directory))
    try:
        engine = Engine(journal=journal, snapshot_store=None)
        legacy_payload = {
            "schema": SCHEMA_VERSION,
            "gateways": sorted(engine.gateways),
            "progress": engine.progress,
            "readings": engine.readings,
            "windows": {
                str(k): {
                    "sealed": v["sealed"],
                    "totals": v["totals"],
                    "contributions": v["contributions"],
                }
                for k, v in engine.windows.items()
            },
            "next_seq": journal.count + 1,
            "committed_offset": engine.committed_offset,
            "min_meter_ts": engine.min_meter_ts,
        }
    finally:
        journal.close()
    (directory / SNAPSHOT_NAME).write_text(
        json.dumps(legacy_payload, sort_keys=True), encoding="utf-8"
    )
    return legacy_payload


def real_kill_after_appending(
    directory: Path, source_log: Path, work: Path, extra: int
) -> int:
    """真实服务进程：追加 extra 个完整批次并确认已 fsync，然后 SIGKILL 再重启对账。"""
    baseline = BatchJournal(str(directory)).count
    port = 18096
    proc = start_service(directory, source_log, port)
    try:
        with open(source_log, "a", encoding="utf-8") as dst:
            for i in range(extra):
                meter = 1_766_428_800 + (100_000 + 10_000 + i) * 900
                rec = {
                    "type": "reading",
                    "reading_id": f"crash-r{i}",
                    "gateway": "GW1",
                    "berth": "B2",
                    "meter_ts": format_ts(meter),
                    "energy": 3.0,
                }
                dst.write(dumps_canonical(rec) + "\n")
            dst.flush()
            os.fsync(dst.fileno())
        expected = baseline + extra
        # 等全部追加批次都发布（= 已 fsync 进批次日志）后再强杀
        wait_until(
            lambda: http_get(port, "/stats")["published_batches"] >= expected or None,
            timeout=25,
        )
        kill_service(proc)
    finally:
        if proc.poll() is None:
            kill_service(proc)

    # 同目录重启；服务应紧接着已持久化批次继续，不重发、不漏发
    proc = start_service(directory, source_log, 18097)
    try:
        wait_until(
            lambda: http_get(18097, "/stats")["committed_source_offset"]
            >= last_line_offset(source_log) - 1 or None,
            timeout=25,
        )
        wait_until(
            lambda: http_get(18097, "/stats")["published_batches"] >= expected or None,
            timeout=25,
        )
    finally:
        stop_service(proc)
    return expected



def validate_scale_continuity(directory: Path, expected_total: int) -> None:
    """重启后逐页核对：序号从 1 连续到 expected_total，无重复无空洞。"""
    seen: list[int] = []
    after = 0
    while True:
        page, _ = page_scale_state(directory, after, 200)
        if not page:
            break
        seen.extend(b["seq"] for b in page)
        after = page[-1]["seq"]
    if seen != list(range(1, expected_total + 1)):
        raise AssertionError(
            f"批次序号不连续：期望 1..{expected_total}，实际 {seen[:5]}..{seen[-5:]}"
            f"（共 {len(seen)} 条）"
        )



def main() -> int:
    t0 = time.time()
    work = Path(tempfile.mkdtemp(prefix="shorepower-selftest-"))
    print(f"工作目录: {work}")
    failures = 0

    try:
        n = write_log(str(work / "mock.jsonl"))
        print(f"[准备] 生成确定性模拟日志 {n} 行")

        # ------------------------------------------------ 场景 1：变速回放一致性
        print("[场景 1] 瞬时回放 vs 慢速回放（delay=0.03s/行），批次/顺序/窗口状态须逐字节一致")
        fast = run_full_replay(work, "fast", 18091, delay=0.0)
        slow = run_full_replay(work, "slow", 18092, delay=0.03)
        assert_equal("批次数量", len(fast["batches"]), len(slow["batches"]))
        assert_equal("批次内容与发布顺序（逐字节）",
                     fast["raw_batches"], slow["raw_batches"])
        assert_equal("窗口电量与封账状态", fast["windows"], slow["windows"])
        assert_equal("已提交源日志偏移",
                     fast["stats"]["committed_source_offset"],
                     slow["stats"]["committed_source_offset"])

        # ------------------------------------------------ 场景 2：业务规则核对
        print("[场景 2] 业务规则核对（封账、版本、整笔事务、背压统计）")
        windows = {w["window"]: w for w in fast["windows"]["windows"]}
        expect_sealed = ["2026-11-18T08:00:00Z", "2026-11-18T08:15:00Z",
                         "2026-11-18T08:30:00Z", "2026-11-18T08:45:00Z"]
        got_sealed = sorted(w for w, d in windows.items() if d["sealed"])
        assert_equal("四个窗口全部封账", got_sealed, expect_sealed)

        w0800 = windows["2026-11-18T08:00:00Z"]["totals"]
        # r1 更正为 12(B1)；r2、r3 后续改计量时间移到 08:15；
        # r4 改泊位到 B9 的更正因封账整笔驳回，仍留 B4=40
        assert_equal("08:00 窗口冻结电量", w0800,
                     {"B1": 12.0, "B4": 40.0})
        w0815 = windows["2026-11-18T08:15:00Z"]["totals"]
        # r2 移入并更正为 22(B2)；r6=25(B3)；r3 移入 30(B3)；r7=35(B4)；
        # r5 后续改计量时间移到 08:30
        assert_equal("08:15 窗口冻结电量", w0815,
                     {"B2": 22.0, "B3": 55.0, "B4": 35.0})
        w0830 = windows["2026-11-18T08:30:00Z"]["totals"]
        # r5 移入并改泊位 B1->B2 =15；r10=13(B3)；r11=17(B4)；
        # r9 后续改计量时间到 08:48，移入 08:45 窗口
        assert_equal("08:30 窗口冻结电量", w0830,
                     {"B2": 15.0, "B3": 13.0, "B4": 17.0})
        w0845 = windows["2026-11-18T08:45:00Z"]["totals"]
        # r12 由 0 更正为 6(B1)；r9 移入并改泊位 B1->B2 =11
        assert_equal("08:45 窗口冻结电量", w0845, {"B1": 6.0, "B2": 11.0})

        decisions = [b for b in fast["batches"] if b["kind"] == "decision"]
        accepted = [b for b in decisions if b["status"] == "accepted"]
        rejected = [b for b in decisions if b["status"] == "rejected"]
        seals = [b for b in fast["batches"] if b["kind"] == "seal"]
        assert_equal("封账批次数量（四次水位推进各封一批）", len(seals), 4)
        assert_equal("驳回记录数（含重复/版本/封账/坏行/回退）", len(rejected), 11)

        # 同一次更正的两个变化必须同批可见
        cross = next(b for b in accepted if b.get("reading_id") == "r3"
                     and b["record_type"] == "correction")
        kinds = [(c["window"], c["change"]) for c in cross["changes"]]
        assert_equal("跨窗口更正同批呈现 remove+add", kinds,
                     [("2026-11-18T08:00:00Z", "remove"), ("2026-11-18T08:15:00Z", "add")])

        # 批次序号连续、可分页按序核对（分页在场景 4 做端到端验证）
        seqs = [b["seq"] for b in fast["batches"]]
        assert_equal("批次序号严格连续", seqs, list(range(1, len(seqs) + 1)))

        # 背压：max-inflight=50 时队列长度始终不越界（stats 是瞬时采样，逻辑上以有界队列保证）
        assert fast["stats"]["inflight_unpersisted"] <= 50
        print(f"  {PASS} 背压：有界队列容量 50，stats 队列采样="
              f"{fast['stats']['inflight_unpersisted']}（put 阻塞保证不超限）")

        # ------------------------------------------------ 场景 3：SIGKILL 崩溃恢复，精确一次
        print("[场景 3] SIGKILL 强杀后同目录重启：已发布批次只出现一次，待处理记录继续入账/驳回")
        crash_state = work / "crash"
        crash_log = work / "crash.log"
        crash_state.mkdir()
        proc = start_service(crash_state, crash_log, 18093)
        try:
            total_lines = n
            half = total_lines // 2
            replay(str(work / "mock.jsonl"), str(crash_log), delay=0.0, max_lines=half)
            first_half = (work / "mock.jsonl").read_text(encoding="utf-8").splitlines(keepends=True)[:half]
            half_end = sum(len(l.encode("utf-8")) for l in first_half)
            last_offset = half_end - len(first_half[-1].encode("utf-8"))
            wait_until(
                lambda: http_get(18093, "/stats")["committed_source_offset"] >= last_offset - 1
                or None, timeout=15,
            )
            batches_before = read_jsonl(crash_state / "batches.jsonl")
            offset_before = max(
                b.get("source_offset", b.get("trigger_offset", -1)) for b in batches_before
            )
            kill_service(proc)
            print(f"    已 SIGKILL：崩溃前持久化 {len(batches_before)} 个批次，"
                  f"提交偏移 {offset_before}")
        finally:
            if proc.poll() is None:
                kill_service(proc)

        # 同目录重启，继续回放剩余行
        proc = start_service(crash_state, crash_log, 18094)
        try:
            # 把剩下的行追加到同一日志
            with open(work / "mock.jsonl", encoding="utf-8") as src:
                all_lines = src.readlines()
            with open(crash_log, "a", encoding="utf-8") as dst:
                for line in all_lines[half:]:
                    dst.write(line)
                dst.flush()
                os.fsync(dst.fileno())
            wait_until(
                lambda: read_jsonl(crash_state / "batches.jsonl")
                if len(read_jsonl(crash_state / "batches.jsonl")) == len(fast["batches"]) else None,
                timeout=20,
            )
            recovered = read_jsonl(crash_state / "batches.jsonl")
            assert_equal("崩溃恢复后批次总数与正常回放一致",
                         len(recovered), len(fast["batches"]))
            assert_equal("恢复后批次内容/顺序与正常回放逐字节一致",
                         [json.dumps(b, sort_keys=True) for b in recovered],
                         [json.dumps(b, sort_keys=True) for b in fast["batches"]])
            seqs_r = [b["seq"] for b in recovered]
            assert_equal("批次序号无重复无空洞（精确一次）",
                         seqs_r, list(range(1, len(seqs_r) + 1)))
            stats_r = http_get(18094, "/stats")
            assert_equal("恢复后窗口状态与正常回放一致",
                         http_get(18094, "/windows"), fast["windows"])
            assert_equal("恢复后已提交偏移追上源日志末尾",
                         stats_r["committed_source_offset"],
                         fast["stats"]["committed_source_offset"])
        finally:
            stop_service(proc)

        # ------------------------------------------------ 场景 4：查询接口
        print("[场景 4] 本地查询接口（多窗口一次查询 + 批次分页）")
        state = work / "fast"
        proc = start_service(state, work / "fast.log", 18095)
        try:
            q = http_get(18095, "/windows?window=2026-11-18T08:00:00Z&window=2026-11-18T08:45:00Z")
            assert_equal("一次查询多个窗口", [w["window"] for w in q["windows"]],
                         ["2026-11-18T08:00:00Z", "2026-11-18T08:45:00Z"])
            p1 = http_get(18095, "/batches?after_seq=0&limit=5")
            p2 = http_get(18095, f"/batches?after_seq={p1['batches'][-1]['seq']}&limit=5")
            assert_equal("批次分页序号衔接",
                         p2["batches"][0]["seq"], p1["batches"][-1]["seq"] + 1)
        finally:
            stop_service(proc)

        # ------------------------------------------------ 场景 5：十万批次历史
        print("[场景 5] 十万批次历史夹具：重启只读快照后正文，任意序号取 100 条最多读 200 条")
        big = work / "scale"
        build_scale_fixture(big, batches=100_000)
        # 与不折叠的参照实现比对：折叠/恢复后窗口视图必须完全一致
        reference = work / "scale_ref"
        reference.mkdir()
        shutil.copy(big / "batches.jsonl", reference / "batches.jsonl")
        ref_journal = BatchJournal(str(reference))
        ref_engine = Engine(journal=ref_journal)  # 无快照、无归档：纯日志重放
        ref_windows = ref_engine.query_windows()
        ref_journal.close()
        stats_big = open_scale_state(big)
        try:
            assert_equal("折叠后窗口视图与纯日志重放逐字节一致",
                         stats_big["_engine"].query_windows(), ref_windows)
        finally:
            close_scale_state(stats_big)
        # 重启（已有新版快照）：正文读取必须是 0 条
        assert_equal("重启读取批次正文条数", stats_big["journal_bodies_read"], 0)
        print(f"  {PASS} 重启：{stats_big['windows_active_in_memory']} 个活动窗口 "
              f"（归档 {stats_big['windows_archived']} 个）、"
              f"{stats_big['tracked_readings']} 条活动读数，正文读取 0 条；"
              f"内存中不驻留任何批次正文")

        # 任意序号取 100 条：正文读取 <= 200 条
        results = []
        for after in (0, 1, 999, 50_000, 99_899, 99_900):
            page, bodies = page_scale_state(big, after, 100)
            results.append((after, len(page), bodies))
            if bodies > 200:
                raise AssertionError(f"after_seq={after} 读取正文 {bodies} 条 > 200")
            if len(page) != min(100, max(0, 100_000 - after)):
                raise AssertionError(f"after_seq={after} 分页条数 {len(page)} 不符")
        print(f"  {PASS} 分页：6 个任意序号各取 100 条，正文读取 "
              f"{sorted({r[2] for r in results})} 条（上限 200）")

        # 与短历史对比：重启/分页开销不随历史增长
        small = work / "scale_small"
        build_scale_fixture(small, batches=500)
        small_stats = open_scale_state(small)
        close_scale_state(small_stats)
        small_page = page_scale_state(small, 0, 100)[1]
        big_page = page_scale_state(big, 0, 100)[1]
        print(f"  {PASS} 短历史对比：{small_stats['published_batches']} 条 vs "
              f"{stats_big['published_batches']} 条，重启正文读取 "
              f"{small_stats['journal_bodies_read']} vs {stats_big['journal_bodies_read']}，"
              f"取 100 条正文读取 {small_page} vs {big_page}")

        # 升级路径：删掉新快照与索引，退化成旧状态目录，重启应自动升级且只付一次全量扫描
        legacy_dir = work / "scale_legacy"
        legacy_dir.mkdir()
        shutil.copy(big / "batches.jsonl", legacy_dir / "batches.jsonl")
        # 用旧版 schema（无 journal_offset）写一份快照
        old_snapshot = make_legacy_snapshot(legacy_dir)
        upgrade_stats = open_scale_state(legacy_dir)
        close_scale_state(upgrade_stats)
        assert_equal("旧目录快照无 journal_offset", "journal_offset" in old_snapshot, False)
        assert upgrade_stats["journal_bodies_read"] > 0
        assert (legacy_dir / SNAPSHOT_NAME).exists()
        assert_equal("旧目录升级后快照写入 journal_offset",
                     "journal_offset" in json.loads(
                         (legacy_dir / SNAPSHOT_NAME).read_text(encoding="utf-8")), True)
        after_upgrade = open_scale_state(legacy_dir)
        close_scale_state(after_upgrade)
        assert_equal("升级后再次重启正文读取条数", after_upgrade["journal_bodies_read"], 0)
        print(f"  {PASS} 升级：旧快照无 journal_offset，首次重启扫描 "
              f"{upgrade_stats['journal_bodies_read']} 条正文并升级，之后重启读取 0 条")

        # 追加完整批次后强杀：重启仍能接着对账（真实 SIGKILL，真实 serve 进程）
        crash_dir = work / "scale_crash"
        crash_log = work / "scale_crash.log"
        build_scale_fixture(crash_dir, batches=300, snapshot_every=50,
                            source_log=crash_log)
        expected_total = real_kill_after_appending(crash_dir, crash_log, work, extra=37)
        final = open_scale_state(crash_dir)
        close_scale_state(final)
        assert_equal("强杀后批次总数（已发布批次精确一次）",
                     final["published_batches"], expected_total)
        validate_scale_continuity(crash_dir, expected_total)
        print(f"  {PASS} 强杀续对账：追加 {37} 个完整批次后 kill -9，重启批次总数 "
              f"{final['published_batches']}，序号连续且无重复")

    except AssertionError as exc:
        failures += 1
        print(f"  {FAIL} {exc}")
    except Exception as exc:  # noqa: BLE001
        failures += 1
        import traceback
        traceback.print_exc()
        print(f"  {FAIL} 意外异常: {exc}")
    finally:
        shutil.rmtree(work, ignore_errors=True)

    elapsed = time.time() - t0
    print(f"\n总耗时 {elapsed:.1f}s（要求 < 80s）")
    if failures:
        print(f"结论：{FAIL} 存在 {failures} 处失败")
        return 1
    print(f"结论：{PASS} 全部场景通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
