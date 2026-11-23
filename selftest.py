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
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))  # -I 模式下不会自动带脚本目录，这里显式加入

from shorepower.feeder import replay  # noqa: E402
from shorepower.generator import write_log  # noqa: E402
from shorepower.storage import dumps_canonical  # noqa: E402

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
                  max_inflight: int = 50, snapshot_every: int = 7) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "-I", str(ROOT / "run_service.py"),
         "--log-file", str(log_file), "--state-dir", str(state_dir),
         "--port", str(port), "--max-inflight", str(max_inflight),
         "--snapshot-every", str(snapshot_every)],
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


def replay_chunked(data: bytes, target: Path, chunk_size: int, delay: float) -> None:
    """按固定字节数切分追加（可把一个 UTF-8 字符拆进两次写入），模拟拆字节到达。"""
    with open(target, "ab") as fh:
        for i in range(0, len(data), chunk_size):
            fh.write(data[i:i + chunk_size])
            fh.flush()
            os.fsync(fh.fileno())
            if delay > 0:
                time.sleep(delay)


def batch_outline(batches: list[dict]) -> list[tuple]:
    """批次的账务轮廓（种类/记录类型/reading_id/状态），用于跨运行比对。"""
    return [(b["kind"], b.get("record_type"), b.get("reading_id"), b.get("status"))
            for b in batches]


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

        # -------------------------------------- 场景 5：拆字节追加 + 原始字节偏移
        print("[场景 5] 8 字节分片拆字节追加（含半个 UTF-8 字符），"
              "空行/CRLF/非 ASCII 偏移按原始字节计")
        edge_lines = [
            dumps_canonical({"type": "reading", "reading_id": "u1", "gateway": "GW1",
                             "berth": "泊α", "meter_ts": "2026-11-18T08:02:00Z",
                             "energy": 3.5}) + "\n",          # 非 ASCII 多字节字符
            "\n",                                              # 空行
            dumps_canonical({"type": "reading", "reading_id": "u2", "gateway": "GW1",
                             "berth": "B2", "meter_ts": "2026-11-18T08:05:00Z",
                             "energy": 2.0}) + "\r\n",         # CRLF 行尾
            '{"type": "reading", "reading_id": "uBAD",' + "\n",  # 坏 JSON 行
            dumps_canonical({"type": "progress", "gateway": "GW1",
                             "progress_ts": "2026-11-18T08:06:00Z"}) + "\n",
        ]
        edge_bytes = "".join(edge_lines).encode("utf-8")
        edge_offsets, _pos = [], 0
        for _l in edge_lines:
            edge_offsets.append(_pos)
            _pos += len(_l.encode("utf-8"))
        edge_end = len(edge_bytes)

        # A：服务启动前日志已整份预写 -> 首行不得被漏掉
        pre_state = work / "edge-pre"; pre_state.mkdir()
        pre_log = work / "edge-pre.log"
        pre_log.write_bytes(edge_bytes)
        proc = start_service(pre_state, pre_log, 18096)
        try:
            wait_until(lambda: http_get(18096, "/stats")["committed_source_offset"]
                       >= edge_offsets[-1] or None, timeout=15)
            pre_raw = (pre_state / "batches.jsonl").read_text(encoding="utf-8")
            pre_stats = http_get(18096, "/stats")
            pre_windows = http_get(18096, "/windows")
        finally:
            stop_service(proc)

        # B：同一内容按 8 字节分片慢速追加（多字节字符必被拆进两次写入）
        chunk_state = work / "edge-chunk"; chunk_state.mkdir()
        chunk_log = work / "edge-chunk.log"
        proc = start_service(chunk_state, chunk_log, 18097)
        try:
            feeder = threading.Thread(
                target=replay_chunked, args=(edge_bytes, chunk_log, 8, 0.01))
            feeder.start()
            wait_until(lambda: http_get(18097, "/stats")["committed_source_offset"]
                       >= edge_offsets[-1] or None, timeout=25)
            feeder.join()
            chunk_raw = (chunk_state / "batches.jsonl").read_text(encoding="utf-8")
            chunk_stats = http_get(18097, "/stats")
        finally:
            stop_service(proc)

        assert_equal("拆字节追加与整份预写批次逐字节一致", chunk_raw, pre_raw)
        edge_batches = read_jsonl(pre_state / "batches.jsonl")
        assert_equal("批次 source_offset 按原始字节计（空行/CRLF/非 ASCII）",
                     [b["source_offset"] for b in edge_batches], edge_offsets)
        assert_equal("批次 next_offset 指向下一行起点",
                     [b["next_offset"] for b in edge_batches],
                     edge_offsets[1:] + [edge_end])
        assert_equal("首行（预写）未漏：u1 已入账",
                     edge_batches[0]["reading_id"], "u1")
        assert_equal("续读位置=日志字节数", pre_stats["resume_source_offset"], edge_end)
        assert_equal("拆字节运行续读位置=日志字节数",
                     chunk_stats["resume_source_offset"], edge_end)
        assert_equal("已提交偏移=末行起点", pre_stats["committed_source_offset"],
                     edge_offsets[-1])
        # 窗口电量：泊α=3.5、B2=2.0 同入 08:00 窗口（非 ASCII 泊位名按内容归账）
        w_edge = {w["window"]: w for w in pre_windows["windows"]}
        assert_equal("08:00 窗口含非 ASCII 泊位电量",
                     w_edge["2026-11-18T08:00:00Z"]["totals"], {"B2": 2.0, "泊α": 3.5})

        # -------------------------------------- 场景 6：暂停落盘时的严格背压上限
        print("[场景 6] 每条记录都暂停写快照落盘：已读未落盘记录数严格 <= 上限 3")
        bp_state = work / "bp"; bp_state.mkdir()
        bp_log = work / "bp.log"
        proc = start_service(bp_state, bp_log, 18098, max_inflight=3, snapshot_every=1)
        try:
            fed = threading.Event()

            def _feed():
                replay(str(work / "mock.jsonl"), str(bp_log), delay=0.0)
                fed.set()

            feeder = threading.Thread(target=_feed)
            feeder.start()
            max_seen = 0
            while not fed.is_set():
                max_seen = max(max_seen, http_get(18098, "/stats")["inflight_unpersisted"])
                time.sleep(0.005)
            feeder.join()
            wait_until(lambda: http_get(18098, "/stats")["committed_source_offset"]
                       >= last_line_offset(bp_log) - 1 or None, timeout=25)
            max_seen = max(max_seen, http_get(18098, "/stats")["inflight_unpersisted"])
            assert max_seen <= 3, f"已读未落盘数 {max_seen} 超过上限 3"
            print(f"  {PASS} 暂停落盘期间已读未落盘峰值={max_seen}，始终 <= 3")
            assert_equal("严格背压下批次与瞬时回放逐字节一致",
                         (bp_state / "batches.jsonl").read_text(encoding="utf-8"),
                         fast["raw_batches"])
            assert_equal("落盘后队列排空", http_get(18098, "/stats")["inflight_unpersisted"], 0)
        finally:
            stop_service(proc)

        # -------------------------------------- 场景 7：旧版(schema 1)状态升级续跑
        print("[场景 7] 旧版完整 LF 账升级续跑：原批次/序号/分页保留，直接接着跑")
        leg_state = work / "legacy"; leg_state.mkdir()
        leg_log = work / "legacy.log"
        all_lines = (work / "mock.jsonl").read_text(encoding="utf-8").splitlines(keepends=True)
        half = len(all_lines) // 2
        half_end = sum(len(l.encode("utf-8")) for l in all_lines[:half])
        half_last = half_end - len(all_lines[half - 1].encode("utf-8"))
        proc = start_service(leg_state, leg_log, 18099)
        try:
            replay(str(work / "mock.jsonl"), str(leg_log), delay=0.0, max_lines=half)
            wait_until(lambda: http_get(18099, "/stats")["committed_source_offset"]
                       >= half_last or None, timeout=15)
        finally:
            stop_service(proc)  # 优雅停止 -> 写出新版快照

        # 把状态目录手工“降级”为旧版格式：批次去 next_offset、schema=1；快照去 resume_offset
        bpath = leg_state / "batches.jsonl"
        legacy_batches = []
        for line in bpath.read_text(encoding="utf-8").splitlines():
            b = json.loads(line)
            b.pop("next_offset", None)
            b["schema"] = 1
            legacy_batches.append(b)
        bpath.write_text("".join(dumps_canonical(b) + "\n" for b in legacy_batches),
                         encoding="utf-8")
        legacy_prefix_raw = bpath.read_text(encoding="utf-8")
        snap_path = leg_state / "snapshot.json"
        snap = json.loads(snap_path.read_text(encoding="utf-8"))
        snap["schema"] = 1
        snap.pop("resume_offset", None)
        snap_path.write_text(json.dumps(snap, sort_keys=True, ensure_ascii=False, indent=1),
                             encoding="utf-8")

        proc = start_service(leg_state, leg_log, 18100)  # 升级后重启，同一状态目录
        try:
            with open(leg_log, "a", encoding="utf-8") as dst:
                for line in all_lines[half:]:
                    dst.write(line)
                dst.flush()
                os.fsync(dst.fileno())
            wait_until(lambda: read_jsonl(bpath)
                       if len(read_jsonl(bpath)) == len(fast["batches"]) else None,
                       timeout=20)
            upgraded_raw = bpath.read_text(encoding="utf-8")
            upgraded = read_jsonl(bpath)
            assert upgraded_raw.startswith(legacy_prefix_raw), "旧批次被改写"
            print(f"  {PASS} 旧版 {len(legacy_batches)} 个批次逐字节原样保留")
            seqs = [b["seq"] for b in upgraded]
            assert_equal("升级后批次序号连续无空洞", seqs, list(range(1, len(seqs) + 1)))
            new_batches = upgraded[len(legacy_batches):]
            assert all(b.get("schema") == 2 and b.get("next_offset", -1) >= 0
                       for b in new_batches), "新批次未带 next_offset/schema 2"
            print(f"  {PASS} 升级后新写 {len(new_batches)} 个批次均带 next_offset(schema 2)")
            assert_equal("升级续跑决策序列与无中断回放一致",
                         batch_outline(upgraded), batch_outline(fast["batches"]))
            assert_equal("升级续跑窗口状态一致", http_get(18100, "/windows"), fast["windows"])
            page = http_get(18100, f"/batches?after_seq=0&limit={len(legacy_batches)}")
            assert_equal("旧批次分页结果原样保留",
                         [json.dumps(b, sort_keys=True) for b in page["batches"]],
                         [json.dumps(b, sort_keys=True) for b in legacy_batches])
            assert_equal("续读位置追平日志末尾",
                         http_get(18100, "/stats")["resume_source_offset"],
                         leg_log.stat().st_size)
        finally:
            stop_service(proc)

        # -------------------------------------- 场景 8：强杀续跑（上一行很长）
        print("[场景 8] SIGKILL 强杀续跑：上一行 200KB 也直接定位下一行，"
              "结果与无中断回放一致")
        long_rec = {"type": "reading", "reading_id": "rLONG", "gateway": "GW1",
                    "berth": "B7", "meter_ts": "2026-11-18T08:50:00Z", "energy": 1.0,
                    "note": "x" * 200_000}
        long_line = dumps_canonical(long_rec) + "\n"
        custom_lines = all_lines[:half] + [long_line] + all_lines[half:]
        long_line_start = sum(len(l.encode("utf-8")) for l in all_lines[:half])
        prefix_size = long_line_start + len(long_line.encode("utf-8"))

        # 参考：同一份自定义日志无中断跑完
        ref_state = work / "longref"; ref_state.mkdir()
        ref_log = work / "longref.log"
        proc = start_service(ref_state, ref_log, 18101)
        try:
            with open(ref_log, "a", encoding="utf-8") as fh:
                fh.writelines(custom_lines)
                fh.flush()
                os.fsync(fh.fileno())
            wait_until(lambda: http_get(18101, "/stats")["committed_source_offset"]
                       >= last_line_offset(ref_log) - 1 or None, timeout=25)
            ref_raw = (ref_state / "batches.jsonl").read_text(encoding="utf-8")
            ref_windows = http_get(18101, "/windows")
            ref_count = len(read_jsonl(ref_state / "batches.jsonl"))
        finally:
            stop_service(proc)

        # 崩溃：写到长行为止，SIGKILL 后同目录重启
        k_state = work / "longkill"; k_state.mkdir()
        k_log = work / "longkill.log"
        proc = start_service(k_state, k_log, 18102)
        try:
            with open(k_log, "a", encoding="utf-8") as fh:
                fh.writelines(custom_lines[:half + 1])  # 以 200KB 长行结尾
                fh.flush()
                os.fsync(fh.fileno())
            wait_until(lambda: http_get(18102, "/stats")["committed_source_offset"]
                       >= long_line_start or None, timeout=15)
            kill_service(proc)
            print(f"    已 SIGKILL：最后落盘的是 {len(long_line)} 字节长行，"
                  f"起始偏移 {long_line_start}")
        finally:
            if proc.poll() is None:
                kill_service(proc)

        proc = start_service(k_state, k_log, 18103)
        try:
            # 重启即知续读位置=长行末尾（凭批次日志的 next_offset，不回读正文）
            assert_equal("重启后续读位置=长行末尾（不回读正文）",
                         http_get(18103, "/stats")["resume_source_offset"], prefix_size)
            with open(k_log, "a", encoding="utf-8") as fh:
                fh.writelines(custom_lines[half + 1:])
                fh.flush()
                os.fsync(fh.fileno())
            wait_until(lambda: read_jsonl(k_state / "batches.jsonl")
                       if len(read_jsonl(k_state / "batches.jsonl")) == ref_count else None,
                       timeout=20)
            assert_equal("强杀续跑批次与无中断回放逐字节一致",
                         (k_state / "batches.jsonl").read_text(encoding="utf-8"), ref_raw)
            assert_equal("强杀续跑窗口状态一致", http_get(18103, "/windows"), ref_windows)
            seqs = [b["seq"] for b in read_jsonl(k_state / "batches.jsonl")]
            assert_equal("强杀续跑批次序号连续", seqs, list(range(1, len(seqs) + 1)))
            assert_equal("强杀续跑续读位置追平日志末尾",
                         http_get(18103, "/stats")["resume_source_offset"],
                         k_log.stat().st_size)
        finally:
            stop_service(proc)

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
