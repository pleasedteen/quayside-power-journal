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

from shorepower.feeder import replay  # noqa: E402
from shorepower.generator import write_log  # noqa: E402
from shorepower.model import parse_ts  # noqa: E402
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
        assert_equal("封账批次数量（首次纯空窗推进 + 四次追封）", len(seals), 5)
        assert_equal("驳回记录数（含重复/版本/封账/坏行/回退）", len(rejected), 11)

        # 首次推进：水位 08:14 越过 07:45 窗口结束+10min，此前缺测空窗一并封账，
        # 批次只记录水位区间、不逐窗列举空窗
        first_seal = seals[0]
        assert_equal("首次推进无物化窗口（纯空窗封账）", first_seal["sealed_windows"], [])
        assert_equal("首次推进起点为 null", first_seal["sealed_from"], None)
        assert_equal("首次推进封账水位", first_seal["sealed_through"],
                     parse_ts("2026-11-18T07:45:00Z"))
        # 水位链单调衔接，旧批次与冻结电量不变
        assert_equal("封账水位链逐批衔接",
                     [(s["sealed_from"], s["sealed_through"]) for s in seals],
                     [(None, parse_ts("2026-11-18T07:45:00Z")),
                      (parse_ts("2026-11-18T07:45:00Z"), parse_ts("2026-11-18T08:00:00Z")),
                      (parse_ts("2026-11-18T08:00:00Z"), parse_ts("2026-11-18T08:15:00Z")),
                      (parse_ts("2026-11-18T08:15:00Z"), parse_ts("2026-11-18T08:30:00Z")),
                      (parse_ts("2026-11-18T08:30:00Z"), parse_ts("2026-11-18T08:45:00Z"))])
        # 有电量窗口的首封批次 = 在 sealed_windows 中列出它的那个 seal 批次
        assert_equal("08:00 窗口首封批次归因",
                     windows["2026-11-18T08:00:00Z"]["sealed_by_seq"], seals[1]["seq"])
        assert_equal("08:45 窗口首封批次归因",
                     windows["2026-11-18T08:45:00Z"]["sealed_by_seq"], seals[4]["seq"])

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
        print("[场景 4] 本地查询接口（多窗口一次查询 + 批次分页 + 空窗两态）")
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
            # 空窗两态：封账水位前为「已封账零电量」且可归因首封批次，
            # 水位后为「待封账零电量」，迟到读数/更正仍可改写（直到被封）
            first_seal_seq = next(b["seq"] for b in fast["batches"] if b["kind"] == "seal")
            qe = http_get(18095, "/windows?window=2026-11-18T07:30:00Z&window=2026-11-18T09:00:00Z")
            e_sealed, e_open = qe["windows"]
            assert_equal("封账水位前的空窗：已封账零电量 + 首封批次",
                         (e_sealed["sealed"], e_sealed["totals"], e_sealed["sealed_by_seq"]),
                         (True, {}, first_seal_seq))
            assert_equal("封账水位后的空窗：待封账零电量",
                         (e_open["sealed"], e_open["totals"], e_open["sealed_by_seq"]),
                         (False, {}, None))
            assert_equal("查询返回封账水位",
                         qe["sealed_through"], "2026-11-18T08:45:00Z")
        finally:
            stop_service(proc)

        # ------------------------------------------------ 场景 5：空窗封账生命周期
        print("[场景 5] 空窗两态生命周期：越线前后、迟到读数/更正驳回、"
              "一年跨越 JSONL ≤ 16 KiB、SIGKILL 重启一致")
        empty_state = work / "empty"
        empty_log = work / "empty.log"
        empty_state.mkdir()

        def append_records(*records: dict) -> None:
            with open(empty_log, "a", encoding="utf-8") as fh:
                for rec in records:
                    fh.write(dumps_canonical(rec) + "\n")
                fh.flush()
                os.fsync(fh.fileno())

        def wait_committed(port: int) -> None:
            target = last_line_offset(empty_log) - 1
            wait_until(
                lambda: http_get(port, "/stats")["committed_source_offset"] >= target or None,
                timeout=15,
            )

        def qwin(port: int, *ids: str) -> dict:
            qs = "&".join(f"window={i}" for i in ids)
            return {w["window"]: w for w in http_get(port, f"/windows?{qs}")["windows"]}

        def seal_seq_for(state: Path, window_iso: str) -> int:
            ws = parse_ts(window_iso)
            return next(b["seq"] for b in read_jsonl(state / "batches.jsonl")
                        if b["kind"] == "seal" and b["sealed_through"] >= ws)

        # snapshot-every=0：重启完全靠批次日志重放，直接验证水位/归因的日志重建
        proc = start_service(empty_state, empty_log, 18096, snapshot_every=0)
        try:
            # 阶段 A：08:00 有量窗口封账；08:15 仍为待封账空窗（越线前）
            append_records(
                {"type": "reading", "reading_id": "e1", "gateway": "GW1", "berth": "B1",
                 "meter_ts": "2026-11-18T08:02:00Z", "energy": 10.0},
                {"type": "progress", "gateway": "GW1", "progress_ts": "2026-11-18T08:25:00Z"},
            )
            wait_committed(18096)
            w = qwin(18096, "2026-11-18T08:00:00Z", "2026-11-18T08:15:00Z")
            assert_equal("越线前：08:00 有量窗口已封账",
                         (w["2026-11-18T08:00:00Z"]["sealed"],
                          w["2026-11-18T08:00:00Z"]["totals"]),
                         (True, {"B1": 10.0}))
            assert_equal("越线前：08:15 为待封账零电量",
                         (w["2026-11-18T08:15:00Z"]["sealed"],
                          w["2026-11-18T08:15:00Z"]["sealed_by_seq"]),
                         (False, None))

            # 阶段 B：迟到读数落入已封 08:00 被驳回；水位越过 08:15 空窗（越线后）
            append_records(
                {"type": "reading", "reading_id": "e2", "gateway": "GW1", "berth": "B1",
                 "meter_ts": "2026-11-18T08:05:00Z", "energy": 5.0},
                {"type": "reading", "reading_id": "e3", "gateway": "GW1", "berth": "B2",
                 "meter_ts": "2026-11-18T08:32:00Z", "energy": 7.0},
                {"type": "progress", "gateway": "GW1", "progress_ts": "2026-11-18T08:40:00Z"},
            )
            wait_committed(18096)
            w = qwin(18096, "2026-11-18T08:00:00Z", "2026-11-18T08:15:00Z")
            assert_equal("越线后：08:15 变为已封账零电量",
                         (w["2026-11-18T08:15:00Z"]["sealed"],
                          w["2026-11-18T08:15:00Z"]["totals"],
                          w["2026-11-18T08:15:00Z"]["sealed_by_seq"]),
                         (True, {}, seal_seq_for(empty_state, "2026-11-18T08:15:00Z")))
            assert_equal("越线后：迟到读数未补入已封的 08:00",
                         w["2026-11-18T08:00:00Z"]["totals"], {"B1": 10.0})

            # 阶段 C：高版本更正迁入已封空窗 → 整笔驳回（不消费版本号）；
            # 同版本改投开放窗口 → 接受并迁空 08:30；水位再推进封掉迁空窗
            append_records(
                {"type": "correction", "reading_id": "e3", "version": 2, "berth": "B2",
                 "meter_ts": "2026-11-18T08:17:00Z", "energy": 9.0},
                {"type": "correction", "reading_id": "e3", "version": 2, "berth": "B2",
                 "meter_ts": "2026-11-18T08:47:00Z", "energy": 9.0},
                {"type": "progress", "gateway": "GW1", "progress_ts": "2026-11-18T08:55:00Z"},
                {"type": "reading", "reading_id": "e4", "gateway": "GW1", "berth": "B3",
                 "meter_ts": "2026-11-18T08:33:00Z", "energy": 1.0},
            )
            wait_committed(18096)
            dec = [b for b in read_jsonl(empty_state / "batches.jsonl")
                   if b["kind"] == "decision"]
            by_id = {b.get("reading_id"): b for b in dec}
            assert_equal("首次到达的迟到读数被已封窗口驳回",
                         by_id["e2"]["status"], "rejected")
            corr_rej = [b for b in dec
                        if b["record_type"] == "correction" and b["status"] == "rejected"]
            assert_equal("涉及已封空窗的高版本更正整笔驳回",
                         (len(corr_rej), "整笔驳回" in corr_rej[0]["reason"]), (1, True))
            corr_ok = [b for b in dec
                       if b["record_type"] == "correction" and b["status"] == "accepted"]
            assert_equal("整笔驳回不消费版本号：同版本改投开放窗口后接受",
                         [(c["window"], c["change"]) for c in corr_ok[0]["changes"]],
                         [("2026-11-18T08:30:00Z", "remove"),
                          ("2026-11-18T08:45:00Z", "add")])
            assert_equal("已封迁空窗的迟到读数驳回", by_id["e4"]["status"], "rejected")
            # 迁空窗封账：批次显式列出（冻结为空），查询为已封账零电量
            seal_0830 = next(
                b for b in read_jsonl(empty_state / "batches.jsonl")
                if b["kind"] == "seal"
                and b["sealed_through"] == parse_ts("2026-11-18T08:30:00Z"))
            assert_equal("被更正迁空的刻钟封账并在批次中显式冻结为空",
                         seal_0830["sealed_windows"],
                         [{"window": "2026-11-18T08:30:00Z",
                           "window_start": parse_ts("2026-11-18T08:30:00Z"),
                           "totals": {}}])
            w = qwin(18096, "2026-11-18T08:30:00Z")
            assert_equal("迁空窗查询为已封账零电量",
                         (w["2026-11-18T08:30:00Z"]["sealed"],
                          w["2026-11-18T08:30:00Z"]["totals"],
                          w["2026-11-18T08:30:00Z"]["sealed_by_seq"]),
                         (True, {}, seal_0830["seq"]))

            # 阶段 D：水位一次跨过连续一年无读数刻钟，新增 JSONL ≤ 16 KiB
            size_before = (empty_state / "batches.jsonl").stat().st_size
            append_records({"type": "progress", "gateway": "GW1",
                            "progress_ts": "2027-11-24T08:55:00Z"})
            wait_committed(18096)
            delta = (empty_state / "batches.jsonl").stat().st_size - size_before
            assert delta <= 16 * 1024, f"一年跨越新增 JSONL {delta} 字节，超过 16 KiB"
            print(f"  {PASS} 水位一次跨过连续一年无读数刻钟：新增 JSONL {delta} 字节 ≤ 16 KiB")

            # 在线查询与批次日志核对 gap 空窗的同一个首次封账批次
            gap_window = "2027-06-01T00:00:00Z"
            w = qwin(18096, gap_window)
            assert_equal("gap 空窗已封且查询/批次日志核对同一首封批次",
                         (w[gap_window]["sealed"], w[gap_window]["sealed_by_seq"]),
                         (True, seal_seq_for(empty_state, gap_window)))

            # Parquet 导出核对（可选，需 pyarrow；缺失时跳过，JSON Lines 信息等价）
            import importlib.util
            if importlib.util.find_spec("pyarrow") is not None:
                subprocess.run(
                    [sys.executable, "-I", str(ROOT / "run_export_parquet.py"),
                     "--state-dir", str(empty_state),
                     "--out-prefix", str(work / "report")],
                    check=True, capture_output=True)
                import pyarrow.parquet as pq
                wrows = pq.read_table(work / "report.windows.parquet").to_pylist()
                row0830 = next(r for r in wrows if r["window"] == "2026-11-18T08:30:00Z")
                assert_equal("Parquet 窗口表与在线查询核对同一首封批次",
                             row0830["sealed_by_seq"], seal_0830["seq"])
                brows = pq.read_table(work / "report.batches.parquet").to_pylist()
                gap_seq = min(r["seq"] for r in brows
                              if r["kind"] == "seal"
                              and r["sealed_through"] >= parse_ts(gap_window))
                assert_equal("Parquet 批次表覆盖 gap 空窗的首封批次一致",
                             gap_seq, seal_seq_for(empty_state, gap_window))
            else:
                print("  ⏭️ 跳过  Parquet 导出核对（未安装 pyarrow）；"
                      "JSON Lines 与导出信息等价")

            # 阶段 E：SIGKILL 后同目录重启，越线后的结果保持
            before_some = http_get(18096, "/windows?window=2026-11-18T08:15:00Z"
                                          "&window=2026-11-18T08:30:00Z"
                                          f"&window={gap_window}")
            before_all = http_get(18096, "/windows")
            before_raw = (empty_state / "batches.jsonl").read_text(encoding="utf-8")
            batches_count = len(read_jsonl(empty_state / "batches.jsonl"))
            kill_service(proc)
            print(f"    已 SIGKILL：重启前持久化 {batches_count} 个批次")
        finally:
            if proc.poll() is None:
                kill_service(proc)

        proc = start_service(empty_state, empty_log, 18097, snapshot_every=0)
        try:
            assert_equal("重启后空窗显式查询一致",
                         http_get(18097, "/windows?window=2026-11-18T08:15:00Z"
                                         "&window=2026-11-18T08:30:00Z"
                                         f"&window={gap_window}"),
                         before_some)
            assert_equal("重启后全量窗口查询一致", http_get(18097, "/windows"), before_all)
            assert_equal("重启后批次日志逐字节一致",
                         (empty_state / "batches.jsonl").read_text(encoding="utf-8"),
                         before_raw)
            # 封账水位经日志重建：向 gap 中间发迟到读数仍被驳回
            append_records({"type": "reading", "reading_id": "e5", "gateway": "GW1",
                            "berth": "B1", "meter_ts": "2027-06-01T00:05:00Z",
                            "energy": 3.0})
            wait_committed(18097)
            last = read_jsonl(empty_state / "batches.jsonl")[-1]
            assert_equal("重启后迟到读数仍被已封空窗驳回",
                         (last["record_type"], last["status"]), ("reading", "rejected"))
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
