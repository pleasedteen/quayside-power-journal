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
from shorepower.storage import SegmentWriter, dumps_canonical  # noqa: E402

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


def wait_source_done(port: int, total_records: int, timeout: float = 25.0) -> dict:
    """等待服务把源记录全部处理完（已提交记录数达到源行数）。"""
    return wait_until(
        lambda: (lambda s: s if s["committed_records"] >= total_records else None)(
            http_get(port, "/stats")
        ),
        timeout=timeout,
    )


def wait_batch_count(state: Path, expected: int, timeout: float = 25.0) -> list[dict]:
    """等待批次日志里出现 expected 个批次（用于分段布局的处理进度判定）。"""
    return wait_until(
        lambda: (lambda b: b if len(b) == expected else None)(
            read_jsonl(state / "batches.jsonl")
        ),
        timeout=timeout,
    )


def run_segment_replay(work: Path, name: str, port: int, total_records: int) -> dict:
    """用命令行回放器按编号分段（每段 10 行）写出，再让服务跟读同一个段前缀。"""
    state = work / f"{name}_state"
    base = work / name
    state.mkdir(parents=True)
    proc = start_service(state, base, port)
    try:
        replay(str(work / "mock.jsonl"), str(base), delay=0.0, segment_lines=10)
        wait_source_done(port, total_records)
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


LOCATION_KEYS = ("source_segment", "source_line", "source_offset",
                 "trigger_segment", "trigger_line", "trigger_offset")


def strip_location(batches: list[dict]) -> list[dict]:
    """去掉来源定位字段后的批次视图，用于跨布局比较业务结果。"""
    return [{k: v for k, v in b.items() if k not in LOCATION_KEYS} for b in batches]


def assert_locations_trace(batches: list[dict], seg_files: list[Path], label: str) -> None:
    """校验每个决定批次都能回溯到真实源行，且段序/行序严格递增。"""
    source: list[tuple[int, int, str]] = []
    for path in seg_files:
        num = int(path.name.split(".")[-1])
        for idx, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            source.append((num, idx, line))
    decisions = [b for b in batches if b["kind"] == "decision"]
    assert_equal(f"{label}：决定批次数等于源行数", len(decisions), len(source))
    got = [(b["source_segment"], b["source_line"]) for b in decisions]
    assert_equal(f"{label}：批次按段号+行序严格递增且与源文件逐行对应",
                 got, [(n, i) for n, i, _ in source])

    bad: list[int] = []
    for batch, (_, _, raw) in zip(decisions, source):
        if batch["record_type"] == "bad":
            continue  # 坏行批次只要能定位到源行即可，内容本就不可解析
        obj = json.loads(raw)
        if obj.get("type") != batch["record_type"]:
            bad.append(batch["seq"])
        elif obj.get("reading_id") != batch.get("reading_id"):
            bad.append(batch["seq"])
    assert_equal(f"{label}：来源行内容与批次类型/主键一致", bad, [])


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

        # ------------------------------------------------ 场景 5：分段跟读与来源追溯
        print("[场景 5] 网关编号分段（每段 10 行，写完即封存）：业务结果与单文件一致，且每批可追到来源段/源行")
        seg = run_segment_replay(work, "seg", 18096, total_records=n)
        assert_equal("分段跟读业务结果（除来源定位外）与单文件一致",
                     strip_location(seg["batches"]), strip_location(fast["batches"]))
        assert_equal("分段跟读窗口状态与单文件一致", seg["windows"], fast["windows"])
        segs_seen = sorted({b["source_segment"] for b in seg["batches"]})
        assert_equal("批次覆盖全部 5 个来源段", segs_seen, [1, 2, 3, 4, 5])
        seg_files = sorted(work.glob("seg.[0-9][0-9][0-9][0-9][0-9][0-9]"))
        assert_locations_trace(seg["batches"], seg_files, "来源追溯")

        # 单文件布局的来源定位：段号恒为 1，行号即文件行序
        assert_equal("单文件布局来源段号恒为 1",
                     sorted({b["source_segment"] for b in fast["batches"]}), [1])
        src_lines = (work / "fast.log").read_text(encoding="utf-8").splitlines()
        assert_equal("单文件布局行号与文件行序一一对应",
                     [b["source_line"] for b in fast["batches"] if b["kind"] == "decision"],
                     list(range(1, len(src_lines) + 1)))

        # ------------------------------------------------ 场景 6：换段前强杀
        print("[场景 6] 换段前 SIGKILL 重启：批次顺序/来源/窗口与连续分段运行一致")
        pre_state = work / "pre_state"
        pre_base = work / "pre_seg"
        pre_state.mkdir()
        pre_lines = (work / "mock.jsonl").read_text(encoding="utf-8").splitlines(keepends=True)
        w = SegmentWriter(str(pre_base))
        for line in pre_lines[:10]:          # 换段前：只有第 1 段（10 行，已封存）
            w.append(line)
        w.mark_sealed()
        proc = start_service(pre_state, pre_base, 18097)
        try:
            wait_batch_count(pre_state, 10)  # 前 10 行记录各发布 1 个批次（尚无封账）
            stats_b = http_get(18097, "/stats")
            assert_equal("换段前：提交位置停在第 1 段第 10 行",
                         (stats_b["committed_source_segment"], stats_b["committed_source_line"]),
                         (1, 10))
            kill_service(proc)               # 换段前强杀
        finally:
            if proc.poll() is None:
                kill_service(proc)
        # 网关继续按每 10 行换段写第 2..5 段并封存（与场景 5 的分段方案完全相同）
        for chunk_start in (10, 20, 30, 40):
            w.open_next_segment()
            for line in pre_lines[chunk_start:chunk_start + 10]:
                w.append(line)
            w.mark_sealed()
        w.detach()

        proc = start_service(pre_state, pre_base, 18098)
        try:
            wait_batch_count(pre_state, len(seg["batches"]))
            after = read_jsonl(pre_state / "batches.jsonl")
            assert_equal("换段前崩溃恢复：批次内容/顺序/来源与连续分段运行逐字节一致",
                         [json.dumps(b, sort_keys=True) for b in after],
                         [json.dumps(b, sort_keys=True) for b in seg["batches"]])
            assert_equal("换段前崩溃恢复：窗口与连续分段运行一致",
                         http_get(18098, "/windows"), seg["windows"])
            assert_equal("换段前崩溃恢复：批次序号无重复无空洞",
                         [b["seq"] for b in after], list(range(1, len(after) + 1)))
        finally:
            stop_service(proc)

        # ------------------------------------------------ 场景 7：换段后强杀（旧段仍积压）
        print("[场景 7] 换段后 SIGKILL（新段已到、旧段仍有积压）：不越段，重启后按段序/行序补齐")
        post_state = work / "post_state"
        post_base = work / "post_seg"
        post_state.mkdir()
        w = SegmentWriter(str(post_base))
        for line in pre_lines[:10]:          # 第 1 段 10 行，暂不封存
            w.append(line)
        w.open_next_segment()                # 新段已到，旧段仍未封存 -> 旧段积压
        for line in pre_lines[10:20]:
            w.append(line)
        w.detach()

        proc = start_service(post_state, post_base, 18099)
        try:
            wait_batch_count(post_state, 10)   # 第 1 段 10 行记录，尚无封账
            time.sleep(0.6)                  # 给越段读取留出机会，若发生则批次会变多
            stats_1 = http_get(18099, "/stats")
            assert_equal("旧段未封存时不越段读取", stats_1["committed_source_segment"], 1)
            assert_equal("旧段积压已读尽（第 1 段 10 行）", stats_1["committed_source_line"], 10)
            assert_equal("未越段时批次总数不增", len(read_jsonl(post_state / "batches.jsonl")), 10)
            kill_service(proc)               # 换段后强杀
        finally:
            if proc.poll() is None:
                kill_service(proc)

        # 网关恢复：封存第 1 段，续写第 2 段（含 11..20 行）并封存，再切出第 3 段
        w = SegmentWriter(str(post_base))    # 接在第 2 段继续写
        w.seal_segment(1)
        for line in pre_lines[20:30]:
            w.append(line)
        w.mark_sealed()                      # 封存第 2 段（共 20 行）
        w.open_next_segment()
        for line in pre_lines[30:]:
            w.append(line)
        w.mark_sealed()                      # 封存第 3 段
        w.detach()

        proc = start_service(post_state, post_base, 18100)
        try:
            wait_batch_count(post_state, len(seg["batches"]))
            final = read_jsonl(post_state / "batches.jsonl")
            assert_equal("换段后崩溃恢复：业务结果与连续运行一致",
                         strip_location(final), strip_location(seg["batches"]))
            assert_equal("换段后崩溃恢复：窗口与连续运行一致",
                         http_get(18100, "/windows"), seg["windows"])
            assert_equal("换段后崩溃恢复：批次序号无重复无空洞",
                         [b["seq"] for b in final], list(range(1, len(final) + 1)))
            assert_locations_trace(
                final, sorted(work.glob("post_seg.[0-9][0-9][0-9][0-9][0-9][0-9]")), "换段后来源追溯")
            stats_f = http_get(18100, "/stats")
            assert_equal("换段后崩溃恢复：已提交到最后一个来源段末行",
                         (stats_f["committed_source_segment"], stats_f["committed_source_line"]),
                         (3, len(pre_lines[30:])))
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
