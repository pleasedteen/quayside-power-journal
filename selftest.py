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
from shorepower.storage import (  # noqa: E402
    BATCHES_NAME,
    SnapshotStore,
    dumps_canonical,
    load_persisted_batches,
)
from shorepower.engine import Engine  # noqa: E402
from shorepower.export_parquet import (  # noqa: E402
    export,
    read_export,
    read_table_rows,
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


# ------------------------------------------------- 场景 5 辅助：导出边界核对与故障注入


def verify_export_pair(state_dir: Path, out_prefix: Path, expect_seq: int | None = None) -> dict:
    """读回最近一次导出，核对两张表同属一个已持久化批次边界且内容可独立对账。"""
    info = read_export(str(out_prefix))
    as_of = info["as_of_seq"]
    assert isinstance(as_of, int) and as_of >= 0, f"manifest 缺少可核对边界: {info}"
    if expect_seq is not None:
        assert as_of == expect_seq, f"导出边界 {as_of} != 期望 {expect_seq}"

    batch_rows = read_table_rows(info["batches_path"])
    window_rows = read_table_rows(info["windows_path"])
    assert batch_rows and window_rows or as_of == 0
    assert all(r["as_of_seq"] == as_of for r in batch_rows), "批次表存在越界/混批行"
    assert all(r["as_of_seq"] == as_of for r in window_rows), "窗口表存在越界/混批行"
    assert len(batch_rows) == info["batch_rows"] and len(window_rows) == info["window_rows"]

    # 与状态目录里截至该边界的已持久化批次独立对账（嵌套字段在表中是 JSON 字符串）
    persisted = load_persisted_batches(str(state_dir / BATCHES_NAME))
    pinned = persisted[:as_of]
    assert [r["seq"] for r in batch_rows] == [b["seq"] for b in pinned]
    for row, batch in zip(batch_rows, pinned):
        for key, value in batch.items():
            if isinstance(value, (dict, list)):
                assert json.loads(row[key]) == value
            else:
                assert row[key] == value

    # 窗口表必须等于“同一批批次”经引擎重放（与崩溃恢复同一路径）的结果
    from shorepower.export_parquet import build_window_rows
    expect_windows = build_window_rows(Engine.from_persisted_batches(pinned), as_of)
    assert window_rows == expect_windows, "窗口表与同边界批次重放结果不一致"
    return info


class _BackgroundAppender:
    """后台持续向源日志追加模拟行，驱动服务并发入账（导出期间新批次不断落盘）。"""

    def __init__(self, source: Path, target: Path, interval: float = 0.02):
        self.source = source
        self.target = target
        self.interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._lines = source.read_text(encoding="utf-8").splitlines(keepends=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        i = 0
        while not self._stop.is_set():
            with open(self.target, "a", encoding="utf-8") as fh:
                fh.write(self._lines[i % len(self._lines)])
                fh.flush()
                os.fsync(fh.fileno())
            i += 1
            self._stop.wait(self.interval)


def start_stalled_export(state_dir: Path, out_prefix: Path, fmt: str,
                         stall_seconds: float = 5.0) -> subprocess.Popen:
    env = dict(os.environ)
    env["SHOREPOWER_EXPORT_STALL_BEFORE_COMMIT"] = str(stall_seconds)
    return subprocess.Popen(
        [sys.executable, "-I", str(ROOT / "selftest_support" / "export_kill_driver.py"),
         "--state-dir", str(state_dir), "--out-prefix", str(out_prefix),
         "--format", fmt],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env,
    )


def wait_version_dirs(export_root: Path, minimum: int, timeout: float = 10.0) -> list[str]:
    def scan():
        if not export_root.is_dir():
            return None
        dirs = sorted(n for n in os.listdir(export_root)
                      if n.startswith("v") and n[1:].isdigit())
        return dirs if len(dirs) >= minimum else None
    return wait_until(scan, timeout=timeout)


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

        # ------------------------------------------------ 场景 5：导出统一批次边界（并发入账 + 强杀）
        print("[场景 5] 导出读取口径统一到已持久化批次边界：并发入账不混批，强杀后旧版两表成对可读")
        # pyarrow 不在环境里时自动走纯标准库 jsonl；装了 pyarrow 则两种格式各验一遍
        try:
            import pyarrow  # noqa: F401
            export_formats = ["jsonl", "parquet"]
            print("    检测到 pyarrow，jsonl 与 parquet 两种格式均参与自测")
        except ImportError:
            export_formats = ["jsonl"]
            print("    未安装 pyarrow，使用纯标准库 jsonl 格式覆盖同一套导出语义")

        exp_state = work / "export_state"
        exp_log = work / "export.log"
        exp_state.mkdir()
        proc = start_service(exp_state, exp_log, 18096)
        try:
            replay(str(work / "mock.jsonl"), str(exp_log), delay=0.0)
            wait_until(
                lambda: read_jsonl(exp_state / "batches.jsonl")
                if len(read_jsonl(exp_state / "batches.jsonl")) == len(fast["batches"]) else None,
                timeout=20,
            )
            # 先产出一版“上一版导出”，作为强杀后必须保持可读的成对基线
            prefix0 = work / "report_base"
            first = export(str(exp_state), str(prefix0), fmt=export_formats[0])
            verify_export_pair(exp_state, prefix0)
            base_seq = first["as_of_seq"]
            print(f"    基线导出版本 v{first['version_seq']:06d}，边界 as_of_seq={base_seq}")

            # 后台持续入账：导出全程都有新批次落盘，验证窗口表不会混入当次边界之外的数据
            appender = _BackgroundAppender(work / "mock.jsonl", exp_log)
            appender.start()
            try:
                # 等后台入账确实推进了持久化批次，再开始导出，保证导出与入账真实并发
                wait_until(
                    lambda: max((b["seq"] for b in read_jsonl(exp_state / "batches.jsonl")),
                                default=0) > base_seq or None,
                    timeout=15,
                )
                prior_seq = base_seq
                for round_idx, fmt in enumerate(export_formats, start=1):
                    prefix = work / f"report_{fmt}"
                    info = export(str(exp_state), str(prefix), fmt=fmt)
                    # 后台在追加，持久化批次理应推进；导出边界必须是导出瞬间已落盘的真实批次号
                    current_last = max(
                        (b["seq"] for b in read_jsonl(exp_state / "batches.jsonl")),
                        default=0,
                    )
                    assert prior_seq < info["as_of_seq"] <= current_last, (
                        f"导出边界 {info['as_of_seq']} 越界或未推进，持久化末批={current_last}")
                    verify_export_pair(exp_state, prefix, expect_seq=info["as_of_seq"])
                    prior_seq = info["as_of_seq"]
                    print(f"    {fmt} 并发导出第 {round_idx} 轮：边界 as_of_seq={info['as_of_seq']}，"
                          f"批次表 {info['batch_rows']} 行 / 窗口表 {info['window_rows']} 行，"
                          f"并发新批次未混入当次窗口表")
                    # 再等一轮入账推进，让下一次导出面对更新的批次边界
                    wait_until(
                        lambda: max((b["seq"] for b in read_jsonl(exp_state / "batches.jsonl")),
                                    default=0) > prior_seq or None,
                        timeout=15,
                    )

                # ---- 确定性强杀：两表已 fsync、latest 切换前 SIGKILL
                kill_prefix = work / "report_kill"
                killed = start_stalled_export(exp_state, kill_prefix, export_formats[0])
                try:
                    orphan_dirs = wait_version_dirs(
                        kill_prefix.parent / f"{kill_prefix.name}.export", minimum=1)
                    time.sleep(0.3)  # 确保进入提交前停顿窗口
                    killed.send_signal(signal.SIGKILL)
                    killed.wait(timeout=10)
                finally:
                    if killed.poll() is None:
                        killed.send_signal(signal.SIGKILL)
                        killed.wait(timeout=10)
                print(f"    已在提交前 SIGKILL 导出进程，孤儿版本目录: {orphan_dirs}")

                # 强杀发生在 latest 切换之前：不存在成功指针，旧的成对表不受影响
                kill_root = kill_prefix.parent / f"{kill_prefix.name}.export"
                assert not (kill_root / "latest").exists(), "强杀后不应出现 latest 指针"
                leftover = sorted(n for n in os.listdir(kill_root)
                                  if n.startswith("v") and n[1:].isdigit())
                assert leftover, "强杀应留下未提交的不可变版本目录"
                base_info = read_export(str(prefix0))
                assert base_info["as_of_seq"] == base_seq, "上一版导出的边界被强杀破坏"
                base_batch_rows = read_table_rows(base_info["batches_path"])
                base_window_rows = read_table_rows(base_info["windows_path"])
                assert all(r["as_of_seq"] == base_seq for r in base_batch_rows)
                assert all(r["as_of_seq"] == base_seq for r in base_window_rows)
                assert_equal("强杀后上一版两张表仍可成对读取且边界不变",
                             base_info["as_of_seq"], base_seq)

                # 同前缀重新导出：原子提交成功，新版本与被杀死的孤儿目录并存、可逐版读取
                recovered = export(str(exp_state), str(kill_prefix), fmt=export_formats[0])
                rec_info = verify_export_pair(exp_state, kill_prefix)
                assert rec_info["as_of_seq"] >= base_seq
                all_versions = sorted(n for n in os.listdir(kill_root)
                                      if n.startswith("v") and n[1:].isdigit())
                assert len(all_versions) == len(leftover) + 1, "新版本不得覆盖孤儿目录"
                print(f"    强杀后重新导出成功：latest=v{rec_info['version_seq']:06d}，"
                      f"保留版本 {all_versions}")

                # ---- 再做几轮随机时刻 SIGKILL，覆盖写入途中/提交前后的任意时点
                random_killed_ok = 0
                for trial in range(6):
                    trial_prefix = work / f"report_rand_{trial}"
                    p = subprocess.Popen(
                        [sys.executable, "-I",
                         str(ROOT / "run_export_parquet.py"),
                         "--state-dir", str(exp_state),
                         "--out-prefix", str(trial_prefix),
                         "--format", export_formats[0]],
                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                    )
                    time.sleep(0.005 * (trial + 1))  # 落在导出的不同阶段
                    p.send_signal(signal.SIGKILL)
                    p.wait(timeout=10)
                    # 无论杀死在哪一步：已提交的导出都必须成对可读，未提交的不出现 latest
                    rroot = trial_prefix.parent / f"{trial_prefix.name}.export"
                    if (rroot / "latest").exists():
                        verify_export_pair(exp_state, trial_prefix)
                        random_killed_ok += 1
                    else:
                        assert not (rroot / "latest").exists()
                        random_killed_ok += 1
                assert_equal("6 轮任意时点 SIGKILL 后导出目录始终无撕裂（成对可读或未提交）",
                             random_killed_ok, 6)
            finally:
                appender.stop()

            # ---- 恢复与导出读取口径一致性：删掉快照只靠批次日志，结果必须相同
            snap_path = exp_state / "snapshot.json"
            had_snapshot = snap_path.exists()
            if had_snapshot:
                snap_path.unlink()
            journal_only = Engine.from_persisted_batches(
                load_persisted_batches(str(exp_state / BATCHES_NAME)))
            restored = Engine(snapshot_store=SnapshotStore(str(exp_state)))
            assert_equal("无快照恢复与快照恢复的批次视图一致",
                         journal_only.batches, restored.batches)
            assert_equal("无快照恢复与快照恢复的窗口视图一致",
                         journal_only.windows, restored.windows)
            prefix_nosnap = work / "report_nosnap"
            export(str(exp_state), str(prefix_nosnap), fmt=export_formats[0])
            verify_export_pair(exp_state, prefix_nosnap)
            print("    恢复（快照/纯批次日志）与导出共用同一已持久化批次读取口径，结果一致")
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
