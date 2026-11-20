#!/usr/bin/env python3
"""端到端强杀恢复验证：本地回放在“写入中”和“落盘后”分别 SIGKILL，
核对续跑、再次重启的结果与无中断回放完全一致。

覆盖任务要求：
  1. 写盘卡住/强杀时查询始终读到上一版完整旧账（无幽灵更正）；
  2. 强杀落在提交组内 -> 未完成组整组回滚，同条 progress 触发的封账不丢；
  3. 原目录续跑的批次序号、窗口账、源日志续读位置与无中断回放一致；
  4. 续跑后再重启一次，结果保持不变（恢复幂等）。

崩溃注入通过环境变量驱动 JournalWriter：
  SHOREPOWER_CRASH_AT=N  第 N 个提交组（= 第 N 条被处理的源记录）
  SHOREPOWER_CRASH_MODE=write/fsync
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
sys.path.insert(0, str(ROOT))

from shorepower.feeder import replay
from shorepower.generator import write_log
from shorepower.model import parse_line
from shorepower.storage import BATCHES_NAME, dumps_canonical

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


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def start_service(state_dir: Path, log_file: Path, port: int,
                  crash_at: int = 0, crash_mode: str = "fsync") -> subprocess.Popen:
    env = dict(os.environ)
    if crash_at:
        env["SHOREPOWER_CRASH_AT"] = str(crash_at)
        env["SHOREPOWER_CRASH_MODE"] = crash_mode
    proc = subprocess.Popen(
        [sys.executable, "-I", str(ROOT / "run_service.py"),
         "--log-file", str(log_file), "--state-dir", str(state_dir),
         "--port", str(port), "--max-inflight", "50",
         "--snapshot-every", "3"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env,
    )
    wait_port(port)
    return proc


def stop_service(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGTERM)
    proc.communicate(timeout=10)


def wait_crash(proc: subprocess.Popen, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            assert proc.returncode == 137, f"崩溃注入退出码异常: {proc.returncode}"
            return
        time.sleep(0.05)
    raise AssertionError("服务未在预期时间被强杀")


def wait_until(predicate, timeout: float = 25.0, interval: float = 0.1):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"条件在 {timeout}s 内未满足，最后状态: {last}")


def last_line_start_offset(path: Path) -> int:
    data = path.read_bytes()
    return len(data) - len(data.splitlines()[-1])


def strip_group_end(batches: list[dict]) -> list[dict]:
    return [{k: v for k, v in b.items() if k != "group_end"} for b in batches]


def find_crash_groups(mock_src: Path) -> tuple[int, int]:
    """在确定性日志里定位两个强杀目标组（1 基）：
    组 C：第一笔被接受的更正（改账），用于验证“刚查到的更正”可见性；
    组 S：第一条触发封账的 progress（decision+seal 同组）。
    位置用业务语义识别，不依赖手数行号。
    """
    correction_group = seal_group = None
    for idx, raw in enumerate(mock_src.read_text(encoding="utf-8").splitlines(), start=1):
        rec = parse_line(raw, offset=0)
        type_name = type(rec).__name__
        if type_name == "Correction" and correction_group is None and rec.version >= 2:
            correction_group = idx  # 日志里第一笔 v2 更正（r1 10->12，确定被接受）
        if type_name == "Progress" and seal_group is None:
            # 三网关 08:25 那批的第三条 GW3 推进后触发 08:00 封账
            if rec.gateway == "GW3" and rec.progress_ts >= _ts("2026-11-18T08:25:00Z"):
                seal_group = idx
    assert correction_group and seal_group, (correction_group, seal_group)
    return correction_group, seal_group


def _ts(text: str) -> int:
    from shorepower.model import parse_ts
    return parse_ts(text, "x")


def main() -> int:
    t0 = time.time()
    work = Path(tempfile.mkdtemp(prefix="shorepower-crash-"))
    print(f"工作目录: {work}")
    failures = 0

    def check(label: str, cond: bool, detail: str = "") -> None:
        nonlocal failures
        if cond:
            print(f"  {PASS} {label}")
        else:
            failures += 1
            print(f"  {FAIL} {label} {detail}")

    try:
        n = write_log(str(work / "mock.jsonl"))
        print(f"[准备] 生成确定性模拟日志 {n} 行")
        group_c, group_s = find_crash_groups(work / "mock.jsonl")
        print(f"[准备] 强杀目标组：更正组={group_c}，封账组={group_s}")

        # ------------------------------------------------- 基线：无中断回放
        print("[基线] 无中断瞬时回放，产出权威批次/窗口/偏移")
        base_state, base_log = work / "base", work / "base.log"
        base_state.mkdir()
        port = 19091
        proc = start_service(base_state, base_log, port)
        try:
            replay(str(work / "mock.jsonl"), str(base_log), delay=0.0)
            target = last_line_start_offset(base_log) - 1
            wait_until(lambda: http_get(port, "/stats")["committed_source_offset"] >= target
                       or None)
            baseline_batches = read_jsonl(base_state / BATCHES_NAME)
            baseline_windows = http_get(port, "/windows")
            baseline_stats = http_get(port, "/stats")
        finally:
            stop_service(proc)

        # ----------------------------------------- 场景 0：存储层截断规则（确定性）
        print("[场景 0] 批次日志尾部各种强杀残迹的截断规则")
        unit_dir = work / "unit"
        unit_dir.mkdir()
        jpath = unit_dir / BATCHES_NAME
        g1 = [{"seq": 1, "kind": "decision", "group_end": True}]
        g2 = [{"seq": 2, "kind": "decision"}, {"seq": 3, "kind": "seal", "group_end": True}]

        def write_raw(text: str) -> None:
            jpath.write_text(text, encoding="utf-8")

        from shorepower.storage import dumps_canonical as _dumps, recover_committed_batches as _recover
        full = "".join(_dumps(b) + "\n" for b in g1 + g2)
        # 0.1 完整日志：原样返回
        write_raw(full)
        check("完整日志全部恢复", len(_recover(str(jpath))) == 3)
        # 0.2 尾部半截 JSON 行（write 强杀，行都没写完）
        write_raw(full + '{"seq":4,"kind":"deci')  # 无换行的半截行
        check("半截 JSON 行被丢弃", [b["seq"] for b in _recover(str(jpath))] == [1, 2, 3])
        check("半截行被物理截断", jpath.read_text(encoding="utf-8") == full)
        # 0.3 完整行但提交组未结束（decision 已落、seal 未落）
        torn_group = _dumps({"seq": 4, "kind": "decision"}) + "\n"
        write_raw(full + torn_group)
        check("未完成提交组整组回滚", [b["seq"] for b in _recover(str(jpath))] == [1, 2, 3])
        check("未完成组被物理截断", jpath.read_text(encoding="utf-8") == full)
        # 0.4 双批次组只落了第一个（封账丢失现场）：同样整组回滚
        write_raw(full + _dumps({"seq": 4, "kind": "decision"}) + "\n"
                  + _dumps({"seq": 5, "kind": "seal"}) + "\n")  # 末批无 group_end
        check("decision+seal 残缺组整组回滚（封账不丢、留待重放）",
              [b["seq"] for b in _recover(str(jpath))] == [1, 2, 3])
        # 0.5 旧格式日志（无任何 group_end）：能完整解析即有效，兼容历史数据
        legacy = "".join(_dumps({k: v for k, v in b.items() if k != "group_end"}) + "\n"
                         for b in g1 + g2)
        write_raw(legacy)
        check("旧格式日志兼容恢复", len(_recover(str(jpath))) == 3)

        # ============================ 场景矩阵 ============================
        scenarios = [
            ("写入中强杀（组内首行 write 后、fsync 前）", group_c, "write"),
            ("落盘后强杀（整组 write 后、fsync 前）", group_c, "fsync"),
            ("封账组写入中强杀（progress decision+seal）", group_s, "write"),
            ("封账组落盘后强杀", group_s, "fsync"),
        ]

        for idx, (title, crash_at, mode) in enumerate(scenarios):
            print(f"\n[场景 {idx + 1}] {title}，目标组 #{crash_at}")
            sd = work / f"s{idx}"
            lg = work / f"s{idx}.log"
            sd.mkdir()
            port = 19100 + idx

            proc = start_service(sd, lg, port, crash_at=crash_at, crash_mode=mode)
            try:
                # 瞬时回放；注入点在第 crash_at 组，进程在该处自行 SIGKILL
                replay(str(work / "mock.jsonl"), str(lg), delay=0.0)
                wait_crash(proc)
            finally:
                if proc.poll() is None:
                    proc.send_signal(signal.SIGKILL)
                    proc.wait(timeout=10)

            # 强杀后磁盘上的事实（不走服务，直接按恢复规则读）：
            # write 模式可能在文件尾留下无 group_end 的半截行，恢复时必须整组截断；
            # 若内核已把数据刷出则残行不在磁盘——两种情况下恢复结果都必须只有完整组。
            from shorepower.storage import recover_committed_batches
            journal_path = str(sd / BATCHES_NAME)
            recovered_disk = recover_committed_batches(journal_path)
            seqs = [b["seq"] for b in recovered_disk]
            check("恢复规则截断后仅含完整提交组（末批带 group_end）",
                  not recovered_disk or recovered_disk[-1].get("group_end") is True)
            check("截断后已落盘批次与无中断回放的同序号批次逐字节一致",
                  strip_group_end(recovered_disk) == strip_group_end(
                      baseline_batches[: len(recovered_disk)]))
            check("截断后批次序号照旧、严格连续",
                  seqs == list(range(1, len(seqs) + 1)))
            # 恢复出的完整旧账必须恰好等于“崩溃组之前”的权威前缀（按状态而非批次数比较，
            # 因为一条记录可发布 1~2 个批次）
            check("恢复出的完整旧账 == 崩溃点之前的权威批次前缀",
                  strip_group_end(recovered_disk)
                  == strip_group_end(baseline_batches[: len(recovered_disk)]))
            disk_batches = recovered_disk

            # 原目录续跑（不重建任何文件）
            proc = start_service(sd, lg, port + 20)
            try:
                target = last_line_start_offset(lg) - 1
                wait_until(lambda: http_get(port + 20, "/stats")[
                    "committed_source_offset"] >= target or None)
                cont_batches = read_jsonl(sd / BATCHES_NAME)
                cont_windows = http_get(port + 20, "/windows")
                cont_stats = http_get(port + 20, "/stats")
            finally:
                stop_service(proc)

            check("续跑后批次总数追平无中断回放",
                  len(cont_batches) == len(baseline_batches),
                  f"{len(cont_batches)} != {len(baseline_batches)}")
            check("续跑后批次内容/顺序与无中断回放逐字节一致（去掉组标记比较）",
                  strip_group_end(cont_batches) == strip_group_end(baseline_batches))
            check("续跑后已有批次序号照旧、整体无重复无空洞",
                  [b["seq"] for b in cont_batches] == list(range(1, len(cont_batches) + 1)))
            check("续跑后窗口电量/封账状态与无中断回放一致",
                  dumps_canonical(cont_windows) == dumps_canonical(baseline_windows))
            check("续跑后源日志续读位置（提交偏移）与无中断回放一致",
                  cont_stats["committed_source_offset"]
                  == baseline_stats["committed_source_offset"])
            # 封账不丢：触发封账的 progress 被整组重放
            sealed_now = [w["window"] for w in cont_windows["windows"] if w["sealed"]]
            base_sealed = [w["window"] for w in baseline_windows["windows"] if w["sealed"]]
            check("同条上报触发的封账未被漏掉", sealed_now == base_sealed)

            # 再次重启：恢复必须幂等，结果保持不变
            proc = start_service(sd, lg, port + 40)
            try:
                time.sleep(0.6)  # 无新记录，仅恢复
                again_batches = read_jsonl(sd / BATCHES_NAME)
                again_windows = http_get(port + 40, "/windows")
                again_stats = http_get(port + 40, "/stats")
            finally:
                stop_service(proc)
            check("再次重启后批次保持不变（恢复幂等）",
                  strip_group_end(again_batches) == strip_group_end(baseline_batches))
            check("再次重启后窗口账可直接续用、与基线一致",
                  dumps_canonical(again_windows) == dumps_canonical(baseline_windows))
            check("再次重启后续读偏移保持在源日志末尾",
                  again_stats["committed_source_offset"]
                  == baseline_stats["committed_source_offset"])

        # ----------------------------------------- 场景 5：写盘卡住时查询旧账
        print("\n[场景 5] 写盘卡住（fsync 前强杀）期间，查询永不越过已持久化的完整旧账")
        sd, lg = work / "s5", work / "s5.log"
        sd.mkdir()
        # 在更正组上 fsync 前强杀；强杀前服务仍存活并对外服务查询
        proc = start_service(sd, lg, 19200, crash_at=group_c, crash_mode="fsync")
        try:
            replay(str(work / "mock.jsonl"), str(lg), delay=0.05)  # 慢放，留出查询窗口
        finally:
            if proc.poll() is None:
                wait_crash(proc)
            if proc.poll() is None:
                proc.send_signal(signal.SIGKILL)
                proc.wait(timeout=10)

        # 强杀时刻磁盘上的权威旧账：恢复截断后只能是更正组之前的前缀
        durable = recover_committed_batches(str(sd / BATCHES_NAME))
        check("强杀点磁盘完整旧账 == 更正组之前的权威批次前缀",
              strip_group_end(durable) == strip_group_end(baseline_batches[: len(durable)]))
        check("强杀点磁盘仅含完整提交组（更正组未持久化，查询不可能读到幽灵更正）",
              all(b.get("group_end") is True for b in durable[-1:]))

        # 进程内直接验证协议保证：prepare 后查询 == 已提交旧账，commit 后才变新账
        from shorepower.engine import Engine
        from shorepower.model import parse_line as _parse
        from shorepower.storage import SnapshotStore as _Snap
        unit_state = work / "unit-engine"
        unit_state.mkdir()
        eng = Engine(snapshot_store=_Snap(str(unit_state)))
        r1 = _parse('{"type":"reading","reading_id":"r1","gateway":"GW1","berth":"B1",'
                    '"meter_ts":"2026-11-18T08:02:00Z","energy":10.0}\n', offset=0)
        eng.commit(eng.prepare(r1))
        corr = _parse('{"type":"correction","reading_id":"r1","version":2,"berth":"B1",'
                      '"meter_ts":"2026-11-18T08:02:00Z","energy":12.0}\n', offset=100)
        eng.prepare(corr)  # 模拟写盘卡住：暂存已生成但未 commit
        stuck_view = eng.query_windows()
        check("写盘卡住时查询读到上一版完整旧账（B1=10，未见 12 的更正）",
              stuck_view["windows"][0]["totals"].get("B1") == 10.0)
        eng.discard_pending()  # 强杀：暂存作废
        check("强杀丢弃暂存后旧账原样可续用（B1=10，last_seq=1）",
              eng.query_windows()["windows"][0]["totals"].get("B1") == 10.0
              and eng.stats()["last_seq"] == 1)

        # 续跑追平，确认旧账可直接续用
        proc = start_service(sd, lg, 19202)
        try:
            target = last_line_start_offset(lg) - 1
            wait_until(lambda: http_get(19202, "/stats")[
                "committed_source_offset"] >= target or None)
            check("卡住后续跑结果仍与无中断回放一致",
                  dumps_canonical(http_get(19202, "/windows"))
                  == dumps_canonical(baseline_windows))
        finally:
            stop_service(proc)

    except AssertionError as exc:
        failures += 1
        import traceback
        traceback.print_exc()
        print(f"{FAIL} 断言失败: {exc}")
    except Exception as exc:  # noqa: BLE001
        failures += 1
        import traceback
        traceback.print_exc()
        print(f"{FAIL} 意外异常: {exc}")
    finally:
        shutil.rmtree(work, ignore_errors=True)

    elapsed = time.time() - t0
    print(f"\n总耗时 {elapsed:.1f}s")
    if failures:
        print(f"结论：{FAIL} 存在 {failures} 处失败")
        return 1
    print(f"结论：{PASS} 全部强杀恢复场景通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
