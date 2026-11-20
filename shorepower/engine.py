"""账务引擎：刻钟窗口归账、版本化整笔更正、进度水位封账、确定性批次发布。

线程模型与崩溃一致性：

* 只有服务写线程调用 :meth:`Engine.prepare` / :meth:`Engine.commit`；
  HTTP 查询线程通过同一把锁读取。
* :meth:`prepare` 只在**私有暂存状态**上推演，绝不触碰可查询的已提交状态；
  服务线程随后把整组批次一次 fsync 进批次日志，成功后才调用 :meth:`commit`
  原子换入。因此写盘卡住或强杀时，查询读到的始终是上一版完整旧账，
  已对外可见的账必然已持久化，不存在“刚查到的更正消失”。
* 一条源记录发布的全部批次（如 progress 同时触发的 decision 与 seal）属于
  同一个提交组，整组同生共死；强杀落在组内会在恢复时整组回滚，源记录从
  原偏移重新进入流程，封账不会被漏掉，批次序号与无中断回放完全一致。
"""

from __future__ import annotations

import copy
import threading
from dataclasses import dataclass

from .model import (
    SCHEMA_VERSION,
    BadLine,
    Correction,
    Progress,
    Reading,
    SEAL_GRACE_SECONDS,
    WINDOW_SECONDS,
    format_ts,
    parse_ts,
    seal_watermark_progress,
    window_start,
)
from .storage import BATCHES_NAME, SnapshotStore, recover_committed_batches


def _new_window() -> dict:
    # totals: 泊位 -> 当前电量；contributions: reading_id -> {berth, energy}
    return {"sealed": False, "totals": {}, "contributions": {}}


@dataclass
class LedgerState:
    """账务的全部可变状态。prepare 时整体深拷贝出私有暂存，commit 时整体换入。"""

    gateways: set
    progress: dict
    readings: dict
    windows: dict
    committed_offset: int
    min_meter_ts: int | None

    @classmethod
    def empty(cls) -> "LedgerState":
        return cls(set(), {}, {}, {}, -1, None)

    def clone(self) -> "LedgerState":
        # 深拷贝容器：暂存上的任何修改都不会泄漏到已提交状态（查询视图）
        return LedgerState(
            set(self.gateways),
            dict(self.progress),
            copy.deepcopy(self.readings),
            copy.deepcopy(self.windows),
            self.committed_offset,
            self.min_meter_ts,
        )


class Engine:
    def __init__(self, snapshot_store: SnapshotStore | None = None):
        self.lock = threading.RLock()
        self.snapshot_store = snapshot_store
        self.state: LedgerState = LedgerState.empty()
        # 已提交批次（权威顺序与序号），只在 commit 成功后追加
        self.batches: list[dict] = []
        # prepare 产出、等待 fsync 的暂存；查询线程永远看不到它
        self._pending: LedgerState | None = None
        self._since_snapshot = 0
        self.restore()

    # ------------------------------------------------------------- 批次工具
    def _next_seq(self) -> int:
        return len(self.batches) + 1

    def _window(self, st: LedgerState, start: int) -> dict:
        win = st.windows.get(start)
        if win is None:
            win = _new_window()
            st.windows[start] = win
        return win

    # ----------------------------------------------------- 提交流程（核心）
    def prepare(self, rec) -> list[dict]:
        """在私有暂存上处理一条记录，返回本次将发布的整组批次（0~2 个）。

        本方法不修改任何可查询状态。调用方必须按返回顺序把整组批次一次
        fsync 进批次日志，再调用 :meth:`commit`；fsync 失败则放弃 commit，
        已提交状态保持上一版完整旧账不变。
        """
        with self.lock:
            staged = self.state.clone()
            batches: list[dict] = []

            def publish(batch: dict) -> dict:
                batch["seq"] = self._next_seq() + len(batches)
                batch.setdefault("schema", SCHEMA_VERSION)
                batches.append(batch)
                return batch

            if isinstance(rec, BadLine):
                self._decision_bad(staged, rec, publish)
            elif isinstance(rec, Reading):
                self._process_reading(staged, rec, publish)
            elif isinstance(rec, Correction):
                self._process_correction(staged, rec, publish)
            elif isinstance(rec, Progress):
                self._process_progress(staged, rec, publish)
            else:  # pragma: no cover - 防御
                raise TypeError(f"未知记录类型: {type(rec)!r}")
            staged.committed_offset = rec.offset
            # 进度记录可能推动封账；其它记录只改账不动水位，封账检查统一做也无害
            self._seal_due_windows(staged, rec.offset, publish)
            if batches:
                # 组内最后一个批次打标记：恢复时据此识别完整提交组
                batches[-1]["group_end"] = True
            self._pending = staged
            return batches

    def commit(self, batches: list[dict]) -> None:
        """整组批次 fsync 成功后调用：暂存状态原子换入，批次并入权威列表。"""
        with self.lock:
            if self._pending is None:
                raise RuntimeError("没有待提交的暂存状态（prepare 未执行或已提交）")
            expected_first = self._next_seq()
            for i, batch in enumerate(batches):
                if batch["seq"] != expected_first + i:
                    raise RuntimeError("提交批次序号与暂存不一致，拒绝换入状态")
            self.state = self._pending
            self._pending = None
            self.batches.extend(batches)
            self._since_snapshot += 1

    def discard_pending(self) -> None:
        """fsync 失败时丢弃暂存，已提交状态（完整旧账）原样保留。"""
        with self.lock:
            self._pending = None

    # ------------------------------------------------------------- reading
    def _process_reading(self, st: LedgerState, rec: Reading, publish) -> None:
        base = {
            "kind": "decision",
            "source_offset": rec.offset,
            "record_type": "reading",
            "reading_id": rec.reading_id,
            "gateway": rec.gateway,
        }
        if rec.reading_id in st.readings:
            publish(
                base
                | {
                    "status": "rejected",
                    "reason": "reading_id 已存在，重复读数不允许入账（修改请发 correction）",
                    "changes": [],
                }
            )
            return
        wstart = window_start(rec.meter_ts)
        if st.windows.get(wstart, {}).get("sealed"):
            publish(
                base
                | {
                    "status": "rejected",
                    "reason": f"窗口 {format_ts(wstart)} 已封账，迟到读数不得入账",
                    "changes": [],
                }
            )
            return
        st.gateways.add(rec.gateway)
        st.min_meter_ts = (
            rec.meter_ts
            if st.min_meter_ts is None
            else min(st.min_meter_ts, rec.meter_ts)
        )
        win = self._window(st, wstart)
        win["totals"][rec.berth] = win["totals"].get(rec.berth, 0.0) + rec.energy
        win["contributions"][rec.reading_id] = {"berth": rec.berth, "energy": rec.energy}
        st.readings[rec.reading_id] = {
            "gateway": rec.gateway,
            "berth": rec.berth,
            "meter_ts": rec.meter_ts,
            "energy": rec.energy,
            "version": 1,
            "offset": rec.offset,
        }
        change = {
            "window": format_ts(wstart),
            "window_start": wstart,
            "berth": rec.berth,
            "change": "set",
            "energy": rec.energy,
            "total": win["totals"][rec.berth],
            "reading_id": rec.reading_id,
        }
        publish(
            base
            | {
                "status": "accepted",
                "meter_ts": format_ts(rec.meter_ts),
                "changes": [change],
            }
        )

    # ------------------------------------------------------------- correction
    def _process_correction(self, st: LedgerState, rec: Correction, publish) -> None:
        base = {
            "kind": "decision",
            "source_offset": rec.offset,
            "record_type": "correction",
            "reading_id": rec.reading_id,
            "version": rec.version,
        }
        current = st.readings.get(rec.reading_id)
        if current is None:
            publish(
                base
                | {
                    "status": "rejected",
                    "reason": "目标 reading_id 不存在，无法更正",
                    "changes": [],
                }
            )
            return
        if rec.version <= current["version"]:
            publish(
                base
                | {
                    "status": "rejected",
                    "reason": (
                        f"版本 {rec.version} 不高于当前生效版本 {current['version']}，"
                        "更正驳回且原账不变"
                    ),
                    "changes": [],
                }
            )
            return
        old_start = window_start(current["meter_ts"])
        new_start = window_start(rec.meter_ts)
        blocked = []
        for wstart in {old_start, new_start}:
            if st.windows.get(wstart, {}).get("sealed"):
                blocked.append(format_ts(wstart))
        if blocked:
            publish(
                base
                | {
                    "status": "rejected",
                    "reason": (
                        "涉及窗口已封账（"
                        + ", ".join(sorted(blocked))
                        + "），整笔驳回，原账不变"
                    ),
                    "changes": [],
                }
            )
            return

        # ---- 整笔事务：下面的改动要么全部完成，要么前面已全部校验通过，不会半途失败
        changes: list[dict] = []
        # 1) 从原窗口/原泊位移除
        old_win = self._window(st, old_start)
        contrib = old_win["contributions"].pop(rec.reading_id)
        old_berth = contrib["berth"]
        old_energy = contrib["energy"]
        old_win["totals"][old_berth] = old_win["totals"].get(old_berth, 0.0) - old_energy
        if old_win["totals"][old_berth] == 0.0:
            del old_win["totals"][old_berth]
        changes.append(
            {
                "window": format_ts(old_start),
                "window_start": old_start,
                "berth": old_berth,
                "change": "remove",
                "energy": -old_energy,
                "total": old_win["totals"].get(old_berth, 0.0),
                "reading_id": rec.reading_id,
            }
        )
        # 2) 落到新窗口/新泊位（窗口或泊位未变时仍成对呈现，保证同批可见完整轨迹）
        new_win = self._window(st, new_start)
        new_win["totals"][rec.berth] = new_win["totals"].get(rec.berth, 0.0) + rec.energy
        new_win["contributions"][rec.reading_id] = {
            "berth": rec.berth,
            "energy": rec.energy,
        }
        changes.append(
            {
                "window": format_ts(new_start),
                "window_start": new_start,
                "berth": rec.berth,
                "change": "add",
                "energy": rec.energy,
                "total": new_win["totals"][rec.berth],
                "reading_id": rec.reading_id,
            }
        )
        # 3) 升版本，原账被新账取代
        current.update(
            {
                "berth": rec.berth,
                "meter_ts": rec.meter_ts,
                "energy": rec.energy,
                "version": rec.version,
                "offset": rec.offset,
            }
        )
        st.min_meter_ts = (
            rec.meter_ts
            if st.min_meter_ts is None
            else min(st.min_meter_ts, rec.meter_ts)
        )
        # 确定性顺序：窗口起点、泊位、变更类型（remove 在 add 前）
        order = {"remove": 0, "add": 1, "set": 2}
        changes.sort(key=lambda c: (c["window_start"], c["berth"], order[c["change"]]))
        publish(
            base
            | {
                "status": "accepted",
                "meter_ts": format_ts(rec.meter_ts),
                "changes": changes,
            }
        )

    # ------------------------------------------------------------- progress
    def _process_progress(self, st: LedgerState, rec: Progress, publish) -> None:
        base = {
            "kind": "decision",
            "source_offset": rec.offset,
            "record_type": "progress",
            "gateway": rec.gateway,
            "changes": [],
        }
        prev = st.progress.get(rec.gateway)
        if prev is not None and rec.progress_ts < prev:
            publish(
                base
                | {
                    "status": "rejected",
                    "reason": (
                        f"进度水位不能回退：当前 {format_ts(prev)}，"
                        f"上报 {format_ts(rec.progress_ts)}"
                    ),
                }
            )
            return
        st.gateways.add(rec.gateway)
        st.progress[rec.gateway] = rec.progress_ts
        publish(
            base
            | {
                "status": "accepted",
                "new_progress": {rec.gateway: format_ts(rec.progress_ts)},
            }
        )

    # ------------------------------------------------------------- bad line
    def _decision_bad(self, st: LedgerState, rec: BadLine, publish) -> None:
        publish(
            {
                "kind": "decision",
                "source_offset": rec.offset,
                "record_type": "bad",
                "status": "rejected",
                "reason": rec.reason,
                "changes": [],
            }
        )

    # ------------------------------------------------------------- 封账
    def _all_gateways_reported(self, st: LedgerState) -> bool:
        return bool(st.gateways) and st.gateways.issubset(st.progress.keys())

    def _seal_due_windows(self, st: LedgerState, trigger_offset: int, publish) -> None:
        if not self._all_gateways_reported(st) or st.min_meter_ts is None:
            return
        slowest = min(st.progress.values())
        top = seal_watermark_progress(slowest)
        if top < 0:
            return
        first = window_start(st.min_meter_ts)
        sealed_now: list[dict] = []
        wstart = first
        while wstart <= top:
            win = st.windows.get(wstart)
            if win is None:
                win = _new_window()
                st.windows[wstart] = win
            if not win["sealed"]:
                win["sealed"] = True
                sealed_now.append(
                    {
                        "window": format_ts(wstart),
                        "window_start": wstart,
                        "totals": dict(sorted(win["totals"].items())),
                    }
                )
            wstart += WINDOW_SECONDS
        if not sealed_now:
            return
        publish(
            {
                "kind": "seal",
                "trigger_offset": trigger_offset,
                "watermark_progress": format_ts(slowest),
                "grace_seconds": SEAL_GRACE_SECONDS,
                "sealed_windows": sealed_now,
            }
        )

    # ------------------------------------------------------------- 查询
    def query_windows(self, window_ids: list[str] | None = None) -> dict:
        with self.lock:
            st = self.state
            slowest = min(st.progress.values()) if st.progress else None
            result = {
                "seal_grace_seconds": SEAL_GRACE_SECONDS,
                "slowest_progress": format_ts(slowest) if slowest is not None else None,
                "all_gateways_reported": self._all_gateways_reported(st),
                "windows": [],
            }
            if window_ids:
                starts = []
                for wid in window_ids:
                    try:
                        starts.append(parse_ts(wid, "window"))
                    except Exception:
                        starts.append(None)
                items = [(s, st.windows.get(s) if s is not None else None) for s in starts]
            else:
                items = sorted(st.windows.items())
            for wstart, win in items:
                if wstart is None:
                    result["windows"].append(
                        {"window": None, "error": "窗口标识无法解析为 UTC 时间"}
                    )
                    continue
                if win is None:
                    result["windows"].append(
                        {
                            "window": format_ts(wstart),
                            "window_start": wstart,
                            "sealed": False,
                            "totals": {},
                            "total_energy": 0.0,
                        }
                    )
                    continue
                totals = dict(sorted(win["totals"].items()))
                result["windows"].append(
                    {
                        "window": format_ts(wstart),
                        "window_start": wstart,
                        "sealed": win["sealed"],
                        "totals": totals,
                        "total_energy": sum(totals.values()),
                    }
                )
            return result

    def query_batches(self, after_seq: int = 0, limit: int | None = None) -> dict:
        with self.lock:
            picked = [b for b in self.batches if b["seq"] > after_seq]
            if limit is not None:
                picked = picked[:limit]
            return {
                "after_seq": after_seq,
                "last_seq": self.batches[-1]["seq"] if self.batches else 0,
                "batches": picked,
            }

    def stats(self, inflight: int = 0) -> dict:
        with self.lock:
            st = self.state
            sealed = sum(1 for w in st.windows.values() if w["sealed"])
            return {
                "published_batches": len(self.batches),
                "last_seq": self.batches[-1]["seq"] if self.batches else 0,
                "committed_source_offset": st.committed_offset,
                "inflight_unpersisted": inflight,
                "known_gateways": sorted(st.gateways),
                "progress": {g: format_ts(ts) for g, ts in sorted(st.progress.items())},
                "tracked_readings": len(st.readings),
                "windows_total": len(st.windows),
                "windows_sealed": sealed,
            }

    # ----------------------------------------------------- 快照与崩溃恢复
    def maybe_snapshot(self, interval: int) -> None:
        if interval <= 0 or self._since_snapshot < interval:
            return
        self.take_snapshot()

    def take_snapshot(self) -> None:
        if self.snapshot_store is None:
            return
        with self.lock:
            st = self.state
            payload = {
                "schema": SCHEMA_VERSION,
                "gateways": sorted(st.gateways),
                "progress": st.progress,
                "readings": st.readings,
                "windows": {
                    str(k): {
                        "sealed": v["sealed"],
                        "totals": v["totals"],
                        "contributions": v["contributions"],
                    }
                    for k, v in st.windows.items()
                },
                "next_seq": len(self.batches) + 1,
                "committed_offset": st.committed_offset,
                "min_meter_ts": st.min_meter_ts,
            }
            self.snapshot_store.write(payload)
            self._since_snapshot = 0

    def restore(self) -> None:
        """从快照 + 批次日志重建已提交状态。

        批次日志先经 :func:`recover_committed_batches` 裁掉强杀留下的未完成
        提交组；快照只可能早于日志，重复回放部分按序号天然跳过。
        """
        store = self.snapshot_store
        if store is None:
            return
        snap = store.read()
        import os

        jpath = os.path.join(store.directory, BATCHES_NAME)
        replayed_batches = recover_committed_batches(jpath)
        if snap is None and not replayed_batches:
            return

        st = LedgerState.empty()
        if snap is not None:
            if snap.get("schema") != SCHEMA_VERSION:
                raise ValueError(f"不支持的快照 schema: {snap.get('schema')!r}")
            st.gateways = set(snap["gateways"])
            st.progress = dict(snap["progress"])
            st.readings = {k: dict(v) for k, v in snap["readings"].items()}
            st.windows = {
                int(k): {
                    "sealed": v["sealed"],
                    "totals": dict(v["totals"]),
                    "contributions": {rid: dict(c) for rid, c in v["contributions"].items()},
                }
                for k, v in snap["windows"].items()
            }
            st.committed_offset = snap["committed_offset"]
            st.min_meter_ts = snap.get("min_meter_ts")
            snap_seq = snap["next_seq"] - 1
        else:
            snap_seq = 0

        for batch in replayed_batches:
            if batch["seq"] <= snap_seq:
                continue  # 快照已包含
            self._replay_batch(st, batch)
        # 日志永远是权威：内存批次列表与日志保持完全一致（去掉组标记不影响重放）
        self.batches = list(replayed_batches)
        self.state = st
        self._pending = None
        if replayed_batches:
            st.committed_offset = max(
                st.committed_offset,
                max(
                    b.get("source_offset", b.get("trigger_offset", -1))
                    for b in replayed_batches
                ),
            )
        # readings 是权威的当前生效版本集合，水位扫描起点直接由其重算
        if st.readings:
            st.min_meter_ts = min(r["meter_ts"] for r in st.readings.values())

    def _replay_batch(self, st: LedgerState, batch: dict) -> None:
        """把批次日志中的一条变更重放进状态（不再次发布）。"""
        kind = batch["kind"]
        if kind == "seal":
            for item in batch["sealed_windows"]:
                wstart = item["window_start"]
                win = st.windows.get(wstart)
                if win is None:
                    win = _new_window()
                    st.windows[wstart] = win
                win["sealed"] = True
                # 封账时的 totals 是权威冻结值
                win["totals"] = dict(item["totals"])
            return
        if kind != "decision":  # pragma: no cover
            raise ValueError(f"未知批次类型: {kind!r}")
        rtype = batch["record_type"]
        if rtype == "progress" and batch["status"] == "accepted":
            gw, ts_text = next(iter(batch["new_progress"].items()))
            st.gateways.add(gw)
            st.progress[gw] = parse_ts(ts_text, "progress_ts")
            return
        if batch["status"] != "accepted":
            return  # 驳回 / 坏行：恢复时无账可改
        rid = batch.get("reading_id")
        if rtype == "reading":
            ch = batch["changes"][0]
            wstart = ch["window_start"]
            win = self._window(st, wstart)
            win["totals"][ch["berth"]] = ch["total"]
            win["contributions"][rid] = {"berth": ch["berth"], "energy": ch["energy"]}
            st.readings[rid] = {
                "gateway": batch["gateway"],
                "berth": ch["berth"],
                "meter_ts": parse_ts(batch["meter_ts"], "meter_ts"),
                "energy": ch["energy"],
                "version": 1,
                "offset": batch["source_offset"],
            }
            st.gateways.add(batch["gateway"])
        elif rtype == "correction":
            current = st.readings[rid]
            for ch in batch["changes"]:
                win = self._window(st, ch["window_start"])
                if ch["change"] == "remove":
                    win["contributions"].pop(rid, None)
                    if ch["total"] == 0.0:
                        win["totals"].pop(ch["berth"], None)
                    else:
                        win["totals"][ch["berth"]] = ch["total"]
                elif ch["change"] == "add":
                    win["contributions"][rid] = {
                        "berth": ch["berth"],
                        "energy": ch["energy"],
                    }
                    win["totals"][ch["berth"]] = ch["total"]
            add_change = next(c for c in batch["changes"] if c["change"] == "add")
            current.update(
                {
                    "berth": add_change["berth"],
                    "meter_ts": parse_ts(batch["meter_ts"], "meter_ts"),
                    "energy": add_change["energy"],
                    "version": batch["version"],
                    "offset": batch["source_offset"],
                }
            )
