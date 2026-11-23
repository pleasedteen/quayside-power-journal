"""账务引擎：刻钟窗口归账、版本化整笔更正、进度水位封账、确定性批次发布。

线程模型：只有服务线程调用 :meth:`Engine.process` 写状态；HTTP 查询线程通过
同一把可重入锁读取，保证查询看到的是已持久化的一致状态。
"""

from __future__ import annotations

import threading

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
from .storage import BATCHES_NAME, SnapshotStore, load_persisted_batches


def _new_window() -> dict:
    # totals: 泊位 -> 当前电量；contributions: reading_id -> {berth, energy}
    return {"sealed": False, "totals": {}, "contributions": {}}


class Engine:
    def __init__(self, snapshot_store: SnapshotStore | None = None):
        self.lock = threading.RLock()
        self.snapshot_store = snapshot_store
        self.gateways: set[str] = set()
        self.progress: dict[str, int] = {}
        # reading_id -> 当前生效版本 {gateway, berth, meter_ts, energy, version, offset}
        self.readings: dict[str, dict] = {}
        # window_start -> {sealed, totals{berth: energy}, contributions{rid: {berth, energy}}}
        self.windows: dict[int, dict] = {}
        self.batches: list[dict] = []
        self.committed_offset = -1
        self.min_meter_ts: int | None = None
        self._since_snapshot = 0
        self.restore()

    @classmethod
    def from_persisted_batches(cls, batches: list[dict]) -> "Engine":
        """只按给定的已持久化批次列表重建内存视图，不依赖快照、也不重读日志。

        离线导出用它保证“窗口表”与“批次表”来自同一个批次边界：``batches`` 通常由
        :func:`shorepower.storage.load_persisted_batches` 一次性读出，读取期间日志
        继续追加的新批次不会进入这里。
        """
        engine = cls.__new__(cls)
        engine.lock = threading.RLock()
        engine.snapshot_store = None
        engine.gateways = set()
        engine.progress = {}
        engine.readings = {}
        engine.windows = {}
        engine.committed_offset = -1
        engine.min_meter_ts = None
        engine._since_snapshot = 0
        engine._apply_batches(batches)
        return engine

    # ------------------------------------------------------------- 批次工具
    def _next_seq(self) -> int:
        return len(self.batches) + 1

    def _publish(self, batch: dict) -> dict:
        batch["seq"] = self._next_seq()
        batch.setdefault("schema", SCHEMA_VERSION)
        self.batches.append(batch)
        return batch

    def _window(self, start: int) -> dict:
        win = self.windows.get(start)
        if win is None:
            win = _new_window()
            self.windows[start] = win
        return win

    # ------------------------------------------------------------- 主入口
    def process(self, rec) -> list[dict]:
        """处理一条已解析记录，返回本次发布的批次（0~2 个，已按发布顺序排列）。

        调用方（服务线程）必须按返回顺序把批次 fsync 进批次日志，再处理下一条。
        """
        with self.lock:
            if isinstance(rec, BadLine):
                batches = [self._decision_bad(rec)]
            elif isinstance(rec, Reading):
                batches = [self._process_reading(rec)]
            elif isinstance(rec, Correction):
                batches = [self._process_correction(rec)]
            elif isinstance(rec, Progress):
                batches = [self._process_progress(rec)]
            else:  # pragma: no cover - 防御
                raise TypeError(f"未知记录类型: {type(rec)!r}")
            self.committed_offset = rec.offset
            # 进度记录可能推动封账；其它记录只改账不动水位，封账检查统一做也无害
            seal_batch = self._seal_due_windows(rec.offset)
            if seal_batch is not None:
                batches.append(seal_batch)
            self._since_snapshot += 1
            return batches

    # ------------------------------------------------------------- reading
    def _process_reading(self, rec: Reading) -> dict:
        base = {
            "kind": "decision",
            "source_offset": rec.offset,
            "record_type": "reading",
            "reading_id": rec.reading_id,
            "gateway": rec.gateway,
        }
        if rec.reading_id in self.readings:
            return self._publish(
                base
                | {
                    "status": "rejected",
                    "reason": "reading_id 已存在，重复读数不允许入账（修改请发 correction）",
                    "changes": [],
                }
            )
        wstart = window_start(rec.meter_ts)
        if self.windows.get(wstart, {}).get("sealed"):
            return self._publish(
                base
                | {
                    "status": "rejected",
                    "reason": f"窗口 {format_ts(wstart)} 已封账，迟到读数不得入账",
                    "changes": [],
                }
            )
        self.gateways.add(rec.gateway)
        self.min_meter_ts = (
            rec.meter_ts
            if self.min_meter_ts is None
            else min(self.min_meter_ts, rec.meter_ts)
        )
        win = self._window(wstart)
        win["totals"][rec.berth] = win["totals"].get(rec.berth, 0.0) + rec.energy
        win["contributions"][rec.reading_id] = {"berth": rec.berth, "energy": rec.energy}
        self.readings[rec.reading_id] = {
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
        return self._publish(
            base
            | {
                "status": "accepted",
                "meter_ts": format_ts(rec.meter_ts),
                "changes": [change],
            }
        )

    # ------------------------------------------------------------- correction
    def _process_correction(self, rec: Correction) -> dict:
        base = {
            "kind": "decision",
            "source_offset": rec.offset,
            "record_type": "correction",
            "reading_id": rec.reading_id,
            "version": rec.version,
        }
        current = self.readings.get(rec.reading_id)
        if current is None:
            return self._publish(
                base
                | {
                    "status": "rejected",
                    "reason": "目标 reading_id 不存在，无法更正",
                    "changes": [],
                }
            )
        if rec.version <= current["version"]:
            return self._publish(
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
        old_start = window_start(current["meter_ts"])
        new_start = window_start(rec.meter_ts)
        blocked = []
        for wstart in {old_start, new_start}:
            if self.windows.get(wstart, {}).get("sealed"):
                blocked.append(format_ts(wstart))
        if blocked:
            return self._publish(
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

        # ---- 整笔事务：下面的改动要么全部完成，要么前面已全部校验通过，不会半途失败
        changes: list[dict] = []
        # 1) 从原窗口/原泊位移除
        old_win = self._window(old_start)
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
        new_win = self._window(new_start)
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
        self.min_meter_ts = (
            rec.meter_ts
            if self.min_meter_ts is None
            else min(self.min_meter_ts, rec.meter_ts)
        )
        # 确定性顺序：窗口起点、泊位、变更类型（remove 在 add 前）
        order = {"remove": 0, "add": 1, "set": 2}
        changes.sort(key=lambda c: (c["window_start"], c["berth"], order[c["change"]]))
        return self._publish(
            base
            | {
                "status": "accepted",
                "meter_ts": format_ts(rec.meter_ts),
                "changes": changes,
            }
        )

    # ------------------------------------------------------------- progress
    def _process_progress(self, rec: Progress) -> dict:
        base = {
            "kind": "decision",
            "source_offset": rec.offset,
            "record_type": "progress",
            "gateway": rec.gateway,
            "changes": [],
        }
        prev = self.progress.get(rec.gateway)
        if prev is not None and rec.progress_ts < prev:
            return self._publish(
                base
                | {
                    "status": "rejected",
                    "reason": (
                        f"进度水位不能回退：当前 {format_ts(prev)}，"
                        f"上报 {format_ts(rec.progress_ts)}"
                    ),
                }
            )
        self.gateways.add(rec.gateway)
        self.progress[rec.gateway] = rec.progress_ts
        return self._publish(
            base
            | {
                "status": "accepted",
                "new_progress": {rec.gateway: format_ts(rec.progress_ts)},
            }
        )

    # ------------------------------------------------------------- bad line
    def _decision_bad(self, rec: BadLine) -> dict:
        return self._publish(
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
    def _all_gateways_reported(self) -> bool:
        return bool(self.gateways) and self.gateways.issubset(self.progress.keys())

    def _seal_due_windows(self, trigger_offset: int) -> dict | None:
        if not self._all_gateways_reported() or self.min_meter_ts is None:
            return None
        slowest = min(self.progress.values())
        top = seal_watermark_progress(slowest)
        if top < 0:
            return None
        first = window_start(self.min_meter_ts)
        sealed_now: list[dict] = []
        wstart = first
        while wstart <= top:
            win = self.windows.get(wstart)
            if win is None:
                win = _new_window()
                self.windows[wstart] = win
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
            return None
        return self._publish(
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
            slowest = min(self.progress.values()) if self.progress else None
            result = {
                "seal_grace_seconds": SEAL_GRACE_SECONDS,
                "slowest_progress": format_ts(slowest) if slowest is not None else None,
                "all_gateways_reported": self._all_gateways_reported(),
                "windows": [],
            }
            if window_ids:
                starts = []
                for wid in window_ids:
                    try:
                        starts.append(parse_ts(wid, "window"))
                    except Exception:
                        starts.append(None)
                items = [(s, self.windows.get(s) if s is not None else None) for s in starts]
            else:
                items = sorted(self.windows.items())
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
            sealed = sum(1 for w in self.windows.values() if w["sealed"])
            return {
                "published_batches": len(self.batches),
                "last_seq": self.batches[-1]["seq"] if self.batches else 0,
                "committed_source_offset": self.committed_offset,
                "inflight_unpersisted": inflight,
                "known_gateways": sorted(self.gateways),
                "progress": {g: format_ts(ts) for g, ts in sorted(self.progress.items())},
                "tracked_readings": len(self.readings),
                "windows_total": len(self.windows),
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
            payload = {
                "schema": SCHEMA_VERSION,
                "gateways": sorted(self.gateways),
                "progress": self.progress,
                "readings": self.readings,
                "windows": {
                    str(k): {
                        "sealed": v["sealed"],
                        "totals": v["totals"],
                        "contributions": v["contributions"],
                    }
                    for k, v in self.windows.items()
                },
                "next_seq": len(self.batches) + 1,
                "committed_offset": self.committed_offset,
                "min_meter_ts": self.min_meter_ts,
            }
            self.snapshot_store.write(payload)
            self._since_snapshot = 0

    def _apply_batches(self, batches: list[dict], replay_only: bool = False) -> None:
        """把一组已持久化批次按序应用到内存（恢复与导出共用同一条重放路径）。

        ``replay_only=True``：只重放账务效果，不重置批次列表/提交偏移（快照恢复路径，
        快照之前的批次已在内存里）。``False``：纯批次重建，批次列表与派生统计同时
        对齐到这组批次（离线导出/无快照恢复路径）。
        """
        with self.lock:
            for batch in batches:
                self._replay_batch(batch)
            if replay_only:
                return
            self.batches = list(batches)
            if batches:
                self.committed_offset = max(
                    b.get("source_offset", b.get("trigger_offset", -1))
                    for b in batches
                )
            if self.readings:
                self.min_meter_ts = min(r["meter_ts"] for r in self.readings.values())

    def restore(self) -> None:
        """从快照 + 批次日志重建状态。快照只可能早于日志，重复回放部分天然幂等跳过。"""
        store = self.snapshot_store
        if store is None:
            return
        snap = store.read()
        # 批次日志与快照同目录；与离线导出共用同一个已持久化批次读取口径
        import os

        jpath = os.path.join(store.directory, BATCHES_NAME)
        persisted = load_persisted_batches(jpath)
        if snap is None and not persisted:
            return

        if snap is not None:
            if snap.get("schema") != SCHEMA_VERSION:
                raise ValueError(f"不支持的快照 schema: {snap.get('schema')!r}")
            self.gateways = set(snap["gateways"])
            self.progress = dict(snap["progress"])
            self.readings = {k: dict(v) for k, v in snap["readings"].items()}
            self.windows = {
                int(k): {
                    "sealed": v["sealed"],
                    "totals": dict(v["totals"]),
                    "contributions": {rid: dict(c) for rid, c in v["contributions"].items()},
                }
                for k, v in snap["windows"].items()
            }
            self.committed_offset = snap["committed_offset"]
            self.min_meter_ts = snap.get("min_meter_ts")
            snap_seq = snap["next_seq"] - 1
        else:
            snap_seq = 0

        fresh = [batch for batch in persisted if batch["seq"] > snap_seq]
        self._apply_batches(fresh, replay_only=True)
        # 日志永远是权威：内存批次列表与日志保持完全一致
        self.batches = list(persisted)
        if persisted:
            self.committed_offset = max(
                self.committed_offset,
                max(
                    b.get("source_offset", b.get("trigger_offset", -1))
                    for b in persisted
                ),
            )
        # readings 是权威的当前生效版本集合，水位扫描起点直接由其重算
        if self.readings:
            self.min_meter_ts = min(r["meter_ts"] for r in self.readings.values())

    def _replay_batch(self, batch: dict) -> None:
        """把批次日志中的一条变更重放进内存（不再次发布）。"""
        kind = batch["kind"]
        if kind == "seal":
            for item in batch["sealed_windows"]:
                wstart = item["window_start"]
                win = self.windows.get(wstart)
                if win is None:
                    win = _new_window()
                    self.windows[wstart] = win
                win["sealed"] = True
                # 封账时的 totals 是权威冻结值
                win["totals"] = dict(item["totals"])
            return
        if kind != "decision":  # pragma: no cover
            raise ValueError(f"未知批次类型: {kind!r}")
        rtype = batch["record_type"]
        if rtype == "progress" and batch["status"] == "accepted":
            gw, ts_text = next(iter(batch["new_progress"].items()))
            self.gateways.add(gw)
            self.progress[gw] = parse_ts(ts_text, "progress_ts")
            return
        if batch["status"] != "accepted":
            return  # 驳回 / 坏行：恢复时无账可改
        rid = batch.get("reading_id")
        if rtype == "reading":
            ch = batch["changes"][0]
            wstart = ch["window_start"]
            win = self._window(wstart)
            win["totals"][ch["berth"]] = ch["total"]
            win["contributions"][rid] = {"berth": ch["berth"], "energy": ch["energy"]}
            self.readings[rid] = {
                "gateway": batch["gateway"],
                "berth": ch["berth"],
                "meter_ts": parse_ts(batch["meter_ts"], "meter_ts"),
                "energy": ch["energy"],
                "version": 1,
                "offset": batch["source_offset"],
            }
            self.gateways.add(batch["gateway"])
        elif rtype == "correction":
            current = self.readings[rid]
            for ch in batch["changes"]:
                win = self._window(ch["window_start"])
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
