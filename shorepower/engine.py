"""账务引擎：刻钟窗口归账、版本化整笔更正、进度水位封账、确定性批次发布。

线程模型：只有服务线程调用 :meth:`Engine.process` 写状态；HTTP 查询线程通过
同一把可重入锁读取，保证查询看到的是已持久化的一致状态。
"""

from __future__ import annotations

import threading

from .model import (
    SCHEMA_VERSION,
    SNAPSHOT_SCHEMA_VERSION,
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
from .storage import HISTORY_NAME, BatchJournal, HistoryStore, SnapshotStore


def _new_window() -> dict:
    # totals: 泊位 -> 当前电量；contributions: reading_id -> {berth, energy}
    return {"sealed": False, "totals": {}, "contributions": {}}


class Engine:
    def __init__(
        self,
        journal: BatchJournal | None = None,
        snapshot_store: SnapshotStore | None = None,
        history: HistoryStore | None = None,
        auto_snapshot: bool = False,
    ):
        self.lock = threading.RLock()
        self.journal = journal
        self.snapshot_store = snapshot_store
        # 已结算历史的磁盘归档：封账窗口与其中的读数可以离开内存
        self.history = history
        # 是否允许在恢复阶段（旧目录升级 / 首次启动）主动写一份新快照
        self.auto_snapshot = auto_snapshot
        self.gateways: set[str] = set()
        self.progress: dict[str, int] = {}
        # reading_id -> 当前生效版本 {gateway, berth, meter_ts, energy, version, offset}
        # 只保留未封账窗口内的读数；已封账的折叠进 history
        self.readings: dict[str, dict] = {}
        # window_start -> {sealed, totals{berth: energy}, contributions{rid: {berth, energy}}}
        # 只保留未归档窗口；已归档的封账窗口只存在 history 里
        self.windows: dict[int, dict] = {}
        # 已发布批次的序号。批次正文不驻留内存：按序号查询时直接走磁盘索引。
        self._seq = 0
        # 已封账窗口构成连续区间 [sealed_lo, sealed_hi]（含已归档的），
        # 用它把封账扫描的起点固定在“尚未封账”的位置，扫描成本只与新增窗口有关
        self.sealed_lo: int | None = None
        self.sealed_hi: int | None = None
        self.committed_offset = -1
        self.min_meter_ts: int | None = None
        self._since_snapshot = 0
        self.snapshot_journal_offset = 0
        self.restore()

    # ------------------------------------------------------------- 批次工具
    def _next_seq(self) -> int:
        return self._seq + 1

    def _publish(self, batch: dict) -> dict:
        batch["seq"] = self._next_seq()
        batch.setdefault("schema", SCHEMA_VERSION)
        self._seq += 1
        return batch

    def _window(self, start: int) -> dict:
        win = self.windows.get(start)
        if win is None:
            win = _new_window()
            self.windows[start] = win
        return win

    # ------------------------------------------------- 活动状态 / 归档历史判定
    def _is_sealed(self, wstart: int) -> bool:
        """窗口是否已封账。

        封账窗口永远构成连续区间 [sealed_lo, sealed_hi]（每次封账从最早计量窗口
        连续推进），区间与内存窗口共同覆盖全部已封账窗口，因此这里不需要查归档。
        """
        win = self.windows.get(wstart)
        if win is not None:
            return win["sealed"]
        return self.sealed_lo is not None and self.sealed_lo <= wstart <= self.sealed_hi

    def _windows_from(self, start: int) -> list[tuple[int, dict | None]]:
        """按序返回起点 >= start 的已知窗口：内存优先，其余取自归档。"""
        merged: dict[int, dict | None] = {}
        if self.history is not None:
            for wstart, totals in self.history.iter_sealed():
                if wstart >= start:
                    merged[wstart] = {"sealed": True, "totals": totals}
        for wstart, win in self.windows.items():
            if wstart >= start:
                merged[wstart] = win
        return sorted(merged.items())

    def _reading_exists(self, reading_id: str) -> bool:
        if reading_id in self.readings:
            return True
        # 已结算读数的完整字段在归档里只保留版本/窗口，但 reading_id 的
        # 唯一性必须对全部历史成立（重复读数无论落在哪个窗口都要驳回）
        return self.history is not None and self.history.reading_record(reading_id) is not None

    def _archived_version(self, reading_id: str) -> int | None:
        record = self._archived_record(reading_id)
        return None if record is None else record[0]

    def _archived_record(self, reading_id: str) -> tuple[int, int] | None:
        if self.history is None:
            return None
        return self.history.reading_record(reading_id)

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
        if self._reading_exists(rec.reading_id):
            return self._publish(
                base
                | {
                    "status": "rejected",
                    "reason": "reading_id 已存在，重复读数不允许入账（修改请发 correction）",
                    "changes": [],
                }
            )
        wstart = window_start(rec.meter_ts)
        if self._is_sealed(wstart):
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
        archived = None if current is not None else self._archived_record(rec.reading_id)
        if current is None and archived is None:
            return self._publish(
                base
                | {
                    "status": "rejected",
                    "reason": "目标 reading_id 不存在，无法更正",
                    "changes": [],
                }
            )
        # 版本判定优先于封账判定（与未归档时同序），文案保持一致
        effective_version = current["version"] if current is not None else archived[0]
        if rec.version <= effective_version:
            return self._publish(
                base
                | {
                    "status": "rejected",
                    "reason": (
                        f"版本 {rec.version} 不高于当前生效版本 {effective_version}，"
                        "更正驳回且原账不变"
                    ),
                    "changes": [],
                }
            )
        if current is None:
            old_start = archived[1]
        else:
            old_start = window_start(current["meter_ts"])
        new_start = window_start(rec.meter_ts)
        blocked = []
        for wstart in {old_start, new_start}:
            if self._is_sealed(wstart):
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
        # 从最早计量时间所在的窗口向上扫描；已知封账区间 [sealed_lo, sealed_hi]
        # 整体跳过，因此每次调用的成本只与“新推进的窗口数”有关，与历史长度无关。
        first = window_start(self.min_meter_ts)
        sealed_now: list[dict] = []
        wstart = first
        while wstart <= top:
            if (
                self.sealed_lo is not None
                and self.sealed_lo <= wstart <= self.sealed_hi
            ):
                wstart = self.sealed_hi + WINDOW_SECONDS
                continue
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
        lo = sealed_now[0]["window_start"]
        hi = sealed_now[-1]["window_start"]
        self.sealed_lo = lo if self.sealed_lo is None else min(self.sealed_lo, lo)
        self.sealed_hi = hi if self.sealed_hi is None else max(self.sealed_hi, hi)
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
                # 全部窗口 = 归档里的封账窗口 + 内存中的活动窗口（顺序合并）
                items = self._windows_from(-1)
            for wstart, win in items:
                if wstart is None:
                    result["windows"].append(
                        {"window": None, "error": "窗口标识无法解析为 UTC 时间"}
                    )
                    continue
                if win is None:
                    # 仅“显式查询某个窗口”会走到这里：可能是归档里的封账窗口
                    archived = (
                        self.history.sealed_totals([wstart]) if self.history is not None else {}
                    )
                    if wstart in archived:
                        totals = dict(sorted(archived[wstart].items()))
                        result["windows"].append(
                            {
                                "window": format_ts(wstart),
                                "window_start": wstart,
                                "sealed": True,
                                "totals": totals,
                                "total_energy": sum(totals.values()),
                            }
                        )
                    elif self._is_sealed(wstart):
                        # 已封账但不在归档里的空窗口（例如封账时还没有任何读数）
                        result["windows"].append(
                            {
                                "window": format_ts(wstart),
                                "window_start": wstart,
                                "sealed": True,
                                "totals": {},
                                "total_energy": 0.0,
                            }
                        )
                    else:
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
        # 只读磁盘索引定位 + 本次请求命中的正文行，内存开销与历史长度无关。
        # last_seq 取已落盘批次数，保证客户端拿着它能续拉、不会跳过尚未 fsync 的批次。
        if self.journal is not None:
            with self.lock:
                durable = self.journal.count
                picked = self.journal.read_page(after_seq, limit)
        else:
            durable, picked = 0, []
        return {
            "after_seq": after_seq,
            "last_seq": durable,
            "batches": picked,
        }

    def stats(self, inflight: int = 0) -> dict:
        with self.lock:
            sealed_active = sum(1 for w in self.windows.values() if w["sealed"])
            archived = self.history.sealed_window_count() if self.history is not None else 0
            durable = self.journal.count if self.journal is not None else self._seq
            result = {
                "published_batches": durable,
                "last_seq": durable,
                "committed_source_offset": self.committed_offset,
                "inflight_unpersisted": inflight,
                "known_gateways": sorted(self.gateways),
                "progress": {g: format_ts(ts) for g, ts in sorted(self.progress.items())},
                "tracked_readings": len(self.readings),
                "windows_total": len(self.windows) + archived,
                "windows_active_in_memory": len(self.windows),
                "windows_archived": archived,
                "windows_sealed": sealed_active + archived,
            }
            if self.journal is not None:
                # 观测值：本进程从批次日志读取的正文条数/字节数
                result["journal_bodies_read"] = self.journal.bodies_read
                result["journal_bytes_read"] = self.journal.bytes_read
                result["journal_index_rebuilt"] = self.journal.index_rebuilt
            return result

    # ----------------------------------------------------- 快照与崩溃恢复
    def maybe_snapshot(self, interval: int) -> None:
        if interval <= 0 or self._since_snapshot < interval:
            return
        self.take_snapshot()

    def take_snapshot(self) -> None:
        if self.snapshot_store is None:
            return
        with self.lock:
            # 先把已结算历史折叠到磁盘（幂等），再写只含活动状态的快照。
            # 若在两者之间被强杀，重启时会重新折叠一次，结果相同。
            self._fold_history()
            # 快照只覆盖已经 fsync 的批次正文：偏移与条数都取自批次日志本身
            if self.journal is not None:
                journal_offset = self.journal.end_offset
                journal_seq = self.journal.count
            else:
                journal_offset = 0
                journal_seq = self._seq
            payload = {
                "schema": SNAPSHOT_SCHEMA_VERSION,
                "journal_offset": journal_offset,
                "journal_seq": journal_seq,
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
                "next_seq": journal_seq + 1,
                "committed_offset": self.committed_offset,
                "min_meter_ts": self.min_meter_ts,
                "sealed_lo": self.sealed_lo,
                "sealed_hi": self.sealed_hi,
                # 快照里的 windows 只含活动窗口；已结算窗口在 history 归档里
                "history_archived_windows": (
                    self.history.sealed_window_count() if self.history is not None else 0
                ),
            }
            self.snapshot_store.write(payload)
            self.snapshot_journal_offset = journal_offset
            self._since_snapshot = 0

    def _fold_history(self) -> None:
        """把已封账窗口及其读数从内存折叠进历史归档（不在内存里留历史）。"""
        if self.history is None or not self.windows:
            return
        sealed_totals: dict[int, dict[str, float]] = {}
        version_map: dict[str, tuple[int, int]] = {}
        for wstart, win in self.windows.items():
            if not win["sealed"]:
                continue
            sealed_totals[wstart] = dict(win["totals"])
            for rid in win["contributions"]:
                record = self.readings.pop(rid, None)
                if record is not None:
                    version_map[rid] = (record["version"], wstart)
        if not sealed_totals:
            return
        # 兜底：读数所在窗口已归档但 contributions 已不在内存窗口里
        for rid, record in list(self.readings.items()):
            wstart = window_start(record["meter_ts"])
            if wstart in sealed_totals:
                version_map[rid] = (record["version"], wstart)
                del self.readings[rid]
        self.history.archive(sealed_totals, version_map)
        for wstart in sealed_totals:
            del self.windows[wstart]


    def restore(self) -> None:
        """从快照 + 快照之后的批次日志重建状态。

        快照记录 ``journal_offset``（已 fsync 的批次日志字节偏移），因此重启只读取
        该偏移之后的正文；历史再长也不影响恢复内存与耗时。旧版快照（schema 1）
        没有该字段，读取一次全量日志后立即写出新版快照完成升级。
        """
        store = self.snapshot_store
        journal = self.journal
        if store is None and journal is None:
            return
        snap = store.read() if store is not None else None
        legacy = False
        snap_seq = 0
        self.snapshot_journal_offset = 0
        if snap is not None:
            schema = snap.get("schema")
            if schema not in (SCHEMA_VERSION, SNAPSHOT_SCHEMA_VERSION):
                raise ValueError(f"不支持的快照 schema: {schema!r}")
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
            snap_seq = snap.get("journal_seq", snap["next_seq"] - 1)
            self.sealed_lo = snap.get("sealed_lo")
            self.sealed_hi = snap.get("sealed_hi")
            archived_windows = snap.get("history_archived_windows", 0)
            if archived_windows and self.history is None:
                # 快照把历史折叠进了 history 归档，没有归档就无法还原封账窗口
                raise ValueError(
                    f"快照包含 {archived_windows} 个已归档窗口，"
                    "但未提供 HistoryStore，无法完整恢复状态"
                )
            if (
                self.auto_snapshot
                and schema == SNAPSHOT_SCHEMA_VERSION
                and self.history is not None
            ):
                self._check_history_consistent(archived_windows)
            if schema == SNAPSHOT_SCHEMA_VERSION:
                self.snapshot_journal_offset = snap.get("journal_offset", 0)
            else:
                # 旧状态目录：旧版快照没有 journal_offset，只能从日志开头重放一次
                legacy = True

        if journal is not None:
            if journal.count < snap_seq:
                raise ValueError(
                    f"批次日志只有 {journal.count} 条，少于快照记录的 {snap_seq} 条，"
                    "状态目录不完整，请同时恢复 batches.jsonl 与 snapshot.json"
                )
            start = 0 if legacy else self.snapshot_journal_offset
            if start > journal.end_offset:
                raise ValueError(
                    f"快照记录的日志偏移 {start} 超过批次日志长度 {journal.end_offset}，"
                    "状态目录与批次日志不匹配"
                )
            # 旧版快照没有 journal_offset：从日志开头重放一次；重放会覆盖
            # 快照里同一读数的条目，且封账批次会重新把这些窗口标为已封账
            for batch in journal.iter_from(start):
                if batch["seq"] <= snap_seq:
                    continue  # 快照已包含（快照与正文之间无空洞时不会发生）
                self._replay_batch(batch)
                self.committed_offset = max(
                    self.committed_offset,
                    batch.get("source_offset", batch.get("trigger_offset", -1)),
                )
            # 批次日志是权威：内存序号与日志行数保持一致
            self._seq = journal.count
        else:
            self._seq = snap_seq

        # readings 是权威的当前生效版本集合；min_meter_ts 取“历史最早计量时间”，
        # 只会随新读数变小。已归档读数不再在内存里，因此这里取快照旧值与活动读数的
        # 较小者，避免把封账扫描起点抬高（那会漏掉活动窗口之前的空窗口）。
        if self.readings:
            earliest_active = min(r["meter_ts"] for r in self.readings.values())
            self.min_meter_ts = (
                earliest_active
                if self.min_meter_ts is None
                else min(self.min_meter_ts, earliest_active)
            )

        if store is not None and self.auto_snapshot and (legacy or snap is None):
            # 升级/首次启动：立即落一份带 journal_offset 的新快照，
            # 之后的重启只读该偏移之后的正文（旧目录同样只需付一次全量扫描）
            self.take_snapshot()
        if self.auto_snapshot:
            # 无论是否写快照，恢复过程本身也要把已结算历史折叠出去，
            # 保证重启后的常驻内存只与活动状态同阶。
            # --snapshot-every 0 表示不写快照/不折叠，完全靠批次日志重放。
            self._fold_history()

    def _check_history_consistent(self, expected_windows: int) -> None:
        """校验历史归档是否覆盖快照记录的已归档窗口。

        归档在快照之前提交，因此归档不能比快照少；多出来是允许的（上次折叠后
        快照未替换就被强杀），重复折叠是幂等的。
        """
        assert self.history is not None
        actual = self.history.sealed_window_count()
        if actual < expected_windows:
            raise ValueError(
                f"历史归档只有 {actual} 个封账窗口，少于快照记录的 {expected_windows} 个，"
                f"请同时恢复 {HISTORY_NAME} 与 snapshot.json"
            )

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
            # 维护封账区间，避免重启后把“从头扫描封账窗口”的成本带进来
            if batch["sealed_windows"]:
                lo = batch["sealed_windows"][0]["window_start"]
                hi = batch["sealed_windows"][-1]["window_start"]
                self.sealed_lo = lo if self.sealed_lo is None else min(self.sealed_lo, lo)
                self.sealed_hi = hi if self.sealed_hi is None else max(self.sealed_hi, hi)
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
