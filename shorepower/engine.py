"""账务引擎：刻钟窗口归账、版本化整笔更正、进度水位封账、确定性批次发布。

线程模型：只有服务线程调用 :meth:`Engine.process` 写状态；HTTP 查询线程通过
同一把可重入锁读取，保证查询看到的是已持久化的一致状态。

封账模型（schema 2）：全网关最慢水位 >= 窗口结束 + 10 分钟时，该窗口即被封账。
引擎维护单调推进的封账水位 ``sealed_through``：所有窗口起点 <= 该水位的刻钟
一律视为已封账——包括从未有读数的缺测空窗和被更正迁空的窗口，二者不再逐窗
物化，而是由水位隐式覆盖（因此水位一次跨越大段空窗时，封账批次仍是常数大小）。
每次水位推进发布一个 seal 批次，批次内只显式列出本批新封的“有状态”窗口
（含被迁空的窗口）及其冻结电量；空窗的首次封账批次由 ``sealed_by_seq`` 按
“首个 sealed_through 覆盖该窗口的 seal 批次”推得，查询与 Parquet 导出一致。
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
    format_ts,
    parse_ts,
    seal_watermark_progress,
    window_start,
)
from .storage import SnapshotStore


def _new_window() -> dict:
    # totals: 泊位 -> 当前电量；contributions: reading_id -> {berth, energy}
    # 窗口是否封账不由窗口自身记录，统一由引擎的封账水位判定
    return {"totals": {}, "contributions": {}}


class Engine:
    def __init__(self, snapshot_store: SnapshotStore | None = None):
        self.lock = threading.RLock()
        self.snapshot_store = snapshot_store
        self.gateways: set[str] = set()
        self.progress: dict[str, int] = {}
        # reading_id -> 当前生效版本 {gateway, berth, meter_ts, energy, version, offset}
        self.readings: dict[str, dict] = {}
        # window_start -> {totals{berth: energy}, contributions{rid: {berth, energy}}}
        # 只物化“有状态”窗口；缺测空窗不物化，由封账水位隐式覆盖
        self.windows: dict[int, dict] = {}
        # 封账水位：窗口起点 <= sealed_through 的刻钟全部已封账（含空窗）；None 表示尚未封账
        self.sealed_through: int | None = None
        # 每次水位推进一条 {seq, sealed_through}，用于空窗的首封批次归因
        self.seal_history: list[dict] = []
        self.batches: list[dict] = []
        self.committed_offset = -1
        self._since_snapshot = 0
        self.restore()

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

    # ------------------------------------------------------------- 封账判定
    def is_sealed(self, wstart: int) -> bool:
        """窗口（含从未物化的空窗）是否已封账：窗口起点 <= 封账水位即已封。"""
        return self.sealed_through is not None and wstart <= self.sealed_through

    def sealed_by_seq(self, wstart: int) -> int | None:
        """首次把该窗口封账的批次序号；未封账返回 None。

        空窗不逐窗物化，其首封批次 = 首个 sealed_through 覆盖该窗口的 seal 批次；
        有状态窗口在对应批次的 sealed_windows 中显式列出，二者同批，归因一致。
        """
        if not self.is_sealed(wstart):
            return None
        for entry in self.seal_history:  # 按 seq 升序，水位单调推进
            if entry["sealed_through"] >= wstart:
                return entry["seq"]
        return None  # 防御：水位来自快照但批次日志缺失

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
        if self.is_sealed(wstart):
            return self._publish(
                base
                | {
                    "status": "rejected",
                    "reason": f"窗口 {format_ts(wstart)} 已封账，迟到读数不再补入电量",
                    "changes": [],
                }
            )
        self.gateways.add(rec.gateway)
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
            if self.is_sealed(wstart):
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
        """水位推进封账水位，必要时发布一个 seal 批次。

        封账水位从 ``sealed_from``（不含）推进到 ``sealed_through``（含）：
        区间内缺测/被迁空的空窗一并封账且不再逐窗物化，因此水位一次跨过
        任意长的无读数区间（哪怕连续一年），批次仍是常数大小；区间内
        “有状态”窗口（含被迁空窗口）显式列出冻结电量，作为月报权威值。
        """
        if not self._all_gateways_reported():
            return None
        slowest = min(self.progress.values())
        top = seal_watermark_progress(slowest)
        if top < 0:
            return None
        prev = self.sealed_through
        if prev is not None and top <= prev:
            return None
        sealed_now: list[dict] = []
        for wstart in sorted(self.windows):
            if wstart > top:
                break
            if prev is not None and wstart <= prev:
                continue  # 更早批次已封
            win = self.windows[wstart]
            sealed_now.append(
                {
                    "window": format_ts(wstart),
                    "window_start": wstart,
                    "totals": dict(sorted(win["totals"].items())),
                }
            )
        batch = self._publish(
            {
                "kind": "seal",
                "trigger_offset": trigger_offset,
                "watermark_progress": format_ts(slowest),
                "grace_seconds": SEAL_GRACE_SECONDS,
                # 上一封账水位（本批不含）；None 表示首次推进，此前全部空窗一并封账
                "sealed_from": prev,
                "sealed_through": top,
                "sealed_windows": sealed_now,
            }
        )
        self.seal_history.append({"seq": batch["seq"], "sealed_through": top})
        self.sealed_through = top
        return batch

    # ------------------------------------------------------------- 查询
    def query_windows(self, window_ids: list[str] | None = None) -> dict:
        with self.lock:
            slowest = min(self.progress.values()) if self.progress else None
            result = {
                "seal_grace_seconds": SEAL_GRACE_SECONDS,
                "slowest_progress": format_ts(slowest) if slowest is not None else None,
                "all_gateways_reported": self._all_gateways_reported(),
                # 封账水位：<= 该值的刻钟（含缺测/迁空空窗）全部已封账
                "sealed_through": (
                    format_ts(self.sealed_through)
                    if self.sealed_through is not None
                    else None
                ),
                "windows": [],
            }
            if window_ids:
                starts = []
                for wid in window_ids:
                    try:
                        starts.append(window_start(parse_ts(wid, "window")))
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
                sealed = self.is_sealed(wstart)
                entry = {
                    "window": format_ts(wstart),
                    "window_start": wstart,
                    "sealed": sealed,
                    # 首封批次序号：区分「待封账零电量」（null）与「已封账零电量」，
                    # 并与 Parquet 导出的同一字段逐窗口核对
                    "sealed_by_seq": self.sealed_by_seq(wstart) if sealed else None,
                }
                if win is None:
                    # 未物化的空窗：sealed 由水位给出，零电量同样区分两态
                    entry |= {"totals": {}, "total_energy": 0.0}
                else:
                    totals = dict(sorted(win["totals"].items()))
                    entry |= {"totals": totals, "total_energy": sum(totals.values())}
                result["windows"].append(entry)
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
            sealed = sum(1 for w in self.windows if self.is_sealed(w))
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
                "sealed_through": (
                    format_ts(self.sealed_through)
                    if self.sealed_through is not None
                    else None
                ),
                "seal_batches": len(self.seal_history),
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
                        "totals": v["totals"],
                        "contributions": v["contributions"],
                    }
                    for k, v in self.windows.items()
                },
                "sealed_through": self.sealed_through,
                "seal_history": self.seal_history,
                "next_seq": len(self.batches) + 1,
                "committed_offset": self.committed_offset,
            }
            self.snapshot_store.write(payload)
            self._since_snapshot = 0

    def restore(self) -> None:
        """从快照 + 批次日志重建状态。快照只可能早于日志，重复回放部分天然幂等跳过。

        兼容 schema 1：旧快照的逐窗 sealed 标志折算为封账水位（旧逻辑封账区间
        连续，最大已封窗口起点即水位）；旧格式 seal 批次（无 sealed_through）
        以其列表中最大窗口起点为水位。无论新旧，封账后的冻结电量都以批次
        日志中的权威值重放，旧批次内容不被改写。
        """
        store = self.snapshot_store
        if store is None:
            return
        snap = store.read()
        # 批次日志与快照同目录
        from .storage import BATCHES_NAME, iter_jsonl
        import os

        jpath = os.path.join(store.directory, BATCHES_NAME)
        replayed_batches = list(iter_jsonl(jpath)) if os.path.exists(jpath) else []
        if snap is None and not replayed_batches:
            return

        if snap is not None:
            schema = snap.get("schema")
            if schema not in (1, SCHEMA_VERSION):
                raise ValueError(f"不支持的快照 schema: {schema!r}")
            self.gateways = set(snap["gateways"])
            self.progress = dict(snap["progress"])
            self.readings = {k: dict(v) for k, v in snap["readings"].items()}
            self.windows = {
                int(k): {
                    "totals": dict(v["totals"]),
                    "contributions": {rid: dict(c) for rid, c in v["contributions"].items()},
                }
                for k, v in snap["windows"].items()
            }
            self.committed_offset = snap["committed_offset"]
            if schema >= 2:
                self.sealed_through = snap.get("sealed_through")
                self.seal_history = [dict(h) for h in snap.get("seal_history", [])]
            else:
                # v1 快照：逐窗 sealed 标志折算为封账水位
                sealed_starts = [
                    int(k) for k, v in snap["windows"].items() if v.get("sealed")
                ]
                self.sealed_through = max(sealed_starts) if sealed_starts else None
                self.seal_history = []
            snap_seq = snap["next_seq"] - 1
        else:
            snap_seq = 0

        # 封账水位与首封批次归因：批次日志是权威来源，全量扫描重建
        history: list[dict] = []
        for batch in replayed_batches:
            if batch.get("kind") != "seal":
                continue
            through = batch.get("sealed_through")
            if through is None:  # v1 批次：封到列表中最大窗口起点
                through = max(
                    (item["window_start"] for item in batch.get("sealed_windows", [])),
                    default=None,
                )
            if through is not None:
                history.append({"seq": batch["seq"], "sealed_through": through})
        if history:  # 日志按 seq 升序，水位单调，末条即当前水位
            self.seal_history = history
            self.sealed_through = history[-1]["sealed_through"]

        for batch in replayed_batches:
            if batch["seq"] <= snap_seq:
                continue  # 快照已包含
            self._replay_batch(batch)
        # 日志永远是权威：内存批次列表与日志保持完全一致
        self.batches = list(replayed_batches)
        if replayed_batches:
            self.committed_offset = max(
                self.committed_offset,
                max(
                    b.get("source_offset", b.get("trigger_offset", -1))
                    for b in replayed_batches
                ),
            )

    def _replay_batch(self, batch: dict) -> None:
        """把批次日志中的一条变更重放进内存（不再次发布）。"""
        kind = batch["kind"]
        if kind == "seal":
            # 新旧格式都只显式列出“有状态”窗口；空窗由水位隐式覆盖，
            # 水位与归因历史已在 restore 中从全量日志重建，这里只落冻结电量
            for item in batch.get("sealed_windows", []):
                wstart = item["window_start"]
                win = self.windows.get(wstart)
                if win is None:
                    win = _new_window()
                    self.windows[wstart] = win
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
