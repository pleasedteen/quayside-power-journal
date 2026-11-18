"""数据模型与纯函数：记录解析、UTC 刻钟窗口、版本规则。

输入日志每行一个 JSON 对象，``type`` 取值：

* ``reading``  电表网关上报的用电增量
* ``correction`` 对某笔历史计量的整笔更正（带版本号）
* ``progress`` 网关上报其已送达数据的计量时间水位

所有时间字段均为带时区的 ISO-861 字符串，内部统一转成 UTC 秒级时间戳处理。
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

WINDOW_SECONDS = 15 * 60
# 封账宽限：最慢网关水位超过窗口结束 10 分钟后封账
SEAL_GRACE_SECONDS = 10 * 60
SCHEMA_VERSION = 1


class RecordError(ValueError):
    """日志记录结构或字段非法。"""


def parse_ts(value: Any, field_name: str = "ts") -> int:
    """把 ISO-861 时间字符串解析为 UTC epoch 秒（要求带时区）。"""
    if not isinstance(value, str):
        raise RecordError(f"{field_name} 必须是 ISO-861 字符串，收到: {value!r}")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise RecordError(f"{field_name} 时间无法解析: {value!r}") from exc
    if dt.tzinfo is None:
        raise RecordError(f"{field_name} 必须带时区偏移: {value!r}")
    return int(dt.astimezone(timezone.utc).timestamp())


def format_ts(epoch_seconds: int) -> str:
    """UTC 秒级时间戳 -> 规范化 ISO 字符串（Z 结尾，便于跨机器比对）。"""
    dt = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def window_start(epoch_seconds: int) -> int:
    """计量时间所属的 UTC 刻钟窗口起点（向下取整到整 15 分钟）。"""
    return int(math.floor(epoch_seconds / WINDOW_SECONDS)) * WINDOW_SECONDS


def window_id(epoch_seconds: int) -> str:
    """刻钟窗口的稳定标识，如 ``2026-11-18T08:15Z``。"""
    return format_ts(window_start(epoch_seconds))


def seal_watermark_progress(progress_ts: int) -> int:
    """给定最慢网关水位，可被封账的最高窗口起点（水位 >= 窗口结束+10 分钟）。"""
    # 窗口 w 封账条件: progress_ts >= w + WINDOW_SECONDS + SEAL_GRACE_SECONDS
    eligible_end = progress_ts - SEAL_GRACE_SECONDS  # >= 窗口结束
    # 最大的满足 w + WINDOW_SECONDS <= eligible_end 的刻钟起点
    if eligible_end < WINDOW_SECONDS:
        return -1
    return (int(math.floor(eligible_end / WINDOW_SECONDS)) - 1) * WINDOW_SECONDS


# --------------------------------------------------------------------------- 输入记录


@dataclass(frozen=True)
class Reading:
    reading_id: str
    gateway: str
    berth: str
    meter_ts: int
    energy: float
    offset: int = -1  # 由读取线程回填：源日志行偏移，不参与业务语义


@dataclass(frozen=True)
class Correction:
    reading_id: str
    version: int
    berth: str
    meter_ts: int
    energy: float
    offset: int = -1


@dataclass(frozen=True)
class Progress:
    gateway: str
    progress_ts: int
    offset: int = -1


@dataclass(frozen=True)
class BadLine:
    """无法解析的原始行：不改动任何账，仅在批次流中留驳回痕迹。"""
    raw: str
    reason: str
    offset: int = -1


def _require_number(obj: dict, key: str) -> float:
    value = obj.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RecordError(f"字段 {key!r} 必须是数字，收到: {value!r}")
    if not math.isfinite(float(value)):
        raise RecordError(f"字段 {key!r} 必须是有限数，收到: {value!r}")
    return float(value)


def _require_str(obj: dict, key: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value:
        raise RecordError(f"字段 {key!r} 必须是非空字符串，收到: {value!r}")
    return value


def parse_line(raw_line: str, offset: int = -1) -> Reading | Correction | Progress | BadLine:
    """解析一行日志。任何结构问题都转成 BadLine，绝不抛给调用方。"""
    line = raw_line.strip()
    if not line:
        return BadLine(raw=raw_line, reason="空行", offset=offset)
    try:
        obj = json.loads(line)
    except ValueError as exc:
        return BadLine(raw=raw_line, reason=f"JSON 解析失败: {exc}", offset=offset)
    if not isinstance(obj, dict):
        return BadLine(raw=raw_line, reason="日志行必须是 JSON 对象", offset=offset)
    kind = obj.get("type")
    try:
        if kind == "reading":
            rec = Reading(
                reading_id=_require_str(obj, "reading_id"),
                gateway=_require_str(obj, "gateway"),
                berth=_require_str(obj, "berth"),
                meter_ts=parse_ts(_require_str(obj, "meter_ts"), "meter_ts"),
                energy=_require_number(obj, "energy"),
            )
        elif kind == "correction":
            version = obj.get("version")
            if isinstance(version, bool) or not isinstance(version, int) or version < 1:
                raise RecordError(f"version 必须是 >=1 的整数，收到: {version!r}")
            rec = Correction(
                reading_id=_require_str(obj, "reading_id"),
                version=version,
                berth=_require_str(obj, "berth"),
                meter_ts=parse_ts(_require_str(obj, "meter_ts"), "meter_ts"),
                energy=_require_number(obj, "energy"),
            )
        elif kind == "progress":
            rec = Progress(
                gateway=_require_str(obj, "gateway"),
                progress_ts=parse_ts(_require_str(obj, "progress_ts"), "progress_ts"),
            )
        else:
            raise RecordError(f"未知 type: {kind!r}")
    except RecordError as exc:
        return BadLine(raw=raw_line, reason=str(exc), offset=offset)
    # 冻结 dataclass 的 offset 回填
    return replace(rec, offset=offset)
