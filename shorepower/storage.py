"""本地持久化：追加式 JSON Lines 批次日志、原子状态快照、源日志跟读。

目录布局（均在用户指定的 state 目录下）：

* ``batches.jsonl``   已发布批次（decision / seal），追加写，重启后仍是精确一次的事实来源
* ``snapshot.json``   内存状态的原子快照（tmp + os.replace），仅用于加速恢复
* 源日志路径由命令行指定，可以放在任意目录（回放工具负责写入）

源日志有两种布局，入口都是同一个 ``--log-file`` 路径：

* **单文件**：``--log-file`` 指向一个持续追加的 JSONL 文件（传统用法）。
  来源段号恒为 1，行号为文件内 1 起的全局行号。
* **编号分段**：``--log-file`` 作为段前缀，网关滚动写出
  ``<前缀>.000001``、``<前缀>.000002`` ……，每段写完后在段旁生成
  ``<段路径>.sealed`` 封存标记。服务严格按段号升序、段内行序升序跟读：
  即使更高编号的段已经出现，也先把低编号段的积压读完再换段。
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

SNAPSHOT_TMP = "snapshot.json.tmp"
SNAPSHOT_NAME = "snapshot.json"
BATCHES_NAME = "batches.jsonl"

_SEGMENT_SUFFIX = re.compile(r"^(\d{6})$")
SEAL_MARKER_SUFFIX = ".sealed"


def dumps_canonical(obj: Any) -> str:
    """确定性 JSON：sort_keys + 固定分隔符，保证两次回放逐字节可比。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


# --------------------------------------------------------------------------- 段文件命名


def segment_path(base: str, segment: int) -> str:
    """第 ``segment`` 段的路径（段号从 1 起，固定 6 位零填充）。"""
    return f"{base}.{segment:06d}"


def seal_marker_path(seg_path: str) -> str:
    """段封存标记路径；标记存在表示该段不会再写入新行。"""
    return seg_path + SEAL_MARKER_SUFFIX


def read_seal_marker(seg_path: str) -> dict | None:
    """读取段封存标记（段号/行数/字节数）；不存在或损坏返回 None。"""
    path = seal_marker_path(seg_path)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            marker = json.load(fh)
    except (OSError, ValueError):
        return None
    if not isinstance(marker, dict) or not isinstance(marker.get("segment"), int):
        return None
    return marker


def list_segments(base: str) -> dict[int, str]:
    """扫描 ``<base>.NNNNNN`` 段文件，返回 ``{段号: 路径}``（升序）。"""
    directory = os.path.dirname(os.path.abspath(base)) or "."
    prefix = os.path.basename(base) + "."
    try:
        names = os.listdir(directory)
    except FileNotFoundError:
        return {}
    found: dict[int, str] = {}
    for name in names:
        if not name.startswith(prefix):
            continue
        match = _SEGMENT_SUFFIX.match(name[len(prefix):])
        if match:
            found[int(match.group(1))] = os.path.join(directory, name)
    return dict(sorted(found.items()))


def _fsync_dir(path: str) -> None:
    """尽力 fsync 目录，保证新建/重命名的目录项可恢复。"""
    try:
        fd = os.open(os.path.dirname(os.path.abspath(path)) or ".", os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


# --------------------------------------------------------------------------- 批次日志


class JournalWriter:
    """批次日志追加器；每条记录的批次一次写入 + flush + fsync。

    一条输入记录可能发布多个批次（例如决定批次 + 随之触发的封账批次）。
    这里用**一次 write** 写完该记录的全部批次，因此进程被 SIGKILL 时
    这些批次要么全部可见、要么全部不可见，不会只留下半组。
    """

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        self._fh = open(path, "a", encoding="utf-8")

    def append(self, batch: dict) -> None:
        self.append_many([batch])

    def append_many(self, batches: list[dict]) -> None:
        if not batches:
            return
        payload = "".join(dumps_canonical(b) + "\n" for b in batches)
        self._fh.write(payload)
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def close(self) -> None:
        try:
            self._fh.flush()
            os.fsync(self._fh.fileno())
        finally:
            self._fh.close()


def _batch_position(batch: dict) -> tuple[int, int, int] | None:
    """批次对应的来源位置 ``(段号, 行号, 段内偏移)``；旧格式批次退化为单文件偏移。"""
    if "source_segment" in batch and "source_line" in batch:
        return (
            int(batch["source_segment"]),
            int(batch["source_line"]),
            int(batch.get("source_offset", -1)),
        )
    if "trigger_segment" in batch and "trigger_line" in batch:
        return (
            int(batch["trigger_segment"]),
            int(batch["trigger_line"]),
            int(batch.get("trigger_offset", -1)),
        )
    if "source_offset" in batch or "trigger_offset" in batch:
        # 兼容旧版单文件日志：段号视为 1，行号未知记为 0（恢复时按偏移续读）
        return (1, 0, int(batch.get("source_offset", batch.get("trigger_offset", -1))))
    return None


def load_journal(path: str) -> tuple[list[dict], int]:
    """读取批次日志，返回 ``(可重放批次, 有效字节长度)``。

    一条输入记录可能发布多个批次（决定批次 + 随之触发的封账批次），它们共享
    同一来源位置，并都带有 ``record_batch_count``。只有当一个来源位置上的批次
    数量恰好等于该值、且 seq 连续时，这一组才算完整提交；末尾不完整的组
    （掉电留下半行等）会被丢弃，调用方应把日志截断到返回的字节长度。
    """
    if not os.path.exists(path):
        return [], 0
    entries: list[tuple[dict, int]] = []
    with open(path, "rb") as fh:
        while True:
            raw = fh.readline()
            if raw == b"":
                break
            end = fh.tell()
            try:
                batch = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                break  # 半截行 / 损坏行：从这里开始不再可信
            if not isinstance(batch, dict) or "seq" not in batch:
                break
            entries.append((batch, end))

    batches: list[dict] = []
    valid_bytes = 0
    expected_seq = 1
    i = 0
    while i < len(entries):
        first = entries[i][0]
        pos = _batch_position(first)
        count = first.get("record_batch_count")
        if not isinstance(count, int) or count < 1:
            count = 1
        group = entries[i : i + count]
        complete = (
            len(group) == count
            and all(_batch_position(b) == pos for b, _ in group)
            and [b["seq"] for b, _ in group]
            == list(range(expected_seq, expected_seq + count))
        )
        if not complete:
            break  # 末尾残组 / seq 空洞：丢弃该组及其后内容
        batches.extend(b for b, _ in group)
        valid_bytes = group[-1][1]
        expected_seq += count
        i += count
    return batches, valid_bytes


class SnapshotStore:
    """原子快照写入与读取。快照永远只反映已经 fsync 进批次日志的状态。"""

    def __init__(self, directory: str):
        self.directory = directory
        self.path = os.path.join(directory, SNAPSHOT_NAME)
        self.tmp_path = os.path.join(directory, SNAPSHOT_TMP)

    def write(self, payload: dict) -> None:
        os.makedirs(self.directory, exist_ok=True)
        with open(self.tmp_path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=1))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(self.tmp_path, self.path)
        _fsync_dir(self.path)

    def read(self) -> dict | None:
        if not os.path.exists(self.path):
            return None
        with open(self.path, "r", encoding="utf-8") as fh:
            return json.load(fh)


# --------------------------------------------------------------------------- 源日志跟读


class SourceReader:
    """按「段号 → 段内行号」跟读源日志，统一单文件与编号分段两种布局。

    :meth:`readline` 返回 ``(段号, 行号, 段内起始字节偏移, 行文本)``；
    行号从 1 起。``resume`` 给出最后一次已提交的位置
    ``(段号, 行号, 段内偏移)``：打开时先跳过该已提交行，从它的下一行继续，
    因此强杀重启不会重复处理，也不会遗漏。

    ``layout``：

    * ``"auto"``：存在 ``<路径>.NNNNNN`` 段文件则按分段读，否则读单文件
    * ``"file"``：单文件（来源段号恒为 1）
    * ``"segments"``：编号分段
    """

    def __init__(
        self,
        path: str,
        layout: str = "auto",
        resume: tuple[int, int, int] | None = None,
        poll_interval: float = 0.05,
    ):
        self.path = path
        self._layout = layout
        self._mode: str | None = None if layout == "auto" else layout
        self.poll_interval = poll_interval
        self._fh = None
        self._inode = None
        self._limit: int | None = None
        self._cur_path = ""
        self._segment = 1
        self._offset = 0
        self._line_no = 0
        self._skip_pending: tuple[int, int] | None = None  # (行号, 偏移)
        if resume is not None:
            seg, line_no, offset = resume
            self._segment = seg or 1
            self._offset = max(offset, 0)
            self._skip_pending = (line_no, max(offset, 0))

    # -- 模式解析 -------------------------------------------------------
    def _resolve_mode(self) -> str | None:
        segments = list_segments(self.path)
        if segments:
            return "segments"
        if os.path.exists(self.path):
            return "file"
        return None

    def mode(self) -> str | None:
        return self._mode

    # -- 打开/定位 ------------------------------------------------------
    def _open_file(self) -> bool:
        if not os.path.exists(self.path):
            return False
        self._fh = open(self.path, "rb")
        self._inode = os.fstat(self._fh.fileno()).st_ino
        self._limit = None
        self._cur_path = self.path
        self._fh.seek(self._offset)
        self._consume_skipped_line()
        return True

    def _consume_skipped_line(self) -> None:
        """跳过恢复点所在的那一行（已提交，不再处理）。

        旧版批次日志只记录字节偏移、没有行号（line_no=0），此时按偏移之前的
        换行数反推行号，保证从此以后发布的批次行号仍与源文件行序一致。
        """
        if self._skip_pending is None:
            return
        line_no, start = self._skip_pending
        if self._offset != start:
            self._fh.seek(self._offset)
        data = self._fh.readline()
        if not data.endswith(b"\n"):
            raise ValueError(
                f"恢复点（段 {self._segment}，第 {line_no} 行，偏移 {start}）"
                "处找不到完整行，源日志可能被截断或改写"
            )
        self._offset += len(data)
        if line_no <= 0:
            # 旧格式：数偏移之前有多少行
            self._fh.seek(0)
            prefix = self._fh.read(start)
            line_no = prefix.count(b"\n") + 1
            self._fh.seek(self._offset)
        self._line_no = line_no
        self._skip_pending = None

    # -- 单文件 ---------------------------------------------------------
    def _read_file_line(self) -> tuple[int, int, int, str] | None:
        if self._fh is None:
            if not self._open_file():
                return None
        else:
            try:
                stat = os.stat(self.path)
            except FileNotFoundError:
                return None
            if stat.st_ino != self._inode or stat.st_size < self._offset:
                # 文件被重建/截断：从新文件开头重新跟随（来源段号仍为 1）
                self._fh.close()
                self._fh = None
                self._offset = 0
                self._line_no = 0
                self._skip_pending = None
                if not self._open_file():
                    return None
        data = self._fh.readline()
        if data == b"":
            return None
        if not data.endswith(b"\n"):
            self._fh.seek(self._offset)
            return None
        start = self._offset
        self._offset += len(data)
        self._line_no += 1
        return (1, self._line_no, start, data.decode("utf-8", errors="replace"))

    # -- 编号分段 -------------------------------------------------------
    def _open_segment(self, path: str) -> bool:
        if not os.path.exists(path):
            return False
        self._fh = open(path, "rb")
        self._inode = os.fstat(self._fh.fileno()).st_ino
        self._cur_path = path
        self._fh.seek(self._offset)
        marker = read_seal_marker(path)
        # 封存标记给出了该段的权威字节数：超出的内容（写完标记后的异常追加）
        # 一律不读，保证「封存后内容冻结」。
        self._limit = marker.get("bytes") if isinstance(marker, dict) else None
        self._consume_skipped_line()
        return True

    def _read_segment_line(self) -> tuple[int, int, int, str] | None:
        while True:
            if self._fh is None:
                seg_path = segment_path(self.path, self._segment)
                if not self._open_segment(seg_path):
                    # 段号必须连续：当前段缺失却已有更高编号段，说明丢段，
                    # 不能静默跳过（会破坏「按段号顺序、一条不落」的语义）
                    later = [n for n in list_segments(self.path) if n > self._segment]
                    if later:
                        raise ValueError(
                            f"源日志缺段：期望段 {self._segment}，但已存在段 "
                            f"{later[0]}，段号不连续"
                        )
                    return None
            if self._limit is not None and self._offset >= self._limit:
                data = b""
            else:
                data = self._fh.readline()
                if self._limit is not None and self._offset + len(data) > self._limit:
                    data = b""  # 不应发生：截断到封存字节数之内
            if data == b"":
                # 当前段读尽；只有已封存才允许换段，否则等待（不能越段）。
                # 已封存段若字节数与标记不符（被截断），说明源日志已不可信。
                marker = read_seal_marker(self._cur_path)
                if marker is None:
                    return None
                limit = marker.get("bytes")
                if isinstance(limit, int) and self._offset < limit:
                    raise ValueError(
                        f"源日志段 {self._segment} 被截断：封存标记字节数 {limit}，"
                        f"实际只读到 {self._offset}"
                    )
                self._fh.close()
                self._fh = None
                self._segment += 1
                self._offset = 0
                self._line_no = 0
                continue  # 立刻尝试读下一段（不存在则下一轮返回 None）
            if not data.endswith(b"\n"):
                self._fh.seek(self._offset)
                return None
            start = self._offset
            self._offset += len(data)
            self._line_no += 1
            return (self._segment, self._line_no, start,
                    data.decode("utf-8", errors="replace"))

    # -- 统一入口 -------------------------------------------------------
    def readline(self) -> tuple[int, int, int, str] | None:
        if self._mode is None:
            self._mode = self._resolve_mode()
            if self._mode is None:
                return None
        if self._mode == "file":
            return self._read_file_line()
        return self._read_segment_line()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None


class SegmentWriter:
    """编号分段写出器：逐个追加写段文件，换段后可在任意时刻封存旧段。

    写出流程与网关侧约定一致：

    1. 追加写当前段，每行 ``flush + fsync``；
    2. :meth:`open_next_segment` 打开下一编号段（旧段此后不再追加）；
    3. :meth:`seal_segment` 为已写完的段原子写入 ``.sealed`` 标记
       （含段号/行数/字节数）。封存可滞后于换段，用于模拟积压。

    构造时若最后一段尚未封存，会接着它继续写；否则从下一段开始。
    """

    def __init__(self, base: str):
        self.base = base
        os.makedirs(os.path.dirname(os.path.abspath(base)) or ".", exist_ok=True)
        existing = list_segments(base)
        if existing:
            last = max(existing)
            self.segment = last if not os.path.exists(
                seal_marker_path(existing[last])
            ) else last + 1
            self.lines = (
                _count_lines(existing[last])
                if self.segment == last
                else 0
            )
        else:
            self.segment = 1
            self.lines = 0
        self._fh = open(segment_path(base, self.segment), "ab")
        self.total_lines = self.lines
        # 记录每个段写完时的 (行数, 字节数)，供延迟封存使用
        self._finished: dict[int, dict] = {}

    @property
    def path(self) -> str:
        return segment_path(self.base, self.segment)

    def append(self, line: str) -> None:
        if not line.endswith("\n"):
            line += "\n"
        data = line.encode("utf-8")
        self._fh.write(data)
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self.lines += 1
        self.total_lines += 1

    def open_next_segment(self) -> None:
        """关掉当前段（此后不再追加），打开下一编号段。旧段尚未封存。"""
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._finished[self.segment] = {
            "lines": self.lines,
            "bytes": os.path.getsize(self.path),
        }
        self._fh.close()
        self.segment += 1
        self.lines = 0
        self._fh = open(segment_path(self.base, self.segment), "ab")

    def seal_segment(self, segment: int) -> None:
        """为段 ``segment`` 写入封存标记（幂等；已封存则跳过）。"""
        seg_path = segment_path(self.base, segment)
        if os.path.exists(seal_marker_path(seg_path)):
            return
        info = self._finished.get(segment)
        if info is None:
            info = {"lines": _count_lines(seg_path), "bytes": os.path.getsize(seg_path)}
            self._finished[segment] = info
        marker = seal_marker_path(seg_path)
        payload = dumps_canonical(
            {"segment": segment, "lines": info["lines"], "bytes": info["bytes"]}
        )
        tmp = marker + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(payload + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, marker)
        _fsync_dir(marker)

    def mark_sealed(self, segment: int | None = None) -> None:
        """封存指定段（默认当前段）；会先 fsync 当前段。"""
        segment = self.segment if segment is None else segment
        if segment == self.segment:
            self._fh.flush()
            os.fsync(self._fh.fileno())
            self._finished[segment] = {
                "lines": self.lines,
                "bytes": os.path.getsize(self.path),
            }
        self.seal_segment(segment)

    def detach(self) -> None:
        """仅关闭文件句柄，不封存。"""
        if self._fh is not None:
            self._fh.close()
            self._fh = None


def _count_lines(path: str) -> int:
    n = 0
    with open(path, "rb") as fh:
        for _ in fh:
            n += 1
    return n
