"""本地持久化：追加式 JSON Lines 批次日志、磁盘序号索引、原子快照、源日志跟读。

目录布局（均在用户指定的 state 目录下）：

* ``batches.jsonl``   已发布批次（decision / seal），追加写，重启后仍是精确一次的事实来源
* ``batches.index``   定长二进制索引（每行 start/length），支持按序号直接定位；
  **不随进程常驻内存**，因此历史再长，分页与重启的内存开销都只取决于活动状态
* ``snapshot.json``   内存状态的原子快照（tmp + os.replace），记录快照覆盖到的批次日志
  字节偏移 ``journal_offset``，重启只需读取该偏移之后的批次正文
* 源日志路径由命令行指定，可以放在任意目录（回放工具负责写入）

旧状态目录（只有 ``batches.jsonl`` + 旧版 ``snapshot.json``，没有索引）首次启动时
会自动扫描一次日志补建索引，随后立刻写出新版快照；此后重启只读快照之后的正文，
且索引文件存在时启动只做一次 O(1) 的尾部检查，不再遍历历史。
"""

from __future__ import annotations

import json
import os
import sqlite3
import struct
from typing import Any, Iterable, Iterator

SNAPSHOT_TMP = "snapshot.json.tmp"
SNAPSHOT_NAME = "snapshot.json"
BATCHES_NAME = "batches.jsonl"
INDEX_NAME = "batches.index"
HISTORY_NAME = "history.db"

# 每条索引记录：该行在 batches.jsonl 中的起始字节偏移与长度（含换行符）。
# 序号由记录位置隐含：第 i 条（0 基）对应 seq = i + 1。
_INDEX_ENTRY = struct.Struct(">QQ")


def dumps_canonical(obj: Any) -> str:
    """确定性 JSON：sort_keys + 固定分隔符，保证两次回放逐字节可比。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def iter_jsonl(path: str) -> Iterator[dict]:
    """逐行读取 JSONL；半截/损坏行跳过。

    批次日志每行写入后都 fsync，因此损坏只可能出现在最后一行（写入途中被强杀）。
    跳过它即可：对应记录尚未发布，重启后会从源日志重新进入流程。
    """
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            text = line.strip()
            if not text:
                continue
            try:
                yield json.loads(text)
            except json.JSONDecodeError:
                continue


class BatchJournal:
    """追加式批次日志 + 磁盘序号索引；按序号分页读取无需载入全量历史。

    序号由索引位置隐含（第 i 条对应 seq=i+1），因此 ``offset_after(seq)`` 与
    ``read_page(after_seq, limit)`` 都是 O(1) 次预读加 O(limit) 次正文解析，
    内存占用只与本次读取的条数有关。

    写入顺序：先写正文并 fsync，再写索引并 fsync。任一步之间被强杀都可由
    :meth:`_recover` 修正：索引落后一行就补扫，正文尾部不完整就截断。
    """

    def __init__(self, directory: str, sync: bool = True):
        self.directory = directory
        # sync=False 仅用于生成测试夹具（批量灌历史），线上服务始终 True
        self.sync = sync
        os.makedirs(directory, exist_ok=True)
        self.path = os.path.join(directory, BATCHES_NAME)
        self.index_path = os.path.join(directory, INDEX_NAME)
        self._data = open(self.path, "ab", buffering=0)
        # 索引既可随机读（os.pread）又只做顺序追加写（O_APPEND），因此用 a+b
        self._index = open(self.index_path, "a+b", buffering=0)
        self.count = 0
        self.end_offset = 0
        # 可观测计数：用于确认重启/分页没有读全量历史
        self.bodies_read = 0
        self.bytes_read = 0
        self.index_bytes_scanned = 0
        self.index_rebuilt = False
        self._recover()

    # ------------------------------------------------------------- 恢复索引
    def _index_entry_sane(self, index: int, data_size: int) -> bool:
        """校验某条索引记录：偏移/长度落在正文内，且该行确实以换行结束。"""
        if index < 0:
            return True
        start, length = self._entry(index)
        if length == 0 or start + length > data_size:
            return False
        if index > 0:
            prev_start, prev_len = self._entry(index - 1)
            if prev_start + prev_len != start:
                return False
        with open(self.path, "rb") as fh:
            fh.seek(start + length - 1)
            return fh.read(1) == b"\n"

    def _recover(self) -> None:
        entry_size = _INDEX_ENTRY.size
        index_size = os.fstat(self._index.fileno()).st_size
        if index_size % entry_size:
            index_size -= index_size % entry_size
            os.ftruncate(self._index.fileno(), index_size)

        count = index_size // entry_size
        last_end = 0
        if count:
            raw = os.pread(self._index.fileno(), entry_size, (count - 1) * entry_size)
            start, length = _INDEX_ENTRY.unpack(raw)
            last_end = start + length
        data_size = os.fstat(self._data.fileno()).st_size
        if count and not self._index_entry_sane(count - 1, data_size):
            # 索引尾部损坏/与正文错位（例如目录被手工改动过）：整份索引重建
            os.ftruncate(self._index.fileno(), 0)
            count = 0
            last_end = 0

        if count == 0 and data_size > 0:
            self.index_rebuilt = True

        pos = last_end
        with open(self.path, "rb") as fh:
            fh.seek(pos)
            while True:
                line = fh.readline()
                if not line:
                    break
                if not line.endswith(b"\n"):
                    # 最后一行写到一半被强杀：它不是已发布批次，直接截掉
                    os.ftruncate(self._data.fileno(), pos)
                    break
                if line in (b"\n", b"\r\n"):
                    pos += len(line)
                    continue
                self._index.write(_INDEX_ENTRY.pack(pos, len(line)))
                self.index_bytes_scanned += len(line)
                pos += len(line)
                count += 1
        self._index.flush()
        os.fsync(self._index.fileno())

        self.count = count
        self.end_offset = pos

    # ------------------------------------------------------------- 写入
    def append(self, batch: dict) -> None:
        self.append_many([batch])

    def append_many(self, batches: list[dict]) -> None:
        """一次写入同一来源记录的整组批次（如 decision + seal），共享一次 fsync。

        整组要么一起可读、要么只留下可被 :meth:`_recover` 截断的半截尾行：
        重启后不会出现“同一条记录的封账批次丢失、决定批次还在”的中间态。
        """
        if not batches:
            return
        lines = [(dumps_canonical(batch) + "\n").encode("utf-8") for batch in batches]
        payload = b"".join(lines)
        start = self.end_offset
        self._data.write(payload)
        if self.sync:
            os.fsync(self._data.fileno())
        pos = start
        index_bytes = bytearray()
        for line in lines:
            index_bytes += _INDEX_ENTRY.pack(pos, len(line))
            pos += len(line)
        self._index.write(bytes(index_bytes))
        if self.sync:
            os.fsync(self._index.fileno())
        self.count += len(batches)
        self.end_offset = pos

    def close(self) -> None:
        try:
            os.fsync(self._data.fileno())
            os.fsync(self._index.fileno())
        finally:
            self._data.close()
            self._index.close()

    # ------------------------------------------------------------- 定位/读取
    def _entry(self, index: int) -> tuple[int, int]:
        raw = os.pread(self._index.fileno(), _INDEX_ENTRY.size, index * _INDEX_ENTRY.size)
        return _INDEX_ENTRY.unpack(raw)

    def offset_after(self, seq: int) -> int:
        """返回序号 <= seq 的全部批次之后的字节偏移（seq<=0 时为 0）。"""
        if seq <= 0:
            return 0
        if seq >= self.count:
            return self.end_offset
        start, length = self._entry(seq - 1)
        return start + length

    def read_page(self, after_seq: int, limit: int | None) -> list[dict]:
        """读取序号大于 after_seq 的批次，最多 limit 条；只读这些行的正文。"""
        start_index = max(after_seq, 0)
        if start_index >= self.count:
            return []
        take = self.count - start_index if limit is None else min(limit, self.count - start_index)
        if take <= 0:
            return []
        first_start, _ = self._entry(start_index)
        last_start, last_length = self._entry(start_index + take - 1)
        length = last_start + last_length - first_start
        with open(self.path, "rb") as fh:
            fh.seek(first_start)
            data = fh.read(length)
        batches: list[dict] = []
        for line in data.split(b"\n"):
            if not line.strip():
                continue
            try:
                batch = json.loads(line)
            except json.JSONDecodeError:  # pragma: no cover - 仅整行损坏时
                continue
            if batch.get("seq", 0) <= after_seq:
                continue
            batches.append(batch)
        self.bodies_read += len(batches)
        self.bytes_read += len(data)
        return batches

    def iter_from(self, offset: int) -> Iterator[dict]:
        """从字节偏移流式读取批次正文，不把历史整体载入内存。"""
        with open(self.path, "rb") as fh:
            fh.seek(offset)
            for line in fh:
                if not line.strip():
                    continue
                try:
                    batch = json.loads(line)
                except json.JSONDecodeError:  # pragma: no cover - 半截尾部
                    continue
                self.bodies_read += 1
                self.bytes_read += len(line)
                yield batch


class HistoryStore:
    """已结算历史的磁盘归档（标准库 sqlite3），让内存只保留活动状态。

    刻钟窗口一旦封账就冻结，其中的读数不会再被接受或更正，因此快照时可以把
    这部分历史整批折叠到磁盘：

    * ``sealed_window``   封账窗口的冻结 totals（不含逐笔 contributions）
    * ``reading_version`` 已结算读数的生效版本与所属窗口（版本单调性与重复判定用）

    恢复与查询都从这里取，内存里只留“未封账窗口 + 未封账读数”，
    两条路径的开销因此只随活动状态增长。
    """

    def __init__(self, directory: str):
        self.path = os.path.join(directory, HISTORY_NAME)
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        # 归档目录需要能被整体复制/备份：用回滚日志模式，提交后 history.db
        # 自身就是完整一致的（WAL 模式会把刚提交的数据留在 -wal 旁文件里），
        # synchronous=FULL 保证归档先于快照落盘。
        self._db.execute("PRAGMA journal_mode=DELETE")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS sealed_window (
                window_start INTEGER PRIMARY KEY,
                totals TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS reading_version (
                reading_id TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                window_start INTEGER NOT NULL
            );
            """
        )
        self._db.commit()

    @staticmethod
    def _chunks(items: list, size: int = 500) -> Iterable[list]:
        for i in range(0, len(items), size):
            yield items[i : i + size]

    # ------------------------------------------------------------- 折叠写入
    def archive(
        self,
        sealed_totals: dict[int, dict[str, float]],
        reading_versions: dict[str, tuple[int, int]],
    ) -> None:
        """把已结算状态写进归档；调用方随后把这些条目从内存中丢弃。

        ``reading_versions`` 的值为 ``(version, window_start)``。重复写入是幂等的，
        因此“归档已提交、快照尚未替换”时被强杀也能安全重放。
        """
        with self._db:
            self._db.executemany(
                "INSERT OR REPLACE INTO sealed_window(window_start, totals) VALUES (?, ?)",
                [
                    (wstart, dumps_canonical(dict(sorted(totals.items()))))
                    for wstart, totals in sealed_totals.items()
                ],
            )
            for chunk in self._chunks(list(reading_versions.items())):
                self._db.executemany(
                    "INSERT OR REPLACE INTO reading_version"
                    "(reading_id, version, window_start) VALUES (?, ?, ?)",
                    [(rid, ver, wstart) for rid, (ver, wstart) in chunk],
                )

    # ------------------------------------------------------------- 读取
    def sealed_totals(self, window_starts: Iterable[int]) -> dict[int, dict[str, float]]:
        result: dict[int, dict[str, float]] = {}
        for wstart in window_starts:
            row = self._db.execute(
                "SELECT totals FROM sealed_window WHERE window_start = ?", (wstart,)
            ).fetchone()
            if row is not None:
                result[wstart] = json.loads(row[0])
        return result

    def iter_sealed(self) -> Iterator[tuple[int, dict[str, float]]]:
        """按窗口起点顺序流式返回全部已封账窗口（用于“全部窗口”查询）。"""
        rows = self._db.execute(
            "SELECT window_start, totals FROM sealed_window ORDER BY window_start"
        )
        for wstart, totals in rows:
            yield wstart, json.loads(totals)

    def is_sealed(self, window_start: int) -> bool:
        row = self._db.execute(
            "SELECT 1 FROM sealed_window WHERE window_start = ?", (window_start,)
        ).fetchone()
        return row is not None

    def sealed_starts(self) -> list[int]:
        return [row[0] for row in self._db.execute("SELECT window_start FROM sealed_window")]

    def sealed_bounds(self) -> tuple[int | None, int | None]:
        row = self._db.execute(
            "SELECT MIN(window_start), MAX(window_start) FROM sealed_window"
        ).fetchone()
        return row[0], row[1]

    def reading_record(self, reading_id: str) -> tuple[int, int] | None:
        """返回已归档读数的 ``(version, window_start)``；不存在返回 None。"""
        row = self._db.execute(
            "SELECT version, window_start FROM reading_version WHERE reading_id = ?",
            (reading_id,),
        ).fetchone()
        return None if row is None else (row[0], row[1])

    def archived_reading_ids(self) -> set[str]:
        return {row[0] for row in self._db.execute("SELECT reading_id FROM reading_version")}

    def sealed_window_count(self) -> int:
        return self._db.execute("SELECT COUNT(*) FROM sealed_window").fetchone()[0]

    def close(self) -> None:
        self._db.close()


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

    def read(self) -> dict | None:
        if not os.path.exists(self.path):
            return None
        with open(self.path, "r", encoding="utf-8") as fh:
            return json.load(fh)


class TailReader:
    """对单个 JSON Lines 文件做 ``tail -F`` 式跟读，支持被截断/重建后从头跟随。"""

    def __init__(self, path: str, start_offset: int = 0, poll_interval: float = 0.05):
        self.path = path
        self.offset = start_offset
        self.poll_interval = poll_interval
        self._fh = None
        self._inode = None

    def _open(self) -> None:
        self._fh = open(self.path, "r", encoding="utf-8")
        self._fh.seek(self.offset)
        self._inode = os.fstat(self._fh.fileno()).st_ino

    def _reopen_if_rotated(self) -> bool:
        try:
            stat = os.stat(self.path)
        except FileNotFoundError:
            return False
        if self._fh is None:
            self._open()
            return True
        if stat.st_ino != self._inode or stat.st_size < self.offset:
            # 文件被重建（新 inode）或截断：从新文件开头跟读
            self._fh.close()
            self.offset = 0
            self._open()
            return True
        return False

    def readline(self) -> str | None:
        """返回下一行（含换行符也可）；暂无数据返回 None。"""
        if self._fh is None:
            if not os.path.exists(self.path):
                return None
            self._open()
        else:
            self._reopen_if_rotated()
        line = self._fh.readline()
        if line == "":
            return None
        # readline 可能读到未写完的半截行（结尾无 \n）：退回偏移等下次
        if not line.endswith("\n"):
            self._fh.seek(self.offset)
            return None
        self.offset = self._fh.tell()
        return line

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
