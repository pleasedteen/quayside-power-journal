"""本地持久化：追加式 JSON Lines 批次日志、原子状态快照、源日志跟读。

目录布局（均在用户指定的 state 目录下）：

* ``batches.jsonl``   已发布批次（decision / seal），追加写，重启后仍是精确一次的事实来源
* ``snapshot.json``   内存状态的原子快照（tmp + os.replace），仅用于加速恢复
* 源日志路径由命令行指定，可以放在任意目录（回放工具负责写入）
"""

from __future__ import annotations

import json
import os
from typing import Any, Iterator

SNAPSHOT_TMP = "snapshot.json.tmp"
SNAPSHOT_NAME = "snapshot.json"
BATCHES_NAME = "batches.jsonl"


def dumps_canonical(obj: Any) -> str:
    """确定性 JSON：sort_keys + 固定分隔符，保证两次回放逐字节可比。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


class JournalWriter:
    """批次日志追加器；每行 flush + fsync，强杀也不会丢已发布批次。"""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        self._fh = open(path, "a", encoding="utf-8")

    def append(self, batch: dict) -> None:
        self._fh.write(dumps_canonical(batch) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def close(self) -> None:
        try:
            self._fh.flush()
            os.fsync(self._fh.fileno())
        finally:
            self._fh.close()


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


def load_persisted_batches(path: str) -> list[dict]:
    """读取当前已持久化的全部批次（权威事实来源的一次性快照视图）。

    崩溃恢复与离线 Parquet/JSONL 导出共用这一个读取口径：调用方拿到的列表在
    返回后不再随日志追加而变化，因此“批次表”和由同一列表重放出的“窗口表”必然
    对应同一个已 fsync 的批次边界。日志在读取期间被追加也不会混入本次结果。
    """
    return list(iter_jsonl(path))


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
