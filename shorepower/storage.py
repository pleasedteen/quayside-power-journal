"""本地持久化：追加式 JSON Lines 批次日志、原子状态快照、源日志跟读。

目录布局（均在用户指定的 state 目录下）：

* ``batches.jsonl``   已发布批次（decision / seal），追加写，重启后仍是精确一次的事实来源
* ``snapshot.json``   内存状态的原子快照（tmp + os.replace），仅用于加速恢复
* 源日志路径由命令行指定，可以放在任意目录（回放工具负责写入）

崩溃一致性约定（关键）：

* 一条源记录发布的全部批次称为一个**提交组**，整组一次 write+fsync 落盘；
  组内最后一个批次带 ``group_end=true`` 标记。
* 恢复时只认到最后一个带标记的完整组，其后的半截行/半截组一律截断丢弃——
  对应记录尚未提交，重启后从源日志原位置重新进入流程，保证“全有或全无”。
* 引擎只在整组 fsync 成功后才把批次并入可查询状态，因此查询永远只读到
  已持久化的完整旧账，写盘卡住/强杀不会让查询暴露未提交的新账。
"""

from __future__ import annotations

import json
import os
from typing import Any, Iterator

SNAPSHOT_TMP = "snapshot.json.tmp"
SNAPSHOT_NAME = "snapshot.json"
BATCHES_NAME = "batches.jsonl"

# 崩溃注入（仅用于本地强杀恢复测试）：
#   SHOREPOWER_CRASH_AT=N    在第 N 个提交组上注入强杀（os._exit，等价 SIGKILL）
#   SHOREPOWER_CRASH_MODE=write  组内首行 write+flush 后、fsync 前强杀（写入中）
#   SHOREPOWER_CRASH_MODE=fsync  整组 write+flush 后、fsync 前强杀（落盘前一刻）
CRASH_AT_ENV = "SHOREPOWER_CRASH_AT"
CRASH_MODE_ENV = "SHOREPOWER_CRASH_MODE"


def dumps_canonical(obj: Any) -> str:
    """确定性 JSON：sort_keys + 固定分隔符，保证两次回放逐字节可比。"""
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _fsync_dir(path: str) -> None:
    """对目录 fsync，保证该目录下的新建/替换/截断项在强杀后仍可见。"""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class JournalWriter:
    """批次日志追加器；以“提交组”为单位整组 write + fsync，强杀也只丢未提交组。

    每条源记录对应一个提交组（1~2 个批次）。正常路径下整组拼在同一个缓冲区里
    一次 write、一次 fsync：要么整组都在日志里（末尾带 group_end 标记），要么
    一行都没有，不会出现“只留下进度批次、漏掉同条上报触发的封账”。
    """

    def __init__(self, path: str):
        self.path = path
        directory = os.path.dirname(os.path.abspath(path)) or "."
        os.makedirs(directory, exist_ok=True)
        self._fh = open(path, "a", encoding="utf-8")
        self._group_count = 0
        crash_at = os.environ.get(CRASH_AT_ENV)
        self._crash_at = int(crash_at) if crash_at else 0
        self._crash_mode = os.environ.get(CRASH_MODE_ENV, "fsync")

    def _maybe_crash(self) -> None:
        if self._crash_at and self._group_count == self._crash_at:
            # 立即终止，不跑任何 finally/缓冲刷新，等价于 SIGKILL
            os._exit(137)

    def append_group(self, batches: list[dict]) -> None:
        """把一条源记录的全部批次作为一个提交组原子追加并 fsync。"""
        if not batches:
            return
        self._group_count += 1
        if self._crash_at and self._group_count == self._crash_at \
                and self._crash_mode == "write":
            # 写入中强杀：只落组内第一行（无换行结束标记），随后立即终止
            self._fh.write(dumps_canonical(batches[0]) + "\n")
            self._fh.flush()
            self._maybe_crash()
        payload = "".join(dumps_canonical(b) + "\n" for b in batches)
        self._fh.write(payload)
        self._fh.flush()
        # 落盘前一刻强杀：数据已进页缓存但未 fsync，组标记同样不会被恢复逻辑承认
        self._maybe_crash()
        os.fsync(self._fh.fileno())

    def close(self) -> None:
        try:
            self._fh.flush()
            os.fsync(self._fh.fileno())
        finally:
            self._fh.close()


def iter_jsonl(path: str) -> Iterator[dict]:
    """逐行读取 JSONL；半截/损坏行跳过。

    批次日志每个提交组写入后都 fsync，因此损坏只可能出现在文件尾部（写入途中
    被强杀）。跳过它即可：对应记录尚未提交，重启后会从源日志重新进入流程。
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


def recover_committed_batches(path: str) -> list[dict]:
    """启动恢复：返回所有**完整提交组**的批次，并把未完成的尾部物理截断。

    新格式日志中每个提交组最后一个批次带 ``group_end=true``。文件尾部可能是：

    * 完整组（末尾有标记）——全部有效；
    * 半截行（无换行/JSON 损坏）——该行及其后内容丢弃；
    * 完整行但组未结束（强杀落在组内两次写入之间）——整组丢弃。

    截断点按字节定位并 ``truncate + fsync``，保证下一次追加紧接在完整旧账之后。
    没有任何 group_end 标记的旧格式日志按“能完整解析即有效”处理（兼容历史数据）。
    """
    if not os.path.exists(path):
        return []
    committed: list[dict] = []
    committed_end_pos = 0  # 最后一个完整提交组之后的字节偏移
    any_marker = False
    pending: list[dict] = []
    with open(path, "r", encoding="utf-8") as fh:
        while True:
            line_start = fh.tell()
            line = fh.readline()
            if line == "":
                break
            text = line.strip()
            if not text:
                continue
            try:
                batch = json.loads(text)
            except json.JSONDecodeError:
                break  # 半截行：写入中强杀，尾部全部不可信
            if not isinstance(batch, dict):
                break
            if batch.get("group_end"):
                any_marker = True
                pending.append(batch)
                committed.extend(pending)
                pending = []
                committed_end_pos = fh.tell()
            else:
                pending.append(batch)
    if not any_marker:
        # 兼容旧格式（无组标记）：所有能完整解析的行都算已提交
        return list(iter_jsonl(path))
    if committed_end_pos < os.path.getsize(path):
        with open(path, "r+", encoding="utf-8") as fh:
            fh.truncate(committed_end_pos)
            fh.flush()
            os.fsync(fh.fileno())
        _fsync_dir(os.path.dirname(os.path.abspath(path)) or ".")
    return committed


def next_line_start(path: str, offset: int) -> int:
    """源日志续读位置：从 ``offset``（上一已提交记录起始偏移）跳到其行尾之后。

    崩溃时该记录可能“已读未处理”，续读必须从下一行起点开始，避免重读；
    源文件不存在（尚未回放）时保持原偏移，等文件出现后由 TailReader 打开。
    """
    try:
        with open(path, "rb") as fh:
            fh.seek(offset)
            line = fh.readline()
            if line:
                return offset + len(line)
    except FileNotFoundError:
        pass
    return offset


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
        # 替换目录项本身也需要 fsync 目录，强杀后新快照才确定可见
        _fsync_dir(self.directory)

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
