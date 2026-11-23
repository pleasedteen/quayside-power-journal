"""本地持久化：追加式 JSON Lines 批次日志、原子状态快照、源日志跟读。

目录布局（均在用户指定的 state 目录下）：

* ``batches.jsonl``   已发布批次（decision / seal），追加写，重启后仍是精确一次的事实来源
* ``snapshot.json``   内存状态的原子快照（tmp + os.replace），仅用于加速恢复
* 源日志路径由命令行指定，可以放在任意目录（回放工具负责写入）
"""

from __future__ import annotations

import json
import os
import threading
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


class SnapshotStore:
    """原子快照写入与读取。快照永远只反映已经 fsync 进批次日志的状态。"""

    def __init__(self, directory: str):
        self.directory = directory
        self.path = os.path.join(directory, SNAPSHOT_NAME)
        self.tmp_path = os.path.join(directory, SNAPSHOT_TMP)

    def write(self, payload: dict) -> None:
        os.makedirs(self.directory, exist_ok=True)
        # 历史失败可能残留临时文件，先清理，避免被前一次的半截文件干扰
        try:
            if os.path.exists(self.tmp_path):
                os.remove(self.tmp_path)
        except OSError:
            pass
        with open(self.tmp_path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=1))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(self.tmp_path, self.path)

    def read(self) -> dict | None:
        if not os.path.exists(self.path):
            return None
        # 快照只是加速恢复的副本，批次日志才是权威事实来源：
        # 快照损坏 / 被非法类型占位（如同名目录）/ JSON 非法时退回「无快照」，
        # 由调用方纯靠批次日志重放，绝不能因为快照问题导致服务起不来。
        try:
            if not os.path.isfile(self.path):
                return None
            with open(self.path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (OSError, ValueError):
            return None


class SnapshotManager:
    """后台快照写线程：把快照写盘与账务线程解耦。

    语义保证：

    * 快照写盘变慢或失败时，``request`` 不阻塞账务线程，服务继续入账；
      已 fsync 的批次与账态通过查询接口照常可见。
    * 负载严格有界：至多 1 个 payload 正在写 + 1 个 payload 待写。
      待写位被占用时新请求被拒收（返回 ``False``），账务线程处理下一条记录
      时会携带最新状态重试，因此内存不会随积压记录数增长。
    * 暂停（``set_paused(True)``）期间一律拒收，调用方因此不会构建 payload，
      快照子系统不引入与业务量相关的额外内存。
    * 写入只在临时文件上进行，成功后才 ``os.replace`` 原子替换；写失败时
      旧版完整快照原样保留，仍可用于恢复。
    """

    def __init__(self, store: SnapshotStore):
        self.store = store
        self._cond = threading.Condition(threading.Lock())
        self._pending: dict | None = None
        self._busy = False
        self._paused = False
        self._closing = False
        self._last_error: str | None = None
        self.successes = 0
        self.failures = 0
        self.skipped_paused = 0
        self.skipped_busy = 0
        self._thread = threading.Thread(
            target=self._run, name="snapshot-writer", daemon=True
        )
        self._thread.start()

    # --------------------------------------------------------- 账务线程侧
    @property
    def paused(self) -> bool:
        with self._cond:
            return self._paused

    def set_paused(self, paused: bool) -> None:
        with self._cond:
            self._paused = paused
            self._cond.notify_all()

    def note_skip(self) -> None:
        """登记一次因暂停而未投递的快照尝试（在构建大 payload 之前调用）。

        账务线程用它避免暂停时白白深拷贝整份账态；暂停计数与
        :meth:`request` 在暂停下的拒收口径一致，状态接口可观察。
        """
        with self._cond:
            if self._closing or not self._paused:
                return
            self.skipped_paused += 1

    def request(self, payload: dict) -> bool:
        """非阻塞投递一个快照 payload。

        返回 True 表示已被后台线程接收；False 表示因暂停/关闭/待写位占用
        而拒收并累计对应计数。拒收后调用方无需重试，后续记录触发时会携带
        最新状态再来——这保证快照相关内存严格有界，不随业务积压增长。
        """
        with self._cond:
            if self._closing:
                return False
            if self._paused:
                self.skipped_paused += 1
                return False
            if self._pending is not None:
                self.skipped_busy += 1
                return False
            self._pending = payload
            self._cond.notify_all()
            return True

    def status(self) -> dict:
        with self._cond:
            return {
                "paused": self._paused,
                "inflight": 1 if self._busy else 0,
                "queued": 1 if self._pending is not None else 0,
                "max_pending_payloads": 1,
                "successes": self.successes,
                "failures": self.failures,
                "skipped_paused": self.skipped_paused,
                "skipped_busy": self.skipped_busy,
                "last_error": self._last_error,
                "snapshot_path": self.store.path,
            }

    def wait_idle(self, timeout: float = 10.0) -> bool:
        """等待当前快照写盘完成（测试/优雅停机用）。"""
        import time

        with self._cond:
            end = time.monotonic() + timeout
            while (self._busy or self._pending is not None) and not self._closing:
                remaining = end - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)
            return not self._busy and self._pending is None

    def close(self, flush: bool = True) -> None:
        """停止后台线程；flush=True 时先把待写 payload 落盘。"""
        with self._cond:
            if self._closing:
                self._thread.join(timeout=5.0)
                return
            if not flush:
                self._pending = None
            self._closing = True
            self._cond.notify_all()
        self._thread.join(timeout=10.0)

    # --------------------------------------------------------- 后台写线程
    def _run(self) -> None:
        while True:
            with self._cond:
                while self._pending is None and not self._closing:
                    self._cond.wait()
                if self._pending is None and self._closing:
                    return
                payload = self._pending
                self._pending = None
                self._busy = True
            try:
                self.store.write(payload)
                with self._cond:
                    self.successes += 1
                    self._last_error = None
            except Exception as exc:  # noqa: BLE001 - 写盘失败只留痕，绝不能带崩服务
                with self._cond:
                    self.failures += 1
                    self._last_error = f"{type(exc).__name__}: {exc}"
                # 清理可能残留的临时文件；正式快照（旧版完整快照）保持不动
                try:
                    if os.path.exists(self.store.tmp_path):
                        os.remove(self.store.tmp_path)
                except OSError:
                    pass
            finally:
                with self._cond:
                    self._busy = False
                    self._cond.notify_all()


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
