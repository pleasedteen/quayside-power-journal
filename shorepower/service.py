"""服务装配：源日志跟读线程 + 单写线程账务引擎 + 本地 HTTP 查询接口。

背压语义：读线程在从源日志读出下一行**之前**必须先拿到一个名额（许可），
写线程把该记录的批次 fsync 落盘后才归还名额。因此无论写线程是否正在暂停落盘
（如快照写入），“已读但尚未落盘”的记录总数始终严格不超过 ``--max-inflight``。
"""

from __future__ import annotations

import argparse
import queue
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .engine import Engine
from .model import parse_line
from .storage import BATCHES_NAME, JournalWriter, SnapshotStore, TailReader, dumps_canonical


class _InflightPermits:
    """“已读未落盘”记录的名额信号量：总量严格 <= limit。"""

    def __init__(self, limit: int):
        self.limit = limit
        self._available = limit
        self._cond = threading.Condition()

    def acquire(self, timeout: float = 0.1) -> bool:
        """拿一个名额；超时返回 False（便于响应停止信号）。"""
        with self._cond:
            deadline = time.monotonic() + timeout
            while self._available == 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)
            self._available -= 1
            return True

    def release(self) -> None:
        with self._cond:
            self._available += 1
            self._cond.notify()

    @property
    def used(self) -> int:
        """已读未落盘的记录数（含队列中、处理中、读取中各占一个名额）。"""
        with self._cond:
            return self.limit - self._available


class _Reader(threading.Thread):
    def __init__(self, path: str, start_offset: int, out: queue.Queue,
                 stop: threading.Event, permits: _InflightPermits):
        super().__init__(name="log-reader", daemon=True)
        self.tail = TailReader(path, start_offset=start_offset)
        self.out = out
        self.stop = stop
        self.permits = permits

    def run(self) -> None:
        try:
            while not self.stop.is_set():
                # 先占名额再读：保证“已读未落盘”总数严格 <= 上限
                if not self.permits.acquire():
                    continue
                raw = self.tail.readline()
                if raw is None:
                    self.permits.release()
                    self.stop.wait(self.tail.poll_interval)
                    continue
                # 偏移按原始字节算：行首 = 下一行起点 - 行字节长度
                offset = self.tail.offset - len(raw)
                # 只有看到 \n 的完整行才会到这里；半个 UTF-8 字符所在的分片
                # 不含换行，不会被提前按行处理。解码失败也不致命：置换成坏行留痕
                text = raw.decode("utf-8", errors="replace")
                rec = parse_line(text, offset=offset, end_offset=self.tail.offset)
                self.out.put(rec)  # 名额随记录移交写线程，落盘后由写线程归还
        finally:
            self.tail.close()


def _legacy_resume_offset(log_path: str, committed_offset: int) -> int:
    """旧版（schema 1）状态的一次性续读位置换算。

    旧格式只存“最后已提交记录的行首偏移”，行尾位置需扫该行一次（旧版完整
    LF 账保证该行以换行结尾）。新版状态直接持久化 resume_offset，重启不再
    回读上一行正文。committed_offset < 0 表示尚未提交任何记录，从头读，
    绝不跳过预写日志的首行。
    """
    if committed_offset < 0:
        return 0
    try:
        with open(log_path, "rb") as fh:
            fh.seek(committed_offset)
            return committed_offset + len(fh.readline())
    except FileNotFoundError:
        return committed_offset + 1  # 与旧版行为一致：文件缺失时在提交偏移之后等待


def _make_handler(engine: Engine, get_inflight: object):
    class Handler(BaseHTTPRequestHandler):
        server_version = "ShorePowerLedger/1.0"

        def log_message(self, fmt, *args):  # 安静些，日志走批次文件
            return

        def _send(self, code: int, payload: dict) -> None:
            body = (dumps_canonical(payload) + "\n").encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802 - stdlib 命名
            url = urlparse(self.path)
            try:
                if url.path == "/health":
                    self._send(200, {"status": "ok"})
                elif url.path == "/windows":
                    qs = parse_qs(url.query)
                    ids = qs.get("window") or None
                    self._send(200, engine.query_windows(ids))
                elif url.path == "/batches":
                    qs = parse_qs(url.query)
                    after = int(qs.get("after_seq", ["0"])[0])
                    limit_raw = qs.get("limit", [None])[0]
                    limit = int(limit_raw) if limit_raw is not None else None
                    if after < 0 or (limit is not None) and limit < 1:
                        raise ValueError("after_seq 需 >=0，limit 需 >=1")
                    self._send(200, engine.query_batches(after, limit))
                elif url.path == "/stats":
                    self._send(200, engine.stats(inflight=get_inflight()))
                else:
                    self._send(404, {"error": "未知路径", "paths": [
                        "/health", "/windows?window=...", "/batches?after_seq=..&limit=..", "/stats"]})
            except (ValueError, KeyError) as exc:
                self._send(400, {"error": f"请求参数非法: {exc}"})

    return Handler


def run_service(args: argparse.Namespace) -> int:
    if args.max_inflight < 1:
        raise SystemExit("--max-inflight 必须 >= 1（已读未落盘记录数的严格上限）")
    snapshot_store = SnapshotStore(args.state_dir)
    engine = Engine(snapshot_store=snapshot_store)
    journal = JournalWriter(f"{args.state_dir.rstrip('/')}/{BATCHES_NAME}")

    # 续读位置：新版状态直接由快照/批次日志给出（下一行起点，无需回读上一行
    # 正文，即使上一行很长）；旧版(schema 1)状态启动时一次性换算，此后按新版运行
    start_offset = engine.resume_offset
    if start_offset is None:
        start_offset = _legacy_resume_offset(args.log_file, engine.committed_offset)
    permits = _InflightPermits(args.max_inflight)
    inflight: queue.Queue = queue.Queue(maxsize=args.max_inflight)
    stop = threading.Event()
    reader = _Reader(args.log_file, start_offset, inflight, stop, permits)
    reader.start()

    httpd = ThreadingHTTPServer((args.host, args.port),
                                _make_handler(engine, lambda: permits.used))
    httpd_thread = threading.Thread(target=httpd.serve_forever, name="http", daemon=True)
    httpd_thread.start()

    def _graceful(_signum, _frame):
        stop.set()

    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)

    try:
        while not stop.is_set():
            try:
                rec = inflight.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                # 引擎处理与批次落盘在同一线程顺序完成 -> 崩溃后批次精确一次
                for batch in engine.process(rec):
                    journal.append(batch)
                engine.maybe_snapshot(args.snapshot_every)
            finally:
                permits.release()  # 记录已落盘，归还“已读未落盘”名额
    finally:
        stop.set()
        httpd.shutdown()
        reader.join(timeout=2.0)
        engine.take_snapshot()
        journal.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="shorepower-service",
        description="港口岸电刻钟窗口电量跟读服务（JSON Lines 本地持久化，可崩溃恢复）",
    )
    p.add_argument("--log-file", required=True, help="待跟读的电表网关 JSON Lines 日志路径")
    p.add_argument("--state-dir", required=True, help="批次日志/快照等恢复数据目录")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument(
        "--max-inflight",
        type=int,
        default=1000,
        help="已读但尚未持久处理的记录数上限（背压阈值，默认 1000）",
    )
    p.add_argument(
        "--snapshot-every",
        type=int,
        default=200,
        help="每处理多少条记录写一次状态快照（0 表示关闭快照，仅靠批次日志恢复）",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return run_service(args)
