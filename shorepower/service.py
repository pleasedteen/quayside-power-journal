"""服务装配：源日志跟读线程 + 单写线程账务引擎 + 本地 HTTP 查询接口。

背压语义：源日志行先进入容量为 ``max_inflight`` 的有界队列；下游发布变慢时，
跟读线程在 ``put`` 处阻塞，使“已读但尚未持久处理”的记录数始终不超过该上限。
"""

from __future__ import annotations

import argparse
import json
import queue
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .engine import Engine
from .model import parse_line
from .storage import BATCHES_NAME, JournalWriter, SnapshotStore, TailReader, dumps_canonical


class _Reader(threading.Thread):
    def __init__(self, path: str, start_offset: int, out: queue.Queue, stop: threading.Event):
        super().__init__(name="log-reader", daemon=True)
        self.tail = TailReader(path, start_offset=start_offset)
        self.out = out
        self.stop = stop

    def run(self) -> None:
        try:
            while not self.stop.is_set():
                line = self.tail.readline()
                if line is None:
                    self.stop.wait(self.tail.poll_interval)
                    continue
                offset = self.tail.offset - len(line.encode("utf-8"))
                rec = parse_line(line, offset=offset)
                self.out.put(rec)  # 有界队列：下游慢时在这里阻塞，形成背压
        finally:
            self.tail.close()


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
    snapshot_store = SnapshotStore(args.state_dir)
    engine = Engine(snapshot_store=snapshot_store)
    journal = JournalWriter(f"{args.state_dir.rstrip('/')}/{BATCHES_NAME}")

    # 从上次已提交偏移继续跟读。committed_offset 是记录起始偏移，其行尾换行在
    # 起始+记录长度处；崩溃时可能已读未处理，续读必须从下一行起点开始，避免重读。
    start_offset = engine.committed_offset + 1
    log_path = args.log_file
    try:
        with open(log_path, "rb") as fh:
            fh.seek(start_offset)
            tail = fh.readline()
            if tail:
                start_offset += len(tail)
    except FileNotFoundError:
        pass
    inflight: queue.Queue = queue.Queue(maxsize=args.max_inflight)
    stop = threading.Event()
    reader = _Reader(args.log_file, start_offset, inflight, stop)
    reader.start()

    httpd = ThreadingHTTPServer((args.host, args.port), _make_handler(engine, inflight.qsize))
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
            # 引擎处理与批次落盘在同一线程顺序完成 -> 崩溃后批次精确一次
            for batch in engine.process(rec):
                journal.append(batch)
            engine.maybe_snapshot(args.snapshot_every)
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
