"""服务装配：源日志跟读线程 + 单写线程账务引擎 + 本地 HTTP 查询接口。

源日志可以是单文件，也可以是网关按编号滚动出的多段（见 :mod:`storage`）。
跟读线程统一交给 :class:`~shorepower.storage.SourceReader`：

* 严格按段号升序、段内行序升序读取；上一段即使写完了、下一段已经出现，
  也先把上一段**已封存**范围内的积压读尽才换段（未封存则等待，不能越段）。
* 每行带上来源 ``(段号, 行号, 段内偏移)``，随批次一起持久化并对外可查。

背压语义：源日志行先进入容量为 ``max_inflight`` 的有界队列；下游发布变慢时，
跟读线程在 ``put`` 处阻塞，使“已读但尚未持久处理”的记录数始终不超过该上限。
"""

from __future__ import annotations

import argparse
import os
import queue
import signal
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .engine import Engine
from .model import parse_line
from .storage import (
    BATCHES_NAME,
    JournalWriter,
    SnapshotStore,
    SourceReader,
    dumps_canonical,
    load_journal,
)


class _Reader(threading.Thread):
    def __init__(
        self,
        path: str,
        layout: str,
        resume: tuple[int, int, int] | None,
        out: queue.Queue,
        stop: threading.Event,
    ):
        super().__init__(name="log-reader", daemon=True)
        self.source = SourceReader(path, layout=layout, resume=resume)
        self.out = out
        self.stop = stop
        self.error: BaseException | None = None

    def run(self) -> None:
        try:
            while not self.stop.is_set():
                item = self.source.readline()
                if item is None:
                    self.stop.wait(self.source.poll_interval)
                    continue
                segment, line_no, offset, raw = item
                rec = parse_line(
                    raw, offset=offset, segment=segment, line_no=line_no
                )
                self.out.put(rec)  # 有界队列：下游慢时在这里阻塞，形成背压
        except BaseException as exc:  # 源日志被截断/改写等致命错误
            self.error = exc
            self.stop.set()
        finally:
            self.source.close()


def _repair_journal_tail(path: str) -> None:
    """启动时按「完整批次组」校验批次日志，把末尾未提交的残组截掉。

    正常 SIGKILL 下每行写入后即 fsync、且一条记录的批次是一次 write，
    不会出现半组；这里仍做防御性校验，保证追加位置永远落在组边界上。
    """
    _, valid_bytes = load_journal(path)
    if not os.path.exists(path):
        return
    size = os.path.getsize(path)
    if valid_bytes >= size:
        return
    with open(path, "r+b") as fh:
        fh.truncate(valid_bytes)
        fh.flush()
        os.fsync(fh.fileno())


def _make_handler(engine: Engine, get_inflight: object):
    class Handler(BaseHTTPRequestHandler):
        server_version = "ShorePowerLedger/1.1"

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
    journal_path = os.path.join(args.state_dir, BATCHES_NAME)
    # 恢复时若发现末尾残组，先截断再打开追加器
    os.makedirs(args.state_dir, exist_ok=True)
    _repair_journal_tail(journal_path)
    journal = JournalWriter(journal_path)

    # 从最后一次已提交的 (段号, 行号, 段内偏移) 继续：SourceReader 会跳过该行。
    # 旧版批次日志只有偏移（段号/行号为 0），也照此续读并按偏移反推行号。
    resume = None
    if engine.committed_offset >= 0:
        resume = (engine.committed_segment or 1, engine.committed_line, engine.committed_offset)
    inflight: queue.Queue = queue.Queue(maxsize=args.max_inflight)
    stop = threading.Event()
    reader = _Reader(args.log_file, args.log_layout, resume, inflight, stop)
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
            # 引擎处理与批次落盘在同一线程顺序完成 -> 崩溃后批次精确一次；
            # 一条记录的全部批次一次写入并 fsync，保证不会只持久化半组
            journal.append_many(engine.process(rec))
            engine.maybe_snapshot(args.snapshot_every)
    finally:
        stop.set()
        httpd.shutdown()
        reader.join(timeout=2.0)
        engine.take_snapshot()
        journal.close()
    if reader.error is not None:
        # 源日志不可继续跟读（截断/改写/恢复点缺失）：明确失败，不静默错位
        print(f"源日志跟读失败: {reader.error}", file=sys.stderr)
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="shorepower-service",
        description="港口岸电刻钟窗口电量跟读服务（JSON Lines 本地持久化，可崩溃恢复）",
    )
    p.add_argument(
        "--log-file",
        required=True,
        help=(
            "待跟读的电表网关 JSON Lines 路径。单文件布局直接读该文件；"
            "分段布局下作为段前缀 <前缀>.000001 等"
        ),
    )
    p.add_argument(
        "--log-layout",
        choices=("auto", "file", "segments"),
        default="auto",
        help=(
            "源日志布局：auto=存在 <前缀>.NNNNNN 段文件则按编号分段读，否则按单文件读；"
            "file=强制单文件；segments=强制编号分段（默认 auto）"
        ),
    )
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
