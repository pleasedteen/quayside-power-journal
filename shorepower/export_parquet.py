"""把批次日志与窗口状态导出为 Parquet（可选能力）。

运行前提：``pip install pyarrow``。未安装时本模块不影响主服务，主服务始终
以 JSON Lines 作为权威持久化，Parquet 仅作为分析侧的离线导出格式。

导出两张表：
  * ``<out>.batches.parquet``   每个批次一行（changes 等嵌套字段存为 JSON 字符串）
  * ``<out>.windows.parquet``   每个窗口一行（窗口/泊位电量打平，一行一个泊位）
"""

from __future__ import annotations

import argparse
import json
import os

from .storage import BATCHES_NAME, iter_jsonl

def export(state_dir: str, out_prefix: str) -> tuple[str, str]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - 取决于环境
        raise SystemExit(
            "Parquet 导出需要 pyarrow：先执行 `pip install pyarrow`，"
            "或直接使用同目录下的 JSON Lines（batches.jsonl），二者信息等价。"
        ) from exc

    batch_rows: list[dict] = []
    for batch in iter_jsonl(os.path.join(state_dir, BATCHES_NAME)):
        row = {k: json.dumps(v, ensure_ascii=False, sort_keys=True)
               if isinstance(v, (dict, list)) else v
               for k, v in batch.items()}
        batch_rows.append(row)
    batches_path = f"{out_prefix}.batches.parquet"
    if batch_rows:
        pq.write_table(pa.Table.from_pylist(batch_rows), batches_path)
    else:
        pq.write_table(pa.table({"seq": pa.int64()}), batches_path)

    # 窗口状态从批次日志重放得到，保证与服务视图一致；历史归档一并读取，
    # 否则已被折叠的封账窗口会漏出（也就是 state 目录里的 history.db）
    from .engine import Engine
    from .storage import BatchJournal, HistoryStore, SnapshotStore

    journal = BatchJournal(state_dir)
    history = HistoryStore(state_dir)
    try:
        engine = Engine(
            journal=journal,
            snapshot_store=SnapshotStore(state_dir),
            history=history,
        )
        windows_view = engine.query_windows()["windows"]
    finally:
        journal.close()
        history.close()
    window_rows: list[dict] = []
    for win in windows_view:
        if win.get("error"):
            continue
        totals = win["totals"]
        if totals:
            for berth, energy in sorted(totals.items()):
                window_rows.append({
                    "window": win["window"], "window_start": win["window_start"],
                    "berth": berth, "energy": energy, "sealed": win["sealed"],
                })
        else:
            window_rows.append({
                "window": win["window"], "window_start": win["window_start"],
                "berth": None, "energy": 0.0, "sealed": win["sealed"],
            })
    windows_path = f"{out_prefix}.windows.parquet"
    pq.write_table(pa.Table.from_pylist(
        window_rows or [{"window": None, "window_start": None,
                         "berth": None, "energy": None, "sealed": None}]),
        windows_path)
    return batches_path, windows_path


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="shorepower-export-parquet",
                                description="把状态目录导出为 Parquet（需 pyarrow）")
    p.add_argument("--state-dir", required=True)
    p.add_argument("--out-prefix", required=True, help="输出前缀，如 ./report 生成两个 .parquet")
    args = p.parse_args(argv)
    paths = export(args.state_dir, args.out_prefix)
    for path in paths:
        print(f"已导出 {path}")
    return 0
