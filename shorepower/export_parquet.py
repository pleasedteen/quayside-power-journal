"""把批次日志与窗口状态导出为 Parquet（可选能力）/ JSON Lines（标准库离线格式）。

运行前提：Parquet 需要 ``pip install pyarrow``。未安装时主服务不受影响，也可以用
``--format jsonl`` 走纯标准库导出；JSON Lines 始终是权威持久化，信息与 Parquet 等价。

统一读取口径
============
持续入账时服务会不断向 ``batches.jsonl`` 追加新批次。为避免“批次表”和“窗口表”读到
不同时刻的数据，每次导出先通过 :func:`shorepower.storage.load_persisted_batches`
**一次性**读出当前已 fsync 的批次列表并固定为本次导出的批次边界（``as_of_seq``）：

* 批次表只写这组批次；窗口表只由这组批次重放得到（:meth:`Engine.from_persisted_batches`），
  两张表因此必然对应同一个已持久化批次号，逐行都带 ``as_of_seq`` 可核对；
* 导出期间服务继续落盘的新批次不会混入当次窗口表，只会出现在下一次导出里。

成对原子提交
============
每次导出写入一个不可变的版本目录（``<prefix>.export/v000001/``），内含批次表、窗口表
和 ``export-manifest.json``；再通过一次原子的 ``latest`` 符号链接切换让两张表同时生效。
导出进程中途被强杀时，要么 ``latest`` 还指向上一版（两张旧表仍可成对读取），要么已经
整体指到新版本；绝不会出现一张新表配一张旧表的中间态。旧版本目录全部保留。

表字段向后兼容：原有列保持不变，仅在末尾追加可核对边界的 ``as_of_seq`` 列。
"""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Protocol

from .engine import Engine
from .model import format_ts
from .storage import BATCHES_NAME, dumps_canonical, load_persisted_batches

EXPORT_DIR_SUFFIX = ".export"
LATEST_NAME = "latest"
MANIFEST_NAME = "export-manifest.json"
EXPORT_MANIFEST_VERSION = 1

# 测试/故障注入钩子：在写完两张表、提交 latest 之前回调（自测用来在此时强杀进程）
_STALL_BEFORE_COMMIT_ENVVAR = "SHOREPOWER_EXPORT_STALL_BEFORE_COMMIT"


# --------------------------------------------------------------- 行构建（纯函数）


def build_batch_rows(batches: list[dict], as_of_seq: int) -> list[dict]:
    """把固定批次列表打平为批次表行；嵌套字段存为确定性 JSON 字符串。"""
    rows = []
    for batch in batches:
        row = {
            k: (json.dumps(v, ensure_ascii=False, sort_keys=True)
                if isinstance(v, (dict, list)) else v)
            for k, v in batch.items()
        }
        row["as_of_seq"] = as_of_seq  # 末尾追加列，旧字段保持不变
        rows.append(row)
    return rows


def build_window_rows(engine: Engine, as_of_seq: int) -> list[dict]:
    """把由固定批次重放出的引擎视图打平为窗口表行（一行一个泊位）。"""
    rows: list[dict] = []
    for wstart in sorted(engine.windows):
        win = engine.windows[wstart]
        if win["totals"]:
            for berth, energy in sorted(win["totals"].items()):
                rows.append({
                    "window": format_ts(wstart), "window_start": wstart,
                    "berth": berth, "energy": energy, "sealed": win["sealed"],
                    "as_of_seq": as_of_seq,
                })
        else:
            rows.append({
                "window": format_ts(wstart), "window_start": wstart,
                "berth": None, "energy": 0.0, "sealed": win["sealed"],
                "as_of_seq": as_of_seq,
            })
    return rows


# --------------------------------------------------------------- 表写入器


class TableWriter(Protocol):
    extension: str
    batches_file: str
    windows_file: str

    def write_batches(self, rows: list[dict], path: str) -> None: ...

    def write_windows(self, rows: list[dict], path: str) -> None: ...


class ParquetTableWriter:
    """写 Parquet（惰性导入 pyarrow，未安装时给出与旧版一致的明确报错）。"""

    extension = "parquet"

    def __init__(self) -> None:
        try:
            import pyarrow as pa  # noqa: F401
            import pyarrow.parquet as pq  # noqa: F401
        except ImportError as exc:  # pragma: no cover - 取决于环境
            raise SystemExit(
                "Parquet 导出需要 pyarrow：先执行 `pip install pyarrow`，"
                "或改用 --format jsonl（纯标准库），也可直接使用状态目录下的 "
                "JSON Lines（batches.jsonl），信息等价。"
            ) from exc

    batches_file = "batches.parquet"
    windows_file = "windows.parquet"

    def write_batches(self, rows: list[dict], path: str) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        if rows:
            pq.write_table(pa.Table.from_pylist(rows), path)
        else:
            # 空批次仍保留可识别的批次表骨架（兼容旧版空导出行为）
            pq.write_table(pa.table({"seq": pa.int64()}), path)

    def write_windows(self, rows: list[dict], path: str) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        pq.write_table(pa.Table.from_pylist(
            rows or [{"window": None, "window_start": None,
                      "berth": None, "energy": None, "sealed": None,
                      "as_of_seq": None}]),
            path)


class JsonlTableWriter:
    """纯标准库 JSON Lines 写入器，供无 pyarrow 环境与离线自测使用。"""

    extension = "jsonl"
    batches_file = "batches.jsonl"
    windows_file = "windows.jsonl"

    def write_batches(self, rows: list[dict], path: str) -> None:
        _write_jsonl(
            path,
            rows,
            empty_placeholder={"seq": None, "as_of_seq": None},
        )

    def write_windows(self, rows: list[dict], path: str) -> None:
        _write_jsonl(
            path,
            rows,
            empty_placeholder={"window": None, "window_start": None,
                               "berth": None, "energy": None, "sealed": None,
                               "as_of_seq": None},
        )


def _write_jsonl(path: str, rows: list[dict], empty_placeholder: dict) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows or [empty_placeholder]:
            fh.write(dumps_canonical(row) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


# --------------------------------------------------------------- 原子提交


def _atomic_relative_symlink(link_path: str, target_relative: str) -> None:
    """原子切换相对符号链接；不支持符号链接的平台退化为原子指针文件。

    相对链接保证整个导出目录被移动后仍然有效。目录型链接（latest）的退化形态是
    一个内容为目标相对名的指针文件，由 :func:`_resolve_latest` 统一解析。
    """
    parent = os.path.dirname(os.path.abspath(link_path))
    tmp_path = os.path.join(parent, f".{os.path.basename(link_path)}.tmp.{os.getpid()}")
    try:
        if os.path.lexists(tmp_path):
            os.unlink(tmp_path)
        os.symlink(target_relative, tmp_path)
        os.replace(tmp_path, link_path)
        try:
            dir_fd = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
    except (OSError, NotImplementedError):
        # 退化：写原子指针文件（内容即目标相对名），调用方按文件/目录两种形态解析
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write(target_relative)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, link_path)


def _resolve_latest(export_root: str) -> str | None:
    """解析 latest 指针（符号链接或退化指针文件），返回版本目录绝对路径；不存在返回 None。"""
    latest = os.path.join(export_root, LATEST_NAME)
    if os.path.isdir(latest):
        return os.path.abspath(latest)
    if os.path.isfile(latest):  # 退化指针文件
        with open(latest, encoding="utf-8") as fh:
            target = fh.read().strip()
        candidate = os.path.normpath(os.path.join(export_root, target))
        if os.path.isdir(candidate):
            return candidate
    return None


def _next_version_dir(export_root: str) -> tuple[str, int]:
    """按既有版本目录取下一个单调递增的版本号（强杀残留的孤儿目录也占号、不覆盖）。"""
    seq = 1
    if os.path.isdir(export_root):
        for name in os.listdir(export_root):
            if name.startswith("v") and name[1:].isdigit():
                seq = max(seq, int(name[1:]) + 1)
    version_dir = os.path.join(export_root, f"v{seq:06d}")
    os.makedirs(version_dir)
    return version_dir, seq


# --------------------------------------------------------------- 导出主入口


def export(state_dir: str, out_prefix: str, *, fmt: str = "parquet",
           stall_before_commit: Any = None) -> dict:
    """把状态目录按一个固定的已持久化批次边界导出为一对表。

    返回 manifest dict（同时落盘到版本目录）。``fmt`` 取 ``parquet`` 或 ``jsonl``；
    ``stall_before_commit`` 是测试钩子，在两张表写完、``latest`` 切换前回调。
    """
    if fmt == "parquet":
        writer: TableWriter = ParquetTableWriter()
    elif fmt == "jsonl":
        writer = JsonlTableWriter()
    else:
        raise ValueError(f"未知导出格式: {fmt!r}（支持 parquet/jsonl）")

    # 1) 一次性固定批次边界：之后日志再追加也与本次导出无关
    batches = load_persisted_batches(os.path.join(state_dir, BATCHES_NAME))
    as_of_seq = batches[-1]["seq"] if batches else 0

    # 2) 两张表都只由这组批次产生，窗口表重放与崩溃恢复走同一条引擎重放路径
    batch_rows = build_batch_rows(batches, as_of_seq)
    window_rows = build_window_rows(Engine.from_persisted_batches(batches), as_of_seq)

    # 3) 写入不可变版本目录（提交前对外不可见）
    export_root = f"{out_prefix}{EXPORT_DIR_SUFFIX}"
    os.makedirs(export_root, exist_ok=True)
    version_dir, version_seq = _next_version_dir(export_root)
    batches_path = os.path.join(version_dir, writer.batches_file)
    windows_path = os.path.join(version_dir, writer.windows_file)
    writer.write_batches(batch_rows, batches_path)
    writer.write_windows(window_rows, windows_path)

    if stall_before_commit is None and os.environ.get(_STALL_BEFORE_COMMIT_ENVVAR):
        # 自测故障注入：此时两张表已 fsync，但 latest 尚未切换
        stall_before_commit = lambda: None  # noqa: E731
        time.sleep(float(os.environ[_STALL_BEFORE_COMMIT_ENVVAR]))
    if stall_before_commit is not None:
        stall_before_commit()

    manifest = {
        "manifest_version": EXPORT_MANIFEST_VERSION,
        "format": fmt,
        "as_of_seq": as_of_seq,
        "version_seq": version_seq,
        "batches_file": writer.batches_file,
        "windows_file": writer.windows_file,
        "batch_rows": len(batch_rows),
        "window_rows": len(window_rows),
        "state_dir": os.path.abspath(state_dir),
    }
    manifest_path = os.path.join(version_dir, MANIFEST_NAME)
    with open(manifest_path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(manifest, sort_keys=True, ensure_ascii=False, indent=1))
        fh.flush()
        os.fsync(fh.fileno())

    # 4) 单次原子切换：latest 指向新版本后，两张表同时整体生效
    _atomic_relative_symlink(
        os.path.join(export_root, LATEST_NAME),
        os.path.basename(version_dir),
    )
    # 固定名兼容入口（旧调用方使用的 <prefix>.batches/windows.<ext>）：符号链接形态
    # 每次原子重指到 latest；若同名位置已是历史普通文件（旧版导出）则保留不动，不覆盖
    fixed_batches = f"{out_prefix}.batches.{writer.extension}"
    fixed_windows = f"{out_prefix}.windows.{writer.extension}"
    latest_rel = os.path.join(
        os.path.basename(export_root), LATEST_NAME, writer.batches_file)
    latest_win_rel = os.path.join(
        os.path.basename(export_root), LATEST_NAME, writer.windows_file)
    for fixed, rel in ((fixed_batches, latest_rel), (fixed_windows, latest_win_rel)):
        if os.path.islink(fixed) or not os.path.lexists(fixed):
            _atomic_relative_symlink(fixed, rel)

    result = dict(manifest)
    result["manifest_path"] = manifest_path
    result["batches_path"] = batches_path
    result["windows_path"] = windows_path
    result["export_root"] = export_root
    return result


# --------------------------------------------------------------- 读回与核对


def read_export(out_prefix: str) -> dict:
    """读回最近一次成功导出的 manifest 与两张表的绝对路径。

    兼容旧版布局：没有版本目录时退回固定名 ``<prefix>.batches/windows.parquet``。
    """
    export_root = f"{out_prefix}{EXPORT_DIR_SUFFIX}"
    version_dir = _resolve_latest(export_root)
    if version_dir is not None:
        manifest_path = os.path.join(version_dir, MANIFEST_NAME)
        with open(manifest_path, encoding="utf-8") as fh:
            manifest = json.load(fh)
        result = dict(manifest)
        result["manifest_path"] = os.path.abspath(manifest_path)
        result["batches_path"] = os.path.abspath(
            os.path.join(version_dir, manifest["batches_file"]))
        result["windows_path"] = os.path.abspath(
            os.path.join(version_dir, manifest["windows_file"]))
        result["export_root"] = os.path.abspath(export_root)
        return result

    # 旧版兼容：固定名普通文件（无 manifest，边界无法核对时 as_of_seq 给 None）
    legacy_batches = f"{out_prefix}.batches.parquet"
    legacy_windows = f"{out_prefix}.windows.parquet"
    if os.path.exists(legacy_batches) and os.path.exists(legacy_windows):
        return {
            "manifest_version": 0, "format": "parquet", "as_of_seq": None,
            "version_seq": 0, "batches_file": os.path.basename(legacy_batches),
            "windows_file": os.path.basename(legacy_windows),
            "batch_rows": None, "window_rows": None,
            "state_dir": None, "manifest_path": None,
            "batches_path": os.path.abspath(legacy_batches),
            "windows_path": os.path.abspath(legacy_windows),
            "export_root": os.path.abspath(export_root),
        }
    raise FileNotFoundError(f"未找到导出：{export_root!r} 下无 latest，也无旧版固定名文件")


def read_table_rows(path: str) -> list[dict]:
    """按扩展名读回表行（.jsonl 走标准库；.parquet 惰性导入 pyarrow）。"""
    if path.endswith(".jsonl"):
        with open(path, encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]
    import pyarrow.parquet as pq  # 延迟导入：无 pyarrow 环境仍可跑 JSONL 自测

    return pq.read_table(path).to_pylist()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="shorepower-export-parquet",
                                description="把状态目录导出为一对表（parquet 需 pyarrow，jsonl 纯标准库）")
    p.add_argument("--state-dir", required=True)
    p.add_argument("--out-prefix", required=True, help="输出前缀，如 ./report 生成两个表与 .export 目录")
    p.add_argument("--format", choices=("parquet", "jsonl"), default="parquet",
                   help="parquet（默认，需 pyarrow）或 jsonl（纯标准库，离线可用）")
    args = p.parse_args(argv)
    result = export(args.state_dir, args.out_prefix, fmt=args.format)
    print(f"已导出批次边界 as_of_seq={result['as_of_seq']}（版本 v{result['version_seq']:06d}）")
    print(f"  批次表: {result['batches_path']}（{result['batch_rows']} 行）")
    print(f"  窗口表: {result['windows_path']}（{result['window_rows']} 行）")
    print(f"  清单:   {result['manifest_path']}")
    return 0
