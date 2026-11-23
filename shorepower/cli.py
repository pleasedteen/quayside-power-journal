"""本地查询客户端：窗口电量/封账状态、批次流、运行统计。"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

from .model import format_ts


def _get(base: str, path: str, params: list[tuple[str, str]] | None = None) -> dict:
    url = base.rstrip("/") + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"无法连接服务 {url}: {exc.reason}") from exc


def _print_windows(payload: dict) -> None:
    print(f"全网关已上报进度: {payload['all_gateways_reported']}  "
          f"最慢水位: {payload['slowest_progress']}  "
          f"封账宽限: {payload['seal_grace_seconds']}s  "
          f"封账水位: {payload.get('sealed_through')}")
    for w in payload["windows"]:
        if "error" in w:
            print(f"  [错误] {w['error']}")
            continue
        if w["sealed"]:
            # 已封账：空窗（零电量）与有电量窗口都冻结，标注首封批次便于核对
            mark = "已封账·空窗" if not w["totals"] else "已封账"
            if w.get("sealed_by_seq") is not None:
                mark += f"(首封批次#{w['sealed_by_seq']})"
        else:
            # 待封账：零电量仍可能被迟到读数/更正改写
            mark = "待封账·空窗" if not w["totals"] else "开放中"
        details = ", ".join(f"{berth}={energy}" for berth, energy in w["totals"].items()) or "-"
        print(f"  {w['window']}  [{mark}]  合计={w['total_energy']}  分泊位: {details}")


def _print_batches(payload: dict) -> None:
    print(f"after_seq={payload['after_seq']}  last_seq={payload['last_seq']}  "
          f"本批返回 {len(payload['batches'])} 条")
    for b in payload["batches"]:
        if b["kind"] == "seal":
            wins = ", ".join(w["window"] for w in b["sealed_windows"]) or "（无物化窗口）"
            if "sealed_through" in b:
                # schema 2：封账水位区间 (sealed_from, sealed_through]，空窗隐式覆盖
                frm = b["sealed_from"]
                frm_txt = format_ts(frm) if frm is not None else "起点"
                print(f"  #{b['seq']:>4} SEAL   触发偏移={b['trigger_offset']} "
                      f"水位={b['watermark_progress']}(+{b['grace_seconds']}s) "
                      f"封账水位 {frm_txt} -> {format_ts(b['sealed_through'])}  "
                      f"物化窗口: {wins}")
            else:  # 旧格式批次（逐窗列举）
                print(f"  #{b['seq']:>4} SEAL   触发偏移={b['trigger_offset']} "
                      f"水位={b['watermark_progress']}(+{b['grace_seconds']}s) 封账: {wins}")
            continue
        status = b["status"].upper()
        if b["record_type"] == "bad":
            print(f"  #{b['seq']:>4} {status:<8} 坏行 偏移={b['source_offset']} 原因: {b['reason']}")
            continue
        rid = b.get("reading_id", "-")
        print(f"  #{b['seq']:>4} {status:<8} {b['record_type']:<10} id={rid} "
              f"偏移={b['source_offset']}" + (f" v{b['version']}" if "version" in b else ""))
        if b["status"] == "rejected":
            print(f"           驳回原因: {b['reason']}")
        for ch in b.get("changes", []):
            print(f"           {ch['window']} 泊位={ch['berth']:<6} {ch['change']:<6} "
                  f"电量={ch['energy']:<8} 窗口泊位合计={ch['total']}")
        if b["record_type"] == "progress" and b["status"] == "accepted":
            print(f"           新水位: {b['new_progress']}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="shorepower-query", description="岸电账务服务本地查询客户端")
    p.add_argument("--base-url", default="http://127.0.0.1:8080")
    sub = p.add_subparsers(dest="cmd", required=True)

    pw = sub.add_parser("windows", help="查询一个或多个刻钟窗口（不给窗口则返回全部）")
    pw.add_argument("window", nargs="*", help="窗口标识，如 2026-11-18T08:15Z，可多个")

    pb = sub.add_parser("batches", help="按发布顺序核对批次")
    pb.add_argument("--after-seq", type=int, default=0, help="只返回该序号之后的批次")
    pb.add_argument("--limit", type=int, default=None)
    pb.add_argument("--raw", action="store_true", help="直接输出原始 JSON")

    sub.add_parser("stats", help="运行统计（含背压队列长度、已提交偏移）")
    sub.add_parser("health", help="健康检查")

    args = p.parse_args(argv)
    if args.cmd == "health":
        print(json.dumps(_get(args.base_url, "/health"), ensure_ascii=False))
    elif args.cmd == "stats":
        print(json.dumps(_get(args.base_url, "/stats"), ensure_ascii=False, indent=2))
    elif args.cmd == "windows":
        params = [("window", w) for w in args.window]
        payload = _get(args.base_url, "/windows", params or None)
        _print_windows(payload)
    elif args.cmd == "batches":
        params = [("after_seq", str(args.after_seq))]
        if args.limit:
            params.append(("limit", str(args.limit)))
        payload = _get(args.base_url, "/batches", params)
        if args.raw:
            print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            _print_batches(payload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
