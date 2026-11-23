# 港口岸电刻钟窗口电量跟读服务

持续跟读本地 JSON Lines 电表日志，按 **UTC 刻钟窗口（15 分钟）** 归账，支持
计量更正的版本化整笔事务、全网关水位驱动的封账、本地 HTTP 查询，以及
SIGKILL 后同目录重启的精确一次恢复。仅用 Python 3.11 标准库，无第三方依赖
（Parquet 导出为可选能力，需要 `pyarrow`）。

## 1. 目录与文件

| 文件 | 作用 |
| --- | --- |
| `shorepower/model.py` | 记录解析、刻钟窗口/封账水位的纯函数、版本规则 |
| `shorepower/engine.py` | 账务引擎：归账、更正事务、封账、查询、快照与恢复 |
| `shorepower/storage.py` | 追加式 JSONL 批次日志（fsync）、原子快照、`tail -F` 跟读 |
| `shorepower/service.py` | 服务装配：有界队列背压 + 跟读线程 + 单写线程 + HTTP 接口 |
| `shorepower/generator.py` | 确定性本地模拟日志生成器 |
| `shorepower/feeder.py` | 按可配置速度把日志逐行追加到跟读目标 |
| `shorepower/cli.py` | 本地查询客户端 |
| `shorepower/export_parquet.py` | 可选：导出 Parquet（需 `pip install pyarrow`） |
| `run_*.py` | 各能力的入口脚本，均支持 `python -I` 隔离模式 |
| `selftest.py` | 端到端自测，80 秒内输出可读结果 |

## 2. 日志格式（每行一个 JSON 对象）

```jsonc
{"type":"reading","reading_id":"r1","gateway":"GW1","berth":"B1",
 "meter_ts":"2026-11-18T08:02:00Z","energy":10.0}
{"type":"correction","reading_id":"r1","version":2,"berth":"B1",
 "meter_ts":"2026-11-18T08:02:00Z","energy":12.0}
{"type":"progress","gateway":"GW1","progress_ts":"2026-11-18T08:25:00Z"}
```

- 时间必须是**带时区**的 ISO-8601，内部统一转 UTC；窗口按 `meter_ts` 向下取整到整 15 分钟。
- `reading` 为用电增量；`correction` 带 `version`，只有 **version 严格大于当前生效版本**
  才能更新同一 `reading_id`，可同时改电量、泊位、计量时间。
- `progress` 是该网关已送达数据的计量时间水位，水位不允许回退。
- 无法解析的行不影响账务，会以 `record_type=bad` 的驳回批次留痕。

## 3. 核心规则

1. **窗口归账**：增量按计量时间进入对应 UTC 刻钟窗口，按泊位累加。
2. **封账**：所有已知网关都上报过进度，且最慢水位 `>= 窗口结束 + 10 分钟` 时封账；
   开放窗口持续接纳迟到记录，封账后窗口电量冻结，任何读数/更正涉及该窗口即驳回。
3. **整笔更正事务**：一笔更正涉及的所有窗口（原窗口、目标窗口）只要有一个已封账，
   或版本不高于生效版本，就**整笔驳回、原账不变**；接受时 remove/add 等全部变化
   落在**同一个批次**里，对查询同时可见。
4. **确定性**：批次只由记录内容与到达顺序决定，与处理速度、墙钟时间无关。

## 4. 快速开始（三个终端）

```bash
# 终端 1：准备模拟日志与空状态目录，启动服务
python3 -I run_mock_log.py --out ./data/mock.jsonl
mkdir -p ./data/state ./data/run
python3 -I run_service.py \
  --log-file ./data/run/gateway.jsonl \
  --state-dir ./data/state \
  --host 127.0.0.1 --port 8080 \
  --max-inflight 1000 --snapshot-every 200

# 终端 2：按速度回放（--delay 0 为瞬时；调大即慢速，结果完全一致）
python3 -I run_replay.py --source ./data/mock.jsonl \
  --target ./data/run/gateway.jsonl --delay 0.03

# 终端 3：查询
python3 -I run_query.py windows                                   # 全部窗口
python3 -I run_query.py windows 2026-11-18T08:00:00Z 2026-11-18T08:15:00Z
python3 -I run_query.py batches --limit 10                        # 按发布顺序核对
python3 -I run_query.py batches --after-seq 10                    # 增量拉取（长轮询式对账）
python3 -I run_query.py stats                                     # 含背压队列长度/提交偏移
```

### HTTP 接口

| 路径 | 说明 |
| --- | --- |
| `GET /windows` | 全部窗口的泊位电量、合计、封账状态 |
| `GET /windows?window=A&window=B` | 一次查询多个窗口 |
| `GET /batches?after_seq=N&limit=M` | 按发布顺序分页核对批次（同更正的变化同批） |
| `GET /stats` | 批次数、已提交源偏移、在途未持久记录数、网关水位 |
| `GET /health` | 健康检查 |

## 5. 持久化与崩溃恢复

状态目录下：

- `batches.jsonl`：**权威事实来源**。每发布一个批次即追加并 `flush + fsync`，
  行内容为确定性 JSON。强杀后已落盘的批次重启后只出现一次。
  每个批次都带 `source_offset`（记录行首字节偏移）与 `next_offset`
  （下一行起点字节偏移），偏移一律按**原始字节**计：空行、CRLF 行尾、
  非 ASCII 多字节字符都按其在文件中的实际字节长度计算。
- `snapshot.json`：加速恢复的原子快照（tmp + `os.replace`），永远只反映已 fsync 的状态；
  设 `--snapshot-every 0` 可关闭，完全靠批次日志重放恢复。

恢复时先加载快照（若有），再重放快照之后的批次日志；随后直接从
`next_offset`/`resume_offset` 指向的**下一行起点**继续跟读——即使上一行很长
也不回读其正文。崩溃时已读未处理的记录会按原规则重新入账或驳回，不会重复发布。

跟读按原始字节切行：只有读到 `\n` 才认为一行完整。UTF-8 多字节字符被拆开
追加（例如 8 字节分片里只有半个字符）时，半个字符所在分片不含换行符，不会
被提前按行处理；非法字节也不会让读线程崩溃，而是置换成 `record_type=bad`
的驳回批次留痕。

### 旧版（schema 1）状态升级

旧版完整 LF 账（批次无 `next_offset`、快照无 `resume_offset`）可直接用新版
接着跑：启动时按「最后已提交记录的行首」一次性换算出续读位置，原批次、序号
与分页结果原样保留；此后新写的批次带 `next_offset`（schema 2），再重启即
直接定位下一行。

### 手工验证强杀恢复

```bash
# 服务运行中直接 kill -9，再用同一 --state-dir / --log-file 重启即可
kill -9 <pid>
python3 -I run_service.py --log-file ./data/run/gateway.jsonl --state-dir ./data/state
```

## 6. 背压

读线程在从源日志读出下一行**之前**必须先拿到一个名额（许可），写线程把该记录
的批次 fsync 落盘后才归还名额。因此无论写线程是否正在暂停落盘（如快照写入），
「已读但尚未落盘」的记录总数始终严格不超过 `--max-inflight`；当前值可通过
`/stats` 的 `inflight_unpersisted` 观察。

## 7. 一条命令跑完测试（< 80 秒）

```bash
python3 -I selftest.py
```

覆盖：瞬时 vs 慢速回放的批次/顺序/窗口状态逐字节一致、四个窗口封账电量、
版本驳回与整笔事务、同更正同批可见、SIGKILL 崩溃恢复精确一次、多窗口查询与批次分页、
8 字节分片拆字节追加（含半个 UTF-8 字符）与空行/CRLF/非 ASCII 的原始字节偏移、
暂停落盘（每条记录写快照）时已读未落盘数严格不超上限、旧版 schema 1 状态升级续跑
（原批次/序号/分页保留）、200KB 长行后强杀续跑直接定位下一行。实测约 10 秒完成，
输出逐项 ✅/❌ 可读结论。

## 8. 可选 Parquet 导出

```bash
pip install pyarrow
python3 -I run_export_parquet.py --state-dir ./data/state --out-prefix ./data/report
# 生成 report.batches.parquet 与 report.windows.parquet
```

未安装 pyarrow 时主服务不受影响，JSON Lines 始终可用且信息等价。
