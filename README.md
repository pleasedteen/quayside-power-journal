# 港口岸电刻钟窗口电量跟读服务

持续跟读本地 JSON Lines 电表日志，按 **UTC 刻钟窗口（15 分钟）** 归账，支持
计量更正的版本化整笔事务、全网关水位驱动的封账、本地 HTTP 查询，以及
SIGKILL 后同目录重启的精确一次恢复。仅用 Python 3.11 标准库，无第三方依赖
（Parquet 导出为可选能力，需要 `pyarrow`）。

## 1. 目录与文件

| 文件 | 作用 |
| --- | --- |
| `shorepower/model.py` | 记录解析、刻钟窗口/封账水位的纯函数、版本规则、来源 `(段号, 行号)` 回填 |
| `shorepower/engine.py` | 账务引擎：归账、更正事务、封账、查询、快照与恢复、批次来源定位 |
| `shorepower/storage.py` | 追加式 JSONL 批次日志（fsync、成组提交）、原子快照、单文件/编号分段跟读（`SourceReader`）、分段写出（`SegmentWriter`） |
| `shorepower/service.py` | 服务装配：有界队列背压 + 跟读线程 + 单写线程 + HTTP 接口 |
| `shorepower/generator.py` | 确定性本地模拟日志生成器 |
| `shorepower/feeder.py` | 按可配置速度把日志逐行追加到跟读目标（可写单文件或编号分段） |
| `shorepower/cli.py` | 本地查询客户端（批次输出含来源 `s段号:行号`） |
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

## 2.1 源日志布局：单文件或网关编号分段

同一个 `--log-file` 入口同时支持两种布局，`--log-layout auto|file|segments` 控制
（默认 `auto`：存在 `--log-file` 对应的段文件即按分段读，否则读单文件）。

| 布局 | 源文件 | 来源定位 |
| --- | --- | --- |
| 单文件 | `--log-file` 指向的 JSONL，持续追加 | 段号恒为 1，行号为文件内 1 起行号 |
| 编号分段 | `<log-file>.000001`、`<log-file>.000002` … | 段号为文件名编号，行号为段内 1 起行号 |

网关写分段的约定：

1. 段文件按编号递增滚动，旧段写完后在**段旁**原子写出 `<段路径>.sealed`
   封存标记（内容为 `{"segment":N,"lines":L,"bytes":B}`），封存即冻结；
2. 新段到达时旧段可能仍有未读积压，甚至旧段尚未封存。

服务跟读规则（`SourceReader`）：

- 严格按 **段号升序 → 段内行序升序** 读取，一条都不跳过；
- 当前段读到 EOF 后，**只有该段已封存**才换到下一段；旧段未封存则等待，
  绝不越过未封存的旧段去读新段（因此“新段已到、旧段仍有积压”不会打乱顺序）；
- 封存标记里的 `bytes` 是权威长度，段封存后的任何追加都不会被读入。

## 3. 核心规则

1. **窗口归账**：增量按计量时间进入对应 UTC 刻钟窗口，按泊位累加。
2. **封账**：所有已知网关都上报过进度，且最慢水位 `>= 窗口结束 + 10 分钟` 时封账；
   开放窗口持续接纳迟到记录，封账后窗口电量冻结，任何读数/更正涉及该窗口即驳回。
3. **整笔更正事务**：一笔更正涉及的所有窗口（原窗口、目标窗口）只要有一个已封账，
   或版本不高于生效版本，就**整笔驳回、原账不变**；接受时 remove/add 等全部变化
   落在**同一个批次**里，对查询同时可见。
4. **确定性**：批次只由记录内容与到达顺序决定，与处理速度、墙钟时间无关。
5. **来源可追溯**：每条发布批次都带 `source_segment` / `source_line` /
   `source_offset`，能唯一指回来源段与源行；一条记录若同时触发封账，其决定批次与
   封账批次共享同一来源位置与 `record_batch_count`（=该记录的批次组大小）。

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
python3 -I run_query.py batches --limit 10                        # 按发布顺序核对（含来源 s段:行）
python3 -I run_query.py batches --after-seq 10                    # 增量拉取（长轮询式对账）
python3 -I run_query.py stats                                     # 含背压队列长度/提交位置
```

### 按编号分段跟读

```bash
# 终端 1：--log-file 给段前缀（同一入口）
python3 -I run_service.py --log-file ./data/run/gateway \
  --state-dir ./data/state --port 8080

# 终端 2：每 10 行滚一段，写完即封存；--seal-lag 2 可让最近两段暂不封存
python3 -I run_replay.py --source ./data/mock.jsonl \
  --target ./data/run/gateway --segment-lines 10 --delay 0.03
```

服务会先读尽并等待旧段封存，再进入下一段；`/stats` 的
`committed_source_segment` / `committed_source_line` 给出当前提交位置。

### HTTP 接口

| 路径 | 说明 |
| --- | --- |
| `GET /windows` | 全部窗口的泊位电量、合计、封账状态 |
| `GET /windows?window=A&window=B` | 一次查询多个窗口 |
| `GET /batches?after_seq=N&limit=M` | 按发布顺序分页核对批次（同更正的变化同批，含 `source_segment`/`source_line`） |
| `GET /stats` | 批次数、已提交来源段/行/偏移、在途未持久记录数、网关水位 |
| `GET /health` | 健康检查 |

## 5. 持久化与崩溃恢复

状态目录下：

- `batches.jsonl`：**权威事实来源**。一条输入记录产生的全部批次（决定批次 + 随它触发的
  封账批次）在**一次 write** 中追加并 `flush + fsync`，行内容为确定性 JSON，因此要么整组
  可见、要么整组不可见。强杀后已落盘的批次重启后只出现一次。
- `snapshot.json`：加速恢复的原子快照（tmp + `os.replace`），永远只反映已 fsync 的状态；
  设 `--snapshot-every 0` 可关闭，完全靠批次日志重放恢复。

恢复流程：

1. 先加载快照（若有），再重放快照之后的完整批次组；日志末尾若有未提交的残组，
   `load_journal` 会丢弃它并把日志截断到最后一个完整组的边界。
2. 从批次日志末条记录的 **`(段号, 行号, 段内偏移)`** 继续：`SourceReader` 跳过该已提交行，
   从下一行开始，所以崩溃时已读未处理的记录会按原规则重新入账或驳回，不会重复发布。
3. 段号/行号同时用于校验断点：若源段被截断导致恢复点找不到完整行，服务会直接报错，
   而不是静默错位。

### 手工验证强杀恢复

```bash
# 服务运行中直接 kill -9，再用同一 --state-dir / --log-file 重启即可（单文件或分段都支持）
kill -9 <pid>
python3 -I run_service.py --log-file ./data/run/gateway --state-dir ./data/state
```

## 6. 背压

读日志线程与账务线程之间是容量 `--max-inflight` 的有界队列。发布变慢时读线程在入队处
阻塞，使「已读但尚未持久处理」的记录数始终不超过该上限；当前值可通过 `/stats` 的
`inflight_unpersisted` 观察。

## 7. 一条命令跑完测试（< 80 秒）

```bash
python3 -I selftest.py
```

覆盖：瞬时 vs 慢速回放的批次/顺序/窗口状态逐字节一致、四个窗口封账电量、
版本驳回与整笔事务、同更正同批可见、SIGKILL 崩溃恢复精确一次、多窗口查询与批次分页、
编号分段跟读与来源追溯（每条批次对回真实源段/源行）、换段前强杀恢复、换段后强杀且
旧段仍积压时不越段、最终批次顺序/来源/窗口与连续运行一致，以及背压约束。
实测约 7 秒完成，输出逐项 ✅/❌ 可读结论。

## 8. 可选 Parquet 导出

```bash
pip install pyarrow
python3 -I run_export_parquet.py --state-dir ./data/state --out-prefix ./data/report
# 生成 report.batches.parquet 与 report.windows.parquet
```

未安装 pyarrow 时主服务不受影响，JSON Lines 始终可用且信息等价。
