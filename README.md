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
| `verify_snapshot.py` | 快照韧性验证：暂停写盘/写失败/强杀重启/4 MiB 内存边界，约 9 秒 |

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
| `GET /stats` | 批次数、已提交源偏移、在途未持久记录数、网关水位、快照状态 |
| `GET /snapshot/status` | 后台快照写盘状态：暂停/在写/待写、成功失败与跳过计数、最近错误 |
| `POST /snapshot/pause` | 暂停快照写盘（不构建 payload；入账与查询完全不受影响） |
| `POST /snapshot/resume` | 恢复快照写盘，后续记录重新触发 |
| `GET /health` | 健康检查 |

## 5. 持久化与崩溃恢复

状态目录下：

- `batches.jsonl`：**权威事实来源**。每发布一个批次即追加并 `flush + fsync`，
  行内容为确定性 JSON。强杀后已落盘的批次重启后只出现一次。
- `snapshot.json`：加速恢复的原子快照（tmp + `os.replace`），永远只反映已 fsync 的状态；
  设 `--snapshot-every 0` 可关闭，完全靠批次日志重放恢复。

恢复时先加载快照（若有），再重放快照之后的批次日志；源日志从「最后提交偏移所在行的
下一行」继续跟读，因此崩溃时已读未处理的记录会按原规则重新入账或驳回，不会重复发布。

### 快照写盘韧性（慢盘 / 暂停 / 写失败）

快照写盘运行在独立的后台守护线程，与单写账务线程解耦：

- **写盘变慢或失败不阻塞入账**：账务线程只在批次 fsync 之后构建一次快照 payload 并
  非阻塞投递，随后立即处理下一条记录；后台写盘耗时多久都不影响发布节奏，查询接口
  始终能看到后续已落盘批次及其账态。
- **成功快照只对应输入完整落盘后的账态**：payload 仅在对应批次 `flush+fsync` 之后
  构建，快照的 `next_seq` 永不超过已落盘批次；写盘仍走 tmp + `os.replace` 原子替换。
- **写失败保留旧快照**：失败只计数、记录 `last_error` 并清理临时文件，正式
  `snapshot.json`（旧版完整快照）原样保留、仍可用于恢复；快照损坏或被非法占位时
  恢复会自动退化为纯批次日志重放，服务不会因此启动失败。
- **负载与内存严格有界**：至多 1 个 payload 在写 + 1 个待写，待写位占用时新请求被
  合并（下条记录携带最新状态重试），不随积压记录数增长；`POST /snapshot/pause`
  暂停期间连 payload 都不构建，快照子系统额外内存为常数级（实测单窗口十万笔计量、
  暂停后更正一百笔，额外占用仅几十字节，远低于 4 MiB 上限）。

### 手工验证强杀恢复

```bash
# 服务运行中直接 kill -9，再用同一 --state-dir / --log-file 重启即可
kill -9 <pid>
python3 -I run_service.py --log-file ./data/run/gateway.jsonl --state-dir ./data/state
```

### 一条命令验证暂停写盘、写失败与强杀重启

```bash
python3 -I verify_snapshot.py   # 约 9 秒，28 项断言全部 ✅/❌ 可读输出
```

覆盖：十万笔单窗口计量 + 暂停后一百笔更正的 4 MiB 内存边界、慢盘积压上限（1 在写
+1 待写）、快照永不领先批次日志的三方一致性（快照恢复 / 纯批次重放 / 在线引擎）、
真实子进程下暂停写盘仍可查询后续批次账态、制造真实写失败后服务存活且旧快照可恢复、
SIGKILL 同目录重启精确一次。

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
背压约束。实测约 5 秒完成，输出逐项 ✅/❌ 可读结论。

## 8. 可选 Parquet 导出

```bash
pip install pyarrow
python3 -I run_export_parquet.py --state-dir ./data/state --out-prefix ./data/report
# 生成 report.batches.parquet 与 report.windows.parquet
```

未安装 pyarrow 时主服务不受影响，JSON Lines 始终可用且信息等价。
