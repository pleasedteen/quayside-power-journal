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
| `shorepower/export_parquet.py` | 可选：导出 Parquet（需 `pip install pyarrow`）；同口径 JSONL 导出纯标准库可用 |
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
- `snapshot.json`：加速恢复的原子快照（tmp + `os.replace`），永远只反映已 fsync 的状态；
  设 `--snapshot-every 0` 可关闭，完全靠批次日志重放恢复。

恢复时先加载快照（若有），再重放快照之后的批次日志；源日志从「最后提交偏移所在行的
下一行」继续跟读，因此崩溃时已读未处理的记录会按原规则重新入账或驳回，不会重复发布。

### 手工验证强杀恢复

```bash
# 服务运行中直接 kill -9，再用同一 --state-dir / --log-file 重启即可
kill -9 <pid>
python3 -I run_service.py --log-file ./data/run/gateway.jsonl --state-dir ./data/state
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
背压约束、导出统一批次边界（并发入账不混批 + 导出进程任意时点 SIGKILL 后旧版两表成对可读）。
实测约 6 秒完成，输出逐项 ✅/❌ 可读结论。无 pyarrow 环境自动以 JSONL 格式覆盖导出语义，
检测到 pyarrow 时 jsonl/parquet 两种格式都会验证。

## 8. 可选 Parquet 导出（统一批次边界、成对原子提交）

```bash
pip install pyarrow
python3 -I run_export_parquet.py --state-dir ./data/state --out-prefix ./data/report
# 未安装 pyarrow 时可改用纯标准库的同口径 JSONL 导出：
python3 -I run_export_parquet.py --state-dir ./data/state --out-prefix ./data/report --format jsonl
```

未安装 pyarrow 时主服务不受影响，JSON Lines 始终可用且信息等价。

### 8.1 统一读取口径

持续入账时服务会不断向 `batches.jsonl` 追加新批次。每次导出开始时先**一次性**读出
当前已 fsync 的批次列表并固定为本次导出的批次边界：

- 边界批次号写入 manifest 与两张表每一行的 `as_of_seq` 列（旧列保持不变，仅末尾追加）；
- 批次表只写这组批次；窗口表只由这组批次经引擎重放得到，与崩溃恢复走同一条重放路径
  （`Engine.from_persisted_batches`），因此两张表必然对应同一个已持久化批次号；
- 导出期间新批次继续落盘，但不会混入当次导出的窗口表，只会出现在下一次导出里。

### 8.2 目录布局与强杀语义

```
<out-prefix>.export/
  v000001/  batches.{parquet,jsonl}  windows.{parquet,jsonl}  export-manifest.json
  v000002/  ...                       ...                       ...
  latest -> v000002/                  # 原子符号链接（不支持符号链接时退化为指针文件）
<out-prefix>.batches.{parquet,jsonl} # 兼容旧调用方的固定名入口，指向 latest
<out-prefix>.windows.{parquet,jsonl}
```

每个版本目录不可变、旧版本全部保留；`latest` 只做一次原子切换，两张表同时生效。
导出进程中途被 SIGKILL 时：尚未提交的版本留在目录里但没有 `latest` 指向，上一版的
两张表仍可成对读取，绝不会出现一张新表配一张旧表。`read_export()` 可按前缀读回
最近一次成功导出的 manifest 与成对表路径；旧版固定名普通文件布局仍被识别。
