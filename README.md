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
| `test_crash_recovery.py` | 写入中/落盘后强杀恢复矩阵：截断规则、续跑、再次重启与无中断回放比对 |

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

- `batches.jsonl`：**权威事实来源**。一条源记录发布的全部批次构成一个**提交组**
  （如 progress 同时产出 decision 与 seal 时二者同组），整组拼在同一缓冲区里
  一次 `write + fsync`，组内最后一个批次带 `"group_end": true` 标记。
  强杀后已落盘的完整组重启后只出现一次。
- `snapshot.json`：加速恢复的原子快照（tmp + `os.replace`，替换后 fsync 目录），
  永远只反映已 fsync 的状态；设 `--snapshot-every 0` 可关闭，完全靠批次日志重放恢复。

### 三段式提交（写盘卡住也能读到完整旧账）

写线程对每条记录严格按以下顺序推进，查询线程只读已提交状态：

1. **prepare**：在私有暂存（深拷贝）上推演并生成整组批次，**不触碰可查询状态**；
2. **落盘**：整组一次 `write + fsync` 进批次日志；失败则丢弃暂存，旧账不变；
3. **commit**：fsync 成功后才把暂存状态原子换入，批次并入权威列表。

因此：

- 写盘卡住或强杀时，HTTP 查询读到的始终是**上一版完整旧账**——已对外可见的账
  必然已持久化，不会出现“刚查到的更正消失”；
- 同一条上报触发的 decision 与 seal 同生共死。强杀落在组内时，恢复逻辑
  （`recover_committed_batches`）把最后一个 `group_end` 之后的半截行/残缺组
  物理截断、整组回滚，源记录从原偏移重新进入流程，封账不会被漏掉。

恢复时先加载快照（若有），再重放快照之后、且属于完整提交组的批次；源日志从
「最后提交偏移所在行的下一行」继续跟读，因此崩溃时已读未处理（含被整组回滚）的
记录会按原规则重新入账或驳回，不会重复发布。续跑后的批次序号、窗口账、源日志
续读位置与无中断回放完全一致，完整旧账可直接续用。

### 手工验证强杀恢复

```bash
# 服务运行中直接 kill -9，再用同一 --state-dir / --log-file 重启即可
kill -9 <pid>
python3 -I run_service.py --log-file ./data/run/gateway.jsonl --state-dir ./data/state
```

`test_crash_recovery.py` 用环境变量在确定位置注入等价 SIGKILL 的强杀
（`os._exit`，不跑任何缓冲刷新），覆盖“写入中”和“落盘后”两个时机：

```bash
SHOREPOWER_CRASH_AT=<第几个提交组> SHOREPOWER_CRASH_MODE=write|fsync \
  python3 -I run_service.py --log-file ... --state-dir ...
# 或直接跑完整矩阵（含续跑、再次重启、与无中断回放逐字节比对）
python3 -I test_crash_recovery.py
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
背压约束。实测约 5 秒完成，输出逐项 ✅/❌ 可读结论。

## 8. 可选 Parquet 导出

```bash
pip install pyarrow
python3 -I run_export_parquet.py --state-dir ./data/state --out-prefix ./data/report
# 生成 report.batches.parquet 与 report.windows.parquet
```

未安装 pyarrow 时主服务不受影响，JSON Lines 始终可用且信息等价。
