# 北京 / 泰州多站点部署开发方案

> 状态：方案文档（待评审，未开始开发）
> 基准 commit：`c4cbe00`（2026-10-30）
> 涉及配置：`config/bj_config.yaml`、`config/tz_config.yaml`

---

## 1. 背景

### 1.1 现状

本系统（仓库 `db-manager`，本地路径 `/home/zhaolei/project/data_management`）目前部署在北京集群 `/nas02/project/zhaolei/pipeline/data_management`，由 `src/main.py` 常驻，用 APScheduler 串起完整链路：

1. LIMS JSON 拉取（每 30 分钟）
2. 信息单解析入库（共用同一入口）
3. 下机数据校验（每 10 分钟）
4. 分析任务生成（每 10 分钟）
5. 分析任务执行（每 5 分钟，代码存在，但 `main.py` 中默认注释未启用）

MySQL 数据库部署在北京集群 `192.168.100.13:3306`，是系统唯一的状态存储。

### 1.2 变化

测序原始数据现在全部产生并保存在**泰州集群**（`/gpfs/kw/sequencing` 一带）。过去为了让北京的分析流程能跑到数据，需要**人工把测序数据从泰州传输到北京**，再在北京完成校验、建任务、提交计算。

这带来三个问题：

- 人工传输有成本和延迟，容易漏传、错传；
- 北京需要额外保留一份原始数据副本，占用存储；
- 泰州本身具备 Nextflow / Singularity / Slurm 环境，数据在本地却要绕到北京处理，不合理。

### 1.3 约束

- **不在泰州再部署一套数据库**：库继续只有北京一份，泰州通过内网直连。
- 北京与泰州之间是内网连通，不暴露公网。
- 两个站点使用同一个仓库、同一份代码，禁止长期分叉。
- 不新增第三方依赖（沿用现有 Python 依赖）。
- 遵循集群文档的安全要求：不记录密码、私钥、token、Webhook；首次运行只读核验。

---

## 2. 目的

1. **数据不再搬家**：原始数据留在泰州本地，跨站只传输数据库里的元数据。
2. **按站点拆分职责**：北京只做数据入口（LIMS 拉取 + 入库），泰州只做下游（校验、任务生成、任务执行）。
3. **保持单一数据库**：北京 MySQL 作为全系统唯一实例，泰州读写同一个库，不做复制、不做双写。
4. **同一份代码、两份配置**：两个站点运行同一个 commit，差异全部收敛到各自的配置文件。
5. **通知不重复**：北京只发 `NEW_SAMPLE`，泰州只发 `READY_TO_RUN`。
6. **可验证、可回滚**：先只读核验，再 dry-run，再单批次真实提交，最后全量；任何一步都能退回北京单站模式。

### 非目标

- 不做数据库主从 / 双主 / 双向同步；
- 不把 LIMS 拉取搬到泰州（JSON 仍由北京侧获取）；
- 不在本次引入 API 网关或消息队列；
- 不改动业务字段映射、分组规则和状态机。

---

## 3. 当前差距

| 能力 | 现状 | 目标 |
|---|---|---|
| 按站点启停调度器 | `main.py` 硬编码注册，执行调度器被注释 | 配置开关控制，代码两边一致 |
| 站点配置 | 已提交 `bj_config.yaml` / `tz_config.yaml`，但启动仍读 `config/config.yaml` | 部署时用站点文件替换默认配置 |
| 执行器 | 只有 SGE `qsub`（`analysis_execution_service.py`） | 支持 `sge` / `slurm` / `dry-run` |
| 流程模板 | 固定读 `run.mk`（`project_type_manager.py` 写死文件名） | 配置项 `run_template` 选择模板 |
| 通知事件过滤 | 两份配置都同时包含 `NEW_SAMPLE` 和 `READY_TO_RUN` | 北京只留 `NEW_SAMPLE`，泰州只留 `READY_TO_RUN` |
| 跨站连接池 | `pool_size=10`、`max_overflow=20`、`pool_recycle=3600`，无 `pool_pre_ping` | 增加探活与超时 |
| 单实例保护 | 无 | `main.py` 入口加 `flock`（建议） |

---

## 4. 总体方案

### 4.1 架构

```
                     ┌──────────────────────────────┐
   北京集群           │  LIMS JSON（/nas02/...）      │
                     └──────────────┬───────────────┘
                                    │ 拉取 + 入库
                                    ▼
                     ┌──────────────────────────────┐
                     │  调度器：lims / input_sample  │
                     │  通知：NEW_SAMPLE             │
                     └──────────────┬───────────────┘
                                    │ 写元数据（raw_data_path 前缀为泰州路径）
                                    ▼
                     ┌──────────────────────────────┐
                     │  MySQL 8 · bio_db             │
                     │  192.168.100.13:3306（唯一）  │
                     └──────────────▲───────────────┘
                                    │ 内网 TCP 3306，读写
                     ┌──────────────┴───────────────┐
   泰州集群           │  调度器：sequencing /         │
                     │          analysis /           │
                     │          analysis_execution   │
                     │  通知：READY_TO_RUN           │
                     └──────────────┬───────────────┘
                                    │ 校验 / 生成 / 提交
                                    ▼
                     ┌──────────────────────────────┐
                     │  原始数据 /gpfs/kw/sequencing │
                     │  Slurm（sbatch）+ Singularity │
                     └──────────────────────────────┘
```

### 4.2 站点职责

| 项目 | 北京 | 泰州 |
|---|---|---|
| `scheduler.lims` | 开 | 关 |
| `scheduler.input_sample` | 开 | 关 |
| `scheduler.sequencing` | 关 | 开 |
| `scheduler.analysis` | 关 | 开 |
| `scheduler.analysis_execution` | 关 | 开（最后启用） |
| 数据库 | 读写 | 读写 |
| 通知事件 | 只发 `NEW_SAMPLE` | 只发 `READY_TO_RUN` |
| 原始数据 | 不再保留副本 | 唯一副本 |
| 作业调度 | SGE（不使用） | Slurm `sbatch` |
| 日志 | `/nas02/project/bacass/log/` | `/gpfs/kw/project/bacass/log/` |

调度器集合不重叠，因此不会有两站抢同一批数据的问题。

### 4.3 数据流

1. **入库（北京）**：拉取 LIMS JSON → 解析 → 写 `project / sample / batch / sequence` 等表；按 `sequence_run.batch_id_path` 模板生成 `sequence.raw_data_path`。此时路径前缀取 `sequence_info.sequence_data_path`，**必须是泰州可见路径**。
2. **校验（泰州）**：取 `data_status='pending'` 的序列 → 用库里的 `raw_data_path` 定位目录 → 检查 `updated.done`、`dir2/barcode` 非空 → 成功后把 `raw_data_path` 更新为最终 barcode 路径；失败且超过 2 小时则发超时告警（按序列去重）。
3. **任务生成（泰州）**：查 `data_status='valid'` 且 `process_status='no'` 的序列 → 按 `project_id + project_type` 分组 → 写 `analysis_tasks` → 生成分析目录、`input.tsv`、`run.sh` → 更新 `process_status` → 发 `READY_TO_RUN`。
4. **任务执行（泰州）**：取 `analysis_status='pending'` 的任务 → 进入分析目录 → 提交作业 → 置 `running`。
5. **通知**：入库时 `NEW_SAMPLE`（北京）；任务生成后 `READY_TO_RUN`（泰州）。

### 4.4 配置方案

#### 4.4.1 每站一份完整配置

已提交两份配置，运行时用站点文件**替换**仓库默认的 `config/config.yaml`，不引入 `--config`，也不做“基础配置 + 覆盖”：

```bash
# 北京
cp config/bj_config.yaml config/config.yaml
# 泰州
cp config/tz_config.yaml config/config.yaml
```

理由：

- 两个站点各自一个长期 clone，站点固定，切换需求不存在；
- 零代码改动，所有脚本（含手工调试入口）自动读到正确配置；
- 避免 `--config` 与模块导入时序（`notification_manager`、`notification_dispatcher` 在 import 时即构造配置）的坑。

代价与对策：`config/config.yaml` 变成站点本地文件，必须防止被 git 操作覆盖。二选一：

1. **推荐**：仓库保留 `config/config.yaml.example`，`.gitignore` 忽略真正的 `config/config.yaml`；
2. 保持现状：`git update-index --skip-worktree config/config.yaml`，同时在项目外保留一份站点配置副本。

#### 4.4.2 两份配置的差异（当前）

| 配置项 | `bj_config.yaml` | `tz_config.yaml` |
|---|---|---|
| `ingestion.lims_data_path` | `/nas02/project/bacass/LimsData/` | `/gpfs/kw/project/bacass/LimsData/` |
| `pull_request.path` | `/nas02/project/bacass/LimsData` | `/gpfs/kw/project/bacass/LimsData` |
| `sequence_info.sequence_data_path` | `/gpfs/kw/sequencing` | `/gpfs/kw/sequencing` |
| `project_type.bacass` | `/nas02/project/bacass` | `/gpfs/kw/project/bacass` |
| `logging.log_dir` | `/nas02/project/bacass/log/` | `/gpfs/kw/project/bacass/log/` |
| `job_submission.sbatch_path` | 未配置 | `/usr/bin/sbatch` |

#### 4.4.3 关键规则：北京写出的路径必须是泰州可见路径

`sequence.raw_data_path` 是北京入库时拼出来的，泰州校验直接 `stat` 这个字段，不会用本地配置重拼。因此：

```yaml
# bj_config.yaml
sequence_info:
  sequence_data_path: /gpfs/kw/sequencing   # 必须与 tz_config.yaml 一致
```

这条规则违反时的表现：入库正常、泰州校验全部失败、2 小时后开始刷超时告警。两份文件顶部都应加注释说明。

#### 4.4.4 仍需按站点补齐的配置键

以下键当前两份文件都还是同一份旧结构，需在开发任务中补齐：

```yaml
scheduler:
  lims:               { enabled: true,  interval_minutes: 30 }  # 北京 true / 泰州 false
  input_sample:       { enabled: true,  interval_minutes: 30 }  # 北京 true / 泰州 false
  sequencing:         { enabled: false, interval_minutes: 10 }  # 北京 false / 泰州 true
  analysis:           { enabled: false, interval_minutes: 10 }  # 北京 false / 泰州 true
  analysis_execution: { enabled: false, interval_minutes: 5 }   # 北京 false / 泰州 true（最后开）

job_submission:
  executor: sge          # 北京 sge / 泰州 slurm / 调试 dry-run
  run_template: run.mk   # 北京 run.mk / 泰州 tz_run.mk
  submit_enabled: false  # 首次部署必须 false

notification:
  yunzhijia:
    send_event_types: [NEW_SAMPLE]     # 北京
    # send_event_types: [READY_TO_RUN] # 泰州
  feishu:
    send_event_types: [NEW_SAMPLE]     # 北京
    # send_event_types: [READY_TO_RUN] # 泰州
```

> 注意：当前两份配置都**没有 `scheduler.input_sample` 节点**，`InputSampleScheduler` 会走默认间隔 30 分钟；且它与 `lims` 都调用同一个 `run_ingestion_process`，同时开启会重复触发（`input_file_metadata` 去重，结果幂等，但浪费一次扫描）。补齐开关时一并确认：北京是否只保留 `lims`、关闭 `input_sample`。

#### 4.4.5 共性键维护

两份文件有大量本应一致的键：`database`、`fields_mapping`、`table_update_triggers`、`project_type_map`、`project_type_to_template`、`data_flow_project_types`、`new_field_rules`、`scheduler.*.interval_minutes`。

建议增加一个轻量校验（测试或启动自检）：比对两份配置中“应一致键”的集合，不一致时输出警告。避免“只改了一边”导致行为差异难以发现。

### 4.5 调度器开关（待开发）

`src/main.py` 当前硬编码：

```python
self.schedulers = [
    LIMSScheduler(),
    InputSampleScheduler(),
    SequencingScheduler(),
    AnalysisScheduler()
    # AnalysisExecutionScheduler()
]
```

改为读取 `scheduler.<name>.enabled` 后再实例化。**不使用注释方式**，因为那会让两个站点的源码分叉，违背“同一份代码”的目标。

验收：北京实例只启动 `lims`、`input_sample`；泰州实例只启动 `sequencing`、`analysis`（执行最后开）。

### 4.6 执行器与流程模板（待开发）

#### 4.6.1 执行器

现状：`src/services/analysis_execution_service.py` 只支持 `qsub`，`job_submission` 只有 `qsub_path`。

目标：

```yaml
job_submission:
  executor: slurm
  qsub_path: "/opt/gridengine/bin/linux-x64/qsub"
  sbatch_path: "/usr/bin/sbatch"
  submit_enabled: false
```

`process_pending_tasks()` 按 `executor` 分支：

- `sge`：现有 `qsub` 逻辑；
- `slurm`：`sbatch` 提交，解析 `Submitted batch job <id>` 为 job id；
- `dry-run`：只记录将要执行的命令，不提交；
- 未知值：启动即报错。

#### 4.6.2 模板选择

`project_type_manager._get_run_sh_template_internal()` 目前写死 `run.mk`。改为读 `job_submission.run_template`，缺省 `run.mk`：

- 北京：`run.mk`（SGE，`#$ -q dev.q`、`-profile sge`）；
- 泰州：新增 `pipeline_templates/<type>/tz_run.mk`（Slurm 指令、`-profile slurm`、泰州本地的 conda / nextflow / 流程主文件路径）。

> 为什么不使用符号链接：git 会把链接目标作为被跟踪内容，切换目标同样产生未提交改动；配置项方式两份模板都能正常进 git，也不会出现“忘记建链导致不生成 run.sh”的静默失败。

### 4.7 路径与文件系统

| 用途 | 北京 | 泰州 |
|---|---|---|
| LIMS JSON | `/nas02/project/bacass/LimsData/` | `/gpfs/kw/project/bacass/LimsData/`（若泰州也入库） |
| 原始测序数据 | 不需要 | `/gpfs/kw/sequencing/...` |
| 分析输出 | `/nas02/project/bacass`、`/nas02/project/large_pla` | 待定，当前 `tz_config.yaml` 仍保留 `/home/zhaolei/project_analysis`、`/nas02/project/large_pla` |
| 日志 | `/nas02/project/bacass/log/` | `/gpfs/kw/project/bacass/log/` |

待确认：

- `tz_config.yaml` 的 `project_type.analysis`、`16SAMP`、`large_pla` 仍是北京路径，若泰州要跑这些类型必须改；
- 泰州下机目录结构是否满足校验逻辑：`批次目录 / 最新一级 / 最新二级 / updated.done`，且 `<二级>/fastq_pass/<barcode>/` 非空；
- 历史遗留：库中 `data_status='pending'` 且 `raw_data_path` 指向北京路径的序列，切换前需一次性处理（改路径或置状态），否则会持续触发超时告警。

### 4.8 通知

| 站点 | 事件 | 触发点 |
|---|---|---|
| 北京 | `NEW_SAMPLE` | `json_data_processor` 入库后 |
| 泰州 | `READY_TO_RUN` | `analysis_service` 任务生成后 |

- 两份配置的 `send_event_types` 各自只保留一个事件，两个渠道（云之家、飞书）都要改；
- 校验的超时 / 恢复告警走 `notification_manager` 直接发送，不经过事件过滤，由泰州发出；
- `log_dir` 每站独立，`notification_deduplicator` 状态也独立，正好配合事件分工；
- 两站都需要各自可用的 webhook；`project_type_filters` 目前只含两种细菌项目，新类型需同步添加。

### 4.9 数据库与网络

- **唯一实例**：北京 `192.168.100.13:3306`，`bio_db`；不做复制。
- **账号**：北京继续 `bio_writer`；泰州建议新建 `bio_tz`（业务表读写，不给 DDL），便于审计与回收；`database.users` 配置支持多角色。
- **网络**：3306 只对两站出口 IP 放行；内网直连，不暴露公网；建议启用 TLS。
- **连接池**：当前 `src/models/database.py` 无 `pool_pre_ping`，跨站链路抖动后首个请求会失败。建议增加 `pool_pre_ping=True`、`connect_timeout`、`read_timeout`，并考虑缓存引擎实例（现在每次 `get_session` 都新建引擎）。
- **断链处理**：调度器按轮次执行，失败只影响当轮；需保证异常被捕获、下一轮可重试，不在断链时堆积任务。
- **凭据**：两份配置当前含明文密码与 webhook，且已进入 git。建议改为 `DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME` 环境变量（代码已支持），配置文件只留占位。

### 4.10 并发与幂等

- 站点分工后不会互抢；风险剩泰州自身的重启、误开双实例。
- 建议：`main.py` 入口加 `flock` 单实例锁（锁文件放本地盘，不放共享文件系统）。
- 可选加固：`analysis_tasks` 增加项目组维度唯一键；任务领取改为原子更新（`FOR UPDATE SKIP LOCKED` 或 `UPDATE ... WHERE status='pending'`）；生成目录前做存在性检查。

---

## 5. 开发任务清单

| # | 模块 | 改动 | 验收 |
|---|---|---|---|
| 1 | `config/bj_config.yaml`、`config/tz_config.yaml` | 增加 `scheduler.*.enabled`、`job_submission.executor/run_template/submit_enabled`、通知事件分工 | 两份配置能通过加载校验 |
| 2 | `src/main.py` | 按 `scheduler.*.enabled` 过滤注册调度器 | 北京只起 2 个、泰州只起 3 个 |
| 3 | `src/services/project_type_manager.py` | 读 `job_submission.run_template`，缺省 `run.mk` | 泰州生成 `run.sh` 使用 `tz_run.mk` 内容 |
| 4 | `src/services/analysis_execution_service.py` | 增加 `sge` / `slurm` / `dry-run` 执行器分支与 `submit_enabled` 开关 | dry-run 不产生作业；slurm 能解析 job id |
| 5 | `pipeline_templates/<type>/tz_run.mk` | 泰州 Slurm 模板（`#SBATCH`、`-profile slurm`、本地路径） | 单批次能在 `compute2` 跑通 |
| 6 | `src/models/database.py`（可选） | `pool_pre_ping`、超时、引擎缓存 | 断链恢复后无需重启服务 |
| 7 | `src/main.py`（可选） | `flock` 单实例 | 第二个进程启动即退出并提示 |
| 8 | `tests/` | 配置一致性、执行器分支、模板选择 | 单元测试通过 |
| 9 | 文档 | 本文件 + `AGENT.md` 增加多站点说明 | 新人可按文档部署 |

---

## 6. 部署与切换步骤

### 6.1 前置核验（只读）

在泰州执行：

```bash
# 站点与工具（版本以实际为准，参考 .agents/remote-clusters/kw_tz.md）
/gpfs/kw/software/git/v2.34.1/bin/git --version
/gpfs/kw/project/zhaolei/software/nextflow/nextflow -version
module load singularity/4.2.1 && singularity --version
sbatch --version
test -r /gpfs/kw/sequencing
# 到北京库的连通性
timeout 5 bash -c 'cat < /dev/null > /dev/tcp/192.168.100.13/3306' && echo OK
```

同时确认：Python 环境、服务常驻方式、固定节点、Slurm account/qos、分析根目录权限。

### 6.2 顺序

1. **代码就位**：两站 `git fetch` + `merge --ff-only origin/main`，确认同一 commit。
2. **北京先切**：替换 `config/config.yaml` 为 `bj_config.yaml`（关下游、只发 `NEW_SAMPLE`），重启；观察日志确认只执行 `lims` / `input_sample`。
3. **泰州只开校验**：替换配置为 `tz_config.yaml`（只开 `sequencing`），启动；观察 `pending` 是否正常消化。
4. **泰州开任务生成**：`analysis` 打开，先确认生成的目录、`input.tsv`、`run.sh` 正确；`submit_enabled=false`。
5. **泰州开执行**：配置 `executor: slurm`，单批次真实提交，核对 Slurm job id 与库中状态。
6. **全量放开**：清理历史 `pending`，持续观察一周。

> 顺序不能反：泰州先跑、北京仍写旧路径，会导致新序列校验全部失败。

### 6.3 验证清单

- 日志：两站只启动预期调度器，无异常堆栈；
- 数据库：`sequence.data_status`、`process_status`、`analysis_tasks.analysis_status` 流转正确；
- 文件：泰州分析目录、`input.tsv`、`run.sh` 生成且路径本地可读；
- 作业：`squeue`/`sacct` 能看到提交的 job，与库中 `job_id` 一致；
- 通知：北京只发 `NEW_SAMPLE`，泰州只发 `READY_TO_RUN`，无重复；
- 幂等：重复执行一轮，不重复生成目录或任务。

### 6.4 回滚

1. 停泰州服务（`SIGTERM`，确认单实例锁释放）；
2. 北京恢复原配置（打开下游调度器）；
3. 泰州已生成但未提交的任务：保留 `analysis_tasks` 记录，人工确认是否删除目录；
4. 已提交到 Slurm 的作业不会被停止，需单独跟踪完成情况。

---

## 7. 风险与待确认

| 风险 | 影响 | 应对 |
|---|---|---|
| 北京/泰州路径不一致 | 泰州校验全部失败 | 北京 `sequence_data_path` 固定为 `/gpfs/kw/sequencing`；切换前用一条真实记录验证 |
| 泰州下机目录结构不匹配 | 校验永远不通过 | 用真实批次核对 `updated.done` 与 `fastq_pass/<barcode>` 层级 |
| Slurm 适配不完整 | 任务提交失败或状态不一致 | dry-run → 单批次真实提交 → 全量 |
| 跨站数据库断链 | 泰州停摆 | 连接池探活、超时、退避重试；监控告警 |
| 数据库单点 | 全系统停摆 | 定期 `mysqldump` + binlog，准备恢复演练 |
| 原始数据单副本 | 数据丢失不可恢复 | 泰州存储快照/异地备份，优先级高于数据库备份 |
| 配置漂移 | 共性键只改一边 | 配置一致性自检 |
| 明文凭据入库 | 泄露风险 | 改用环境变量，清理 git 历史中的密码 |
| 泰州误开双实例 | 重复生成/提交 | `flock` 单实例锁 |

待确认事项：

1. 泰州分析根目录（`project_type.analysis`、`16SAMP`、`large_pla`）最终路径；
2. 泰州是否只跑 `bacass` 两类细菌项目，其余类型是否进入泰州流程；
3. 泰州 webhook 是否与北京共用；
4. 泰州 Python 环境与常驻方式（screen / systemd / podman）；
5. MySQL 授权、防火墙与 TLS 由谁负责；
6. 库中 `pending` 且指向北京路径的历史序列数量与处理方式。

---

## 8. 决策记录

| 议题 | 结论 | 放弃的方案及原因 |
|---|---|---|
| 数据库 | 北京单实例，泰州直连 | 泰州再部署一套 / 主从复制：增加同步复杂度与写冲突 |
| 角色拆分 | 配置开关，非注释 | 注释 `main.py`：两站源码分叉，易漏改 |
| 配置加载 | 每站完整文件替换默认 `config/config.yaml` | `--config` 参数：import 时序复杂；基础+覆盖：本次不需要 |
| 流程模板 | 配置项 `run_template` | 符号链接：git 仍会记录目标变化，且漏建链会静默失败 |
| 通知 | 按站点分工事件 | 共享去重状态：跨站文件不可见，反而容易重复 |
| 执行器 | 抽象 `sge`/`slurm`/`dry-run` | 直接改死 `sbatch`：北京失去兼容与回滚能力 |

---

## 9. 关键代码索引

| 功能 | 位置 |
|---|---|
| 主入口 / 调度器注册 | `src/main.py` `SchedulerManager._register_all_schedulers` |
| 配置加载 | `src/utils/yaml_config.py` `YAMLConfig`、`get_yaml_config` |
| 数据库连接 | `src/models/database.py` `get_db_config`、`get_engine`、`get_session` |
| LIMS 拉取 | `src/ingestion/lims_puller.py` |
| 入库 | `src/services/ingestion_service.py`、`src/processing/json_data_processor.py` |
| 校验 | `src/services/validation_service.py`、`src/processing/sequence_validation.py` |
| 任务生成 | `src/services/analysis_service.py`、`src/query/sequence_analysis_query.py`、`src/services/project_type_manager.py` |
| 任务执行 | `src/services/analysis_execution_service.py` |
| 通知分发 | `src/notifications/dispatcher.py`、`src/utils/notification_manager.py` |
| 通知去重 | `src/utils/notification_deduplicator.py` |
| 流程模板 | `pipeline_templates/<type>/run.mk`、`parameter.yaml` |