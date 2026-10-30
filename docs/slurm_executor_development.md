# Slurm 执行器开发方案

> 开发分支：`feature/slurm-executor`
>
> 范围：只实现分析任务提交执行器，不修改数据库结构、不修改流程模板、不编写测试。

## 1. 背景

当前 `AnalysisExecutionService` 直接读取 `job_submission.qsub_path`，并在业务代码中执行：

```text
qsub run.sh
```

泰州集群使用 Slurm，需要支持：

```text
sbatch run.sh
```

如果直接把 `qsub` 替换为 `sbatch`，北京环境将失去兼容性，提交命令也会继续和任务状态处理混在一起。因此本次把作业提交抽成独立执行器，并通过配置选择 SGE 或 Slurm。

## 2. 目标

1. 新增统一的作业执行器接口。
2. 保留现有 SGE `qsub` 提交能力。
3. 新增 Slurm `sbatch` 提交能力。
4. 通过 `job_submission.executor` 选择执行器。
5. 使用参数列表调用命令，不启用 shell。
6. 提交成功后保持现有行为，将 `analysis_status` 更新为 `running`。
7. Slurm 提交成功时解析并记录 job ID，但本次不修改数据库表保存 job ID。

## 3. 配置

北京或旧环境保持：

```yaml
job_submission:
  executor: sge
  qsub_path: /opt/gridengine/bin/linux-x64/qsub
  submit_timeout_seconds: 60
```

泰州使用：

```yaml
job_submission:
  executor: slurm
  sbatch_path: /usr/bin/sbatch
  submit_timeout_seconds: 60
```

兼容规则：未配置 `executor` 时默认使用 `sge`，保证现有配置行为不变。执行器名称不在支持列表中时直接报错，不静默选择其他命令。

## 4. 代码结构

新增 `src/executors/job_executor.py`：

- `SubmissionResult`：统一返回提交成功状态、退出码、标准输出、错误输出和可选 job ID。
- `JobExecutor`：执行器抽象接口。
- `SGEExecutor`：执行 `qsub run.sh`，兼容现有环境。
- `SlurmExecutor`：执行 `sbatch run.sh`，解析 `Submitted batch job <id>`。
- `create_job_executor`：根据配置创建执行器。

修改 `src/services/analysis_execution_service.py`：

- 初始化时通过工厂创建执行器；
- 删除服务内部的 `qsub` 路径选择和命令执行逻辑；
- 使用执行器提交 `run.sh`；
- 根据统一结果记录日志并更新任务状态；
- 保留现有通知、统计和数据库状态处理。

修改配置文件：

- `config/config.yaml` 和 `config/bj_config.yaml` 显式配置 `executor: sge`；
- `config/tz_config.yaml` 配置 `executor: slurm`；
- 三份配置增加提交超时。

## 5. 错误处理

- 命令路径未配置、文件不存在或不可执行：初始化执行器时报出明确错误。
- `run.sh` 或分析目录不存在：沿用服务现有检查，任务保持 `pending`。
- 提交命令超时：本次提交失败，任务保持 `pending`，下一个调度周期可以重试。
- 提交命令返回非零：记录标准错误，任务保持 `pending`。
- job ID 解析失败但命令返回零：视为提交成功，同时记录警告；避免作业已进入队列却被重复提交。

## 6. 本次不处理

- 不修改 `analysis_tasks` 表，不新增 `job_id` 字段。
- 不修改 `run.mk`；Slurm 指令和泰州运行路径由后续模板调整处理。
- 不启用 `main.py` 中当前被注释的 `AnalysisExecutionScheduler`，避免本次代码部署后自动提交历史 `pending` 任务。
- 不新增 dry-run 执行器。
- 按要求不编写、不运行测试。本地没有 SGE/Slurm 调度环境，最终由代码审查和泰州环境验证。

## 7. 泰州验证要点

部署后先独立调用分析执行服务，并只保留一条确认可提交的 `pending` 任务：

1. 日志显示选择 `slurm` 执行器和正确的 `sbatch` 路径。
2. `sbatch run.sh` 返回成功。
3. 日志记录 Slurm job ID。
4. `squeue` 或 `sacct` 能查询到该 job ID。
5. 对应 `analysis_tasks.analysis_status` 更新为 `running`。
6. 提交失败时状态仍为 `pending`，不会误标为 `running`。
