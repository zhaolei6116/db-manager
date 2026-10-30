# 执行环境信息：`kw_tz`

泰州集群，Slurm；第一阶段目标运行站点。环境信息最后来源核对日期：2026-07-31；项目业务路径确认日期：2026-09-22。执行前必须重新核验关键事实。

> 禁止记录密码、私钥、token、Webhook 或带凭据 URL。未知项明确标记为 `TBD`，不根据目录名猜版本。

## 标识与系统

| 字段 | 值 |
|---|---|
| Site ID | `kw_tz` |
| 地址/用户 | `zhaolei@10.18.2.11` |
| SSH | `ssh -o BatchMode=yes -o ConnectTimeout=10 zhaolei@10.18.2.11` |
| 认证 | 已配置密钥，可免密登录（执行前复核） |
| 架构/系统 | `x86_64`，Red Hat Enterprise Linux 8.8 |
| 调度器 | Slurm `23.11.4` |
| 已验证分区 | `compute2` |
| Git | `/gpfs/kw/software/git/v2.34.1/bin/git`，`2.34.1` |

## 已知工具

| 工具 | 固定路径/初始化 | 已知版本 | 本项目用途 |
|---|---|---|---|
| Nextflow launcher | `/gpfs/kw/project/zhaolei/software/nextflow/nextflow` | `25.10.7` | PBSEQ 流程版本与用法待确认 |
| Java | `/gpfs/kw/project/zhaolei/software/bin/java` | JDK `22.0.1` | Nextflow 控制作业 |
| Singularity CE | `module load singularity/4.2.1` | `4.2.1` | 流程容器运行 |
| Git | `/gpfs/kw/software/git/v2.34.1/bin/git` | `2.34.1` | 部署 clone 管理 |
| Python | `TBD` | `TBD` | 中台服务运行 |

Nextflow、Java 和 Singularity 信息来自 `kwbio-bacass` 环境记录，仅说明站点已有这些工具；在 PBSEQ 流程和 `run.sh` 未确认前，不能据此认定本项目会使用相同初始化或缓存。

## 项目与数据路径

| 用途 | 路径/状态 |
|---|---|
| JSON 根目录 | `/gpfs/kw/labgrid/pbseq/IT_PUSHJSON`，上游只读 |
| 路径协议 | `<project_type>/<batch_id>/<analysis_name>.json` |
| 长期项目 clone | `/gpfs/kw/project/zhaolei/pipeline/kwbio_bio_center`，已用于 2026-10-09 集成测试 |
| 测试分析根目录 | `/gpfs/kw/project/zhaolei/project/16_test_bio_center/analysis` |
| 正式分析根目录 | `TBD` |
| SQLite 本地持久目录 | `TBD`；不得放 GPFS/NFS |
| 服务固定运行节点 | `TBD` |
| PBSEQ 流程仓库/版本 | `TBD` |
| PBSEQ `run.sh` | `TBD` |

## 环境与调度器检查

获得远程连接授权后，先做只读检查：

```bash
source /etc/profile
module load singularity/4.2.1
/gpfs/kw/software/git/v2.34.1/bin/git --version
/gpfs/kw/project/zhaolei/software/nextflow/nextflow -version
/gpfs/kw/project/zhaolei/software/bin/java -version
singularity --version
sbatch --version
squeue --version
test -r /gpfs/kw/labgrid/pbseq/IT_PUSHJSON
```

还需核验 Python 运行环境、服务节点本地持久盘、分析根目录权限、Slurm account/qos 和 PBSEQ 实际资源参数。检查不得修改上游 JSON。

## Git 同步约定

长期 clone 创建后，测试准确 commit，不在集群长期直接开发。切换前确认没有服务或测试使用 clone，并检查工作区：

```bash
cd /gpfs/kw/project/zhaolei/pipeline/kwbio_bio_center
/gpfs/kw/software/git/v2.34.1/bin/git status --short --branch
/gpfs/kw/software/git/v2.34.1/bin/git fetch origin
/gpfs/kw/software/git/v2.34.1/bin/git switch <candidate-branch>
/gpfs/kw/software/git/v2.34.1/bin/git pull --ff-only origin <candidate-branch>
/gpfs/kw/software/git/v2.34.1/bin/git rev-parse HEAD
```

不得自动 stash、reset、clean，也不得部署未提交修改。连接、fetch、switch、pull 和远端写入均需用户授权。

## 分级验证

1. **只读集成测试**：读取目录结构和批准的 JSON，输出扫描计划；不创建分析目录，不执行 `sbatch`。
2. **写入 dry-run**：只在专用测试根目录生成 `source.json`、`input.tsv`、`run.sh`、`metadata.json` 和日志；不执行 `sbatch`。
3. **真实 Slurm 测试**：仅处理一个明确批准的 JSON；保存 launcher/process job ID、Slurm 日志、程序事件和准确 commit。
4. 重复扫描并模拟服务重启，确认不重复生成或提交。

`runtime.submit_enabled` 在新环境首次运行必须为 `false`。只有 dry-run 人工核对通过后，才可在受控测试配置中临时启用真实投递。命令超时或断连导致结果不明时记录 `SUBMIT_UNKNOWN`，先按 analysis ID/job name 查询，禁止直接重投。

## 待补充

- PBSEQ `input.tsv`、流程版本、启动命令和 Slurm 资源参数。
- 测试与正式分析根目录。
- 服务固定节点、Python 环境和 SQLite 本地持久目录。
- Slurm account/qos、计算节点网络、磁盘配额与清理周期。
- 上游 JSON 是否通过临时文件 rename 发布、原路径是否可能修改。
- 常驻服务方式和运行账号权限。

## 2026-10-09 `serve` 实际提交测试

- 代码 commit：`7a509ad161c60c74e55202d9146fac2f0d2fe85d`。
- 远端 clone：`/gpfs/kw/project/zhaolei/pipeline/kwbio_bio_center`，`main` 与
  `origin/main` 一致；既有未跟踪 `.tools/uv` 保留。
- 实际配置：远端未跟踪文件 `config/sites/kw_tz.yaml`，测试时
  `runtime.submit_enabled=true`。
- 测试 SQLite：
  `/gpfs/kw/project/zhaolei/bio_center_db/kwbio_analysis_test2.sqlite3`。该路径位于 GPFS，
  仅供本次单实例集成测试，不作为正式常驻数据库。
- 分析根：`/gpfs/kw/project/zhaolei/project/16_test_bio_center/analysis`。
- 应用日志：`/gpfs/kw/project/zhaolei/project/16_test_bio_center/logs/app.jsonl`，权限
  `0600`。
- 常驻方式：detached screen `kwbio-center`；服务间隔 300 秒。
- 测试操作脚本：
  `/gpfs/kw/project/zhaolei/project/16_test_bio_center/analysis/kw_tz_serve_real_test.sh`，
  提供带显式真实提交确认的 `start`、只读 `status` 和基于锁 PID 的 `stop`。
- 首轮服务 `run_id=5d3a6950531b4475b99f9c3be94a5ca1`，
  `round_id=2c0ee90ff1244684ae829588694dfd16`。
- 输入：`PBSEQ/20260929/TA260929001.json`；首轮统计为发现、登记、生成、真实提交各 1，
  无失败。
- 插件在 `kw_tz` 使用 `local` launcher，数据库保存 launcher PID `3202392`。launcher
  正常生成流程脚本，并向 Slurm `compute2` 提交 13 个依赖作业：`6814006` 至 `6814018`。
- launcher 日志为分析目录下 `nohup.run.TA260929001.log`，记录流程生成 13 个作业并全部
  提交成功。数据库记录为 `SUBMITTED`、`submit_attempts=1`，无错误。
- 恢复扫描 `*.json` 后，另外 3 条来源因未匹配当前产品插件而保持
  `registered=false`，`last_error=ProjectPluginError`，没有生成或提交任务。服务会按间隔重试
  这些记录并记录警告。
- 恢复全量扫描后的服务 `run_id=e55fb85c5e124387bc9bcde1ab85e93f`，完成 3 轮。第二、
  第三轮均为 `new_files=0`、`submission_selected=0`、`submitted=0`，证明已提交记录没有被
  重复提交。
- 2026-10-09 14:20（Asia/Shanghai）通过操作脚本向锁文件记录的 Python PID `3207498`
  发送 `SIGTERM`。日志依次写入 `SERVICE_STOPPING`、`SERVICE_STOPPED`；screen socket 和
  Python 进程均消失，`flock` 非阻塞复取成功，确认单实例锁已释放。锁文件保留旧 PID 文本是
  设计行为。
- 停止中台服务没有取消已进入 Slurm 的作业。停止后核对：`6814006` 至 `6814009` 已
  `COMPLETED (0:0)`，`6814010` 为 `RUNNING`，`6814011` 至 `6814018` 为 `PENDING`。
- 本次真实测试使用的是全新测试库，未修改旧库中错误关联的历史 `job_id=1769500`。
