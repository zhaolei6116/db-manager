# 执行环境信息：`kw_bj`

北京集群。最后整理日期：2026-09-22。

> 北京尚不是第一阶段已确认运行站点。以下基础设施事实来自同机参考项目，必须在本项目首次使用前重新核验。禁止记录密码、私钥、token、Webhook 或带凭据 URL。

## 标识与系统

| 字段 | 值 |
|---|---|
| Site ID | `kw_bj` |
| 地址/用户 | `zhaolei@192.168.100.13` |
| SSH | `ssh -o BatchMode=yes -o ConnectTimeout=10 zhaolei@192.168.100.13` |
| 认证 | 已配置密钥，可免密登录（执行前复核） |
| 架构/系统 | `x86_64`，CentOS 7 |
| 已知调度器 | OGS/GE `2011.11` |
| 本项目调度器配置 | SGE；queue、parallel environment 和运行方式使用前必须确认 |
| Git | `/nas02/project/huyifan/software/nextflow/v24.04.4/bin/git`，`2.45.2` |

## 已知工具

| 工具 | 固定路径/初始化 | 已知版本 | 状态 |
|---|---|---|---|
| Nextflow launcher | `/home/zhaolei/software/nextflow/nextflow` | `25.10.7` | 站点已有，PBSEQ 是否使用待确认 |
| Java | `/home/zhaolei/software/jdk/amazon-corretto-17.0.20.8.1-linux-x64/bin/java` | Corretto `17.0.20` | 站点已有 |
| Singularity CE | PATH 中的 `singularity` | `4.2.1` | 执行前复核 |
| Git | `/nas02/project/huyifan/software/nextflow/v24.04.4/bin/git` | `2.45.2` | 避免使用系统旧 Git |
| Python | `TBD` | `TBD` | 中台服务运行 |

这些工具信息不代表本项目已经在北京验证。不得复制 `kwbio-bacass` 的项目路径、测试数据、队列、数据库或运行脚本。

## 本项目待确认路径

| 用途 | 路径/状态 |
|---|---|
| 长期项目 clone | `TBD` |
| JSON 根目录 | `TBD` |
| 测试分析根目录 | `TBD` |
| 正式分析根目录 | `TBD` |
| SQLite 本地持久目录 | `TBD`；不得放共享文件系统 |
| 服务固定运行节点 | `TBD` |
| PBSEQ 流程与启动脚本 | `TBD` |
| queue/parallel environment | `TBD` |

## 首次接入前检查

获得远程连接授权后，仅做只读核验：

```bash
/nas02/project/huyifan/software/nextflow/v24.04.4/bin/git --version
/home/zhaolei/software/nextflow/nextflow -version
/home/zhaolei/software/jdk/amazon-corretto-17.0.20.8.1-linux-x64/bin/java -version
singularity --version
qsub -help 2>&1 | head
```

必须确认可用队列、登录节点是否允许控制进程、Python 环境、数据入口和读写权限。参数核验完成前，不宣称北京 SGE Executor 已可用。

## 安全与验证顺序

- 连接远程站点、同步代码、写入目录或提交任务前需要用户明确授权。
- 先只读核验，再在专用目录 dry-run，最后才进行一个明确批准输入的真实测试。
- 上游 JSON 只读；默认 `runtime.submit_enabled: false`。
- 不自动 stash、reset、clean，不覆盖分析目录，不直接在长期 clone 开发。
- 保存准确 commit、配置身份、输入相对路径、调度器 job ID、日志和测试结论。

## 待补充

- 北京是否属于 MVP 范围，以及 SGE 的正式 queue 和 parallel environment。
- 本项目的代码、数据、分析、状态和日志路径。
- 队列、资源、环境初始化和提交脚本。
- Python/数据库部署方式与服务运行节点。
