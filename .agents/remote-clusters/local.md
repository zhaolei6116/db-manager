# 执行环境信息：`local`

本地开发与测试环境。最后整理日期：2026-09-22。

> 本文件中的版本和路径来自同机参考项目，使用前仍须执行版本检查。项目代码不得依赖本机绝对路径。

## 标识与系统

| 字段 | 值 |
|---|---|
| Site ID | `local` |
| 项目路径 | `/home/zhaolei/project/kwbio_bio_center` |
| 架构 | `x86_64` |
| 系统 | Ubuntu 24.04.3 LTS（待本项目首次执行时复核） |
| 调度器 | 无；禁止从本地调用真实 `sbatch`/`qsub` |
| Git | `2.43.0`（待复核） |
| Python | uv 管理的 CPython `3.11.16` |
| uv | `0.12.17`，`/home/zhaolei/.local/bin/uv` |

## 当前用途

- Python 工程开发、配置校验和 CLI 测试。
- 使用临时目录模拟 `IT_PUSHJSON/<project_type>/<batch_id>/*.json`。
- 使用脱敏 fixture、临时 SQLite 和假调度器完成单元/集成测试。
- 验证 `scan → validate → prepare → dry-run → record`，不访问真实 GPFS。

## 执行前检查

项目使用 `.python-version` 固定 Python 3.11，由 `pyproject.toml` 声明依赖，并由 uv 在项目根目录维护 `.venv`。首次检出或锁文件更新后执行：

```bash
cd /home/zhaolei/project/kwbio_bio_center
git status --short --branch
uv --version
uv sync --all-groups --frozen
uv lock --check
uv run python --version
uv run pytest --version
```

`pyproject.toml` 是直接依赖和工具配置的权威来源，`uv.lock` 记录完整解析结果并提交 Git。不得手工修改 `.venv` 后把本机状态当作依赖声明。

## 本地测试约束

- 测试数据必须脱敏，不复制真实姓名、手机号或完整生产 JSON 到仓库。
- SQLite、日志、状态、分析输出和临时 work 放在 pytest 临时目录或项目外的 `/tmp`。
- 调度器测试使用 fake/mock 命令，覆盖成功、明确失败、超时和异常输出。
- dry-run 必须证明不会执行真实提交命令。
- 不修改 shell 启动文件，不依赖当前 Conda 环境或隐式 PATH。

## 待补充

- 建立源码和测试目录后验证 lint、类型检查和完整测试命令。
- 本地开发配置示例及不入库配置路径。
