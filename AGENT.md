# AGENT 快速上手：生物样本测序数据管理系统

目标：让后续提出新需求时，可以快速定位到该改哪里（入口/调度/服务/处理器/配置/模板），并理解系统端到端运行链路。

权威规则与约束请同时参考：[project_rules.md](file:///home/zhaolei/project/data_management/.trae/rules/project_rules.md)（代码风格/目录约束/业务规则/测试要求）。

## 一句话概览
- 这是一个“定时拉取/扫描 LIMS JSON → 入库 → 校验下机数据 → 生成分析任务与输入文件 →（可选）触发分析执行”的自动化系统。
- 长驻入口：运行 [main.py](file:///home/zhaolei/project/data_management/src/main.py#L1-L92)，启动多个 APScheduler 定时任务。

## 运行入口与调度器
### 主入口（线上/长驻）
- [main.py](file:///home/zhaolei/project/data_management/src/main.py#L1-L92)
  - `SchedulerManager` 注册并启动全部 scheduler。
  - `signal.pause()` 使主进程常驻（被 supervisor/docker 管理时通常用这个模式）。

### 调度器（APScheduler）
调度器统一继承 [base_scheduler.py](file:///home/zhaolei/project/data_management/src/schedulers/base_scheduler.py#L16-L99)，间隔从 `config/config.yaml` 的 `scheduler.*.interval_minutes` 读取。

- LIMS 拉取/录入：[lims_scheduler.py](file:///home/zhaolei/project/data_management/src/schedulers/lims_scheduler.py#L8-L49)
  - 调用：[run_ingestion_process](file:///home/zhaolei/project/data_management/src/services/ingestion_service.py#L134-L154)
- 信息单入库（当前复用同一录入入口）：[input_sample_scheduler.py](file:///home/zhaolei/project/data_management/src/schedulers/input_sample_scheduler.py#L10-L47)
  - 调用同一个：[run_ingestion_process](file:///home/zhaolei/project/data_management/src/services/ingestion_service.py#L134-L154)
- 下机路径扫描 + 数据验证：[sequenceing_scheduler.py](file:///home/zhaolei/project/data_management/src/schedulers/sequenceing_scheduler.py#L13-L53)
  - 调用：[run_validation_process](file:///home/zhaolei/project/data_management/src/services/validation_service.py#L112-L140)
- 分析任务生成 + 文件生成：[analysis_scheduler.py](file:///home/zhaolei/project/data_management/src/schedulers/analysis_scheduler.py#L13-L67)
  - 调用：[run_analysis_process](file:///home/zhaolei/project/data_management/src/services/analysis_service.py#L333-L363)
- 分析执行（当前默认未启用）：[analysis_execution_scheduler.py](file:///home/zhaolei/project/data_management/src/schedulers/analysis_execution_scheduler.py)
  - `main.py` 中被注释：[main.py](file:///home/zhaolei/project/data_management/src/main.py#L31-L37)

## 端到端链路（从 JSON 到分析目录）
下面按“系统真正跑起来时”的顺序描述，方便定位问题发生在哪一段。

### 1) 录入（Ingestion）：LIMS JSON → 解析 → 多表入库 → 文件元数据更新
- 调用入口：[run_ingestion_process](file:///home/zhaolei/project/data_management/src/services/ingestion_service.py#L134-L154)
- 编排逻辑：[IngestionService.process_all_new_files](file:///home/zhaolei/project/data_management/src/services/ingestion_service.py#L97-L131)
  - 获取新文件（并在 `input_file_metadata` 去重登记）：[FileManager.get_new_files_from_run_lims_puller](file:///home/zhaolei/project/data_management/src/processing/file_management.py#L122-L162)
  - 单文件处理：[IngestionService.process_single_json_file](file:///home/zhaolei/project/data_management/src/services/ingestion_service.py#L50-L96)
    - JSON 解析：[json_data_processor.py](file:///home/zhaolei/project/data_management/src/processing/json_data_processor.py)
    - 入库协调器（按表 processor 依次写入）：[LIMSDataProcessor.process_parsed_json_dict](file:///home/zhaolei/project/data_management/src/processing/lims_data_processor.py#L44-L160)
    - 更新文件处理状态（成功/失败）：[FileManager.update_file_process_status](file:///home/zhaolei/project/data_management/src/processing/file_management.py#L164-L199)

定位建议：
- “没拉到新 JSON / 重复处理同一个 JSON”：优先看 `FileManager` + `input_file_metadata`。
- “JSON 解析失败 / 字段映射不对”：看 `json_data_processor.py` 与 `config/config.yaml` 的 `fields_mapping.*`。
- “写库失败 / 主键冲突 / 更新策略”：看各 `processing/*_processor.py` 与 `repositories/*_repository.py`。

### 2) 验证（Validation）：待验证序列 → 文件系统检查 → 更新序列状态
- 调用入口：[run_validation_process](file:///home/zhaolei/project/data_management/src/services/validation_service.py#L112-L140)
- 编排逻辑：[ValidationService.validate_sequence_data](file:///home/zhaolei/project/data_management/src/services/validation_service.py#L30-L109)
- 具体验证实现（检查 raw 数据路径、关键文件等）：[sequence_validation.py](file:///home/zhaolei/project/data_management/src/processing/sequence_validation.py)

定位建议：
- “明明下机了但系统认为没数据”：看 `sequence_info` / `sequence_run.*` 的路径模板，以及 `sequence_validation.py` 的检查规则。
- “验证通过但状态没更新”：看 `SequenceRepository` 的更新方法与字段名一致性。

### 3) 任务生成（Analysis Scheduling）：可分析序列 → 分组 → 任务入库 → 生成目录/TSV/run.sh → 更新序列处理标记
- 调用入口：[run_analysis_process](file:///home/zhaolei/project/data_management/src/services/analysis_service.py#L333-L363)
- 编排逻辑：[AnalysisService.process_analysis_tasks](file:///home/zhaolei/project/data_management/src/services/analysis_service.py#L36-L110)
  - 待处理数据查询与分组：`SequenceAnalysisQueryGenerator.execute_query()`：[sequence_analysis_query.py](file:///home/zhaolei/project/data_management/src/query/sequence_analysis_query.py)
  - 任务入库：`AnalysisTaskProcessor.process_single_project_group(...)`：[sequence_analysis_query.py](file:///home/zhaolei/project/data_management/src/query/sequence_analysis_query.py)
  - 生成分析目录/输入文件/脚本：`ProjectTypeManager.*`：[project_type_manager.py](file:///home/zhaolei/project/data_management/src/services/project_type_manager.py)
  - 更新序列已处理标记：`SequenceRepository.update_sequence_process_status(...)`：[analysis_service.py](file:///home/zhaolei/project/data_management/src/services/analysis_service.py#L112-L140)
  - 通知（云之家 webhook）：[notification_manager.py](file:///home/zhaolei/project/data_management/src/utils/notification_manager.py)

定位建议：
- “某 project_type 没生成目录/TSV”：优先看 `project_type_to_template` 与 `ProjectTypeManager`。
- “分组逻辑不符合预期”：看 `SequenceAnalysisQueryGenerator` 的查询与 group key。
- “生成的 TSV 格式要改”：看 `ProjectTypeManager.generate_input_tsv`。

## 模块职责速查（新需求定位表）
### 需求类型 → 最可能要改的模块
- LIMS 拉取策略 / JSON 文件来源 / 归档保留：`src/ingestion/` 与 [file_management.py](file:///home/zhaolei/project/data_management/src/processing/file_management.py)、`config/config.yaml` 的 `pull_request/ingestion`
- JSON 字段映射变更 / 新增字段处理：`config/config.yaml` 的 `fields_mapping/*`、`table_update_triggers/*`，以及 `src/processing/*_processor.py`
- 新增/调整数据库字段与表：`src/models/`、`src/repositories/`、`src/processing/*_processor.py`（注意与更新触发规则联动）
- 下机数据目录规则 / 校验条件（html/done/fastq）：[sequence_validation.py](file:///home/zhaolei/project/data_management/src/processing/sequence_validation.py)、`config/config.yaml` 的 `sequence_info` / `sequence_run`
- 新 project_type 接入（模板/参数/分析根目录）：`pipeline_templates/<type>/`、`config/config.yaml` 的 `project_type*` 与 `project_type_to_template`、[project_type_manager.py](file:///home/zhaolei/project/data_management/src/services/project_type_manager.py)
- 任务生成策略（哪些序列进入分析/分组方式/状态字段）：[sequence_analysis_query.py](file:///home/zhaolei/project/data_management/src/query/sequence_analysis_query.py)、[analysis_service.py](file:///home/zhaolei/project/data_management/src/services/analysis_service.py)
- 通知渠道/路由策略：`config/config.yaml` 的 `notification.webhooks` 与 [notification_manager.py](file:///home/zhaolei/project/data_management/src/utils/notification_manager.py)
- 调度间隔/启停某个流程：`config/config.yaml` 的 `scheduler.*` 与 [main.py](file:///home/zhaolei/project/data_management/src/main.py#L28-L39)

## 配置与约束（融合摘要）
### 配置入口
- 统一配置加载：`get_yaml_config()`：[yaml_config.py](file:///home/zhaolei/project/data_management/src/utils/yaml_config.py#L18-L242)
- 默认读取：`config/config.yaml`：[config.yaml](file:///home/zhaolei/project/data_management/config/config.yaml)
- 数据库连接：由 YAML 中 `database` 节点提供；运行时可用环境变量覆盖（见 [database.py](file:///home/zhaolei/project/data_management/src/models/database.py#L18-L69)）。

### 重要约束（建议在提需求时明确）
- 不能新增第三方依赖（除非明确要调整依赖策略）。
- 路径/映射/模板应尽量由 `config/config.yaml` 驱动，而不是硬编码。
- 新增 project_type 时，需要同步：`project_type_map`（如涉及中英文映射）、`data_flow_project_types`（是否进入后续流转）、`project_type_to_template`（模板目录映射）、以及 `pipeline_templates/` 下模板文件。

## 数据库与事务行为（与规则对齐的“运行时事实”）
- ORM 与 session：`with get_session() as db_session:` 会在退出时自动 `commit()`，异常自动 `rollback()`：[database.py](file:///home/zhaolei/project/data_management/src/models/database.py#L101-L128)
- 数据表：项目包含 `project/sample/batch/sequence/analysis_tasks/input_file_metadata/field_corrections`（详见 README 与模型定义）。

## 日志与排错
- 日志初始化工具：`setup_logger(...)`：[logging_config.py](file:///home/zhaolei/project/data_management/src/utils/logging_config.py)
- 常见排查入口：
  - “某个流程没跑”：先看 scheduler 是否启用（`main.py`）+ interval 配置（`config.yaml` 的 `scheduler`）。
  - “跑了但没效果”：看服务层 `run_*_process()` 返回统计信息与 ERROR 堆栈。
  - “某条数据卡住”：按 ingestion → validation → analysis 顺序，分别看对应状态字段与处理标记。

## 单独运行（手工触发）
用于调试时跳过调度器，直接跑单流程：
- 录入：运行 [ingestion_service.py](file:///home/zhaolei/project/data_management/src/services/ingestion_service.py#L157-L165)
- 验证：运行 [validation_service.py](file:///home/zhaolei/project/data_management/src/services/validation_service.py#L142-L151)
- 任务生成：运行 [analysis_service.py](file:///home/zhaolei/project/data_management/src/services/analysis_service.py#L365-L373)

## 提新需求时的 I/O 清单（推荐模板）
为避免反复沟通，建议在需求里写清楚下面这些点（尤其是数据管理系统很依赖“状态字段 + 文件路径 + 模板”）：
- 输入：来自哪里（LIMS JSON / 文件系统路径 / 手工补录）、字段变化（新增/改名/类型变化）、触发频率（定时/手工）。
- 输出：要生成哪些文件（TSV/run.sh/目录结构）、输出路径规则、是否需要通知。
- 状态：涉及哪些状态字段/枚举，期望的状态流转图（成功/失败/重试/补测）。
- 分组与幂等：以什么维度分组（project_id、project_type、batch_id、版本号等），重复触发是否安全（是否允许覆盖、是否要版本递增）。
- 验收口径：如何判断“已生效”（数据库记录、生成文件、日志关键字、通知消息）。

