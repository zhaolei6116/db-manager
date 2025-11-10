# 生物样本测序数据管理系统

## 项目概述

本项目是一个基于MySQL数据库的自动化生物样本测序数据管理系统，旨在实现从LIMS系统数据获取、解析、验证到分析任务生成的全流程自动化。系统支持高频补测、按project_id+project_type分组分析以及Nextflow --resume机制，有效提高生物信息分析流程的自动化水平和管理效率。

## 系统架构

### 核心流程

1. **数据获取**：定时扫描LIMS系统导出的JSON文件或通过API直接拉取数据
2. **数据解析**：将JSON数据映射到系统内部数据结构
3. **数据验证**：验证样本数据状态和完整性
4. **任务生成**：为验证通过的样本生成分析任务和TSV输入文件
5. **状态管理**：跟踪和更新样本及分析任务的状态

### 数据库结构

系统包含7个核心数据表，用于存储和管理生物样本测序数据：
- `project`: 存储项目基本信息
- `sample`: 存储样本信息
- `batch`: 存储批次信息
- `sequence`: 存储测序数据信息
- `analysis_tasks`: 存储分析任务信息
- `input_file_metadata`: 存储输入文件元数据
- `field_corrections`: 存储字段修正记录

所有数据库操作基于SQLAlchemy ORM实现，确保代码的可维护性和安全性。

## 技术栈

- **Python**: 3.10+
- **数据库**: MySQL 8.0+
- **ORM框架**: SQLAlchemy
- **定时任务**: APScheduler
- **配置管理**: PyYAML
- **日志系统**: Python标准logging模块
- **容器化**: Docker和Docker Compose（可选部署方式）

## 配置指南

### 1. 数据库配置

在`config/database.ini`文件中配置MySQL数据库连接信息：

```ini
[mysql]
host = localhost
user = username
password = password
database = data_management
port = 3306
```

### 2. 系统配置

编辑`config/config.yaml`文件，配置系统运行所需的各项参数：

#### 数据库连接配置
```yaml
database:
  host: localhost
  port: 3306
  user: username
  password: password
  dbname: data_management
```

#### 输入文件配置
```yaml
input_files:
  lims_data_path: "/path/to/lims/data"
  scan_interval: 300  # 扫描间隔（秒）
```

#### LIMS数据配置
```yaml
lims_data:
  labs_available_to_pull: ["lab1", "lab2"]  # 可拉取数据的实验室列表
```

#### 项目类型配置
```yaml
project_type:
  16SAMP:  # 项目类型名称
    template_dir: "/pipeline_templates/16SAMP"  # 模板目录
    parameters:  # 项目参数
      param1: value1
      param2: value2
  bacass:
    template_dir: "/pipeline_templates/bacass"
    parameters:
      param1: value1
```

#### 日志配置
```yaml
logging:
  log_dir: "/path/to/logs"
  log_level: "INFO"
  log_format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

#### 调度器配置
```yaml
schedulers:
  lims_pull_interval_minutes: 30
  sequence_validation_interval_minutes: 15
  analysis_scheduling_interval_minutes: 10
  analysis_execution_interval_minutes: 5
```

## 安装与部署

### 1. 环境准备

```bash
# 克隆仓库
git clone <repository_url>
cd data_management

# 安装依赖
pip install -r requirements.txt
```

### 2. 数据库初始化

确保MySQL数据库已创建，并配置了正确的用户权限。系统启动时会自动创建必要的数据表结构。

### 3. 运行系统

```bash
# 启动主程序
python src/main.py
```

### 4. 使用Docker部署（可选）

```bash
# 构建并启动Docker容器
docker-compose -f docker/docker-compose.yml up -d
```

## 项目运行逻辑

### 1. 数据获取阶段
- `lims_puller.py` 负责从LIMS系统拉取数据
- 支持文件系统扫描和API调用两种方式
- 获取的数据保存为JSON格式

### 2. 数据处理阶段
- `json_data_processor.py` 解析JSON数据并映射到系统内部结构
- 各实体处理器（`project_processor.py`, `sample_processor.py`等）处理特定类型的数据
- 数据插入到相应的数据库表中

### 3. 数据验证阶段
- `validation_service.py` 验证样本数据状态
- 检查原始数据文件是否存在且完整
- 更新验证后的data_status

### 4. 任务生成阶段
- `analysis_service.py` 为验证通过的样本生成分析任务
- 按project_id+project_type分组创建TSV输入文件
- 支持参数化配置和模板渲染

### 5. 任务执行阶段
- `analysis_execution_scheduler.py` 定时检查待执行任务
- 调用外部分析流程（如Nextflow）执行任务
- 更新任务执行状态

## 目录结构

```
data_management/
├── src/                       # 源代码目录
│   ├── models/                # ORM模型
│   │   ├── __init__.py
│   │   ├── database.py        # 数据库连接管理
│   │   └── models.py          # 数据模型定义
│   ├── repositories/          # 数据访问层
│   │   ├── __init__.py
│   │   ├── base_repository.py # 基础仓库类
│   │   └── *.py               # 各实体仓库实现
│   ├── processing/            # 数据处理层
│   │   ├── __init__.py
│   │   ├── json_data_processor.py  # JSON解析器
│   │   └── *.py               # 各处理器实现
│   ├── services/              # 业务逻辑层
│   │   ├── __init__.py
│   │   ├── analysis_service.py     # 分析任务服务
│   │   └── *.py               # 各服务实现
│   ├── schedulers/            # 调度器
│   │   ├── __init__.py
│   │   ├── base_scheduler.py  # 基础调度器
│   │   └── *.py               # 各调度器实现
│   ├── utils/                 # 工具类
│   │   ├── __init__.py
│   │   ├── yaml_config.py     # 配置加载
│   │   └── logging_config.py  # 日志配置
│   └── main.py                # 主程序入口
├── config/                    # 配置文件目录
│   ├── config.yaml            # 系统配置
│   └── database.ini           # 数据库配置
├── pipeline_templates/        # 分析流程模板
│   ├── 16SAMP/                # 16S分析模板
│   └── bacass/                # 细菌组装模板
├── logs/                      # 日志目录
├── tests/                     # 测试目录
├── requirements.txt           # Python依赖列表
└── README.md                  # 项目说明文档
```

## 使用指南

### 添加新的项目类型

1. 在`pipeline_templates`目录下创建新的项目类型目录
2. 添加必要的模板文件（如`parameter.yaml`和`run.mk`）
3. 在`config/config.yaml`中配置新的项目类型参数
4. 重启服务使配置生效

### 监控系统运行

1. 查看`logs`目录下的日志文件监控系统运行状态
2. 使用数据库查询检查数据导入和任务生成情况
3. 通过Webhook配置接收任务状态通知（如果已配置）

## 故障排除

### 常见问题

1. **数据导入失败**
   - 检查JSON文件格式是否正确
   - 确认字段映射配置是否匹配
   - 查看日志获取详细错误信息

2. **任务生成失败**
   - 验证数据状态是否为有效
   - 检查项目类型配置是否正确
   - 确认模板文件路径是否存在

3. **数据库连接问题**
   - 验证数据库配置信息
   - 确认MySQL服务是否运行
   - 检查网络连接和防火墙设置

## 贡献指南

欢迎对本项目进行贡献！如果您有任何改进建议或发现问题，请通过以下方式参与：

1. Fork本项目仓库
2. 创建您的功能分支 (`git checkout -b feature/amazing-feature`)
3. 提交您的更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 打开一个Pull Request

## 许可证

[此处放置许可证信息]

## 联系方式

[zhao.ldb@qq.com]