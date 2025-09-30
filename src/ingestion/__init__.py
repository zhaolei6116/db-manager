"""数据导入模块：负责从外部源（如LIMS系统）获取数据文件、解析数据并映射到内部模型字段。

核心功能包括：
- 文件发现：扫描指定目录获取待处理数据文件
- 字段映射：将外部JSON数据字段转换为内部模型字段
- 元数据管理：跟踪文件处理状态及导入进度
"""

# 从子模块导入核心功能，简化外部调用
from .lims_puller import run_lims_puller
from .lims_puller import get_all_json_in_lims_dir

# 定义公共API，控制`from ingestion import *`的行为
__all__ = [
    "get_all_json_in_lims_dir"  # LIMS目录JSON文件发现函数
    "run_lims_puller"  # LIMS数据拉取函数
]
