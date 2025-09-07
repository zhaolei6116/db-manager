# src/utils/yaml_config.py
"""
YAML配置文件处理工具
提供统一接口加载和解析YAML格式的配置文件，为每个配置模块提供专用接口。
支持按节点路径查询配置项，自动处理配置文件不存在、节点缺失等异常情况。
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging

# 初始化日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class YAMLConfig:
    """YAML配置文件处理器"""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        初始化配置处理器
        
        Args:
            config_file: YAML配置文件路径，默认使用项目根目录下的config/config.yaml
        """
        self.config_path = self._get_default_config_path() if not config_file else config_file
        self.config_data = self._load_config()
        self._validate_core_config()
        

    def _get_default_config_path(self) -> str:
        """获取默认配置文件路径（config/config.yaml）"""
        current_dir = Path(__file__).absolute().parent.parent.parent  # src/utils/ -> src/ -> 项目根目录
        default_path = current_dir / "config" / "config.yaml"
        return str(default_path)

    def _load_config(self) -> Dict[str, Any]:
        """加载并解析YAML配置文件"""
        config_path = Path(self.config_path).absolute()
        
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在：{config_path}")
        
        if not config_path.is_file():
            raise IsADirectoryError(f"配置路径不是文件：{config_path}")
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml.safe_load(f) or {}
            logger.info(f"成功加载YAML配置文件：{config_path}")
            return config_data
        except yaml.YAMLError as e:
            raise ValueError(f"YAML配置文件解析错误：{str(e)}（文件：{config_path}）")
        except Exception as e:
            raise RuntimeError(f"加载配置文件失败：{str(e)}（文件：{config_path}）")

    def _validate_core_config(self) -> None:
        """验证核心配置节点（字段处理器必需）"""
        required_sections = [
            "database",
            "fields_mapping",
            "table_update_triggers",
            "pull_request",
            "sequence_info",
            "ingestion",
            "sequence_run",
            "project_type",
            "logging",
            "scheduler"
        ]
        missing = [sec for sec in required_sections if sec not in self.config_data]
        if missing:
            raise ValueError(f"配置文件缺少必填节点：{missing}（文件：{self.config_path}）")

    def get(self, path: str, default: Any = None, required: bool = False) -> Any:
        """
        按路径获取配置项
        
        Args:
            path: 配置节点路径，使用点分隔（如"pull_request.labs"）
            default: 当配置项不存在时返回的默认值
            required: 是否为必填项，若为True且配置项不存在则抛出异常
        
        Returns:
            配置项的值
        
        Examples:
            >>> config.get("pull_request.labs")
            ['W', 'S', 'B', 'G', 'T']
            >>> config.get("ingestion.scan_interval")
            1800
        """
        keys = path.split('.')
        current = self.config_data
        
        for key in keys:
            if not isinstance(current, dict) or key not in current:
                if required:
                    raise KeyError(f"配置文件中缺少必填节点：{path}（文件：{self.config_path}）")
                return default
            current = current[key]
        
        return current

    # 专用接口：为每个配置模块提供独立的方法
    def get_database_config(self) -> Dict[str, Any]:
        """获取数据库配置（database节点）"""
        return self.get("database", required=True)

    def get_ingestion_config(self) -> Dict[str, Any]:
        """获取数据摄入配置（ingestion节点）"""
        return self.get("ingestion", required=True)

    def get_pull_request_config(self) -> Dict[str, Any]:
        """获取LIMS拉取配置（pull_request节点）"""
        return self.get("pull_request", required=True)

    def get_fields_mapping(self, table: Optional[str] = None) -> Dict[str, Any]:
        """
        获取字段映射配置（fields_mapping节点）
        
        Args:
            table: 可选，指定表名（如"project"、"sample"），只返回该表的映射
        
        Returns:
            完整字段映射或指定表的映射
        """
        mappings = self.get("fields_mapping", required=True)
        if table:
            if table not in mappings:
                raise KeyError(f"字段映射中缺少表{table}的配置（fields_mapping.{table}）")
            return mappings[table]
        return mappings

    def get_sequence_info_config(self) -> Dict[str, Any]:
        """获取测序数据配置（sequence_data节点）"""
        return self.get("sequence_info", required=True)

    def get_sequence_run_config(self) -> Dict[str, str]:
        """获取测序运行路径模板配置（sequence_run节点）"""
        return self.get("sequence_run", required=True)

    def get_project_type_config(self) -> Dict[str, str]:
        """获取项目类型分析路径配置（project_type节点）"""
        return self.get("project_type", required=True)

    def get_new_field_rules(self) -> List[Dict[str, Any]]:
        """获取新字段处理规则（new_field_rules节点）"""
        return self.get("new_field_rules", default=[], required=False)

    def get_table_update_triggers(self, table: Optional[str] = None) -> Dict[str, Any]:
        """
        获取表更新触发规则（table_update_triggers节点）
        
        Args:
            table: 可选表名，指定则只返回该表的规则
        """
        triggers = self.get("table_update_triggers", required=True)
        if table:
            if table not in triggers:
                raise KeyError(f"更新触发规则中缺少表{table}的配置（table_update_triggers.{table}）")
            return triggers[table]
        return triggers

    def get_log_config(self) -> Dict[str, Any]:
        """获取日志配置（logging节点）"""
        return {
            "log_dir": self.get("logging.log_dir", default="./logs/", required=True),
            "log_level": self.get("logging.log_level", default="INFO", required=True),
            "max_bytes": self.get("logging.max_bytes", default=10485760, required=True),
            "backup_count": self.get("logging.backup_count", default=5, required=True)
        }

    def get_scheduler_config(self) -> Dict[str, Any]:
        """获取调度器配置（scheduler节点）"""
        return self.get("scheduler", required=True)

    def render_template(self, template_path: str, **kwargs: Any) -> str:
        """
        渲染模板字符串，使用 src/utils/template_renderer.py 的 TemplateRenderer
        
        Args:
            template_path: 配置中的模板路径（如"sequence_run.lab_sequencer_id"）
            **kwargs: 额外的模板变量
        
        Returns:
            渲染后的字符串
        """
        template_str = self.get(template_path, required=True)
        context = {**self.get_all_config(), **kwargs}
        return self.template_renderer.render_template(template_str, context)

    def get_all_config(self) -> Dict[str, Any]:
        """返回完整的配置字典"""
        return self.config_data

    def __str__(self) -> str:
        return f"YAMLConfig(file={self.config_path})"

# 单例模式：全局共享一个配置实例
_config_instance: Optional[YAMLConfig] = None

def get_yaml_config(config_file: Optional[str] = None) -> YAMLConfig:
    """
    获取YAML配置实例（单例模式）
    
    Args:
        config_file: 配置文件路径，首次调用时有效
    
    Returns:
        YAMLConfig实例
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = YAMLConfig(config_file)
    return _config_instance




# 添加测试代码块
if __name__ == "__main__":
    """测试YAMLConfig类的基本功能：配置加载、解析和获取配置项"""
    import sys
    from pprint import pformat  # 用于格式化输出配置内容

    # 配置日志为DEBUG级别，显示详细测试过程
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    test_logger = logging.getLogger("YAMLConfigTest")

    try:
        # 测试单例模式获取配置实例
        test_logger.info("=== 测试单例模式获取配置实例 ===")
        config = get_yaml_config()
        test_logger.debug(f"成功获取配置实例: {config}")

        # 验证配置文件路径
        test_logger.info("\n=== 验证配置文件路径 ===")
        test_logger.info(f"当前加载的配置文件路径: {config.config_path}")
        if Path(config.config_path).exists():
            test_logger.info("配置文件存在，路径验证通过")
        else:
            test_logger.warning("配置文件路径不存在，请检查默认路径是否正确")

        # 测试获取核心配置节点
        test_logger.info("\n=== 测试获取核心配置节点 ===")
        
        # 1. 测试数据库配置
        db_config = config.get_database_config()
        test_logger.debug(f"数据库配置内容: \n{pformat(db_config)}")
        test_logger.info(f"数据库类型: {db_config.get('type')}, 主机地址: {db_config.get('host')}")

        # 2. 测试字段映射配置（以project表为例）
        project_mapping = config.get_fields_mapping("project")
        test_logger.debug(f"project表字段映射: \n{pformat(project_mapping)}")
        test_logger.info(f"project表映射字段数量: {len(project_mapping)}")

        # 3. 测试序列信息配置
        sequence_info = config.get_sequence_info_config()
        test_logger.debug(f"序列信息配置: \n{pformat(sequence_info)}")
        test_logger.info(f"序列数据路径: {sequence_info.get('sequence_data_path')}")

        # 4. 测试按路径获取配置项（嵌套节点）
        test_logger.info("\n=== 测试按路径获取嵌套配置项 ===")
        scan_interval = config.get("ingestion.scan_interval")
        test_logger.info(f"数据摄入扫描间隔: {scan_interval}秒")

        test_logger.info("\n=== 所有测试项执行完成 ===")

    except Exception as e:
        test_logger.error(f"测试过程中发生错误: {str(e)}", exc_info=True)
        sys.exit(1)
