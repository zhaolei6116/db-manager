"""YAML配置文件处理工具

提供统一接口加载和解析YAML格式的配置文件，支持按节点路径查询配置项，
自动处理配置文件不存在、节点缺失等异常情况。
"""
import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging

# 初始化日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("YAMLConfig")


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
        # 从当前文件路径向上查找项目根目录
        current_dir = Path(__file__).absolute().parent.parent.parent  # src/utils/ → src/ → 项目根目录
        default_path = current_dir / "config" / "config.yaml"
        return str(default_path)

    def _load_config(self) -> Dict[str, Any]:
        """加载并解析YAML配置文件"""
        config_path = Path(self.config_path).absolute()
        
        # 检查文件是否存在
        if not config_path.exists():
            raise FileNotFoundError(f"配置文件不存在：{config_path}")
        
        # 检查是否为文件
        if not config_path.is_file():
            raise IsADirectoryError(f"配置路径不是文件：{config_path}")
        
        # 读取并解析YAML
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
            "fields_mapping", 
            "table_update_triggers",
            "pull_request",
            "sequence_data",
            "ingestion"
        ]
        
        missing = [sec for sec in required_sections if sec not in self.config_data]
        if missing:
            raise ValueError(f"字段处理器所需配置节点缺失：{missing}（文件：{self.config_path}）")



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
        
        # 逐级查找节点
        for key in keys:
            if not isinstance(current, dict) or key not in current:
                if required:
                    raise KeyError(f"配置文件中缺少必填节点：{path}（文件：{self.config_path}）")
                return default
            current = current[key]
        
        return current



    def get_table_update_triggers(self, table: Optional[str] = None) -> Dict[str, Any]:
        """
        获取表更新触发规则
        
        Args:
            table: 可选表名，指定则只返回该表的规则
        """
        triggers = self.get("table_update_triggers", required=True)
        if table:
            if table not in triggers:
                raise KeyError(f"更新触发规则中缺少表{table}的配置（table_update_triggers.{table}）")
            return triggers[table]
        return triggers
    

    def get_new_field_rules(self) -> List[Dict[str, Any]]:
        """获取新字段处理规则"""
        return self.get("new_field_rules", default=[], required=False)
    
    def get_sequence_run_templates(self) -> Dict[str, str]:
        """获取测序运行路径模板"""
        return self.get("sequence_run", required=True)


    def get_pull_request_config(self) -> Dict[str, Any]:
        """获取lims拉取相关配置（pull_request节点）"""
        return self.get("pull_request", required=True)

    def get_ingestion_config(self) -> Dict[str, Any]:
        """获取数据摄入相关配置（ingestion节点）"""
        return self.get("ingestion", required=True)

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

    def get_sequence_run_templates(self) -> Dict[str, str]:
        """获取测序路径模板配置（sequence_run节点）"""
        return self.get("sequence_run", required=True)

    def get_project_type_paths(self) -> Dict[str, str]:
        """获取项目类型分析路径配置（project_type节点）"""
        return self.get("project_type", required=True)

    def get_all_config(self) -> Dict[str, Any]:
        """返回完整的配置字典"""
        return self.config_data
    
    # 在YAMLConfig类中添加
    def get_log_config(self) -> Dict[str, Any]:
        """获取日志配置（默认路径和级别）"""
        return {
            "log_dir": self.get("logging.log_dir", "./logs/"),
            "log_level": self.get("logging.log_level", "INFO"),
            "max_bytes": self.get("logging.max_bytes", 10485760),  # 10MB
            "backup_count": self.get("logging.backup_count", 5)
        }

    def __str__(self) -> str:
        return f"YAMLConfig(file={self.config_path})"
    
    def render_template(self, template_path: str, **kwargs: Any) -> str:
        """渲染模板字符串"""
        template_str = self.get(template_path, required=True)
        try:
            context = {** self.get_all_config(), **kwargs}
            return Template(template_str).substitute(context)
        except KeyError as e:
            raise ValueError(f"模板渲染缺少变量：{str(e)}，模板路径：{template_path}")
        except Exception as e:
            raise RuntimeError(f"模板渲染失败：{str(e)}，模板路径：{template_path}")


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


# 测试配置加载
if __name__ == "__main__":
    try:
        # 加载默认配置文件
        config = get_yaml_config()
        
        # 示例：获取各类配置
        print("拉取配置:", config.get_pull_request_config())
        print("实验室列表:", config.get("pull_request.labs"))
        print("摄入配置:", config.get_ingestion_config())
        print("样本字段映射:", config.get_fields_mapping("sample"))
        print("路径模板:", config.get_sequence_run_templates())
        # 测试字段映射配置
        print("项目表字段映射:", config.get_fields_mapping("project"))
        
        # 测试更新触发规则
        print("样本表更新规则:", config.get_table_update_triggers("sample"))
        
        # 测试新字段规则
        print("新字段处理规则:", config.get_new_field_rules())

        # 测试必填项缺失（故意查询不存在的节点）
        try:
            config.get("non_existent.node", required=True)
        except KeyError as e:
            print(f"正确捕获异常: {e}")
            
    except Exception as e:
        logger.error(f"配置测试失败: {str(e)}", exc_info=True)
