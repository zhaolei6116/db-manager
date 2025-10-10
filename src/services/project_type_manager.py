from typing import Dict, List, Any, Optional
import os
import yaml
import logging
from pathlib import Path
from datetime import datetime
import shutil
import functools
from typing import Optional, List, Dict, Any, Callable, TypeVar, cast
from src.utils.yaml_config import YAMLConfig

# 配置日志
logger = logging.getLogger(__name__)

T = TypeVar('T')


def log_method_call(func: Callable[..., T]) -> Callable[..., T]:
    """
    记录方法调用的装饰器，记录方法的输入参数和返回值
    
    Args:
        func: 被装饰的方法
        
    Returns:
        包装后的方法
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # 获取方法名称和类名称
        method_name = func.__name__
        class_name = self.__class__.__name__
        
        # 记录方法调用开始
        logger.debug(f"{class_name}.{method_name} 方法调用开始，参数: args={args}, kwargs={kwargs}")
        
        try:
            # 执行原始方法
            result = func(self, *args, **kwargs)
            
            # 记录方法调用结束和返回值
            logger.debug(f"{class_name}.{method_name} 方法调用结束，返回值: {result}")
            return result
        except Exception as e:
            # 记录异常信息
            logger.debug(f"{class_name}.{method_name} 方法调用异常: {str(e)}")
            raise
    
    return cast(Callable[..., T], wrapper)


def handle_exceptions(func: Callable[..., T]) -> Callable[..., T]:
    """
    统一异常处理的装饰器，捕获并记录方法中可能出现的异常
    
    Args:
        func: 被装饰的方法
        
    Returns:
        包装后的方法
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # 获取方法名称和类名称
        method_name = func.__name__
        class_name = self.__class__.__name__
        
        try:
            # 执行原始方法
            return func(self, *args, **kwargs)
        except ValueError as e:
            # 处理值错误
            logger.error(f"{class_name}.{method_name} 值错误: {str(e)}")
            raise
        except FileNotFoundError as e:
            # 处理文件不存在错误
            logger.error(f"{class_name}.{method_name} 文件不存在错误: {str(e)}")
            raise
        except PermissionError as e:
            # 处理权限错误
            logger.error(f"{class_name}.{method_name} 权限错误: {str(e)}")
            raise
        except Exception as e:
            # 处理其他所有异常
            logger.error(f"{class_name}.{method_name} 发生未知错误: {str(e)}", exc_info=True)
            raise
    
    return cast(Callable[..., T], wrapper)


class ProjectTypeManager:
    """
    项目类型管理器，用于处理不同项目类型的配置和操作
    
    该类提供了以下功能：
    1. 在初始化时获取并存储所有项目类型相关信息
    2. 提供分析路径生成
    3. 提供输入文件生成
    4. 提供运行脚本生成
    """
    
    def __init__(self, project_type: str):
        """
        初始化项目类型管理器，一次性获取所有项目类型相关信息
        
        Args:
            project_type: 项目类型
        
        Raises:
            ValueError: 当初始化过程中出现错误时抛出
        """
        try:
            # 记录初始化开始
            logger.info(f"开始初始化项目类型管理器，项目类型: '{project_type}'")
            
            self.config = YAMLConfig()  # 使用默认配置文件路径
            self.project_type = project_type
            
            # 在初始化时获取并存储所有项目类型相关信息
            self.english_name = self.get_english_project_type()
            self.analysis_path = self._get_analysis_path_internal()
            self.template_dir = self._get_template_dir_internal()
            self.input_headers = self._get_input_headers_internal()
            self.run_sh_template = self._get_run_sh_template_internal()
            self.parameter_config = self._load_parameter_config()
            
            logger.info(f"项目类型 '{project_type}' 初始化完成，英文名称: {self.english_name}")
        except Exception as e:
            logger.error(f"项目类型 '{project_type}' 初始化失败: {str(e)}", exc_info=True)
            raise ValueError(f"项目类型管理器初始化失败: {str(e)}")
    
    @log_method_call
    @handle_exceptions
    def get_english_project_type(self) -> str:
        """
        获取项目类型对应的英文名称
        
        Returns:
            str: 项目类型对应的英文名称（未配置时返回原始项目类型）
        """
        project_type_mapping = self.config.get('project_type_to_template', {})
        
        if self.project_type not in project_type_mapping:
            logger.warning(f"项目类型 '{self.project_type}' 未配置对应英文名称")
            return self.project_type
        
        english_name = project_type_mapping[self.project_type]
        logger.debug(f"项目类型 '{self.project_type}' 对应英文名称: {english_name}")
        return english_name
    
    @log_method_call
    @handle_exceptions
    def get_analysis_path(self) -> str:
        """
        获取项目类型的基础分析路径
        
        Returns:
            str: 基础分析路径
            
        Raises:
            ValueError: 当项目类型未配置分析路径时抛出
        """
        # 直接返回初始化时存储的分析路径
        return self.analysis_path
    
    @log_method_call
    @handle_exceptions
    def get_template_dir(self) -> str:
        """
        获取项目类型对应的模板目录路径
        
        Returns:
            str: 模板目录路径
        """
        # 直接返回初始化时存储的模板目录
        return self.template_dir
    
    @log_method_call
    @handle_exceptions
    def get_input_headers(self) -> List[str]:
        """
        获取项目类型的输入文件表头
        
        Returns:
            List[str]: 表头列表
        """
        # 直接返回初始化时存储的表头
        return self.input_headers
    
    @log_method_call
    @handle_exceptions
    def get_run_sh_template(self) -> Optional[str]:
        """
        获取项目类型的run.sh模板文件内容
        
        Returns:
            Optional[str]: run.sh模板文件内容，如果不存在则返回None
        """
        # 直接返回初始化时存储的模板
        return self.run_sh_template
    
    @log_method_call
    @handle_exceptions
    def generate_project_analysis_path(self, project_id: str) -> str:
        """
        根据project_id生成完整的分析路径
        如果路径不存在，则创建
        
        Args:
            project_id: 项目ID
            
        Returns:
            str: 完整的分析路径
        """
        # 直接使用初始化时存储的基础分析路径
        # 生成完整分析路径
        full_path = os.path.join(self.analysis_path, project_id)
        
        # 如果路径不存在，创建目录
        if not os.path.exists(full_path):
            os.makedirs(full_path, exist_ok=True)
            logger.info(f"创建分析目录成功: {full_path}")
        
        # 设置目录权限
        os.chmod(full_path, 0o755)
        
        return full_path
    
    @log_method_call
    @handle_exceptions
    def generate_input_tsv(self, analysis_path: str, sequences: List[Dict[str, Any]]) -> bool:
        """
        生成input.tsv文件
        
        Args:
            analysis_path: 分析目录路径
            sequences: 序列数据列表
            
        Returns:
            bool: 生成是否成功
        """
        if not sequences:
            logger.warning("序列数据为空，无法生成input.tsv文件")
            return False
        
        input_file_path = os.path.join(analysis_path, "input.tsv")
        
        # 备份已存在的文件
        self._backup_existing_file(input_file_path)
        
        # 直接使用初始化时存储的表头
        headers = self.input_headers
        
        with open(input_file_path, 'w', encoding='utf-8') as f:
            # 写入表头
            f.write("\t".join(headers) + "\n")
            
            # 写入数据
            for seq in sequences:
                # 获取parameters字典
                parameters = seq.get('parameters', {})
                
                # 根据headers生成行数据
                row_data = []
                for header in headers:
                    # 从parameters中获取对应的值
                    value = parameters.get(header, "")
                    
                    # 特殊处理，确保值是字符串且不含制表符和换行符
                    if isinstance(value, (dict, list)):
                        value = str(value).replace('\t', ' ').replace('\n', ' ')
                    elif not isinstance(value, str):
                        value = str(value)
                    
                    row_data.append(value)
                
                # 写入一行数据
                f.write("\t".join(row_data) + "\n")
        
        logger.info(f"生成input.tsv文件成功: {input_file_path}")
        return True
            
    @log_method_call
    @handle_exceptions
    def generate_run_sh(self, analysis_path: str, project_id: str) -> bool:
        """
        生成run.sh执行脚本
        
        Args:
            analysis_path: 分析目录路径
            project_id: 项目ID
            
        Returns:
            bool: 生成是否成功
        """
        run_sh_path = os.path.join(analysis_path, "run.sh")
        
        # 备份已存在的文件
        self._backup_existing_file(run_sh_path)
        
        # 直接使用初始化时存储的模板
        run_template = self.run_sh_template
        
        # 如果没有模板，使用默认内容
        if not run_template:
            logger.warning(f"使用默认run.sh内容")
            default_content = f"#!/bin/bash\n\n# 项目分析脚本\n# 项目ID: {project_id}\n# 项目类型: {self.project_type}\n\n# 在这里添加分析命令\necho \"开始分析项目 {project_id}\"\n# nextflow run main.nf --input input.tsv --outdir results\necho \"分析完成\"\n"
            run_template = default_content
        
        # 写入run.sh文件
        with open(run_sh_path, 'w', encoding='utf-8') as f:
            f.write(run_template)
        
        # 设置执行权限
        os.chmod(run_sh_path, 0o755)
        
        logger.info(f"生成run.sh文件成功: {run_sh_path}")
        return True
    
    @log_method_call
    @handle_exceptions
    def _backup_existing_file(self, file_path: str) -> bool:
        """
        备份已存在的文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            bool: 备份是否成功
        """
        if not os.path.exists(file_path):
            return True
        
        # 生成带时间戳的备份文件名
        backup_path = f"{file_path}.{datetime.now().strftime('%Y%m%d%H%M%S')}.bak"
        shutil.copy2(file_path, backup_path)
        logger.info(f"备份文件成功: {file_path} -> {backup_path}")
        return True
    
    @log_method_call
    @handle_exceptions
    def _load_parameter_config(self) -> Dict[str, Any]:
        """
        加载项目类型的parameter.yaml配置
        
        Returns:
            Dict[str, Any]: 参数配置字典
        """
        parameter_file = os.path.join(self.template_dir, "parameter.yaml")
        
        if not os.path.exists(parameter_file):
            logger.warning(f"项目类型 '{self.project_type}' 的parameter.yaml文件不存在: {parameter_file}")
            return {}
        
        with open(parameter_file, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f) or {}
        
        logger.info(f"成功加载项目类型 '{self.project_type}' 的parameter.yaml配置")
        return config_data
    
    @log_method_call
    @handle_exceptions
    def _get_analysis_path_internal(self) -> str:
        """
        内部方法：获取项目类型的基础分析路径
        
        Returns:
            str: 基础分析路径
            
        Raises:
            ValueError: 当项目类型未配置分析路径时抛出
        """
        project_type_paths = self.config.get('project_type', {})
        
        # 先尝试直接查找
        if self.project_type in project_type_paths:
            path = project_type_paths[self.project_type]
            return path
        
        # 如果直接查找失败，尝试转换为英文项目类型再查找
        english_type = self.get_english_project_type()
        if english_type in project_type_paths:
            path = project_type_paths[english_type]
            return path
        
        # 如果都找不到，使用默认分析路径
        default_path = project_type_paths.get('analysis', '/home/zhaolei/project_analysis')
        logger.warning(f"项目类型 '{self.project_type}' 未配置对应的分析路径，使用默认路径: {default_path}")
        return default_path
    
    @log_method_call
    @handle_exceptions
    def _get_template_dir_internal(self) -> str:
        """
        内部方法：获取项目类型对应的模板目录路径
        
        Returns:
            str: 模板目录路径
        """
        # 获取英文项目类型
        english_type = self.get_english_project_type()
        
        # 构建模板目录路径
        config_dir = os.path.dirname(self.config.config_path)
        template_dir = os.path.join(
            config_dir, 
            "../pipeline_templates", 
            english_type
        )
        
        return template_dir
    
    @log_method_call
    @handle_exceptions
    def _get_input_headers_internal(self) -> List[str]:
        """
        内部方法：获取项目类型的输入文件表头
        
        Returns:
            List[str]: 表头列表
        """
        template_dir = self._get_template_dir_internal()
        parameter_file = os.path.join(template_dir, "parameter.yaml")
        
        # 检查parameter.yaml文件是否存在
        if not os.path.exists(parameter_file):
            logger.warning(f"项目类型 '{self.project_type}' 的parameter.yaml文件不存在: {parameter_file}")
            # 返回默认表头
            default_headers = ["sample_id", "version", "project_type", "raw_data_path", "parameters_json"]
            return default_headers
        
        # 读取parameter.yaml文件
        with open(parameter_file, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f) or {}
        
        # 获取export_headers
        headers = config_data.get('export_headers', [])
        if not headers:
            logger.warning(f"项目类型 '{self.project_type}' 的parameter.yaml文件中未配置export_headers")
            # 返回默认表头
            default_headers = ["sample_id", "version", "project_type", "raw_data_path", "parameters_json"]
            return default_headers
        
        logger.info(f"成功获取项目类型 '{self.project_type}' 的输入文件表头")
        return headers
    
    @log_method_call
    @handle_exceptions
    def _get_run_sh_template_internal(self) -> Optional[str]:
        """
        内部方法：获取项目类型的run.sh模板文件内容
        
        Returns:
            Optional[str]: run.sh模板文件内容，如果不存在则返回None
        """
        template_dir = self._get_template_dir_internal()
        # 注意：通常是run.mk而不是run.sh
        run_file = os.path.join(template_dir, "run.mk")
        
        if not os.path.exists(run_file):
            logger.warning(f"项目类型 '{self.project_type}' 的run.mk文件不存在: {run_file}")
            return None
        
        with open(run_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        logger.info(f"成功获取项目类型 '{self.project_type}' 的run.mk模板")
        return content