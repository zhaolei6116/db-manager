import json
import yaml
import logging
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from typing import Dict, Any, Optional

from src.repositories.sequence_repository import SequenceRepository
from src.repositories.sample_repository import SampleRepository
from src.repositories.project_repository import ProjectRepository
from src.utils.yaml_config import YAMLConfig
from pathlib import Path

logger = logging.getLogger(__name__)


class SequenceParameterGenerator:
    """序列参数生成器类，负责生成并更新sequence记录的parameter字段"""
    
    def __init__(self, db_session: Session, sequence_repo: Optional[SequenceRepository] = None, 
                 sample_repo: Optional[SampleRepository] = None, 
                 project_repo: Optional[ProjectRepository] = None):
        """
        初始化SequenceParameterGenerator
        
        Args:
            db_session: 数据库会话对象
            sequence_repo: 可选的外部传入SequenceRepository实例
            sample_repo: 可选的外部传入SampleRepository实例
            project_repo: 可选的外部传入ProjectRepository实例
        """
        if db_session is None:
            raise ValueError("数据库会话对象必须外部输入")
        self.db_session = db_session
        
        # 使用外部传入的Repository实例，如果没有则创建新实例
        self.sequence_repo = sequence_repo if sequence_repo is not None else SequenceRepository(db_session)
        self.sample_repo = sample_repo if sample_repo is not None else SampleRepository(db_session)
        self.project_repo = project_repo if project_repo is not None else ProjectRepository(db_session)
        self.config = YAMLConfig()
    
       
    def _load_pipeline_config(self, project_type: str) -> Optional[Dict[str, Any]]:
        """
        加载指定project_type的pipeline模板配置
        
        Args:
            project_type: 项目类型
        
        Returns:
            Optional[Dict[str, Any]]: 配置字典，加载失败返回None
        """
        try:
            # 从配置中获取项目类型与模板目录的对应关系
            template_mapping = self.config.get('project_type_to_template', {})
            
            # 确定使用的模板目录名
            template_dir_name = template_mapping.get(project_type, project_type)
            
            # 从pipeline_templates目录加载对应的配置文件（使用相对路径）
            current_dir = Path(__file__).parent
            project_root = current_dir.parent.parent  # 向上两级到达项目根目录
            template_dir = project_root / "pipeline_templates" / template_dir_name
            config_file = template_dir / "parameter.yaml"
            print(config_file)
            
            if not config_file.exists():
                logger.warning(f"未找到配置文件: {config_file}")
                logger.warning(f"项目类型 '{project_type}' 对应的模板目录名: '{template_dir_name}'")
                return None
            
            # 直接加载YAML配置文件
            with open(config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
            
            
        except Exception as e:
            logger.error(f"加载pipeline配置失败: {str(e)}")
            return None
    
    def _generate_parameter_json(self, sequence: Any, pipeline_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        根据sequence记录和pipeline配置生成parameter JSON
        
        Args:
            sequence: sequence记录对象
            pipeline_config: pipeline配置字典
        
        Returns:
            Optional[Dict[str, Any]]: 生成的parameter JSON字典，失败返回None
        """
        try:
            parameter_json = {}
            
            # 预先获取关联对象，避免重复查询数据库
            sample = None
            project = None
            if sequence.sample_id:
                sample = self.sample_repo.get_by_pk(sequence.sample_id)
            if sequence.project_id:
                project = self.project_repo.get_by_pk(sequence.project_id)
                    
            # 根据pipeline_config定制参数
            if pipeline_config:
                # 处理field_mappings
                field_mappings = pipeline_config.get('field_mappings', {})
                for key, value in field_mappings.items():
                    # 解析'表.字段'格式
                    if '.' in value:
                        table_name, field_name = value.split('.', 1)
                        # 根据表名和字段名获取对应的值
                        try:
                            # 使用预先加载的对象，通过.属性方式获取值
                            if table_name == 'sequence' and hasattr(sequence, field_name):
                                parameter_json[key] = getattr(sequence, field_name)
                            elif table_name == 'sample' and sample and hasattr(sample, field_name):
                                parameter_json[key] = getattr(sample, field_name)
                            elif table_name == 'project' and project and hasattr(project, field_name):
                                parameter_json[key] = getattr(project, field_name)
                        except Exception as e:
                            logger.warning(f"获取表{table_name}的字段{field_name}失败: {str(e)}")
                    else:
                        # 直接使用字段名（默认从sequence表获取）
                        if hasattr(sequence, value):
                            parameter_json[key] = getattr(sequence, value)
                            
            return parameter_json
            
        except Exception as e:
            logger.error(f"生成parameter JSON失败: {str(e)}")
            return None

    def generate_and_update_parameter(self, sequence_id: str) -> bool:
        """
        为指定的sequence记录生成并更新parameter字段
        
        Args:
            sequence_id: sequence的主键
        
        Returns:
            bool: 更新是否成功
        """
        try:
            # 1. 获取sequence记录
            sequence = self.sequence_repo.get_by_pk(sequence_id)
            if not sequence:
                logger.error(f"未找到sequence_id={sequence_id}的记录")
                return False
            
            # 2. 获取project_type
            project_type = sequence.project_type
            if not project_type:
                logger.error(f"sequence_id={sequence_id}的project_type为空")
                return False
            
            # 3. 加载对应的pipeline模板配置
            pipeline_config = self._load_pipeline_config(project_type)
            if not pipeline_config:
                logger.error(f"未找到project_type={project_type}的pipeline模板配置，当前还不支持这个项目类型")
                # 不使用默认配置，直接返回失败
                return False
            
            # 4. 生成parameter JSON
            parameter_json = self._generate_parameter_json(sequence, pipeline_config)
            if not parameter_json:
                logger.error(f"为sequence_id={sequence_id}生成parameter JSON失败")
                return False
            
            # 5. 更新sequence记录的parameter字段
            update_success = self.sequence_repo.update_sequence_fields(
                sequence_id=sequence_id,
                update_data={'parameters': parameter_json},
                operator='system'
            )
            
            if update_success:
                logger.info(f"已成功更新sequence_id={sequence_id}的parameter字段")
                return True
            else:
                logger.error(f"更新sequence_id={sequence_id}的parameter字段失败")
                return False
                
        except SQLAlchemyError as e:
            logger.error(f"数据库错误：生成或更新sequence_id={sequence_id}的parameter字段失败", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"生成或更新sequence_id={sequence_id}的parameter字段失败", exc_info=True)
            return False
 
    def batch_generate_and_update_parameters(self, sequence_ids: list) -> Dict[str, int]:
        """
        批量为多个sequence记录生成并更新parameter字段
        
        Args:
            sequence_ids: sequence的主键列表
        
        Returns:
            Dict[str, int]: 批量更新结果统计
        """
        result_stats = {
            'total': len(sequence_ids),
            'success': 0,
            'failure': 0
        }
        
        try:
            logger.info(f"开始批量生成并更新{len(sequence_ids)}条sequence记录的parameter字段")
            
            for sequence_id in sequence_ids:
                if self.generate_and_update_parameter(sequence_id):
                    result_stats['success'] += 1
                else:
                    result_stats['failure'] += 1
            
            logger.info(f"批量更新parameter字段完成：共{result_stats['total']}条，成功{result_stats['success']}条，失败{result_stats['failure']}条")
            return result_stats
            
        except Exception as e:
            logger.error(f"批量更新parameter字段失败", exc_info=True)
            return result_stats


if __name__ == "__main__":
    from src.models.database import get_session
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )   
    
    # 测试更新单个sequence的parameter字段
    sequence_id = 'Seq_de31e6bb94'  # 替换为实际的sequence_id
    with get_session() as db_session:
        generator = SequenceParameterGenerator(db_session)
        success = generator.generate_and_update_parameter(sequence_id)
        print(f"更新sequence_id={sequence_id}的parameter字段{'成功' if success else '失败'}")
        
