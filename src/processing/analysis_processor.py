from typing import Dict, Any, List, Optional
import logging
import uuid
from typing import Dict, Any, List
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.analysis_task_repository import AnalysisTaskRepository
from src.utils.yaml_config import YAMLConfig

logger = logging.getLogger(__name__)


class AnalysisTaskProcessor:
    """分析任务处理器，用于处理 analysis_tasks 表的记录录入"""
    
    def __init__(self, db_session: Session):
        """
        初始化 AnalysisTaskProcessor
        
        Args:
            db_session: 数据库会话对象（必须外部输入）
        """
        if db_session is None:
            raise ValueError("数据库会话对象必须外部输入")
        self.db_session = db_session
        self.analysis_task_repo = AnalysisTaskRepository(db_session)
        # 加载配置
        self.config = YAMLConfig()
    
    def complete_task_dict(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        补全字典中缺失的 analysis_tasks 表必须字段的值
        
        Args:
            data_dict: 输入的字典数据
            
        Returns:
            Dict[str, Any]: 补全后的字典数据
        """
        # 创建副本，避免修改原始数据
        result_dict = data_dict.copy()
        
        # 补全必须字段
        # 1. 生成 task_id
        if 'task_id' not in result_dict or result_dict['task_id'] is None:
            # 使用 uuid 生成唯一的任务ID，格式为 analysis_uuid
            task_uuid = str(uuid.uuid4()).replace('-', '')
            result_dict['task_id'] = f"analysis_{task_uuid}"
            logger.info(f"自动生成 task_id: {result_dict['task_id']}")
        
        # project_id 和 project_type 必须存在，不需要在这里补全
        
        # 补全有默认值的字段
        # analysis_status
        if 'analysis_status' not in result_dict or result_dict['analysis_status'] is None:
            result_dict['analysis_status'] = 'pending'
        
        # retry_count
        if 'retry_count' not in result_dict or result_dict['retry_count'] is None:
            result_dict['retry_count'] = 1
        
        # parameters
        if 'parameters' not in result_dict or result_dict['parameters'] is None:
            result_dict['parameters'] = {}
        
        # sample_ids
        if 'sample_ids' not in result_dict or result_dict['sample_ids'] is None:
            result_dict['sample_ids'] = []
        
        # analysis_path
        if 'analysis_path' not in result_dict or result_dict['analysis_path'] is None:
            project_id = result_dict.get('project_id', '')
            project_type = result_dict.get('project_type', '')
            
            # 从配置中获取分析路径模板
            try:
                path_template = self.config.get('analysis', {}).get('path_template', '/analysis/{project_id}/{project_type}')
                result_dict['analysis_path'] = path_template.format(
                    project_id=project_id,
                    project_type=project_type.replace('/', '_').replace('\\', '_')
                )
            except Exception as e:
                logger.warning(f"无法生成分析路径: {str(e)}")
                result_dict['analysis_path'] = f"/analysis/{project_id}/{project_type}"
        
        return result_dict
    
    def process(self, data_dict: Dict[str, Any], source: str = "system") -> bool:
        """
        仅负责更新分析任务记录字段
        
        Args:
            data_dict: 包含分析任务数据的字典
            source: 数据来源，用于日志记录
            
        Returns:
            bool: 更新是否成功
        """
        try:
            # 补全缺失字段
            complete_data = self.complete_task_dict(data_dict)
            
            # 获取主键字段名
            pk_field = self.analysis_task_repo.get_pk_field()
            
            # 检查字典中是否包含主键字段
            if pk_field not in complete_data or complete_data[pk_field] is None:
                logger.error(f"来源[{source}]的分析任务数据缺少主键字段 '{pk_field}'")
                return False
            
            # 获取主键值
            pk_value = complete_data[pk_field]
            
            # 判断记录是否已存在（通过主键）
            if self.analysis_task_repo.exists_by_pk(pk_value):
                logger.info(f"来源[{source}]的分析任务数据主键 '{pk_value}' 已存在，更新现有记录")
                
                # 使用父类的upsert方法更新记录，专注于更新所有字段，特别是sample_ids和parameters
                orm_instance = self.analysis_task_repo.dict_to_orm_with_validation(complete_data)
                
                # 执行更新操作
                updated_instance = self.analysis_task_repo.upsert(orm_instance)
                logger.info(f"分析任务记录 '{pk_value}' 更新成功")
                
                # 特别记录sample_ids和parameters的更新信息
                if hasattr(updated_instance, 'sample_ids'):
                    logger.debug(f"更新后的sample_ids数量: {len(updated_instance.sample_ids)}")
                if hasattr(updated_instance, 'parameters'):
                    logger.debug(f"更新后的parameters字段: {updated_instance.parameters}")
                
                return True
            else:
                logger.error(f"无法找到分析任务记录 '{pk_value}'，此方法仅用于更新现有记录")
                return False
                
        except ValueError as e:
            logger.error(f"验证错误：处理[{source}]的分析任务数据失败：{str(e)}", exc_info=True)
            return False
        except SQLAlchemyError as e:
            logger.error(f"数据库错误：处理[{source}]的分析任务数据失败：{str(e)}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"系统错误：处理[{source}]的分析任务数据失败：{str(e)}", exc_info=True)
            return False
    
    def process_batch(self, data_dicts: List[Dict[str, Any]], source: str = "system") -> Dict[str, Any]:
        """
        批量处理分析任务数据
        
        Args:
            data_dicts: 包含多个分析任务数据的字典列表
            source: 数据来源，用于日志记录
            
        Returns:
            Dict[str, Any]: 处理结果统计，包含成功数量和失败数量
        """
        success_count = 0
        failure_count = 0
        
        for data_dict in data_dicts:
            if self.process(data_dict, source):
                success_count += 1
            else:
                failure_count += 1
        
        logger.info(f"批量处理分析任务完成：成功 {success_count} 条，失败 {failure_count} 条")
        
        return {
            'total': len(data_dicts),
            'success': success_count,
            'failure': failure_count
        }
        
    def create_task_with_validation(self, data_dict: Dict[str, Any], source: str = "system") -> bool:
        """
        创建分析任务记录，包含数据补全、验证和去重插入
        
        Args:
            data_dict: 输入的字典数据
            source: 数据来源，用于日志记录
            
        Returns:
            bool: 操作是否成功
        """
        try:
            # 1. 补全任务字典中的缺失字段
            complete_data = self.complete_task_dict(data_dict)
            logger.info(f"来源[{source}]的分析任务数据补全完成")
            
            # 2. 定义必要字段列表
            required_fields = ['project_id', 'project_type', 'task_id']
            
            # 3. 检查必要字段是否存在
            for field in required_fields:
                if field not in complete_data or complete_data[field] is None:
                    logger.error(f"来源[{source}]的分析任务数据缺少必要字段 '{field}'")
                    return False
            
            # 4. 使用dict_to_orm_with_validation生成ORM实例
            orm_instance = self.analysis_task_repo.dict_to_orm_with_validation(complete_data, required_fields=required_fields)
            logger.info(f"来源[{source}]的分析任务数据转换为ORM实例成功")
            
            # 5. 使用insert_if_not_exists方法插入记录（避免重复）
            inserted = self.analysis_task_repo.insert_if_not_exists(orm_instance, conflict_fields=['project_id', 'project_type'])
            
            if inserted:
                logger.info(f"来源[{source}]的分析任务记录插入成功，任务ID: {complete_data['task_id']}")
            else:
                logger.info(f"来源[{source}]的分析任务记录已存在，跳过插入")
            
            return True
            
        except ValueError as e:
            logger.error(f"验证错误：处理[{source}]的分析任务数据失败：{str(e)}", exc_info=True)
            return False
        except SQLAlchemyError as e:
            logger.error(f"数据库错误：处理[{source}]的分析任务数据失败：{str(e)}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"系统错误：处理[{source}]的分析任务数据失败：{str(e)}", exc_info=True)
            return False