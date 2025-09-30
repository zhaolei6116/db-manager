from typing import Dict, Any, List, Optional
import logging
import uuid
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from src.repositories.sequence_repository import SequenceRepository
from src.utils.yaml_config import YAMLConfig

logger = logging.getLogger(__name__)


class CombinedSequenceProcessor:
    """组合处理器，处理合并后的sequence数据，包含原sequence_run表的信息"""
    
    def __init__(self, db_session: Session):
        """
        初始化CombinedSequenceProcessor
        
        Args:
            db_session: 数据库会话对象（必须外部输入）
        """
        if db_session is None:
            raise ValueError("数据库会话对象必须外部输入")
        self.db_session = db_session
        self.sequence_repo = SequenceRepository(db_session)
        # 加载配置
        self.config = YAMLConfig()
    
    def complete_sequence_dict(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        补全字典中缺失的sequence表必须字段的值
        
        Args:
            data_dict: 输入的字典数据
            
        Returns:
            Dict[str, Any]: 补全后的字典数据
        """
        # 创建副本，避免修改原始数据
        result_dict = data_dict.copy()
        
        # 补全必须字段
        # 1. 生成sequence_id (UUID格式)，不会重复
        if 'sequence_id' not in result_dict or result_dict['sequence_id'] is None:
            sequence_uuid = str(uuid.uuid4()).replace('-', '')[:10]
            result_dict['sequence_id'] = f"Seq_{sequence_uuid}"
            logger.info(f"自动生成sequence_id: {result_dict['sequence_id']}")
        
        # project_id, project_type, sample_id, batch_id, barcode 必须存在，不需要在这里补全
        
        # 4. 补全有默认值的字段
        # version
        if 'version' not in result_dict or result_dict['version'] is None:
            result_dict['version'] = 1
        
        # run_type: 基于sample_id存在性（参考sequence_repository.py的实现）
        sample_id = result_dict.get('sample_id')
        if 'run_type' not in result_dict or result_dict['run_type'] is None:
            result_dict['run_type'] = 'initial'
        
        # parameters
        if 'parameters' not in result_dict or result_dict['parameters'] is None:
            result_dict['parameters'] = {}
        
        # analysis_status
        if 'analysis_status' not in result_dict or result_dict['analysis_status'] is None:
            result_dict['analysis_status'] = 'no'
        
        # process_status
        if 'process_status' not in result_dict or result_dict['process_status'] is None:
            result_dict['process_status'] = 'no'
        
        # data_status
        if 'data_status' not in result_dict or result_dict['data_status'] is None:
            result_dict['data_status'] = 'pending'
        
        return result_dict
    
    def process(self, data_dict: Dict[str, Any], file_name: str) -> bool:
        """
        处理合并后的sequence数据
        
        Args:
            data_dict: 包含sequence数据的字典
            file_name: 文件名，用于日志记录
            
        Returns:
            bool: 处理是否成功
        """
        try:
            # 补全缺失字段
            complete_data = self.complete_sequence_dict(data_dict)
            
            # 获取主键字段名
            pk_field = self.sequence_repo.get_pk_field()
            
            # 检查字典中是否包含主键字段
            if pk_field not in complete_data or complete_data[pk_field] is None:
                logger.error(f"文件[{file_name}]的sequence数据缺少主键字段 '{pk_field}'")
                return False
            
            # 获取主键值
            pk_value = complete_data[pk_field]
            
            # 判断记录是否已存在（通过主键）
            if self.sequence_repo.exists_by_pk(pk_value):
                logger.info(f"文件[{file_name}]的sequence数据主键 '{pk_value}' 已存在，跳过处理")
                return True
            
            # 获取组合键字段值（UniqueConstraint('sample_id', 'batch_id', 'project_type', 'barcode')）
            sample_id = complete_data.get("sample_id")
            batch_id = complete_data.get("batch_id")
            project_type = complete_data.get("project_type")
            barcode = complete_data.get("barcode")
            
            # 检查必要的组合键字段是否存在
            if sample_id is None or batch_id is None or project_type is None or barcode is None:
                logger.error(f"文件[{file_name}]的sequence数据缺少必要的组合键字段：sample_id={sample_id}, batch_id={batch_id}, project_type={project_type}, barcode={barcode}")
                return False
            
            # 通过组合键判断记录是否存在
            if self.sequence_repo.exists_by_fields(sample_id=sample_id, batch_id=batch_id, project_type=project_type, barcode=barcode):
                logger.info(f"文件[{file_name}]的sequence数据已存在（组合键：sample_id={sample_id}, batch_id={batch_id}, project_type={project_type}, barcode={barcode}），跳过处理")
                return True
            
            # 字典转换为ORM实例（带字段验证）
            # 检查必须存在的字段：project_id, project_type, sample_id, batch_id, barcode
            required_fields = ['project_id', 'project_type', 'sample_id', 'batch_id', 'barcode']
            orm_instance = self.sequence_repo.dict_to_orm_with_validation(complete_data, required_fields=required_fields)
            
            # 插入记录（如果不存在），使用父类方法并指定冲突字段
            inserted = self.sequence_repo.insert_if_not_exists(orm_instance, conflict_fields=('sample_id', 'batch_id', 'project_type', 'barcode'))
            
            if inserted:
                logger.info(f"文件[{file_name}]的sequence数据插入成功，主键: {pk_value}（自动设置run_type，包含原sequence_run信息）")
                return True
            else:
                logger.warning(f"文件[{file_name}]的sequence数据插入失败，可能是并发插入导致")
                return False
                
        except ValueError as e:
            logger.error(f"处理[{file_name}]的sequence数据失败：{str(e)}")
            return False
        except SQLAlchemyError as e:
            logger.error(f"数据库错误：处理[{file_name}]的sequence数据失败", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"处理[{file_name}]的sequence数据失败", exc_info=True)
            return False
    
    